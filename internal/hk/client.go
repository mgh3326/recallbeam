package hk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/recallbeam/internal/core"
)

// A 404 without hk's not-found or deleted marker means the route is missing,
// not the row; it must never become a delete.
var errReadEndpoint = fmt.Errorf("hk read endpoint unavailable: %w", core.ErrUnavailable)

const maxRow = 16 << 20

// ExtractVersion names how Fetch maps source rows to indexed fields. Bump it
// with any change to that mapping; it is an input to core.Generation, so the
// next start rebuilds the index. TestExtractionPinsVersion enforces the bump.
const ExtractVersion = 1

// rowShape lists, per model, the fields Fetch reads and their JSON kind.
// hk serves raw database rows (to_jsonb emits every column, NULLs included)
// and API structs that always emit these keys, so a missing or retyped field
// means an hk migration changed the shape for the whole model.
var rowShape = map[string]map[string]byte{
	"document":     {"key": '"', "body": '"', "kind": '"', "updated_at": '"'},
	"task":         {"title": '"', "refs": '{', "updated_at": '"'},
	"task_comment": {"body": '"', "created_at": '"'},
	"decision":     {"question": '"', "updated_at": '"'},
}

// checkShape rejects a row the extractor cannot read faithfully, instead of
// indexing empty text for a renamed field.
func checkShape(model string, body []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return err
	}
	for name, kind := range rowShape[model] {
		v := bytes.TrimSpace(fields[name])
		if len(v) == 0 || v[0] != kind {
			return fmt.Errorf("hk %s field %q missing or not %s: %w", model, name, map[byte]string{'"': "a string", '{': "an object"}[kind], core.ErrIncompatible)
		}
	}
	return nil
}

type Client struct {
	Base, Token string
	HTTP        *http.Client
}

func (c Client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.Base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	// Validate the destination before attaching credentials or invoking a transport.
	u := req.URL
	addr, ipErr := netip.ParseAddr(u.Hostname())
	if u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && ipErr == nil && addr.IsLoopback())) {
		return nil, errors.New("hk source URL requires HTTPS; HTTP is allowed only for literal loopback IPs")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	client := http.Client{Timeout: 60 * time.Second}
	if c.HTTP != nil {
		client = *c.HTTP
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	r, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hk request failed: %w", core.ErrUnavailable)
	}
	if r.StatusCode != http.StatusOK {
		defer r.Body.Close()
		switch r.StatusCode {
		case http.StatusGone:
			return nil, core.ErrReset
		case http.StatusNotFound:
			var problem struct {
				Error   string `json:"error"`
				Deleted bool   `json:"deleted"`
			}
			if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") && json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&problem) == nil && (problem.Error == "not_found" || problem.Deleted) {
				return nil, core.ErrNotFound
			}
			return nil, errReadEndpoint
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return nil, fmt.Errorf("hk status %d: %w", r.StatusCode, core.ErrUnavailable)
		}
		return nil, fmt.Errorf("hk status %d", r.StatusCode)
	}

	return r, nil
}
func (c Client) Bootstrap(ctx context.Context, model, cursor string) (core.Page, error) {
	var p core.Page
	r, err := c.get(ctx, "/v1/sync/bootstrap?"+url.Values{"model": {model}, "cursor": {cursor}, "limit": {"1000"}}.Encode())
	if err != nil {
		return p, err
	}
	defer r.Body.Close()
	err = json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&p)
	if err != nil {
		return p, err
	}
	if p.Schema < 1 || p.Max < 0 || len(p.Rows) > 1000 {
		return p, errors.New("invalid bootstrap page")
	}
	for _, r := range p.Rows {
		if r.ID == "" || (model == "document" && r.Key == "") {
			return p, errors.New("invalid bootstrap row")
		}
	}
	return p, nil
}
func (c Client) Delta(ctx context.Context, after int64) (core.Delta, error) {
	var d core.Delta
	r, err := c.get(ctx, "/v1/sync/delta?after="+strconv.FormatInt(after, 10)+"&limit=1000")
	if err != nil {
		return d, err
	}
	defer r.Body.Close()
	scan := bufio.NewScanner(io.LimitReader(r.Body, 16<<20))
	scan.Buffer(make([]byte, 4096), 1<<20)
	meta := false
	for scan.Scan() {
		if meta {
			return d, errors.New("data after delta metadata")
		}
		var row struct {
			Metadata *core.Metadata `json:"_metadata_"`
			core.Change
		}
		if err = json.Unmarshal(scan.Bytes(), &row); err != nil {
			return d, err
		}
		if row.Metadata != nil {
			d.Metadata = *row.Metadata
			meta = true
		} else {
			d.Changes = append(d.Changes, row.Change)
		}
		if len(d.Changes) > 1000 {
			return d, errors.New("delta exceeds limit")
		}
	}
	if err = scan.Err(); err != nil {
		return d, err
	}
	if !meta {
		return d, errors.New("delta missing final metadata")
	}
	return d, nil
}

// Fetch reads one current row. Documents, comments and decisions come from
// hk's /v1/sync/rows lookup. Tasks use /v1/tasks/{id}, a direct lookup too,
// because the raw task row has no event notes and those are indexed.
func (c Client) Fetch(ctx context.Context, model string, ref core.Ref) (core.Doc, error) {
	var d core.Doc
	var path string
	switch model {
	case "task":
		path = "/v1/tasks/" + url.PathEscape(ref.ID)
	case "document", "task_comment", "decision":
		// The model ID consumes the rest of the path; decision IDs may contain slashes.
		parts := strings.Split(ref.ID, "/")
		for i, p := range parts {
			parts[i] = url.PathEscape(p)
		}
		path = "/v1/sync/rows/" + model + "/" + strings.Join(parts, "/")
	default:
		return d, errors.New("unknown model")
	}

	r, err := c.get(ctx, path)
	if err != nil {
		return d, err
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRow+1))
	if err != nil {
		return d, fmt.Errorf("hk read failed: %w", core.ErrUnavailable)
	}
	if len(body) > maxRow {
		return d, errors.New("hk row exceeds 16 MiB")
	}
	if err = checkShape(model, body); err != nil {
		return d, err
	}
	var raw struct {
		Key, Title, Body, Kind, Question string
		UpdatedAt                        time.Time       `json:"updated_at"`
		CreatedAt                        time.Time       `json:"created_at"`
		Refs                             json.RawMessage `json:"refs"`
		Events                           json.RawMessage `json:"events"`
	}
	if err = json.Unmarshal(body, &raw); err != nil {
		return d, err
	}
	d.Key = raw.Key
	d.Title = raw.Title
	d.Body = raw.Body
	d.Tags = raw.Kind
	d.UpdatedAt = raw.UpdatedAt
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = raw.CreatedAt
	}
	if d.Title == "" {
		d.Title = d.Key
	}
	switch model {
	case "task":
		d.Body = string(raw.Refs) + "\n" + string(raw.Events)
		d.Identifier = taskIdentifier.FindString(d.Title)
	case "decision":
		// The whole request is indexed: question, options and resolution.
		if raw.Question != "" {
			d.Title = raw.Question
		}
		d.Body = string(body)
	}
	if strings.HasPrefix(d.Key, "linear/") {
		d.Identifier = strings.TrimPrefix(d.Key, "linear/")
	}
	return d, nil
}

var taskIdentifier = regexp.MustCompile(`^[A-Z][A-Z0-9]*-[0-9]+\b`)
