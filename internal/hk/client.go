package hk

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/recallbeam/internal/core"
)

var errReadEndpoint = errors.New("hk read endpoint unavailable")

type Client struct {
	Base, Token string
	HTTP        *http.Client
}

func (c Client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.Base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	r, err := client.Do(req)
	if err != nil {
		return nil, errors.New("hk request failed")
	}
	if r.StatusCode != http.StatusOK {
		defer r.Body.Close()
		switch r.StatusCode {
		case http.StatusGone:
			return nil, core.ErrReset
		case http.StatusNotFound:
			var problem struct {
				Error string `json:"error"`
			}
			if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") && json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&problem) == nil && problem.Error == "not_found" {
				return nil, core.ErrNotFound
			}
			return nil, errReadEndpoint
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

// Fetch reads current content through hk's existing read APIs.
func (c Client) Fetch(ctx context.Context, model string, ref core.Ref) (core.Doc, error) {
	var d core.Doc
	var path string
	switch model {
	case "document":
		if ref.Key == "" {
			return c.resolve(ctx, model, ref)
		}
		path = "/v1/documents/" + url.PathEscape(ref.Key)
	case "task":
		path = "/v1/tasks/" + url.PathEscape(ref.ID)
	case "task_comment", "decision":
		return c.resolve(ctx, model, ref)
	default:
		return d, errors.New("unknown model")
	}

	r, err := c.get(ctx, path)
	if err != nil {
		return d, err
	}
	defer r.Body.Close()
	var raw struct {
		Key, Title, Body, Kind string
		UpdatedAt              time.Time       `json:"updated_at"`
		CreatedAt              time.Time       `json:"created_at"`
		Refs                   json.RawMessage `json:"refs"`
		Events                 json.RawMessage `json:"events"`
	}
	if err = json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&raw); err != nil {
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
	if model == "task" {
		d.Body = string(raw.Refs) + "\n" + string(raw.Events)
	}
	if strings.HasPrefix(d.Key, "linear/") {
		d.Identifier = strings.TrimPrefix(d.Key, "linear/")
	}
	if model == "task" {
		d.Identifier = taskIdentifier.FindString(d.Title)
	}
	return d, nil
}

var taskIdentifier = regexp.MustCompile(`^[A-Z][A-Z0-9]*-[0-9]+\b`)
