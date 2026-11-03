package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

var Models = []string{"document", "task", "task_comment", "decision"}
var ErrReset = errors.New("source requires bootstrap")
var ErrNotFound = errors.New("source row missing")

// ErrUnavailable marks a source failure that is not about one row: the source
// is unreachable, refuses the credentials, or lacks the read endpoint. Such
// failures always hold the cursor; only row-specific failures can be skipped.
var ErrUnavailable = errors.New("source unavailable")

// RowAttempts is how many polls one row may fail to fetch before the engine
// leaves it out of its page so the cursor can advance. The skipped row keeps
// its previously indexed content until a later change or the daily rebuild
// fetches it successfully.
const RowAttempts = 3

func ValidModel(m string) bool {
	for _, v := range Models {
		if m == v {
			return true
		}
	}
	return false
}

type Doc struct {
	Source     string    `json:"source"`
	Model      string    `json:"model"`
	ID         string    `json:"model_id"`
	Key        string    `json:"doc_key"`
	Identifier string    `json:"identifier,omitempty"`
	Title      string    `json:"title"`
	Body       string    `json:"body"`
	Tags       string    `json:"tags"`
	UpdatedAt  time.Time `json:"updated_at"`
	SyncID     int64     `json:"source_sync_id"`
}
type Ref struct {
	ID        string    `json:"id"`
	Key       string    `json:"key,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Change struct {
	SyncID int64     `json:"sync_id"`
	Model  string    `json:"model"`
	ID     string    `json:"model_id"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
}
type Metadata struct {
	Last   int64 `json:"last_sync_id"`
	Max    int64 `json:"max_sync_id"`
	Schema int   `json:"schema_version"`
}
type Page struct {
	Rows   []Ref  `json:"rows"`
	Next   string `json:"cursor"`
	Max    int64  `json:"max_sync_id"`
	Schema int    `json:"schema_version"`
}
type Delta struct {
	Changes []Change
	Metadata
}
type Cursor struct {
	Last   int64
	Schema int
}
type Source interface {
	Bootstrap(context.Context, string, string) (Page, error)
	Delta(context.Context, int64) (Delta, error)
	Fetch(context.Context, string, Ref) (Doc, error)
}
type Mutation struct {
	Doc    Doc
	Delete bool
}

// Apply must persist the rows and cursor in one transaction. Publish changes
// the active table set in that same transaction; Reset only clears the inactive set.
type Index interface {
	Lock(context.Context) (func(), error)
	Cursor(context.Context, bool) (Cursor, bool, error)
	Reset(context.Context) error
	Apply(context.Context, bool, []Mutation, Cursor, bool) error
}
type Engine struct {
	Source Source
	Store  Index
	Name   string
	// failures counts consecutive failed fetches per model/ID. Tick holds the
	// writer lock, so access is serialized.
	failures map[string]int
}

func (e *Engine) Tick(ctx context.Context, rebuild bool) error {
	release, err := e.Store.Lock(ctx)
	if err != nil {
		return err
	}
	defer release()
	c, ok, err := e.Store.Cursor(ctx, false)
	if err != nil {
		return err
	}
	if rebuild || !ok {
		return e.rebuild(ctx)
	}
	_, err = e.delta(ctx, false, c, false)
	if errors.Is(err, ErrReset) {
		return e.rebuild(ctx)
	}
	return err
}

type fetch struct {
	model  string
	ref    Ref
	syncID int64
}

// mutations fetches the current row for each item. A missing row becomes a
// delete. A row-specific failure holds the batch until it has failed
// RowAttempts times, after which the row is skipped and logged; every other
// row is still fetched so their attempts are counted in the same poll.
func (e *Engine) mutations(ctx context.Context, items []fetch) ([]Mutation, error) {
	out := make([]Mutation, 0, len(items))
	var held error
	for _, it := range items {
		key := it.model + "/" + it.ref.ID
		d, err := e.Source.Fetch(ctx, it.model, it.ref)
		if err != nil && !errors.Is(err, ErrNotFound) {
			if ctx.Err() != nil || errors.Is(err, ErrReset) || errors.Is(err, ErrUnavailable) {
				return nil, err
			}
			if e.failures == nil {
				e.failures = map[string]int{}
			}
			e.failures[key]++
			if n := e.failures[key]; n >= RowAttempts {
				log.Printf("sync.row_skipped model=%s model_id=%q attempts=%d err=%q", it.model, it.ref.ID, n, err)
			} else if held == nil {
				held = err
			}
			continue
		}
		delete(e.failures, key)
		if err != nil {
			out = append(out, Mutation{Doc: Doc{Source: e.Name, Model: it.model, ID: it.ref.ID, SyncID: it.syncID}, Delete: true})
			continue
		}
		d.Source = e.Name
		d.Model = it.model
		d.ID = it.ref.ID
		d.SyncID = it.syncID
		out = append(out, Mutation{Doc: d})
	}
	if held != nil {
		return nil, held
	}
	return out, nil
}
func (e *Engine) rebuild(ctx context.Context) error {
	if err := e.Store.Reset(ctx); err != nil {
		return err
	}
	c := Cursor{Last: -1}
	schema := 0
	for _, model := range Models {
		next := ""
		seen := map[string]bool{}
		for {
			p, err := e.Source.Bootstrap(ctx, model, next)
			if err != nil {
				return err
			}
			if p.Schema < 1 || p.Max < 0 {
				return errors.New("invalid bootstrap metadata")
			}
			if schema != 0 && schema != p.Schema {
				return ErrReset
			}
			schema = p.Schema
			// Different model snapshots start at different watermarks. Replay from the
			// earliest one so writes during any model scan cannot fall through a gap.
			if c.Last < 0 || p.Max < c.Last {
				c.Last = p.Max
			}
			c.Schema = schema
			items := make([]fetch, 0, len(p.Rows))
			for _, r := range p.Rows {
				items = append(items, fetch{model: model, ref: r, syncID: p.Max})
			}
			ms, err := e.mutations(ctx, items)
			if err != nil {
				return err
			}
			if err = e.Store.Apply(ctx, true, ms, c, false); err != nil {
				return err
			}
			if p.Next == "" {
				break
			}
			if seen[p.Next] {
				return errors.New("repeated bootstrap cursor")
			}
			seen[p.Next] = true
			next = p.Next
		}
	}
	_, err := e.delta(ctx, true, c, true)
	return err
}
func (e *Engine) delta(ctx context.Context, shadow bool, c Cursor, publish bool) (Cursor, error) {
	target := int64(-1)
	for {
		d, err := e.Source.Delta(ctx, c.Last)
		if err != nil {
			return c, err
		}
		if d.Schema != c.Schema {
			return c, ErrReset
		}
		if d.Last < c.Last || d.Max < d.Last {
			return c, errors.New("invalid delta metadata")
		}
		if target < 0 {
			target = d.Max
		}
		// Rows resolve to their current state, so several events for one row in
		// a page need one fetch. The row's last event decides between a delete
		// and a fetch, and its sync ID is recorded.
		type rowKey struct{ model, id string }
		latest := map[rowKey]Change{}
		order := []rowKey{}
		last := c.Last
		for _, ch := range d.Changes {
			if ch.SyncID <= last || ch.SyncID > d.Last || !ValidModel(ch.Model) || ch.ID == "" {
				return c, errors.New("invalid delta change")
			}
			if ch.Action != "I" && ch.Action != "U" && ch.Action != "D" {
				return c, fmt.Errorf("unknown change action %q", ch.Action)
			}
			last = ch.SyncID
			k := rowKey{ch.Model, ch.ID}
			if _, ok := latest[k]; !ok {
				order = append(order, k)
			}
			latest[k] = ch
		}
		var deletes []Mutation
		var items []fetch
		for _, k := range order {
			ch := latest[k]
			if ch.Action == "D" {
				deletes = append(deletes, Mutation{Doc: Doc{Source: e.Name, Model: ch.Model, ID: ch.ID, SyncID: ch.SyncID}, Delete: true})
			} else {
				items = append(items, fetch{model: ch.Model, ref: Ref{ID: ch.ID}, syncID: ch.SyncID})
			}
		}
		ms, err := e.mutations(ctx, items)
		if err != nil {
			return c, err
		}
		ms = append(deletes, ms...)
		if d.Last == c.Last && d.Last < target {
			return c, errors.New("delta made no progress")
		}
		c = Cursor{Last: d.Last, Schema: d.Schema}
		done := c.Last >= target
		if err = e.Store.Apply(ctx, shadow, ms, c, publish && done); err != nil {
			return c, err
		}
		if done {
			return c, nil
		}
	}
}

type Query struct {
	Q     string `json:"q"`
	Model string `json:"model,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

func (q *Query) Validate() error {
	q.Q = strings.TrimSpace(q.Q)
	if q.Q == "" || utf8.RuneCountInString(q.Q) > 256 {
		return errors.New("q must contain 1 to 256 characters")
	}
	if q.Model != "" && !ValidModel(q.Model) {
		return errors.New("unknown model")
	}
	if q.Limit == 0 {
		q.Limit = 20
	}
	if q.Limit < 1 || q.Limit > 100 {
		return errors.New("limit must be 1 to 100")
	}
	return nil
}

type Hit struct {
	Doc
	Tier         int     `json:"tier"`
	Score        float64 `json:"score"`
	PrefixLength int     `json:"-"`
}
type Searcher interface {
	Search(context.Context, Query) ([]Hit, error)
}

func Rank(q string, docs []Hit) []Hit {
	q = strings.ToLower(q)
	out := make([]Hit, 0, len(docs))
	for _, d := range docs {
		key, title, id := strings.ToLower(d.Key), strings.ToLower(d.Title), strings.ToLower(d.ID)
		d.Tier = 3
		d.PrefixLength = 0
		if key == q || id == q || strings.ToLower(d.Identifier) == q {
			d.Tier = 1
		} else {
			if strings.HasPrefix(key, q) {
				d.Tier = 2
				d.PrefixLength = utf8.RuneCountInString(key)
			}
			if strings.HasPrefix(title, q) && (d.Tier != 2 || utf8.RuneCountInString(title) < d.PrefixLength) {
				d.Tier = 2
				d.PrefixLength = utf8.RuneCountInString(title)
			}
		}
		if d.Tier < 3 || d.Score > 0 {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Tier != b.Tier {
			return a.Tier < b.Tier
		}
		if a.Tier == 2 && a.PrefixLength != b.PrefixLength {
			return a.PrefixLength < b.PrefixLength
		}
		if a.Tier == 3 && a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.Source+"/"+a.Model+"/"+a.ID < b.Source+"/"+b.Model+"/"+b.ID
	})
	return out
}
