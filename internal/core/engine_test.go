package core_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mgh3326/recallbeam/internal/core"
)

type memory struct {
	active    int
	docs      [2]map[string]core.Doc
	cursors   [2]core.Cursor
	exists    [2]bool
	fail      bool
	published int
	locked    bool
}

func newMemory() *memory { return &memory{docs: [2]map[string]core.Doc{{}, {}}} }
func (m *memory) Lock(context.Context) (func(), error) {
	if m.locked {
		return nil, errors.New("already locked")
	}
	m.locked = true
	return func() { m.locked = false }, nil
}
func (m *memory) slot(shadow bool) int {
	if shadow {
		return 1 - m.active
	}
	return m.active
}
func (m *memory) Cursor(_ context.Context, shadow bool) (core.Cursor, bool, error) {
	i := m.slot(shadow)
	return m.cursors[i], m.exists[i], nil
}
func (m *memory) Reset(context.Context) error {
	i := m.slot(true)
	m.docs[i] = map[string]core.Doc{}
	m.exists[i] = false
	return nil
}
func (m *memory) Apply(_ context.Context, shadow bool, ms []core.Mutation, c core.Cursor, publish bool) error {
	if !m.locked {
		panic("write without writer lock")
	}
	if m.fail {
		return errors.New("commit failed")
	}
	i := m.slot(shadow)
	for _, v := range ms {
		key := v.Doc.Model + "/" + v.Doc.ID
		if v.Delete {
			delete(m.docs[i], key)
		} else {
			m.docs[i][key] = v.Doc
		}
	}
	m.cursors[i] = c
	m.exists[i] = true
	if publish {
		m.active = i
		m.published++
	}
	return nil
}

type source struct {
	bootstrap func(string, string) (core.Page, error)
	delta     func(int64) (core.Delta, error)
	fetch     func(string, core.Ref) (core.Doc, error)
}

func (s source) Bootstrap(_ context.Context, m, c string) (core.Page, error) {
	return s.bootstrap(m, c)
}
func (s source) Delta(_ context.Context, a int64) (core.Delta, error) { return s.delta(a) }
func (s source) Fetch(_ context.Context, m string, r core.Ref) (core.Doc, error) {
	if s.fetch != nil {
		return s.fetch(m, r)
	}
	return core.Doc{Body: "current", Key: r.Key}, nil
}
func emptyBootstrap(m, c string) (core.Page, error) { return core.Page{Max: 10, Schema: 1}, nil }
func seed(m *memory) {
	m.exists[0] = true
	m.cursors[0] = core.Cursor{Last: 10, Schema: 1}
	m.docs[0]["document/old"] = core.Doc{ID: "old", Body: "old"}
}
func TestDeltaIdempotentUpsertDeleteAndCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(a int64) (core.Delta, error) {
		if a == 12 {
			return core.Delta{Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "1", Action: "I"}, {SyncID: 12, Model: "document", ID: "old", Action: "D"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for range 2 {
		if err := e.Tick(t.Context(), false); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.docs[m.active]) != 1 || m.docs[m.active]["task/1"].SyncID != 11 || m.cursors[m.active].Last != 12 {
		t.Fatalf("bad state: %+v", m)
	}
}
func TestFailedFetchDoesNotAdvanceCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) { return core.Doc{}, errors.New("offline") }}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if e.Tick(t.Context(), false) == nil {
		t.Fatal("expected failure")
	}
	if m.cursors[0].Last != 10 || len(m.docs[0]) != 1 {
		t.Fatal("partial write")
	}
}
func TestBootstrapShadowSwapReplaysEarliestWatermark(t *testing.T) {
	m := newMemory()
	seed(m)
	var after []int64
	s := source{bootstrap: func(model, cursor string) (core.Page, error) {
		max := int64(12)
		if model == "document" {
			max = 10
		}
		return core.Page{Rows: []core.Ref{{ID: model}}, Max: max, Schema: 1}, nil
	}, delta: func(a int64) (core.Delta, error) {
		after = append(after, a)
		if m.active != 0 {
			t.Fatal("published before replay")
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "document", ID: "document", Action: "D"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if err := e.Tick(t.Context(), true); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, []int64{10}) || m.active != 1 || m.published != 1 || len(m.docs[1]) != 3 || m.cursors[1].Last != 12 {
		t.Fatalf("bad rebuild %+v after %v", m, after)
	}
}
func TestResetAndSchemaChangeRebuild(t *testing.T) {
	for _, schema := range []bool{false, true} {
		t.Run(map[bool]string{false: "410", true: "schema"}[schema], func(t *testing.T) {
			m := newMemory()
			seed(m)
			calls := 0
			s := source{bootstrap: emptyBootstrap, delta: func(int64) (core.Delta, error) {
				calls++
				if calls == 1 {
					if schema {
						return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 2}}, nil
					}
					return core.Delta{}, core.ErrReset
				}
				return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 1}}, nil
			}}
			e := core.Engine{Source: s, Store: m, Name: "hk"}
			if err := e.Tick(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			if m.active != 1 || len(m.docs[1]) != 0 {
				t.Fatal("did not replace old index")
			}
		})
	}
}
func TestFailedRebuildKeepsActiveAndRetryResetsShadow(t *testing.T) {
	m := newMemory()
	seed(m)
	fail := true
	s := source{bootstrap: emptyBootstrap, delta: func(int64) (core.Delta, error) {
		if fail {
			return core.Delta{}, errors.New("truncated")
		}
		return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if e.Tick(t.Context(), true) == nil {
		t.Fatal("expected failure")
	}
	if m.active != 0 || m.cursors[0].Last != 10 || len(m.docs[0]) != 1 {
		t.Fatal("active index changed")
	}
	m.docs[1]["task/abandoned"] = core.Doc{}
	fail = false
	if err := e.Tick(t.Context(), true); err != nil {
		t.Fatal(err)
	}
	if len(m.docs[1]) != 0 {
		t.Fatal("stale shadow row survived retry")
	}
}
func TestMissingCurrentRowBecomesDelete(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "document", ID: "old", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) { return core.Doc{}, core.ErrNotFound }}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if len(m.docs[0]) != 0 {
		t.Fatal("stale row")
	}
}
func TestDeltaRejectsMalformedProgress(t *testing.T) {
	for _, d := range []core.Delta{{Metadata: core.Metadata{Last: 9, Max: 11, Schema: 1}}, {Metadata: core.Metadata{Last: 10, Max: 11, Schema: 1}}, {Changes: []core.Change{{SyncID: 11, Model: "unknown", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}, {Changes: []core.Change{{SyncID: 10, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}} {
		m := newMemory()
		seed(m)
		e := core.Engine{Source: source{delta: func(int64) (core.Delta, error) { return d, nil }}, Store: m, Name: "hk"}
		if e.Tick(t.Context(), false) == nil {
			t.Fatal("accepted invalid feed")
		}
		if m.cursors[0].Last != 10 {
			t.Fatal("advanced cursor")
		}
	}
}
func TestRankingTiers(t *testing.T) {
	hits := []core.Hit{{Doc: core.Doc{ID: "z", Title: "unrelated"}, Score: 100}, {Doc: core.Doc{ID: "b", Title: "ROB-1234 details"}}, {Doc: core.Doc{ID: "c", Key: "ROB-1234"}}, {Doc: core.Doc{ID: "a", Key: "ROB-123"}}, {Doc: core.Doc{ID: "ROB-123"}}, {Doc: core.Doc{ID: "d", Title: "unrelated"}, Score: 50}}
	out := core.Rank("rob-123", hits)
	ids := []string{}
	for _, h := range out {
		ids = append(ids, h.ID)
	}
	if !reflect.DeepEqual(ids, []string{"ROB-123", "a", "c", "b", "z", "d"}) {
		t.Fatal(ids)
	}
	ko := core.Rank("검색", []core.Hit{{Doc: core.Doc{ID: "2", Title: "검색 도구"}}, {Doc: core.Doc{ID: "1", Title: "검색"}}})
	if ko[0].ID != "1" {
		t.Fatal(ko)
	}
}
