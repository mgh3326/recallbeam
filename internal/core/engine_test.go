package core_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	m.cursors[0] = core.Cursor{Last: 10}
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
	if !reflect.DeepEqual(after, []int64{10}) || m.active != 1 || m.published != 1 || len(m.docs[1]) != len(core.IndexedModels)-1 || m.cursors[1].Last != 12 { // the delta deletes document
		t.Fatalf("bad rebuild %+v after %v", m, after)
	}
}
func TestResetAndGenerationChangeRebuild(t *testing.T) {
	for _, reason := range []string{"410", "generation"} {
		t.Run(reason, func(t *testing.T) {
			m := newMemory()
			seed(m)
			calls := 0
			s := source{bootstrap: emptyBootstrap, delta: func(int64) (core.Delta, error) {
				calls++
				if calls == 1 && reason == "410" {
					return core.Delta{}, core.ErrReset
				}
				return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 1}}, nil
			}}
			e := core.Engine{Source: s, Store: m, Name: "hk"}
			if reason == "generation" {
				e.Generation = core.Generation("extract 2")
			}
			if err := e.Tick(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			if m.active != 1 || len(m.docs[1]) != 0 || m.cursors[1].Generation != e.Generation {
				t.Fatalf("did not replace old index: %+v", m)
			}
		})
	}
}
func TestSourceSchemaChangeDoesNotRebuild(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{bootstrap: func(string, string) (core.Page, error) {
		t.Fatal("source schema change started a rebuild")
		return core.Page{}, nil
	}, delta: func(a int64) (core.Delta, error) {
		if a == 11 {
			return core.Delta{Metadata: core.Metadata{Last: 11, Max: 11, Schema: 31}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 30}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for range 2 {
		if err := e.Tick(t.Context(), false); err != nil {
			t.Fatal(err)
		}
	}
	if m.active != 0 || m.cursors[0].Last != 11 || m.docs[0]["task/1"].Body != "current" {
		t.Fatalf("bad state %+v", m)
	}
}
func TestIncompatibleRowShapeHoldsCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "document", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 31}}, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) {
		return core.Doc{}, fmt.Errorf("body renamed: %w", core.ErrIncompatible)
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for range core.RowAttempts * 2 {
		if err := e.Tick(t.Context(), false); !errors.Is(err, core.ErrIncompatible) {
			t.Fatal("incompatible shape was not reported", err)
		}
	}
	if m.cursors[0].Last != 10 || m.active != 0 {
		t.Fatal("incompatible shape was skipped or rebuilt")
	}
}
func TestGenerationKey(t *testing.T) {
	a := core.Generation("hk extract 1", "oracle text 1", "migrations x")
	if a <= 0 || a != core.Generation("hk extract 1", "oracle text 1", "migrations x") {
		t.Fatal("generation must be positive and deterministic")
	}
	for _, parts := range [][]string{{"hk extract 2", "oracle text 1", "migrations x"}, {"hk extract 1", "oracle text 2", "migrations x"}, {"hk extract 1", "oracle text 1", "migrations y"}, {"hk extract 1oracle text 1", "migrations x"}} {
		if core.Generation(parts...) == a {
			t.Fatalf("generation ignored a change: %q", parts)
		}
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
func TestDeltaCoalescesEventsPerRow(t *testing.T) {
	m := newMemory()
	seed(m)
	m.docs[0]["document/3"] = core.Doc{ID: "3"}
	fetched := map[string]int{}
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{
			{SyncID: 11, Model: "task", ID: "1", Action: "I"},
			{SyncID: 12, Model: "task", ID: "1", Action: "U"},
			{SyncID: 13, Model: "document", ID: "2", Action: "U"},
			{SyncID: 14, Model: "document", ID: "3", Action: "U"},
			{SyncID: 15, Model: "task", ID: "1", Action: "U"},
			{SyncID: 16, Model: "document", ID: "3", Action: "D"},
			{SyncID: 17, Model: "decision", ID: "old", Action: "D"},
			{SyncID: 18, Model: "decision", ID: "old", Action: "I"},
		}, Metadata: core.Metadata{Last: 18, Max: 18, Schema: 1}}, nil
	}, fetch: func(model string, r core.Ref) (core.Doc, error) {
		fetched[model+"/"+r.ID]++
		return core.Doc{Body: "current"}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fetched, map[string]int{"task/1": 1, "document/2": 1, "decision/old": 1}) {
		t.Fatalf("fetches %v", fetched)
	}
	docs := m.docs[0]
	if docs["task/1"].SyncID != 15 || docs["decision/old"].SyncID != 18 || docs["document/2"].SyncID != 13 {
		t.Fatalf("sync IDs %+v", docs)
	}
	if _, ok := docs["document/3"]; ok || m.cursors[0].Last != 18 {
		t.Fatalf("bad state %+v", m)
	}
}
func TestFailingRowDoesNotFreezeCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	m.docs[0]["task/bad"] = core.Doc{Body: "stale"}
	attempts := 0
	s := source{delta: func(a int64) (core.Delta, error) {
		if a == 12 {
			return core.Delta{Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "bad", Action: "U"}, {SyncID: 12, Model: "task", ID: "ok", Action: "U"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}, fetch: func(_ string, r core.Ref) (core.Doc, error) {
		if r.ID == "bad" {
			attempts++
			return core.Doc{}, errors.New("hk status 500")
		}
		return core.Doc{Body: "current"}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i < core.RowAttempts; i++ {
		if e.Tick(t.Context(), false) == nil || m.cursors[0].Last != 10 {
			t.Fatalf("attempt %d: row failure must hold the cursor before the limit", i)
		}
	}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if m.cursors[0].Last != 12 || m.docs[0]["task/ok"].Body != "current" || m.docs[0]["task/bad"].Body != "stale" || attempts != core.RowAttempts {
		t.Fatalf("bad state %+v attempts=%d", m, attempts)
	}
}
func TestSourceUnavailableAlwaysHoldsCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) {
		return core.Doc{}, fmt.Errorf("hk status 503: %w", core.ErrUnavailable)
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for range core.RowAttempts * 2 {
		if e.Tick(t.Context(), false) == nil {
			t.Fatal("outage skipped a row")
		}
	}
	if m.cursors[0].Last != 10 {
		t.Fatal("advanced cursor during outage")
	}
}
func TestRebuildSkipsRowThatKeepsFailing(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{bootstrap: func(model, _ string) (core.Page, error) {
		if model == "document" {
			return core.Page{Rows: []core.Ref{{ID: "bad", Key: "k"}, {ID: "ok", Key: "k2"}}, Max: 10, Schema: 1}, nil
		}
		return core.Page{Max: 10, Schema: 1}, nil
	}, delta: func(int64) (core.Delta, error) {
		return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 1}}, nil
	}, fetch: func(_ string, r core.Ref) (core.Doc, error) {
		if r.ID == "bad" {
			return core.Doc{}, errors.New("row exceeds limit")
		}
		return core.Doc{Body: "current"}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i < core.RowAttempts; i++ {
		if e.Tick(t.Context(), true) == nil || m.active != 0 {
			t.Fatal("rebuild published before the row exhausted its attempts")
		}
	}
	if err := e.Tick(t.Context(), true); err != nil {
		t.Fatal(err)
	}
	if m.active != 1 || len(m.docs[1]) != 1 || m.docs[1]["document/ok"].Body != "current" {
		t.Fatalf("bad rebuild %+v", m)
	}
}
func TestRebuildAttemptsRowsOnLaterPagesInSameRun(t *testing.T) {
	m := newMemory()
	seed(m)
	attempts := map[string]int{}
	s := source{bootstrap: func(model, cursor string) (core.Page, error) {
		if model != "document" {
			return core.Page{Max: 10, Schema: 1}, nil
		}
		if cursor == "" {
			return core.Page{Rows: []core.Ref{{ID: "bad1", Key: "k1"}}, Next: "p2", Max: 10, Schema: 1}, nil
		}
		return core.Page{Rows: []core.Ref{{ID: "bad2", Key: "k2"}}, Max: 10, Schema: 1}, nil
	}, delta: func(int64) (core.Delta, error) {
		return core.Delta{Metadata: core.Metadata{Last: 10, Max: 10, Schema: 1}}, nil
	}, fetch: func(_ string, r core.Ref) (core.Doc, error) {
		attempts[r.ID]++
		return core.Doc{}, errors.New("hk status 500")
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i < core.RowAttempts; i++ {
		if e.Tick(t.Context(), true) == nil || m.active != 0 {
			t.Fatal("rebuild published while a row was still held")
		}
	}
	if err := e.Tick(t.Context(), true); err != nil {
		t.Fatal(err)
	}
	if attempts["bad2"] != core.RowAttempts {
		t.Fatalf("row on a later page got %d attempts, want %d", attempts["bad2"], core.RowAttempts)
	}
}
func TestSkippedRowGetsFreshBudgetAfterItsPageCommits(t *testing.T) {
	m := newMemory()
	seed(m)
	s := source{delta: func(a int64) (core.Delta, error) {
		if a == 10 {
			return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "bad", Action: "U"}}, Metadata: core.Metadata{Last: 11, Max: 11, Schema: 1}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 12, Model: "task", ID: "bad", Action: "U"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) { return core.Doc{}, errors.New("hk status 500") }}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i < core.RowAttempts; i++ {
		if e.Tick(t.Context(), false) == nil || m.cursors[0].Last != 10 {
			t.Fatalf("attempt %d: row failure must hold the cursor before the limit", i)
		}
	}
	if err := e.Tick(t.Context(), false); err != nil || m.cursors[0].Last != 11 {
		t.Fatalf("skipped row must let its page commit: %v %+v", err, m.cursors[0])
	}
	if e.Tick(t.Context(), false) == nil || m.cursors[0].Last != 11 {
		t.Fatal("one failure after a committed skip skipped the row again")
	}
}
func TestDeletedRowLosesItsFailureCount(t *testing.T) {
	m := newMemory()
	seed(m)
	page := func(sync int64, action string) core.Delta {
		return core.Delta{Changes: []core.Change{{SyncID: sync, Model: "task", ID: "bad", Action: action}}, Metadata: core.Metadata{Last: sync, Max: sync, Schema: 1}}
	}
	pages := []core.Delta{page(11, "U"), page(11, "U"), page(12, "D"), page(13, "I")}
	s := source{delta: func(a int64) (core.Delta, error) {
		if len(pages) == 0 {
			return core.Delta{Metadata: core.Metadata{Last: a, Max: a, Schema: 1}}, nil
		}
		d := pages[0]
		pages = pages[1:]
		return d, nil
	}, fetch: func(string, core.Ref) (core.Doc, error) { return core.Doc{}, errors.New("hk status 500") }}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i < core.RowAttempts; i++ {
		if e.Tick(t.Context(), false) == nil {
			t.Fatal("expected hold")
		}
	}
	if err := e.Tick(t.Context(), false); err != nil || m.cursors[0].Last != 12 {
		t.Fatalf("delete must commit: %v %+v", err, m.cursors[0])
	}
	if e.Tick(t.Context(), false) == nil || m.cursors[0].Last != 12 {
		t.Fatal("re-created row was skipped on its first failure")
	}
}
func TestSkipIsLoggedOnlyWhenItsPageCommits(t *testing.T) {
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	m := newMemory()
	seed(m)
	tick := 0
	s := source{delta: func(int64) (core.Delta, error) {
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task", ID: "bad", Action: "U"}, {SyncID: 12, Model: "task", ID: "late", Action: "U"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}, fetch: func(_ string, r core.Ref) (core.Doc, error) {
		if r.ID == "bad" || (r.ID == "late" && tick == core.RowAttempts) {
			return core.Doc{}, errors.New("hk status 500")
		}
		return core.Doc{Body: "current"}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	for i := 1; i <= core.RowAttempts; i++ {
		tick = i
		if e.Tick(t.Context(), false) == nil || m.cursors[0].Last != 10 {
			t.Fatalf("tick %d: a page with a held row must not commit", i)
		}
	}
	if strings.Contains(logs.String(), "sync.row_skipped") {
		t.Fatalf("skip logged for a page that did not commit: %q", logs.String())
	}
	tick++
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `model_id="bad"`) {
		t.Fatalf("committed skip not logged: %q", logs.String())
	}
}

// FetchConcurrency fetches one page's rows at most N at a time and applies
// them in page order; a source-wide failure still fails the whole page.
func TestConcurrentFetchBoundedAndOrdered(t *testing.T) {
	const n, rows = 4, 40
	var inFlight, peak atomic.Int32
	var changes []core.Change
	for i := range rows {
		changes = append(changes, core.Change{SyncID: int64(11 + i), Model: "task", ID: strconv.Itoa(i), Action: "U"})
	}
	last := int64(10 + rows)
	fetch := func(_ string, r core.Ref) (core.Doc, error) {
		v := inFlight.Add(1)
		for p := peak.Load(); v > p && !peak.CompareAndSwap(p, v); p = peak.Load() {
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return core.Doc{Body: "row " + r.ID}, nil
	}
	m := newMemory()
	seed(m)
	s := source{fetch: fetch, delta: func(a int64) (core.Delta, error) {
		if a >= last {
			return core.Delta{Metadata: core.Metadata{Last: last, Max: last, Schema: 1}}, nil
		}
		return core.Delta{Changes: changes, Metadata: core.Metadata{Last: last, Max: last, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk", FetchConcurrency: n}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p < 2 || p > n {
		t.Fatalf("peak in-flight fetches = %d, want 2..%d", p, n)
	}
	for i := range rows {
		if d := m.docs[m.active]["task/"+strconv.Itoa(i)]; d.Body != "row "+strconv.Itoa(i) || d.SyncID != int64(11+i) {
			t.Fatalf("row %d: %+v", i, d)
		}
	}

	// A source-wide failure on one row fails the page and moves no cursor.
	m2 := newMemory()
	seed(m2)
	s.fetch = func(_ string, r core.Ref) (core.Doc, error) {
		if r.ID == "7" {
			return core.Doc{}, core.ErrUnavailable
		}
		return core.Doc{Body: "x"}, nil
	}
	e2 := core.Engine{Source: s, Store: m2, Name: "hk", FetchConcurrency: n}
	if err := e2.Tick(t.Context(), false); !errors.Is(err, core.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if m2.cursors[m2.active].Last != 10 {
		t.Fatalf("cursor moved on a failed page: %+v", m2.cursors[m2.active])
	}
}

// A change to a model outside IndexedModels moves the cursor and fetches
// nothing.
func TestNonIndexedModelAdvancesCursor(t *testing.T) {
	m := newMemory()
	seed(m)
	fetched := 0
	s := source{fetch: func(string, core.Ref) (core.Doc, error) { fetched++; return core.Doc{Body: "x"}, nil }, delta: func(a int64) (core.Delta, error) {
		if a >= 12 {
			return core.Delta{Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "task_comment", ID: "5", Action: "I"}, {SyncID: 12, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk"}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if fetched != 1 || m.cursors[m.active].Last != 12 {
		t.Fatalf("fetched=%d cursor=%+v", fetched, m.cursors[m.active])
	}
	if _, ok := m.docs[m.active]["task_comment/5"]; ok {
		t.Fatal("task_comment indexed")
	}
}

// Review on #6: fetches cut short by a page's source-wide failure are not
// row failures (no attempts spent, so no row is skipped after recovery), and
// a concurrent 410 wins over the cancellation fallout so the engine rebuilds.
func TestConcurrentFetchCancellationIsNotARowFailure(t *testing.T) {
	const rows = 12
	var changes []core.Change
	for i := range rows {
		changes = append(changes, core.Change{SyncID: int64(11 + i), Model: "task", ID: strconv.Itoa(i), Action: "U"})
	}
	last := int64(10 + rows)
	delta := func(a int64) (core.Delta, error) {
		if a >= last {
			return core.Delta{Metadata: core.Metadata{Last: last, Max: last, Schema: 1}}, nil
		}
		return core.Delta{Changes: changes, Metadata: core.Metadata{Last: last, Max: last, Schema: 1}}, nil
	}
	outage := true
	fetch := func(ctx context.Context, r core.Ref) (core.Doc, error) {
		if outage {
			if r.ID == "0" {
				return core.Doc{}, core.ErrUnavailable
			}
			// Cancelled by row 0's failure; the timer only keeps a broken
			// engine from hanging the test.
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			return core.Doc{}, fmt.Errorf("hk request failed: %w", core.ErrUnavailable)
		}
		return core.Doc{Body: "row " + r.ID}, nil
	}
	m := newMemory()
	seed(m)
	e := core.Engine{Source: ctxSource{source{delta: delta, bootstrap: emptyBootstrap}, fetch}, Store: m, Name: "hk", FetchConcurrency: 4}
	for range core.RowAttempts + 1 {
		if err := e.Tick(t.Context(), false); !errors.Is(err, core.ErrUnavailable) {
			t.Fatalf("outage tick: %v", err)
		}
	}
	outage = false
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if got := len(m.docs[m.active]); got != rows+1 { // the rows plus seed's document/old
		t.Fatalf("after recovery %d docs, want %d (a row was skipped)", got, rows+1)
	}

	// A 410 on one row beats the ErrUnavailable fallout of the rest.
	rebuilt := false
	m2 := newMemory()
	seed(m2)
	e2 := core.Engine{Source: ctxSource{source{delta: delta, bootstrap: func(string, string) (core.Page, error) {
		rebuilt = true
		return core.Page{Max: last, Schema: 1}, nil
	}}, func(ctx context.Context, r core.Ref) (core.Doc, error) {
		if r.ID == "1" {
			return core.Doc{}, core.ErrReset
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		return core.Doc{}, fmt.Errorf("hk request failed: %w", core.ErrUnavailable)
	}}, Store: m2, Name: "hk", FetchConcurrency: 4}
	_ = e2.Tick(t.Context(), false)
	if !rebuilt {
		t.Fatal("a 410 during a concurrent page did not trigger a rebuild")
	}
}

// ctxSource is source with a context-aware fetch.
type ctxSource struct {
	source
	fetch func(context.Context, core.Ref) (core.Doc, error)
}

func (s ctxSource) Fetch(ctx context.Context, _ string, r core.Ref) (core.Doc, error) {
	return s.fetch(ctx, r)
}

// A row the index cannot store (CheckDoc) is a row failure: it holds the
// page, then is skipped after RowAttempts so the cursor moves and the other
// rows are indexed (review on recallbeam #10: a failed write would block
// the page forever).
func TestCheckDocRejectionIsSkippedAfterAttempts(t *testing.T) {
	m := newMemory()
	seed(m)
	// An earlier version of the row is already indexed; once the row is
	// rejected it must not stay searchable.
	m.docs[m.active]["document/long"] = core.Doc{Source: "hk", Model: "document", ID: "long", SyncID: 5, Title: "old version"}
	s := source{delta: func(a int64) (core.Delta, error) {
		if a >= 12 {
			return core.Delta{Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
		}
		return core.Delta{Changes: []core.Change{{SyncID: 11, Model: "document", ID: "long", Action: "U"}, {SyncID: 12, Model: "task", ID: "1", Action: "U"}}, Metadata: core.Metadata{Last: 12, Max: 12, Schema: 1}}, nil
	}}
	e := core.Engine{Source: s, Store: m, Name: "hk", CheckDoc: func(d core.Doc) error {
		if d.ID == "long" {
			return errors.New("doc_key over the column limit")
		}
		return nil
	}}
	for i := 1; i < core.RowAttempts; i++ {
		if err := e.Tick(t.Context(), false); !errors.Is(err, core.ErrRowHeld) {
			t.Fatalf("attempt %d: %v, want ErrRowHeld", i, err)
		}
		if m.cursors[m.active].Last != 10 {
			t.Fatalf("cursor moved while the row was held: %+v", m.cursors[m.active])
		}
	}
	if err := e.Tick(t.Context(), false); err != nil {
		t.Fatalf("final attempt: %v", err)
	}
	if m.cursors[m.active].Last != 12 || m.docs[m.active]["task/1"].SyncID != 12 {
		t.Fatalf("page did not commit after the skip: %+v", m)
	}
	if d, ok := m.docs[m.active]["document/long"]; ok {
		t.Fatalf("rejected row is still indexed: %+v", d)
	}
}
