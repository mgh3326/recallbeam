package oracle

import (
	"database/sql/driver"
	"errors"
	goora "github.com/sijms/go-ora/v2"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/mgh3326/recallbeam/internal/core"
)

func TestApplyCommitsDeleteAndCursorTogether(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := Store{DB: db, Source: "hk"}
	m.ExpectBegin()
	m.ExpectQuery("SELECT active_slot").WithArgs("hk").WillReturnRows(sqlmock.NewRows([]string{"slot"}).AddRow(0))
	m.ExpectExec("DELETE FROM rb_docs_a").WithArgs("hk", "task", "1").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("MERGE INTO rb_cursor_a").WithArgs("hk", 11, 1).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	err = s.Apply(t.Context(), false, []core.Mutation{{Doc: core.Doc{Source: "hk", Model: "task", ID: "1"}, Delete: true}}, core.Cursor{Last: 11, Schema: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestApplyCursorFailureRollsBackRows(t *testing.T) {
	db, m, _ := sqlmock.New()
	defer db.Close()
	s := Store{DB: db, Source: "hk"}
	m.ExpectBegin()
	m.ExpectQuery("SELECT active_slot").WillReturnRows(sqlmock.NewRows([]string{"slot"}).AddRow(0))
	m.ExpectExec("DELETE FROM rb_docs_a").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectExec("MERGE INTO rb_cursor_a").WillReturnError(errors.New("disk failure"))
	m.ExpectRollback()
	if s.Apply(t.Context(), false, []core.Mutation{{Doc: core.Doc{Source: "hk", Model: "task", ID: "1"}, Delete: true}}, core.Cursor{Last: 11, Schema: 1}, false) == nil {
		t.Fatal("expected failure")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestPublishCommitsCursorAndPointerTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		db, m, _ := sqlmock.New()
		s := Store{DB: db, Source: "hk"}
		m.ExpectBegin()
		m.ExpectQuery("SELECT active_slot").WillReturnRows(sqlmock.NewRows([]string{"slot"}).AddRow(0))
		m.ExpectExec("MERGE INTO rb_cursor_b").WillReturnResult(sqlmock.NewResult(0, 1))
		ex := m.ExpectExec("UPDATE rb_state SET active_slot=1-active_slot")
		if fail {
			ex.WillReturnError(errors.New("failure"))
			m.ExpectRollback()
		} else {
			ex.WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectCommit()
		}
		err := s.Apply(t.Context(), true, nil, core.Cursor{Last: 10, Schema: 1}, true)
		if (err != nil) != fail {
			t.Fatal(err)
		}
		if err = m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
func TestSearchUsesOneSnapshotAndTieredOrder(t *testing.T) {
	db, m, _ := sqlmock.New()
	defer db.Close()
	s := Store{DB: db, Source: "hk"}
	m.ExpectQuery(`WITH candidates AS .*rb_docs_a.*UNION ALL.*rb_docs_b.*ORDER BY tier,CASE WHEN tier=2 THEN plen ELSE 0 END,CASE WHEN tier=3 THEN text_score ELSE 0 END DESC`).WithArgs("rob-123", "rob-123%", "hk", "task", "{ROB-123}", 20).WillReturnRows(sqlmock.NewRows([]string{"source", "model", "id", "key", "identifier", "title", "body", "tags", "at", "sync", "tier", "plen", "score"}).AddRow("hk", "task", "123", "ROB-123", "ROB-123", "title", "한글", nil, time.Now(), 10, 1, 0, 0))
	hits, err := s.Search(t.Context(), core.Query{Q: "ROB-123", Model: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Body != "한글" || hits[0].Tier != 1 {
		t.Fatal(hits)
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestLiteralSearchEscapesOperators(t *testing.T) {
	if got := escapeLike(`a%_\`); got != `a\%\_\\%` {
		t.Fatal(got)
	}
	if got := TextQuery("one OR two"); got != "{one} AND {OR} AND {two}" {
		t.Fatal(got)
	}
	if got := TextQuery("a}b"); !strings.Contains(got, "}}") {
		t.Fatal(got)
	}
}

func TestUpsertUsesMergeWithLOBsAndAtomicCursor(t *testing.T) {
	db, m, err := sqlmock.New(sqlmock.ValueConverterOption(lobConverter{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := Store{DB: db, Source: "hk"}
	at := time.Now()
	for range 2 {
		m.ExpectBegin()
		m.ExpectQuery("SELECT active_slot").WillReturnRows(sqlmock.NewRows([]string{"slot"}).AddRow(0))
		m.ExpectExec("MERGE INTO rb_docs_a.*WHEN MATCHED THEN UPDATE.*WHEN NOT MATCHED THEN INSERT").WithArgs("hk", "task", "1", "ROB-1", "title", "body", "tag", at, 11, "title\nbody", "ROB-1").WillReturnResult(sqlmock.NewResult(0, 1))
		m.ExpectExec("MERGE INTO rb_cursor_a").WillReturnResult(sqlmock.NewResult(0, 1))
		m.ExpectCommit()
		err = s.Apply(t.Context(), false, []core.Mutation{{Doc: core.Doc{Source: "hk", Model: "task", ID: "1", Key: "ROB-1", Identifier: "ROB-1", Title: "title", Body: "body", Tags: "tag", UpdatedAt: at, SyncID: 11}}}, core.Cursor{Last: 11, Schema: 1}, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type lobConverter struct{}

func (lobConverter) ConvertValue(v any) (driver.Value, error) {
	if c, ok := v.(goora.Clob); ok {
		return c.String, nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}
