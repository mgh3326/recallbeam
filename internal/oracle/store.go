package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/recallbeam/internal/core"
	goora "github.com/sijms/go-ora/v2"
)

type Store struct {
	DB     *sql.DB
	Source string
}

func Open(host string, port int, service, user, password, wallet string) (*sql.DB, error) {
	dsn := goora.BuildUrl(host, port, service, user, password, map[string]string{"SSL": "true", "SSL VERIFY": "true", "WALLET": wallet, "CONNECTION TIMEOUT": "15", "TIMEOUT": "60"})
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, errors.New("open Oracle connection failed")
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)
	return db, nil
}
func (s *Store) Lock(ctx context.Context) (func(), error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	var id int
	if err = tx.QueryRowContext(ctx, "SELECT id FROM rb_writer WHERE id=1 FOR UPDATE").Scan(&id); err != nil {
		tx.Rollback()
		return nil, err
	}
	return func() { _ = tx.Rollback() }, nil
}

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) slot(ctx context.Context, q querier, shadow bool) (string, error) {
	var slot int
	if err := q.QueryRowContext(ctx, "SELECT active_slot FROM rb_state WHERE source=:1", s.Source).Scan(&slot); err != nil {
		return "", err
	}
	if shadow {
		slot = 1 - slot
	}
	if slot == 0 {
		return "a", nil
	}
	if slot == 1 {
		return "b", nil
	}
	return "", errors.New("invalid active slot")
}
func (s *Store) Cursor(ctx context.Context, shadow bool) (core.Cursor, bool, error) {
	var c core.Cursor
	slot, err := s.slot(ctx, s.DB, shadow)
	if err != nil {
		return c, false, err
	}
	err = s.DB.QueryRowContext(ctx, "SELECT last_sync_id,schema_version FROM rb_cursor_"+slot+" WHERE source=:1", s.Source).Scan(&c.Last, &c.Schema)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}
func (s *Store) Reset(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	slot, err := s.slot(ctx, tx, true)
	if err != nil {
		return err
	}
	for _, table := range []string{"rb_docs_", "rb_cursor_"} {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+slot+" WHERE source=:1", s.Source); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) Apply(ctx context.Context, shadow bool, ms []core.Mutation, c core.Cursor, publish bool) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	slot, err := s.slot(ctx, tx, shadow)
	if err != nil {
		return err
	}
	for _, m := range ms {
		d := m.Doc
		if d.Source != s.Source {
			return errors.New("wrong source")
		}
		if m.Delete {
			_, err = tx.ExecContext(ctx, "DELETE FROM rb_docs_"+slot+" WHERE source=:1 AND model=:2 AND model_id=:3", d.Source, d.Model, d.ID)
		} else {
			_, err = tx.ExecContext(ctx, `MERGE INTO rb_docs_`+slot+` d USING (SELECT :p1 source,:p2 model,:p3 model_id FROM dual) x ON (d.source=x.source AND d.model=x.model AND d.model_id=x.model_id)
WHEN MATCHED THEN UPDATE SET d.doc_key=:p4,d.identifier=:p11,d.title=:p5,d.body=:p6,d.tags=:p7,d.updated_at=:p8,d.source_sync_id=:p9,d.search_text=:p10
WHEN NOT MATCHED THEN INSERT(source,model,model_id,doc_key,title,body,tags,updated_at,source_sync_id,search_text,identifier) VALUES(x.source,x.model,x.model_id,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11)`, binds(d.Source, d.Model, d.ID, d.Key, d.Title, goora.Clob{String: d.Body, Valid: true}, d.Tags, d.UpdatedAt, d.SyncID, goora.Clob{String: d.Title + "\n" + d.Body, Valid: true}, d.Identifier)...)
		}
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `MERGE INTO rb_cursor_`+slot+` c USING (SELECT :p1 source FROM dual) x ON (c.source=x.source) WHEN MATCHED THEN UPDATE SET c.last_sync_id=:p2,c.schema_version=:p3 WHEN NOT MATCHED THEN INSERT(source,last_sync_id,schema_version) VALUES(x.source,:p2,:p3)`, binds(s.Source, c.Last, c.Schema)...)
	if err != nil {
		return err
	}
	if publish {
		if !shadow {
			return errors.New("publish requires shadow")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE rb_state SET active_slot=1-active_slot WHERE source=:1", s.Source); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) Keepalive(ctx context.Context) error {
	var n int
	return s.DB.QueryRowContext(ctx, "SELECT 1 FROM dual").Scan(&n)
}

// TextQuery treats the input as literal words rather than accepting Oracle Text
// operators. Exact keys and prefixes are queried separately from the text tier.
func TextQuery(q string) string {
	parts := strings.Fields(q)
	for i, p := range parts {
		parts[i] = "{" + strings.ReplaceAll(p, "}", "}}") + "}"
	}
	return strings.Join(parts, " AND ")
}
func escapeLike(q string) string {
	r := strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_")
	return r.Replace(strings.ToLower(q)) + "%"
}
func (s *Store) Search(ctx context.Context, q core.Query) ([]core.Hit, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	// A single SQL statement pins both branches to one Oracle read-consistent
	// snapshot, including the active-slot pointer during a concurrent publish.
	branches := make([]string, 0, 2)
	for i, slot := range []string{"a", "b"} {
		label := i + 1
		branches = append(branches, fmt.Sprintf(`SELECT d.source,d.model,d.model_id,d.doc_key,d.identifier,d.title,d.tags,d.updated_at,d.source_sync_id,
CASE WHEN LOWER(d.doc_key)=:q OR LOWER(d.model_id)=:q OR LOWER(d.identifier)=:q THEN 1 WHEN LOWER(d.doc_key) LIKE :prefix ESCAPE '\' OR LOWER(d.title) LIKE :prefix ESCAPE '\' THEN 2 ELSE 3 END tier,
CASE WHEN LOWER(d.doc_key) LIKE :prefix ESCAPE '\' AND LOWER(d.title) LIKE :prefix ESCAPE '\' THEN LEAST(LENGTH(d.doc_key),LENGTH(d.title)) WHEN LOWER(d.doc_key) LIKE :prefix ESCAPE '\' THEN LENGTH(d.doc_key) WHEN LOWER(d.title) LIKE :prefix ESCAPE '\' THEN LENGTH(d.title) ELSE 0 END plen,
SCORE(%d) text_score, %d slot FROM rb_docs_%s d JOIN rb_state s ON s.source=d.source AND s.active_slot=%d
WHERE d.source=:source AND (:model IS NULL OR d.model=:model) AND (LOWER(d.doc_key)=:q OR LOWER(d.model_id)=:q OR LOWER(d.identifier)=:q OR LOWER(d.doc_key) LIKE :prefix ESCAPE '\' OR LOWER(d.title) LIKE :prefix ESCAPE '\' OR CONTAINS(d.search_text,:text,%d)>0)`, label, i, slot, i, label))
	}
	query := `WITH candidates AS (` + strings.Join(branches, " UNION ALL ") + `), ranked AS (
 SELECT * FROM candidates ORDER BY tier,CASE WHEN tier=2 THEN plen ELSE 0 END,CASE WHEN tier=3 THEN text_score ELSE 0 END DESC,source,model,model_id FETCH FIRST :lim ROWS ONLY
 ) SELECT r.source,r.model,r.model_id,r.doc_key,r.identifier,r.title,NVL(a.body,b.body),r.tags,r.updated_at,r.source_sync_id,r.tier,r.plen,r.text_score
 FROM ranked r LEFT JOIN rb_docs_a a ON r.slot=0 AND a.source=r.source AND a.model=r.model AND a.model_id=r.model_id
 LEFT JOIN rb_docs_b b ON r.slot=1 AND b.source=r.source AND b.model=r.model AND b.model_id=r.model_id
 ORDER BY r.tier,CASE WHEN r.tier=2 THEN r.plen ELSE 0 END,CASE WHEN r.tier=3 THEN r.text_score ELSE 0 END DESC,r.source,r.model,r.model_id`

	rows, err := s.DB.QueryContext(ctx, query, sql.Named("q", strings.ToLower(q.Q)), sql.Named("prefix", escapeLike(q.Q)), sql.Named("source", s.Source), sql.Named("model", q.Model), sql.Named("text", TextQuery(q.Q)), sql.Named("lim", q.Limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []core.Hit{}
	for rows.Next() {
		var h core.Hit
		var key, identifier, title, tags sql.NullString
		var body goora.Clob
		if err = rows.Scan(&h.Source, &h.Model, &h.ID, &key, &identifier, &title, &body, &tags, &h.UpdatedAt, &h.SyncID, &h.Tier, &h.PrefixLength, &h.Score); err != nil {
			return nil, err
		}
		h.Key = key.String
		h.Identifier = identifier.String
		h.Title = title.String
		h.Body = body.String
		h.Tags = tags.String
		out = append(out, h)
	}
	return out, rows.Err()
}

func binds(values ...any) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = sql.Named(fmt.Sprintf("p%d", i+1), v)
	}
	return out
}
