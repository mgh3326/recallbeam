package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"
)

//go:embed *.sql
var Files embed.FS

// Run is an explicit operator command. Oracle DDL commits implicitly: a failed
// migration requires operator inspection before retrying, never an automatic loop.
func Run(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `BEGIN EXECUTE IMMEDIATE 'CREATE TABLE rb_migrations (version NUMBER PRIMARY KEY)'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;`)
	if err != nil {
		return err
	}
	var n int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rb_migrations WHERE version=1").Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	b, err := Files.ReadFile("001_index.sql")
	if err != nil {
		return err
	}
	for i, s := range strings.Split(string(b), "\n/\n") {
		if strings.TrimSpace(s) == "" {
			continue
		}
		if _, err = db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("migration 001 statement %d: %w", i+1, err)
		}
	}
	_, err = db.ExecContext(ctx, "INSERT INTO rb_migrations(version) VALUES (1)")
	return err
}
