//go:build integration

package test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/config"
	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/pgstore"
)

// serviceRole prepares, as the deployment does, a LOGIN role without CREATE
// on the database and a schema it owns; it returns journal's DB settings
// for that role.
func serviceRole(t *testing.T, admin *sql.DB, dbName, role, schema string) config.DB {
	t.Helper()
	password, _ := randomToken(12)
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, password),
		fmt.Sprintf(`REVOKE ALL ON DATABASE "%s" FROM PUBLIC`, dbName),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE "%s" TO %s`, dbName, role),
		`REVOKE ALL ON SCHEMA public FROM PUBLIC`,
		fmt.Sprintf(`CREATE SCHEMA %s AUTHORIZATION %s`, schema, role),
	} {
		if _, err := admin.Exec(stmt); err != nil {
			t.Fatalf("prepare role: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema))
		_, _ = admin.Exec(fmt.Sprintf(`DROP OWNED BY %s`, role))
		_, _ = admin.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	return config.DB{Host: shared.host, Port: shared.port, User: role, Password: password, Name: dbName, Schema: schema, SSLMode: "disable"}
}

// openAs opens a connection with journal's own DSN builder.
func openAs(t *testing.T, c config.DB) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", config.Config{DB: c}.PostgresDSN())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// pgHarness is the unit harness with the PostgreSQL store.
func pgHarness(t *testing.T, role string) (*harness, *sql.DB, config.DB) {
	t.Helper()
	admin, name := freshDB(t)
	c := serviceRole(t, admin, name, role, role+"_schema")
	db := openAs(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pgstore.Migrate(ctx, db, c.Schema); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store, err := pgstore.New(db, c.Schema)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	h := newHarness(t)
	h.eng = engine.New(store, engine.Options{Now: h.clock.Now, OnCommit: h.hub.Notify})
	h.rpc.Engine = h.eng
	return h, admin, c
}

func tablesIn(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	rows, err := db.Query(`SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var s, n string
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatal(err)
		}
		out[s] = append(out[s], n)
	}
	return out
}

func isPermissionDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), "permission denied")
}
