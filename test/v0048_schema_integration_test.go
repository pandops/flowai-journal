//go:build integration

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pandops/flowai-journal/internal/pgstore"
)

// Coverage row: each service writes only its own <name>_schema with its own
// role; another schema is refused. Also the integration part of
// service-platform "Configured schema and host are used" and "Access to a
// foreign schema is denied".
func TestV0048JournalSchemaIntegration(t *testing.T) {
	t.Run("Migrations create tables only in the configured schema as the configured role", func(t *testing.T) {
		admin, name := freshDB(t)
		c := serviceRole(t, admin, name, "journal_x", "journal_x_schema")
		db := openAs(t, c)
		var user, port string
		if err := db.QueryRow(`SELECT current_user, current_setting('port')`).Scan(&user, &port); err != nil {
			t.Fatalf("connect with journal's DSN: %v", err)
		}
		if user != "journal_x" || port != "5432" {
			t.Fatalf("connected as %s on server port %s", user, port)
		}
		// The role has no CREATE on the database: the migration must not
		// try CREATE SCHEMA when the schema exists (it would fail).
		if err := pgstore.Migrate(context.Background(), db, c.Schema); err != nil {
			t.Fatalf("migrate into the pre-created schema: %v", err)
		}
		if err := pgstore.Migrate(context.Background(), db, c.Schema); err != nil {
			t.Fatalf("second migrate: %v", err)
		}
		tables := tablesIn(t, admin)
		if len(tables["journal_x_schema"]) == 0 {
			t.Fatalf("no tables in journal_x_schema: %v", tables)
		}
		for s := range tables {
			if s != "journal_x_schema" {
				t.Fatalf("migration created tables in %s: %v", s, tables[s])
			}
		}
	})

	t.Run("Missing schema is created when the role may", func(t *testing.T) {
		admin, name := freshDB(t)
		c := serviceRole(t, admin, name, "journal_y", "journal_y_other")
		if _, err := admin.Exec(fmt.Sprintf(`GRANT CREATE ON DATABASE "%s" TO journal_y`, name)); err != nil {
			t.Fatal(err)
		}
		c.Schema = "journal_y_schema"
		db := openAs(t, c)
		if err := pgstore.Migrate(context.Background(), db, c.Schema); err != nil {
			t.Fatalf("migrate into a missing schema: %v", err)
		}
		if len(tablesIn(t, admin)["journal_y_schema"]) == 0 {
			t.Fatalf("schema not created")
		}
		_, _ = admin.Exec(`DROP SCHEMA IF EXISTS journal_y_schema CASCADE`)
	})

	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		admin, name := freshDB(t)
		c := serviceRole(t, admin, name, "journal_z", "journal_z_schema")
		for _, stmt := range []string{
			`CREATE ROLE task_manager_z`,
			`CREATE SCHEMA task_manager_schema AUTHORIZATION task_manager_z`,
			`CREATE TABLE task_manager_schema.tasks (id bigint)`,
			`INSERT INTO task_manager_schema.tasks VALUES (1010)`,
		} {
			if _, err := admin.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			_, _ = admin.Exec(`DROP SCHEMA IF EXISTS task_manager_schema CASCADE`)
			_, _ = admin.Exec(`DROP ROLE IF EXISTS task_manager_z`)
		})
		db := openAs(t, c)
		if err := pgstore.Migrate(context.Background(), db, c.Schema); err != nil {
			t.Fatal(err)
		}
		var id int64
		if err := db.QueryRow(`SELECT id FROM task_manager_schema.tasks`).Scan(&id); !isPermissionDenied(err) {
			t.Fatalf("read of task_manager_schema: %v, want permission denied", err)
		}
		if _, err := db.Exec(`INSERT INTO task_manager_schema.tasks VALUES (2)`); !isPermissionDenied(err) {
			t.Fatalf("write to task_manager_schema: %v, want permission denied", err)
		}
		if _, err := db.Exec(`CREATE TABLE public.leak (id bigint)`); !isPermissionDenied(err) {
			t.Fatalf("create in public: %v, want permission denied", err)
		}
		var n int
		_ = admin.QueryRow(`SELECT count(*) FROM task_manager_schema.tasks`).Scan(&n)
		if n != 1 {
			t.Fatalf("foreign table changed: %d rows", n)
		}
	})
}
