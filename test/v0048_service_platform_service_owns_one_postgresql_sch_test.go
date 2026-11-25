package test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// Requirement: service-platform service owns one PostgreSQL schema and role
// (unit part; the database behavior is in v0048_schema_integration_test.go).
func TestV0048ServicePlatformServiceOwnsOnePostgreSQLSchema(t *testing.T) {
	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		// The migrations never name a schema: tables land only in the
		// configured one (search_path), never in another service's schema.
		files, _ := filepath.Glob(filepath.Join(journalDir(t), "internal", "pgstore", "migrations", "*.sql"))
		if len(files) == 0 {
			t.Fatalf("journal has no migrations")
		}
		qualified := regexp.MustCompile(`(?i)\b[a-z_]+_schema\.|\bpublic\.|SET\s+search_path`)
		for _, f := range files {
			raw, _ := os.ReadFile(f)
			if qualified.Match(raw) {
				t.Fatalf("%s names a schema explicitly", filepath.Base(f))
			}
		}
	})
}
