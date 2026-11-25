package test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Requirement: service-platform service runs from its own directory and image.
// "E2E uses the deployed services" and "Behavior with a stopped service is a
// unit or integration test" are not applicable to journal (E2E and API
// Gateway rows).
func TestV0048ServicePlatformServiceRunsFromItsOwnDirectoryAndImage(t *testing.T) {
	t.Run("Change to one service selects only its tests", func(t *testing.T) {
		// The production build copies only journal's module and build sources.
		assertOwnImports(t)
		raw, err := os.ReadFile(filepath.Join(journalDir(t), "Containerfile"))
		if err != nil {
			t.Fatalf("Containerfile: %v", err)
		}
		copied := 0
		for _, m := range regexp.MustCompile(`(?m)^\s*COPY\s+(.+)$`).FindAllStringSubmatch(string(raw), -1) {
			if strings.Contains(m[1], "--from=") {
				continue
			}
			var tokens []string
			for _, tok := range strings.Fields(m[1]) {
				if !strings.HasPrefix(tok, "--") {
					tokens = append(tokens, tok)
				}
			}
			for _, src := range tokens[:len(tokens)-1] {
				copied++
				switch strings.TrimSuffix(strings.TrimPrefix(src, "./"), "/") {
				case "go.mod", "go.sum", "cmd", "internal", "schemas":
				default:
					t.Fatalf("Containerfile copies %s, outside journal's build inputs", src)
				}
			}
		}
		if copied == 0 {
			t.Fatalf("Containerfile copies no source")
		}
	})
}
