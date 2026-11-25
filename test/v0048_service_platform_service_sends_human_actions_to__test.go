package test

import (
	"os"
	"path/filepath"
	"testing"
)

// Requirement: service-platform service sends human actions to audit and
// others to journal. "Operator cancels a task", "Forwarded administrator
// action is audited by its owner" and "Rejected human action is audited" are
// not applicable to journal (it takes no human action: its operations are
// reads, which are not audited).
func TestV0048ServicePlatformServiceSendsHumanActionsToAuditAndOthersToJournal(t *testing.T) {
	t.Run("Executor event goes to journal", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		if got[len(got)-1] != "running" {
			t.Fatalf("running not stored: %v", got)
		}
		// journal declares no audit schema: it publishes nothing to audit.
		if m, _ := filepath.Glob(filepath.Join(journalDir(t), "schemas", "audit.*")); len(m) != 0 {
			t.Fatalf("journal declares audit messages %v", m)
		}
		if _, err := os.Stat(filepath.Join(journalDir(t), "schemas", "consumers.json")); err != nil {
			t.Fatalf("consumers.json: %v", err)
		}
	})
}
