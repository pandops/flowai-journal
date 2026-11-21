package test

import (
	"bytes"
	"testing"
)

// Requirement: service-platform service returns non-revealing not-found for
// foreign resources. "Operator writes a foreign environment" is not
// applicable to journal (it has no writes).
func TestV0048ServicePlatformServiceReturnsNonRevealingNotFound(t *testing.T) {
	t.Run("Operator reads a foreign task", func(t *testing.T) {
		h := newHarness(t)
		h.register(4, 9, "gpu")
		h.claimTask(4, 9, 1010)
		h.mustAck(h.logChunk(4, 9, 1010, 1, 1, "work", "team four output"))
		before := len(h.store.Events())
		for _, op := range []string{"task-events", "task-logs"} {
			foreign := h.ui(op, 3, map[string]any{"task_id": 1010})
			unknown := h.ui(op, 3, map[string]any{"task_id": 999})
			if foreign.Status != 404 || foreign.code() != "not_found" || !bytes.Equal(foreign.Raw, unknown.Raw) {
				t.Fatalf("%s foreign %d %s vs unknown %d %s", op, foreign.Status, foreign.Raw, unknown.Status, unknown.Raw)
			}
			validateReply(t, op, foreign)
		}
		foreign := h.ui("executor-events", 3, map[string]any{"executor_id": 9})
		unknown := h.ui("executor-events", 3, map[string]any{"executor_id": 99})
		if foreign.Status != 404 || !bytes.Equal(foreign.Raw, unknown.Raw) {
			t.Fatalf("foreign Executor %d %s vs unknown %s", foreign.Status, foreign.Raw, unknown.Raw)
		}
		if len(h.store.Events()) != before {
			t.Fatalf("a read changed journal's data")
		}
	})
}
