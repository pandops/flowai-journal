package test

import (
	"encoding/json"
	"testing"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Requirement: journal consumes ALL_EVENT durably and stores every event once.
func TestV0048JournalConsumesEventsDurablyAndStoresEveryEventOnce(t *testing.T) {
	t.Run("Event is redelivered", func(t *testing.T) {
		h := newHarness(t)
		m := h.svcMsg("task-manager", "queued", "3", map[string]any{"publisher_seq": 512, "task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "intake_id": 1})
		if d := h.redeliver(m, 40); d != engine.Ack {
			t.Fatalf("first delivery: %v", d)
		}
		events, frames := len(h.store.Events()), len(h.store.AllFrames())
		firstID := h.store.Events()[events-1].EventID
		// journal stopped before acknowledging; JetStream redelivers.
		if d := h.redeliver(m, 40); d != engine.Ack {
			t.Fatalf("redelivery must be acknowledged, got %v", d)
		}
		if got := len(h.store.Events()); got != events {
			t.Fatalf("redelivery stored a second row: %d events, want %d", got, events)
		}
		if got := len(h.store.AllFrames()); got != frames {
			t.Fatalf("redelivery emitted a second stream frame")
		}
		if h.store.Events()[events-1].EventID != firstID {
			t.Fatalf("redelivery assigned a second event_id")
		}
	})

	t.Run("journal assigns event IDs at commit", func(t *testing.T) {
		h := newHarness(t)
		h.register(4, 9, "cpu")
		start := len(h.store.Events())
		h.svc("task-manager", "queued", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "intake_id": 1, "event_id": 999999})
		h.exec(4, 9, "created", map[string]any{"task_id": 77, "attempt": 1, "claim_seq": 1, "event_id": 5})
		h.svc("task-manager", "queued", 4, map[string]any{"task_id": 78, "task_name": taskName(4, 78), "attempt": 1, "intake_id": 2})
		stored := h.store.Events()[start:]
		if len(stored) != 3 {
			t.Fatalf("stored %d, want 3", len(stored))
		}
		for i, e := range stored {
			if e.EventID <= 0 {
				t.Fatalf("event_id %d is not positive", e.EventID)
			}
			if i > 0 && e.EventID <= stored[i-1].EventID {
				t.Fatalf("event_ids not increasing in commit order: %d then %d", stored[i-1].EventID, e.EventID)
			}
			if e.EventID == 999999 || e.EventID == 5 {
				t.Fatalf("a publisher payload value became the event_id")
			}
		}
	})

	t.Run("Intake and poll outcomes are stored", func(t *testing.T) {
		h := newHarness(t)
		start := len(h.store.Events())
		h.svc("source-controller", "poll_finalized", 3, map[string]any{"source_id": 4, "poll_id": 12, "outcome": "succeeded", "tasks": 2})
		h.svc("task-manager", "intake_processed", 3, map[string]any{"source_id": 5, "intake_id": 31, "outcome": "invalid_source_task"})
		stored := h.store.Events()[start:]
		if len(stored) != 2 {
			t.Fatalf("stored %d, want 2", len(stored))
		}
		if stored[0].Kind != "poll_finalized" || stored[0].Source != "source-controller" || stored[0].TeamID != 3 {
			t.Fatalf("poll outcome stored as %+v", stored[0])
		}
		if stored[1].Kind != "intake_processed" || stored[1].Source != "task-manager" || stored[1].TeamID != 3 {
			t.Fatalf("intake outcome stored as %+v", stored[1])
		}
		var payload map[string]any
		if err := json.Unmarshal(stored[1].Data, &payload); err != nil || payload["outcome"] != "invalid_source_task" {
			t.Fatalf("intake payload not kept: %s", stored[1].Data)
		}
	})

	t.Run("Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.queued(3, 1010, 1)
		m := h.execMsg(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "team_id": 4})
		m.Header["team"] = []string{"4"}
		h.mustAck(m)
		last := h.store.Events()[len(h.store.Events())-1]
		if last.Kind != "running" || last.TeamID != 3 {
			t.Fatalf("event stored under team %d, want 3 (derived from Executor 7)", last.TeamID)
		}
		if r := h.ui("task-events", 4, map[string]any{"task_id": 1010}); r.Status != 404 {
			t.Fatalf("team 4 read of task 1010: status %d, want 404", r.Status)
		}
		for _, f := range h.store.AllFrames() {
			if f.TeamID == 4 {
				t.Fatalf("team 4 stream got a frame: %+v", f)
			}
		}
	})

	t.Run("Contract test reads the publisher's schema", func(t *testing.T) {
		h := newHarness(t)
		m := contract(t, "svc/task-manager/schemas", "queued",
			h.svcMsg("task-manager", "queued", "3", queuedFields(3, 1010, 1)))
		h.mustAck(m)
		last := h.store.Events()[len(h.store.Events())-1]
		if last.Kind != "queued" || last.TeamID != 3 || last.TaskID == nil || *last.TaskID != 1010 {
			t.Fatalf("schema-valid queued not stored: %+v", last)
		}
	})
}
