package test

import (
	"testing"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Requirement: message-broker carries the team in the team header
// (journal's consumer and responder side).
func TestV0048MessageBrokerCarriesTheTeamInTheTeamHeader(t *testing.T) {
	t.Run("Executor header names another team", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		h.queued(3, 1010, 1)
		m := h.execMsg(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		m.Header["team"] = []string{"4"}
		h.mustAck(m)
		last := h.store.Events()[len(h.store.Events())-1]
		if last.TeamID != 3 {
			t.Fatalf("Executor 7 event stored under team %d, want 3", last.TeamID)
		}
		for _, e := range h.store.Events() {
			if e.TeamID == 4 && e.Kind == "running" {
				t.Fatalf("stored a running event for team 4")
			}
		}
	})

	t.Run("Administrator action carries team admin", func(t *testing.T) {
		h := newHarness(t)
		start := len(h.store.Events())
		h.mustAck(h.svcMsg("team-registry", "team_updated", "admin", map[string]any{"team_id": 3, "display_name": "Ops"}))
		stored := h.store.Events()[start:]
		if len(stored) != 1 || stored[0].TeamID != 3 {
			t.Fatalf("admin event stored as %+v, want the payload team 3", stored)
		}
	})

	t.Run("ui request claims the admin team", func(t *testing.T) {
		h := newHarness(t)
		hdr := uiHeaders(3)
		hdr["team"] = []string{"admin"}
		r := h.call("executor-states", hdr, map[string]any{"team_id": 3})
		if r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("ui request with team admin: %d %s, want 400 invalid_request", r.Status, r.Raw)
		}
		validateReply(t, "executor-states", r)
	})

	t.Run("Unknown Executor is not yet projected", func(t *testing.T) {
		h := newHarness(t)
		h.queued(3, 1010, 1)
		created := h.execMsg(3, 9, "created", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		if d := h.redeliver(created, 100); d != engine.Nak {
			t.Fatalf("event of an unprojected Executor: %v, want nak (redelivered)", d)
		}
		// A later message of the same task waits for the earlier one.
		running := h.execMsg(3, 9, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		if d := h.redeliver(running, 102); d != engine.Nak {
			t.Fatalf("later event of the held task: %v, want nak", d)
		}
		h.register(3, 9, "gpu")
		if d := h.redeliver(running, 102); d != engine.Nak {
			t.Fatalf("running committed before the earlier created: %v", d)
		}
		if d := h.redeliver(created, 100); d != engine.Ack {
			t.Fatalf("redelivery after executor_registered: %v, want ack", d)
		}
		if d := h.redeliver(running, 102); d != engine.Ack {
			t.Fatalf("held later event after the earlier commit: %v, want ack", d)
		}
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		if len(got) != 3 || got[1] != "created" || got[2] != "running" {
			t.Fatalf("task events %v, want queued, created, running in stream order", got)
		}
	})
}
