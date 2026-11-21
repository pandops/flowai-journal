package test

import (
	"bytes"
	"testing"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Requirement: service-platform services use integer identifiers.
// "First record in each team is 1" and "Concurrent creates get unique
// increasing numbers" are not applicable to journal (it allocates no
// per-team record numbers); event_id is one global sequence.
func TestV0048ServicePlatformServicesUseIntegerIdentifiers(t *testing.T) {
	t.Run("Invalid identifier is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		bad := []string{`"abc"`, `0`, `-1`, `1.5`, `9223372036854775808`, `"3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f"`, `"1010"`, `null`}
		for _, v := range bad {
			for _, body := range []string{
				`{"params":{"team_id":3,"task_id":` + v + `}}`,
				`{"params":{"team_id":` + v + `,"task_id":1010}}`,
			} {
				r := h.callRaw("task-events", uiHeaders(3), []byte(body))
				if r.Status != 400 || r.code() != "invalid_request" {
					t.Fatalf("%s: %d %s, want 400 invalid_request", body, r.Status, r.Raw)
				}
				validateReply(t, "task-events", r)
			}
			r := h.callRaw("executor-events", uiHeaders(3), []byte(`{"params":{"team_id":3,"executor_id":`+v+`}}`))
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Fatalf("executor_id %s: %d, want 400 invalid_request", v, r.Status)
			}
		}
	})

	t.Run("Equal numbers in two teams grant nothing", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		h.claimTask(3, 7, 1010)
		h.claimTask(4, 9, 1010)
		h.exec(4, 9, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		for _, it := range h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items() {
			if num(it["team_id"]) != 3 || it["kind"] == "running" {
				t.Fatalf("team 3 read of task 1010 returned %v", it)
			}
		}
		if n := len(h.mustOK("task-events", 4, map[string]any{"task_id": 1010}).items()); n != 4 {
			t.Fatalf("team 4 task 1010 has %d events, want 4", n)
		}
	})

	t.Run("Rolled-back transaction neither publishes nor reuses its sequence", func(t *testing.T) {
		h := newHarness(t)
		h.queued(3, 1010, 1)
		first := h.store.Events()[len(h.store.Events())-1].EventID
		m := h.svcMsg("task-manager", "archived", "3", map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "flow_id": 1, "source_id": 1, "final_state": "finished", "final_reason": "done", "attempts": 1})
		h.store.FailCommit = true
		if d := h.redeliver(m, 50); d != engine.Nak {
			t.Fatalf("failed commit: %v, want nak", d)
		}
		h.store.FailCommit = false
		if d := h.redeliver(m, 50); d != engine.Ack {
			t.Fatalf("redelivery: %v, want ack", d)
		}
		if h.store.IDsBurned < 1 {
			t.Fatalf("the rolled-back write consumed no event_id")
		}
		last := h.store.Events()[len(h.store.Events())-1]
		if last.EventID <= first+h.store.IDsBurned {
			t.Fatalf("event_id %d reuses a value of the rolled-back write (first %d, burned %d)", last.EventID, first, h.store.IDsBurned)
		}
	})

	t.Run("Redelivered message is deduplicated by publisher sequence", func(t *testing.T) {
		h := newHarness(t)
		m := h.svcMsg("team-registry", "team_updated", "3", map[string]any{"publisher_seq": 120, "display_name": "Ops"})
		h.mustAck(m)
		n := len(h.store.Events())
		frames := len(h.store.AllFrames())
		h.mustAck(m)
		if len(h.store.Events()) != n || len(h.store.AllFrames()) != frames {
			t.Fatalf("(team-registry, 120) applied twice")
		}
		if !bytes.Contains(h.store.Events()[n-1].Data, []byte("Ops")) {
			t.Fatalf("team_updated payload not kept")
		}
	})
}
