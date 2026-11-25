package test

import (
	"bytes"
	"testing"
)

// Requirement: service-platform service trusts team context only from ui
// requests.
func TestV0048ServicePlatformServiceTrustsTeamContextOnlyFromUIRequests(t *testing.T) {
	t.Run("Gateway forwards verified operator context", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		with := h.mustOK("task-events", 3, map[string]any{"task_id": 1010})
		hdr := uiHeaders(3)
		delete(hdr, "team_name")
		without := h.call("task-events", hdr, map[string]any{"team_id": 3, "task_id": 1010})
		other := uiHeaders(3)
		other["team_name"] = []string{"Team Four"}
		renamed := h.call("task-events", other, map[string]any{"team_id": 3, "task_id": 1010})
		if without.Status != 200 || renamed.Status != 200 || !bytes.Equal(with.Raw, without.Raw) || !bytes.Equal(with.Raw, renamed.Raw) {
			t.Fatalf("team_name changed the reply: %d %d", without.Status, renamed.Status)
		}
	})

	t.Run("Payload names another team", func(t *testing.T) {
		h := newHarness(t)
		h.register(4, 9, "gpu")
		h.claimTask(4, 9, 1010)
		r := h.call("task-events", uiHeaders(3), map[string]any{"team_id": 4, "task_id": 1010})
		unknown := h.ui("task-events", 3, map[string]any{"task_id": 999})
		if r.Status != 404 || r.code() != "not_found" || !bytes.Equal(r.Raw, unknown.Raw) {
			t.Fatalf("header team 3 with payload team 4: %d %s, want the unknown-task 404", r.Status, r.Raw)
		}
		validateReply(t, "task-events", r)
		if s := h.call("executor-states", uiHeaders(3), map[string]any{"team_id": 4}); s.Status != 404 || bytes.Contains(s.Raw, []byte(`"executor_id"`)) {
			t.Fatalf("executor-states for payload team 4 under header 3: %d %s", s.Status, s.Raw)
		}
	})

	t.Run("Context is missing", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		for _, missing := range []string{"team", "operator_id", "request_id"} {
			hdr := uiHeaders(3)
			delete(hdr, missing)
			r := h.call("task-events", hdr, map[string]any{"team_id": 3, "task_id": 1010})
			if r.Status != 400 || r.code() != "invalid_request" || bytes.Contains(r.Raw, []byte(`"items"`)) {
				t.Fatalf("without %s: %d %s, want 400 invalid_request", missing, r.Status, r.Raw)
			}
			validateReply(t, "task-events", r)
			// D12 owner decision 2026-11-09: no valid request_id, no
			// request_id field; every other error keeps it.
			rid, has := r.Body["request_id"]
			if missing == "request_id" && has {
				t.Fatalf("without request_id: %s, want no request_id field", r.Raw)
			}
			if missing != "request_id" && rid != float64(9001) {
				t.Fatalf("without %s: %s, want request_id 9001", missing, r.Raw)
			}
		}
		for _, bad := range []string{"0", "-1", "09", "r-1", "9223372036854775808"} {
			hdr := uiHeaders(3)
			hdr["request_id"] = []string{bad}
			r := h.call("task-events", hdr, map[string]any{"team_id": 3, "task_id": 1010})
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Fatalf("request_id %q: %d %s, want 400 invalid_request", bad, r.Status, r.Raw)
			}
			validateReply(t, "task-events", r)
			if _, has := r.Body["request_id"]; has {
				t.Fatalf("request_id %q: %s, want no request_id field", bad, r.Raw)
			}
		}
		hdr := uiHeaders(3)
		hdr["team"] = []string{"3x"}
		if r := h.call("task-events", hdr, map[string]any{"team_id": 3, "task_id": 1010}); r.Status != 400 {
			t.Fatalf("non-integer team header: %d, want 400", r.Status)
		}
	})
}
