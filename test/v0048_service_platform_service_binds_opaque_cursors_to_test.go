package test

import (
	"encoding/base64"
	"testing"
)

func cursorPages(t *testing.T) (*harness, string) {
	t.Helper()
	h := newHarness(t)
	h.register(3, 7, "gpu")
	h.register(4, 9, "gpu")
	h.claimTask(3, 7, 1010)
	h.claimTask(3, 7, 1011)
	h.claimTask(4, 9, 1010)
	for _, k := range []string{"running", "finished"} {
		h.exec(3, 7, k, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
	}
	first := h.mustOK("task-events", 3, map[string]any{"task_id": 1010, "limit": 2})
	c, _ := first.Body["next_cursor"].(string)
	if c == "" {
		t.Fatalf("first page has no next_cursor: %s", first.Raw)
	}
	return h, c
}

func mustInvalidPagination(t *testing.T, r uiReply, what string) {
	t.Helper()
	if r.Status != 400 || r.code() != "invalid_pagination" {
		t.Fatalf("%s: %d %s, want 400 invalid_pagination", what, r.Status, r.Raw)
	}
	if _, ok := r.Body["items"]; ok {
		t.Fatalf("%s returned rows", what)
	}
}

// Requirement: service-platform service binds opaque cursors to their read.
func TestV0048ServicePlatformServiceBindsOpaqueCursorsToTheirRead(t *testing.T) {
	t.Run("Forward page advances", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		h.claimTask(3, 7, 1010)
		h.claimTask(4, 9, 1010)
		for _, k := range []string{"running", "finished"} {
			h.exec(3, 7, k, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		}
		var all []int64
		params := map[string]any{"task_id": 1010, "limit": 2}
		for pages := 0; ; pages++ {
			if pages > 5 {
				t.Fatalf("paging does not end")
			}
			r := h.mustOK("task-events", 3, params)
			for _, it := range r.items() {
				if num(it["team_id"]) != 3 || num(it["task_id"]) != 1010 {
					t.Fatalf("page carries a foreign row %v", it)
				}
				if len(all) > 0 && num(it["event_id"]) <= all[len(all)-1] {
					t.Fatalf("pages overlap or go back: %d after %d", num(it["event_id"]), all[len(all)-1])
				}
				all = append(all, num(it["event_id"]))
			}
			next, _ := r.Body["next_cursor"].(string)
			if next == "" {
				break
			}
			params = map[string]any{"task_id": 1010, "limit": 2, "cursor": next}
		}
		if len(all) != 5 {
			t.Fatalf("paged %d events, want queued, created, claim_accepted, running, finished", len(all))
		}
	})

	t.Run("Tampered cursor", func(t *testing.T) {
		h, c := cursorPages(t)
		raw, err := base64.RawURLEncoding.DecodeString(c)
		if err != nil {
			t.Fatalf("cursor is not base64url: %v", err)
		}
		for i := 0; i < len(raw); i += 7 {
			b := append([]byte(nil), raw...)
			b[i] ^= 0x01
			mustInvalidPagination(t, h.ui("task-events", 3, map[string]any{"task_id": 1010, "limit": 2, "cursor": base64.RawURLEncoding.EncodeToString(b)}), "tampered cursor")
		}
		mustInvalidPagination(t, h.ui("task-events", 3, map[string]any{"task_id": 1010, "limit": 2, "cursor": "not-a-cursor"}), "garbage cursor")
	})

	t.Run("Cursor reused for another operation or team", func(t *testing.T) {
		h, c := cursorPages(t)
		mustInvalidPagination(t, h.ui("task-logs", 3, map[string]any{"task_id": 1010, "limit": 2, "cursor": c}), "task-logs")
		mustInvalidPagination(t, h.ui("executor-events", 3, map[string]any{"executor_id": 7, "limit": 2, "cursor": c}), "executor-events")
		mustInvalidPagination(t, h.ui("task-events", 4, map[string]any{"task_id": 1010, "limit": 2, "cursor": c}), "team 4")
		hdr := uiHeaders(3)
		hdr["operator_id"] = []string{"502"}
		mustInvalidPagination(t, h.call("task-events", hdr, map[string]any{"team_id": 3, "task_id": 1010, "limit": 2, "cursor": c}), "another operator")
	})

	t.Run("Changed filter", func(t *testing.T) {
		h, c := cursorPages(t)
		mustInvalidPagination(t, h.ui("task-events", 3, map[string]any{"task_id": 1011, "limit": 2, "cursor": c}), "other task")
		mustInvalidPagination(t, h.ui("task-events", 3, map[string]any{"task_id": 1010, "limit": 3, "cursor": c}), "other limit")
		mustInvalidPagination(t, h.ui("control-events", 3, map[string]any{"task_id": 1010, "control_id": 1, "limit": 2, "cursor": c}), "added control filter")
	})

	t.Run("Limit out of range", func(t *testing.T) {
		h, _ := cursorPages(t)
		for _, l := range []any{0, 201, 1.5, "10", -1} {
			mustInvalidPagination(t, h.ui("task-events", 3, map[string]any{"task_id": 1010, "limit": l}), "limit")
		}
		if r := h.mustOK("task-events", 3, map[string]any{"task_id": 1010, "limit": 200}); len(r.items()) != 5 {
			t.Fatalf("limit 200 returned %d", len(r.items()))
		}
	})
}
