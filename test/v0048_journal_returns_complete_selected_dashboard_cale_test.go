package test

import (
	"testing"
	"time"
)

var stateNames = []string{"queued", "pending", "created", "running", "finished", "failed", "cancelled"}

func buckets(r uiReply) []map[string]any {
	raw, _ := r.Body["buckets"].([]any)
	out := []map[string]any{}
	for _, b := range raw {
		m, _ := b.(map[string]any)
		out = append(out, m)
	}
	return out
}

func countsOf(v any) map[string]int64 {
	m, _ := v.(map[string]any)
	out := map[string]int64{}
	for _, s := range stateNames {
		out[s] = num(m[s])
	}
	return out
}

// queuedAt publishes the first queued of a task at a given instant.
func (h *harness) queuedAt(team, task int64, at time.Time) {
	h.t.Helper()
	saved := h.clock.Now()
	h.clock.Set(at)
	h.queued(team, task, 1)
	h.clock.Set(saved)
}

// Requirement: journal returns complete selected dashboard calendars.
func TestV0048JournalReturnsCompleteSelectedDashboardCalendars(t *testing.T) {
	t.Run("Selected week contains sparse activity", func(t *testing.T) {
		h := newHarness(t) // now: Thursday 2026-10-15, ISO week 2026-W42
		h.queuedAt(3, 1010, time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC))
		h.queuedAt(3, 1011, time.Date(2026, 10, 13, 11, 0, 0, 0, time.UTC))
		r := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42"})
		b := buckets(r)
		keys := []string{"2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17", "2026-10-18"}
		if len(b) != 7 {
			t.Fatalf("buckets = %d, want 7", len(b))
		}
		zero := 0
		for i, bucket := range b {
			if bucket["key"] != keys[i] {
				t.Fatalf("bucket %d key %v, want %s (Monday to Sunday)", i, bucket["key"], keys[i])
			}
			c := countsOf(bucket["counts"])
			if len(c) != 7 {
				t.Fatalf("bucket counts %v lack a state", bucket["counts"])
			}
			if num(bucket["total"]) == 0 {
				zero++
			}
		}
		if zero != 6 || num(b[1]["total"]) != 2 || countsOf(b[1]["counts"])["queued"] != 2 {
			t.Fatalf("zero buckets %d, Tuesday %v, want six zero and two queued on Tuesday", zero, b[1])
		}
		if b[4]["isFuture"] != true || b[3]["isFuture"] != false {
			t.Fatalf("future flags wrong: Thu %v Fri %v", b[3]["isFuture"], b[4]["isFuture"])
		}
		// Omitted selection resolves to the current week.
		current := h.mustOK("dashboard", 3, map[string]any{"period": "week"})
		if current.Body["calendar_key"] != "2026-W42" {
			t.Fatalf("omitted week resolved to %v, want 2026-W42", current.Body["calendar_key"])
		}
		// Future and malformed selections are rejected.
		for _, p := range []map[string]any{
			{"period": "week", "week": "2026-W50"},
			{"period": "week", "week": "2026-42"},
			{"period": "month", "month": "2026-13"},
			{"period": "month", "month": "2027-01"},
			{"period": "day"},
			{"period": "week", "month": "2026-10"},
		} {
			if rr := h.ui("dashboard", 3, p); rr.Status != 400 {
				t.Fatalf("selection %v: status %d, want 400", p, rr.Status)
			} else {
				validateReply(t, "dashboard", rr)
			}
		}
	})

	t.Run("Selected month has no tasks", func(t *testing.T) {
		h := newHarness(t)
		h.queuedAt(4, 1010, time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)) // another team
		r := h.mustOK("dashboard", 3, map[string]any{"period": "month", "month": "2026-09"})
		b := buckets(r)
		if len(b) != 30 {
			t.Fatalf("September buckets = %d, want 30", len(b))
		}
		for i, bucket := range b {
			want := time.Date(2026, 9, i+1, 0, 0, 0, 0, time.UTC).Format(time.DateOnly)
			if bucket["key"] != want || num(bucket["total"]) != 0 {
				t.Fatalf("bucket %d = %v, want %s with zero total", i, bucket, want)
			}
			for s, n := range countsOf(bucket["counts"]) {
				if n != 0 {
					t.Fatalf("bucket %s count %s = %d", want, s, n)
				}
			}
		}
		for s, n := range countsOf(r.Body["counts"]) {
			if n != 0 {
				t.Fatalf("period count %s = %d, want 0", s, n)
			}
		}
	})

	t.Run("Selection crosses a calendar boundary", func(t *testing.T) {
		h := newHarness(t)
		h.queuedAt(3, 1010, time.Date(2021, 1, 2, 12, 0, 0, 0, time.UTC))
		r := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2020-W53"})
		b := buckets(r)
		if len(b) != 7 || b[0]["key"] != "2020-12-28" || b[6]["key"] != "2021-01-03" {
			t.Fatalf("2020-W53 buckets %v .. %v, want 2020-12-28 .. 2021-01-03", b[0]["key"], b[len(b)-1]["key"])
		}
		if num(b[5]["total"]) != 1 {
			t.Fatalf("task of 2021-01-02 not in the Saturday bucket: %v", b[5])
		}
		feb := buckets(h.mustOK("dashboard", 3, map[string]any{"period": "month", "month": "2024-02"}))
		if len(feb) != 29 || feb[28]["key"] != "2024-02-29" {
			t.Fatalf("February 2024 buckets = %d, want 29 ending 2024-02-29", len(feb))
		}
	})

	t.Run("Retried task is counted once", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		first := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
		h.queuedAt(3, 1010, first)
		h.created(3, 7, 1010, 1, 1)
		h.claimAccepted(3, 7, 1010, 1, 1)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.exec(3, 7, "failed", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "agent_error", "retryable": true})
		h.svc("task-manager", "retry_scheduled", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "attempt": 2})
		r := h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42"})
		total := countsOf(r.Body["counts"])
		if total["queued"] != 1 || total["failed"] != 0 || total["running"] != 0 {
			t.Fatalf("period counts %v, want the task once under queued", total)
		}
		b := buckets(r)
		if num(b[1]["total"]) != 1 {
			t.Fatalf("task not in the bucket of its first queued: %v", b[1])
		}
		// The next attempt's queued keeps the first bucket.
		h.queued(3, 1010, 2)
		r = h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42"})
		if c := countsOf(r.Body["counts"]); c["queued"] != 1 {
			t.Fatalf("after attempt 2 queued: %v", c)
		}
	})
}
