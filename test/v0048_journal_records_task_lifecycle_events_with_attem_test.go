package test

import (
	"context"
	"testing"
)

// Requirement: journal records task lifecycle events with attempts and claims.
func TestV0048JournalRecordsTaskLifecycleEventsWithAttemptsAndClaims(t *testing.T) {
	t.Run("Task is released and claimed again", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.exec(3, 7, "pending", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "reason": "all_selected_models_paused"})
		h.exec(3, 7, "released", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "reason": "pending_timeout"})
		h.created(3, 7, 1010, 1, 2)
		h.claimAccepted(3, 7, 1010, 1, 2)
		items := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()
		want := []string{"queued", "created", "claim_accepted", "pending", "released", "created", "claim_accepted"}
		got := kinds(items)
		if len(got) != len(want) {
			t.Fatalf("kinds %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("kinds %v, want %v", got, want)
			}
		}
		if num(items[1]["claim_seq"]) != 1 || num(items[5]["claim_seq"]) != 2 {
			t.Fatalf("claim_seq of the two created = %v, %v, want 1, 2", items[1]["claim_seq"], items[5]["claim_seq"])
		}
		if num(items[4]["attempt"]) != 1 || items[4]["reason"] != "pending_timeout" {
			t.Fatalf("released stored without attempt or reason: %v", items[4])
		}
		if items[3]["reason"] != "all_selected_models_paused" {
			t.Fatalf("pending reason missing: %v", items[3])
		}
	})

	t.Run("Two Executors publish created for one claim", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		h.created(3, 8, 1010, 1, 1)
		if _, ok, err := h.eng.ClaimHolder(context.Background(), 3, 1010, 1, 1); err != nil || ok {
			t.Fatalf("holder before claim_accepted: ok=%v err=%v, want none", ok, err)
		}
		h.claimAccepted(3, 7, 1010, 1, 1)
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		if len(got) != 4 || got[1] != "created" || got[2] != "created" || got[3] != "claim_accepted" {
			t.Fatalf("kinds %v, want both created and claim_accepted", got)
		}
		holder, ok, err := h.eng.ClaimHolder(context.Background(), 3, 1010, 1, 1)
		if err != nil || !ok || holder != 7 {
			t.Fatalf("holder = %d ok=%v err=%v, want 7", holder, ok, err)
		}
	})

	t.Run("Holder comes only from claim_accepted", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.queued(3, 1010, 1)
		// Executor 8's created is first in stream order with the earlier time.
		h.created(3, 8, 1010, 1, 1)
		h.clock.Advance(1)
		h.created(3, 7, 1010, 1, 1)
		if _, ok, _ := h.eng.ClaimHolder(context.Background(), 3, 1010, 1, 1); ok {
			t.Fatalf("journal derived a holder before claim_accepted")
		}
		h.claimAccepted(3, 7, 1010, 1, 1)
		holder, ok, _ := h.eng.ClaimHolder(context.Background(), 3, 1010, 1, 1)
		if !ok || holder != 7 {
			t.Fatalf("holder = %d, want 7 from claim_accepted", holder)
		}
	})

	t.Run("Claim released as missing", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.svc("task-manager", "released", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "claim_seq": 1, "reason": "claim_missing"})
		h.created(3, 7, 1010, 1, 2)
		items := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()
		var released, next map[string]any
		for _, it := range items {
			if it["kind"] == "released" {
				released = it
			}
			if it["kind"] == "created" && num(it["claim_seq"]) == 2 {
				next = it
			}
		}
		if released == nil || released["reason"] != "claim_missing" || released["publisher"] != "task-manager" {
			t.Fatalf("released(claim_missing) not stored: %v", released)
		}
		if next == nil {
			t.Fatalf("next claim not shown with claim_seq 2: %v", kinds(items))
		}
	})

	t.Run("Archived task keeps its history", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.exec(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.svc("task-manager", "archived", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010)})
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		want := []string{"queued", "created", "claim_accepted", "running", "finished", "archived"}
		if len(got) != len(want) {
			t.Fatalf("history after archive %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("history after archive %v, want %v", got, want)
			}
		}
	})
}
