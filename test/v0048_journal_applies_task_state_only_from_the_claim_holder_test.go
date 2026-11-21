package test

import "testing"

// Requirement: journal records task lifecycle events with attempts and
// claims, treating the executor_id that task-manager's claim_accepted names
// as the claim's holder (specs/journal/spec.md), and counts each task under
// its current state in the dashboard calendar. An Executor's lifecycle event
// moves the task state only when it comes from the holder of an open claim;
// an Executor that holds no claim, or a claim_seq that no claim_accepted
// named, changes nothing and cannot freeze the holder's later events.
func TestV0048JournalAppliesTaskStateOnlyFromTheClaimHolder(t *testing.T) {
	counts := func(h *harness) map[string]int64 {
		h.t.Helper()
		d := h.mustOK("dashboard", 3, map[string]any{"period": "week", "time_zone": "UTC"})
		raw, _ := d.Body["counts"].(map[string]any)
		out := map[string]int64{}
		for k, v := range raw {
			if n := num(v); n != 0 {
				out[k] = n
			}
		}
		return out
	}
	only := func(t *testing.T, h *harness, state string) {
		t.Helper()
		got := counts(h)
		if len(got) != 1 || got[state] != 1 {
			t.Fatalf("dashboard counts %v, want the one task under %q", got, state)
		}
	}
	// held gives team 3 task 1010 whose claim (1, 1) Executor 7 holds, and
	// registers Executor 8 of the same team.
	held := func(t *testing.T) *harness {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.claimTask(3, 7, 1010)
		return h
	}

	for _, kind := range []string{"finished", "failed", "cancelled", "running"} {
		t.Run("Non-holder "+kind+" with an unclaimed claim_seq changes nothing", func(t *testing.T) {
			h := held(t)
			h.exec(3, 8, kind, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 999, "failure_reason": "runtime_error", "retryable": false})
			only(t, h, "created")
			h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
			only(t, h, "running")
			h.exec(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
			only(t, h, "finished")
		})
	}

	t.Run("Non-holder event on the held claim changes nothing", func(t *testing.T) {
		h := held(t)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.exec(3, 8, "failed", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "runtime_error", "retryable": false})
		only(t, h, "running")
	})

	t.Run("Event of a claim without claim_accepted changes nothing", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.queued(3, 1010, 1)
		h.exec(3, 8, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "queued")
		h.created(3, 7, 1010, 1, 1)
		h.claimAccepted(3, 7, 1010, 1, 1)
		only(t, h, "created")
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "running")
	})

	// task-manager publishes claim_accepted from its outbox after commit,
	// while the holder publishes its own events as soon as it has the claim
	// token, so the holder's events can reach journal first. They apply,
	// in order, once claim_accepted names their sender as the holder.
	early := func(t *testing.T) *harness {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		return h
	}
	for _, kind := range []string{"running", "failed", "finished", "pending"} {
		t.Run("Holder's "+kind+" before claim_accepted applies once the claim is accepted", func(t *testing.T) {
			h := early(t)
			h.exec(3, 7, kind, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "runtime_error", "retryable": false, "reason": "all_selected_models_paused"})
			only(t, h, "queued")
			h.claimAccepted(3, 7, 1010, 1, 1)
			only(t, h, kind)
		})
	}

	t.Run("Holder's events before claim_accepted apply in order and end the claim", func(t *testing.T) {
		h := early(t)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.exec(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.claimAccepted(3, 7, 1010, 1, 1)
		only(t, h, "finished")
		// The replayed finished ended the claim: a later event of it moves nothing.
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "finished")
	})

	t.Run("Holder's released before claim_accepted returns the task to queued", func(t *testing.T) {
		h := early(t)
		h.exec(3, 7, "released", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "reason": "capacity"})
		h.claimAccepted(3, 7, 1010, 1, 1)
		only(t, h, "queued")
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "queued")
	})

	for _, kind := range []string{"running", "failed", "finished"} {
		t.Run("Non-holder "+kind+" before claim_accepted stays ignored after it", func(t *testing.T) {
			h := early(t)
			h.exec(3, 8, kind, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "runtime_error", "retryable": false})
			h.exec(3, 8, kind, map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 999, "failure_reason": "runtime_error", "retryable": false})
			h.claimAccepted(3, 7, 1010, 1, 1)
			only(t, h, "created")
			h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
			only(t, h, "running")
		})
	}

	t.Run("Holder's early event of another claim_seq does not apply to the accepted claim", func(t *testing.T) {
		h := early(t)
		h.exec(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 999})
		h.claimAccepted(3, 7, 1010, 1, 1)
		only(t, h, "created")
	})

	t.Run("Holder's event after its claim ended changes nothing", func(t *testing.T) {
		h := held(t)
		h.svc("task-manager", "released", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "claim_seq": 1, "reason": "claim_missing"})
		only(t, h, "queued")
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "queued")
		h.created(3, 8, 1010, 1, 2)
		h.claimAccepted(3, 8, 1010, 1, 2)
		h.exec(3, 8, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 2})
		only(t, h, "running")
	})

	t.Run("Holder moves its claim through its states", func(t *testing.T) {
		h := held(t)
		only(t, h, "created")
		h.exec(3, 7, "pending", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "reason": "all_selected_models_paused"})
		only(t, h, "pending")
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		only(t, h, "running")
		h.exec(3, 7, "failed", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "runtime_error", "retryable": false})
		only(t, h, "failed")
	})
}
