package test

import "testing"

// Requirement: service-platform service stays independent from execution
// and identity. "Image equality across teams grants no authority" and
// "Flow cannot be created from a Source path" are not applicable to
// journal (it stores no images, Flows or models).
func TestV0048ServicePlatformServiceStaysIndependentFromExecutionAndIdentity(t *testing.T) {
	t.Run("Executor capacity is not decided centrally", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.svc("executor-registry", "executor_status", 3, map[string]any{"executor_id": 7, "connection_state": "connected", "max_parallel_tasks": 1, "held_claims": 1, "available_capacity": 0})
		// Events of further claims are stored and acknowledged anyway.
		for task := int64(1010); task < 1013; task++ {
			h.claimTask(3, 7, task)
			h.exec(3, 7, "running", map[string]any{"task_id": task, "attempt": 1, "claim_seq": 1})
		}
		for task := int64(1010); task < 1013; task++ {
			if n := len(h.mustOK("task-events", 3, map[string]any{"task_id": task}).items()); n != 4 {
				t.Fatalf("task %d has %d events, want 4 despite no free capacity", task, n)
			}
		}
		if st := executorStates(h, 3)[7]; num(st["available_capacity"]) != 0 {
			t.Fatalf("capacity is the reported observation, not recomputed: %v", st)
		}
	})
}
