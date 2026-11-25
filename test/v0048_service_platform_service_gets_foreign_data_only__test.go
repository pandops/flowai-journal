package test

import "testing"

// Requirement: service-platform service gets foreign data only from the
// broker. journal's projection is the team's Executor list.
func TestV0048ServicePlatformServiceGetsForeignDataOnlyFromTheBroker(t *testing.T) {
	t.Run("Team archive reaches a projection", func(t *testing.T) {
		// journal has no team projection; its projection of another
		// service's data is executor-registry's Executor list.
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "cpu")
		if len(executorStates(h, 3)) != 2 {
			t.Fatalf("executor_registered did not reach the projection")
		}
		h.svc("executor-registry", "executor_removed", 3, map[string]any{"executor_id": 8, "execution_tag": "cpu", "executor_type": "executor_docker_openhands"})
		st := executorStates(h, 3)
		if _, ok := st[8]; ok || len(st) != 1 {
			t.Fatalf("removed Executor still listed: %v", st)
		}
		assertOwnImports(t)
	})

	t.Run("Projection ignores a replayed event", func(t *testing.T) {
		h := newHarness(t)
		reg := h.svcMsg("executor-registry", "executor_registered", "3", map[string]any{"executor_id": 7, "execution_tag": "gpu", "executor_type": "executor_docker_openhands"})
		h.mustAck(reg)
		status := h.svcMsg("executor-registry", "executor_status", "3", map[string]any{"executor_id": 7, "connection_state": "connected", "max_parallel_tasks": 2, "held_claims": 1, "available_capacity": 1})
		h.mustAck(status)
		later := h.svcMsg("executor-registry", "connection_lost", "3", map[string]any{"executor_id": 7, "last_heartbeat_at": "2026-10-15T09:00:00.000Z"})
		h.mustAck(later)
		events := len(h.store.Events())
		// Replays of the earlier messages change nothing.
		h.mustAck(reg)
		h.mustAck(status)
		if len(h.store.Events()) != events {
			t.Fatalf("replay stored a second event")
		}
		st := executorStates(h, 3)
		if len(st) != 1 || st[7]["connection_state"] != "connection_lost" {
			t.Fatalf("replayed executor_status changed the projection: %v", st)
		}
	})
}
