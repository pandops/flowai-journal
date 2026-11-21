package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

func executorStates(h *harness, team int64) map[int64]map[string]any {
	h.t.Helper()
	out := map[int64]map[string]any{}
	for _, it := range h.mustOK("executor-states", team, nil).items() {
		out[num(it["executor_id"])] = it
	}
	return out
}

func registryEvent(h *harness, team, executor int64, kind string, extra map[string]any) {
	h.t.Helper()
	fields := map[string]any{"executor_id": executor}
	for k, v := range extra {
		fields[k] = v
	}
	h.svc("executor-registry", kind, team, fields)
}

func status(h *harness, team, executor int64, state string, claims, capacity int) {
	h.t.Helper()
	registryEvent(h, team, executor, "executor_status", map[string]any{"connection_state": state, "held_claims": claims, "available_capacity": capacity, "max_parallel_tasks": claims + capacity})
}

// Requirement: journal records Executor events and capacity without gating claims.
func TestV0048JournalRecordsExecutorEventsAndCapacityWithoutGatingClaims(t *testing.T) {
	t.Run("Executor reports connection loss and restoration", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		status(h, 3, 7, "connected", 1, 1)
		h.claimTask(3, 7, 1010)
		lost := h.clock.Now().Add(-45 * time.Second)
		restored := h.clock.Now()
		h.exec(3, 7, "executor_connection_lost", map[string]any{"lost_at": engine.FormatTime(lost)})
		// The Executor's own observation never changes the served state.
		if st := executorStates(h, 3)[7]["connection_state"]; st != "connected" {
			t.Fatalf("connection_state right after executor_connection_lost = %v, want connected", st)
		}
		h.exec(3, 7, "executor_connection_restored", map[string]any{"restored_at": engine.FormatTime(restored)})
		items := h.mustOK("executor-events", 3, map[string]any{"executor_id": 7}).items()
		var lostAt, restoredAt any
		for _, it := range items {
			switch it["kind"] {
			case "executor_connection_lost":
				lostAt = it["lost_at"]
			case "executor_connection_restored":
				restoredAt = it["restored_at"]
			}
		}
		if lostAt != engine.FormatTime(lost) || restoredAt != engine.FormatTime(restored) {
			t.Fatalf("Executor history lost_at %v restored_at %v, want %s and %s", lostAt, restoredAt, engine.FormatTime(lost), engine.FormatTime(restored))
		}
		if st := executorStates(h, 3)[7]["connection_state"]; st != "connected" {
			t.Fatalf("connection_state = %v, want connected from executor-registry's latest event", st)
		}
		// executor-registry's connection_lost is not turned back to connected
		// by the Executor's own restoration report.
		registryEvent(h, 3, 7, "connection_lost", nil)
		h.exec(3, 7, "executor_connection_restored", map[string]any{"restored_at": engine.FormatTime(h.clock.Now())})
		if st := executorStates(h, 3)[7]["connection_state"]; st != "connection_lost" {
			t.Fatalf("connection_state after registry connection_lost and executor_connection_restored = %v, want connection_lost", st)
		}
		timeline := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		var inTimeline bool
		for _, k := range timeline {
			inTimeline = inTimeline || k == "executor_connection_lost"
		}
		if !inTimeline {
			t.Fatalf("task timeline %v lacks the claim holder's connection observation", timeline)
		}
	})

	t.Run("executor-registry reports a lost Executor", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		status(h, 3, 7, "connected", 0, 2)
		registryEvent(h, 3, 7, "connection_lost", nil)
		h.clock.Advance(10 * time.Minute)
		lostAt := h.clock.Now()
		registryEvent(h, 3, 7, "executor_lost", map[string]any{"last_heartbeat_at": engine.FormatTime(lostAt.Add(-10 * time.Minute))})
		st := executorStates(h, 3)[7]
		if st["connection_state"] != "executor_lost" || st["connection_changed_at"] != engine.FormatTime(lostAt) {
			t.Fatalf("state %v at %v, want executor_lost at %s", st["connection_state"], st["connection_changed_at"], engine.FormatTime(lostAt))
		}
		var history []string
		for _, k := range kinds(h.mustOK("executor-events", 3, map[string]any{"executor_id": 7}).items()) {
			if k == "connection_lost" || k == "executor_lost" {
				history = append(history, k)
			}
		}
		if len(history) != 2 || history[0] != "connection_lost" || history[1] != "executor_lost" {
			t.Fatalf("Executor history %v, want connection_lost then executor_lost", history)
		}
	})

	t.Run("Lost Executor connects again", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		registryEvent(h, 3, 7, "executor_lost", nil)
		if st := executorStates(h, 3)[7]["connection_state"]; st != "executor_lost" {
			t.Fatalf("state %v, want executor_lost", st)
		}
		registryEvent(h, 3, 7, "connection_restored", nil)
		if st := executorStates(h, 3)[7]["connection_state"]; st != "connected" {
			t.Fatalf("state after connection_restored %v, want connected", st)
		}
		status(h, 3, 7, "connected", 0, 2)
		if st := executorStates(h, 3)[7]["connection_state"]; st != "connected" {
			t.Fatalf("state %v, want connected", st)
		}
	})

	t.Run("New Executor has never connected", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 8, "gpu")
		st := executorStates(h, 3)[8]
		if st == nil || st["connection_state"] != "never_connected" || st["connection_changed_at"] != nil {
			t.Fatalf("new Executor state %v, want never_connected", st)
		}
		if _, ok := st["name"]; ok {
			t.Fatalf("Executor list carries a name")
		}
		status(h, 3, 8, "connected", 0, 2)
		if s := executorStates(h, 3)[8]["connection_state"]; s != "connected" {
			t.Fatalf("state after first executor_status %v, want connected", s)
		}
	})

	t.Run("Status after a loss carries the state", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		status(h, 3, 7, "connected", 1, 1)
		registryEvent(h, 3, 7, "connection_lost", nil)
		status(h, 3, 7, "connection_lost", 2, 0)
		st := executorStates(h, 3)[7]
		if st["connection_state"] != "connection_lost" || num(st["held_claims"]) != 2 {
			t.Fatalf("state %v claims %v, want connection_lost with 2 claims", st["connection_state"], st["held_claims"])
		}
	})

	t.Run("Removed Executor leaves the inventory", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		status(h, 3, 7, "connected", 0, 1)
		stored := len(h.store.Events())
		registryEvent(h, 3, 7, "executor_removed", nil)
		states := executorStates(h, 3)
		if _, ok := states[7]; ok {
			t.Fatalf("removed Executor 7 still listed")
		}
		if _, ok := states[8]; !ok {
			t.Fatalf("Executor 8 missing")
		}
		if len(h.store.Events()) != stored+1 {
			t.Fatalf("journal deleted stored events on removal")
		}
		if n := len(h.mustOK("executor-events", 3, map[string]any{"executor_id": 7}).items()); n == 0 {
			t.Fatalf("removed Executor's events are gone")
		}
	})

	t.Run("Reported IP comes from the latest ip_changed", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 8, "gpu")
		status(h, 3, 8, "connected", 1, 1)
		h.claimTask(3, 8, 1010)
		registryEvent(h, 3, 8, "ip_changed", map[string]any{"old_ip": nil, "new_ip": "10.42.0.9"})
		h.clock.Set(time.Date(2026, 10, 15, 8, 58, 0, 0, time.UTC))
		registryEvent(h, 3, 8, "ip_changed", map[string]any{"old_ip": "10.42.0.9", "new_ip": "10.42.0.17"})
		st := executorStates(h, 3)[8]
		if st["reported_ip"] != "10.42.0.17" || st["ip_changed_at"] != "2026-10-15T08:58:00.000Z" {
			t.Fatalf("reported_ip %v at %v, want 10.42.0.17 at 2026-10-15T08:58:00.000Z", st["reported_ip"], st["ip_changed_at"])
		}
		if st["connection_state"] != "connected" || num(st["held_claims"]) != 1 || num(st["available_capacity"]) != 1 {
			t.Fatalf("ip_changed altered state, activity or capacity: %v", st)
		}
		var ipEvents int
		for _, k := range kinds(h.mustOK("executor-events", 3, map[string]any{"executor_id": 8}).items()) {
			if k == "ip_changed" {
				ipEvents++
			}
		}
		if ipEvents != 2 {
			t.Fatalf("Executor history has %d ip_changed, want 2", ipEvents)
		}
		for _, k := range kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()) {
			if k == "ip_changed" {
				t.Fatalf("a task timeline shows ip_changed")
			}
		}
	})

	t.Run("Executor without a reported IP", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 12, "gpu")
		st := executorStates(h, 3)[12]
		if v, ok := st["reported_ip"]; !ok || v != nil {
			t.Fatalf("reported_ip = %v, want null", v)
		}
		if v, ok := st["ip_changed_at"]; !ok || v != nil {
			t.Fatalf("ip_changed_at = %v, want null", v)
		}
	})

	t.Run("Activity and capacity come from executor_status", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		status(h, 3, 7, "connected", 3, 1)
		st := executorStates(h, 3)[7]
		if num(st["held_claims"]) != 3 || num(st["available_capacity"]) != 1 {
			t.Fatalf("claims %v capacity %v, want 3 and 1", st["held_claims"], st["available_capacity"])
		}
	})

	t.Run("Claim at full capacity is not rejected", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		status(h, 3, 7, "connected", 2, 0)
		h.queued(3, 1010, 1)
		m := h.execMsg(3, 7, "created", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		if d := h.deliver(m); d != engine.Ack {
			t.Fatalf("created at full capacity: %v, want stored and acknowledged", d)
		}
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		if len(got) != 2 || got[1] != "created" {
			t.Fatalf("claim at full capacity not stored: %v", got)
		}
		if st := executorStates(h, 3)[7]; num(st["held_claims"]) != 2 || num(st["available_capacity"]) != 0 {
			t.Fatalf("observation changed: %v", st)
		}
	})

	t.Run("Foreign Executor events are hidden", func(t *testing.T) {
		h := newHarness(t)
		h.register(4, 7, "gpu")
		registryEvent(h, 4, 7, "connection_lost", nil)
		foreign := h.ui("executor-events", 3, map[string]any{"executor_id": 7})
		unknown := h.ui("executor-events", 3, map[string]any{"executor_id": 4242})
		if foreign.Status != 404 || string(foreign.Raw) != string(unknown.Raw) {
			t.Fatalf("foreign Executor history %d %s, unknown %s: want the same 404", foreign.Status, foreign.Raw, unknown.Raw)
		}
		if n := len(executorStates(h, 3)); n != 0 {
			t.Fatalf("team 3 executor-states lists %d team 4 Executors", n)
		}
	})
}
