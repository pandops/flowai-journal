package test

import (
	"context"
	"testing"

	"github.com/pandops/flowai-journal/internal/engine"
)

func controlFrames(h *harness, team int64) []engine.Frame {
	var out []engine.Frame
	for _, f := range h.store.AllFrames() {
		if f.TeamID == team && f.FrameType == engine.FrameControlEvent {
			out = append(out, f)
		}
	}
	return out
}

func requestCancel(h *harness, team, task, control int64) {
	h.t.Helper()
	h.svc("task-manager", "control_requested", team, map[string]any{"task_id": task, "task_name": taskName(team, task), "control_id": control, "control": "cancel"})
}

func claimResult(h *harness, team, executor, task, control int64, kind string, extra map[string]any) {
	h.t.Helper()
	fields := map[string]any{"task_id": task, "attempt": 1, "claim_seq": 1, "control_id": control}
	for k, v := range extra {
		fields[k] = v
	}
	h.exec(team, executor, kind, fields)
}

// Requirement: journal records task-control events and results.
func TestV0048JournalRecordsTaskControlEventsAndResults(t *testing.T) {
	t.Run("Cancellation request enters the task feed", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		requestCancel(h, 3, 1010, 41)
		frames := controlFrames(h, 3)
		if len(frames) != 1 || frames[0].ControlID == nil || *frames[0].ControlID != 41 {
			t.Fatalf("control_event frames = %+v, want one for control 41", frames)
		}
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		if got[len(got)-1] != "control_requested" {
			t.Fatalf("control_requested not in task feed: %v", got)
		}
	})

	t.Run("Operator reads a control's events", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		requestCancel(h, 3, 1010, 41)
		claimResult(h, 3, 7, 1010, 41, "control_acknowledged", nil)
		items := h.mustOK("control-events", 3, map[string]any{"task_id": 1010, "control_id": 41}).items()
		got := kinds(items)
		if len(got) != 2 || got[0] != "control_requested" || got[1] != "control_acknowledged" {
			t.Fatalf("control events %v, want [control_requested control_acknowledged]", got)
		}
		if num(items[0]["event_id"]) >= num(items[1]["event_id"]) {
			t.Fatalf("control events not in event_id order")
		}
		// Team 4 has a task 1010 with control 41 too; team 3 cannot see it,
		// and team 4 cannot see team 3's.
		h.register(4, 9, "gpu")
		h.queued(4, 2020, 1)
		requestCancel(h, 4, 2020, 41)
		foreign := h.ui("control-events", 4, map[string]any{"task_id": 1010, "control_id": 41})
		unknown := h.ui("control-events", 4, map[string]any{"task_id": 2020, "control_id": 99})
		if foreign.Status != 404 || unknown.Status != 404 || string(foreign.Raw) != string(unknown.Raw) {
			t.Fatalf("foreign control %d %s, unknown %d %s: want the same 404", foreign.Status, foreign.Raw, unknown.Status, unknown.Raw)
		}
	})

	t.Run("Claim holder reports the result", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		requestCancel(h, 3, 1010, 41)
		claimResult(h, 3, 7, 1010, 41, "control_acknowledged", nil)
		claimResult(h, 3, 7, 1010, 41, "cancelled", nil)
		if n := len(controlFrames(h, 3)); n != 2 {
			t.Fatalf("control_event frames = %d, want 2 (requested, acknowledged)", n)
		}
		got := kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		n := len(got)
		if n < 2 || got[n-2] != "control_acknowledged" || got[n-1] != "cancelled" {
			t.Fatalf("task feed %v, want control_acknowledged then the separate cancelled", got)
		}
		var lifecycle int
		for _, f := range h.store.AllFrames() {
			if f.FrameType == engine.FrameTaskEvent {
				var view struct {
					Kind string `json:"kind"`
				}
				_ = jsonUnmarshal(f.Data, &view)
				if view.Kind == "cancelled" {
					lifecycle++
				}
			}
		}
		if lifecycle != 1 {
			t.Fatalf("cancelled task_event frames = %d, want 1", lifecycle)
		}
	})

	t.Run("Non-holder reports a result", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.claimTask(3, 7, 1010)
		requestCancel(h, 3, 1010, 41)
		before := len(controlFrames(h, 3))
		claimResult(h, 3, 8, 1010, 41, "control_acknowledged", nil)
		last := h.store.Events()[len(h.store.Events())-1]
		if last.Kind != "control_acknowledged" {
			t.Fatalf("non-holder result not stored")
		}
		if status, _, _ := h.store.HeldStatus(context.Background(), last.EventID); status != engine.StatusNotClaimHolder {
			t.Fatalf("non-holder result status %q, want not_claim_holder", status)
		}
		if len(controlFrames(h, 3)) != before {
			t.Fatalf("non-holder result emitted a frame")
		}
	})

	t.Run("Control events are grouped by control_id", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		requestCancel(h, 3, 1010, 41)
		claimResult(h, 3, 7, 1010, 41, "control_acknowledged", nil)
		claimResult(h, 3, 7, 1010, 41, "cancelled", nil)
		h.queued(3, 1011, 1)
		requestCancel(h, 3, 1011, 42)
		got := kinds(h.mustOK("control-events", 3, map[string]any{"task_id": 1010, "control_id": 41}).items())
		if len(got) != 2 || got[0] != "control_requested" || got[1] != "control_acknowledged" {
			t.Fatalf("control 41 events %v, want requested and acknowledged only", got)
		}
		var cancelled map[string]any
		for _, it := range h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items() {
			if it["kind"] == "cancelled" {
				cancelled = it
			}
		}
		if cancelled == nil || num(cancelled["control_id"]) != 41 {
			t.Fatalf("cancelled not shown with control_id 41: %v", cancelled)
		}
	})

	t.Run("task-manager cancels a task directly", func(t *testing.T) {
		h := newHarness(t)
		h.queued(3, 1010, 1)
		requestCancel(h, 3, 1010, 41)
		h.svc("task-manager", "cancelled", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010), "control_id": 41})
		items := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()
		last := items[len(items)-1]
		if last["kind"] != "cancelled" || last["publisher"] != "task-manager" || num(last["control_id"]) != 41 {
			t.Fatalf("direct cancel stored as %v", last)
		}
		for _, e := range h.store.Events() {
			if status, ok, _ := h.store.HeldStatus(context.Background(), e.EventID); ok && status == engine.StatusNotClaimHolder {
				t.Fatalf("%s marked not_claim_holder", e.Kind)
			}
		}
	})
}
