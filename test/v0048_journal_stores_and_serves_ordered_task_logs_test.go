package test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

func taskLogFrames(h *harness, team int64) []engine.Frame {
	var out []engine.Frame
	for _, f := range h.store.AllFrames() {
		if f.TeamID == team && f.FrameType == engine.FrameTaskLog {
			out = append(out, f)
		}
	}
	return out
}

// wantLog is the baseline Log (ui-operator.openapi.yaml) a chunk must have.
type wantLog struct {
	eventID, task, executor, offset int64
	stream, content                 string
}

// isInteger reports whether a decoded JSON value is an integral number.
func isInteger(v any) bool {
	f, ok := v.(float64)
	return ok && f == float64(int64(f))
}

// assertBaselineLog checks one served log object against the baseline Log
// shape: log_chunk_id is the stored event id as an integer, content holds the
// chunk text, and the text field of the Executor message is not repeated.
func assertBaselineLog(t *testing.T, got map[string]any, want wantLog) {
	t.Helper()
	for _, k := range []string{"log_chunk_id", "task_id", "executor_id", "log_offset", "stream", "content", "occurred_at"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("log %v lacks baseline field %s", got, k)
		}
	}
	for _, k := range []string{"log_chunk_id", "task_id", "executor_id", "log_offset"} {
		if !isInteger(got[k]) {
			t.Fatalf("log field %s = %#v, want an integer", k, got[k])
		}
	}
	if num(got["log_chunk_id"]) != want.eventID || num(got["task_id"]) != want.task ||
		num(got["executor_id"]) != want.executor || num(got["log_offset"]) != want.offset {
		t.Fatalf("log ids %v, want log_chunk_id %d task_id %d executor_id %d log_offset %d",
			got, want.eventID, want.task, want.executor, want.offset)
	}
	if got["stream"] != want.stream || got["content"] != want.content {
		t.Fatalf("log stream/content %v, want %q %q", got, want.stream, want.content)
	}
	if at, ok := got["occurred_at"].(string); !ok {
		t.Fatalf("occurred_at %#v is not a string", got["occurred_at"])
	} else if _, err := time.Parse(time.RFC3339Nano, at); err != nil {
		t.Fatalf("occurred_at %q is not RFC 3339: %v", at, err)
	}
	if _, ok := got["text"]; ok {
		t.Fatalf("log %v still carries text next to content", got)
	}
}

// assertFrameMatchesLog checks that a task_log frame carries the served log
// object unchanged and validates against journal's frame schema.
func assertFrameMatchesLog(t *testing.T, f engine.Frame, item map[string]any) {
	t.Helper()
	raw, err := json.Marshal(f.View())
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	if err := validateJSON(t, journalDir(t)+"/schemas/frame.schema.json", raw); err != nil {
		t.Fatalf("task_log frame violates frame.schema.json: %v: %s", err, raw)
	}
	var data map[string]any
	if err := json.Unmarshal(f.Data, &data); err != nil {
		t.Fatalf("frame data: %v", err)
	}
	if !reflect.DeepEqual(data, item) {
		t.Fatalf("task_log frame data %v differs from the served log %v", data, item)
	}
}

// Requirement: journal stores and serves ordered task logs.
func TestV0048JournalStoresAndServesOrderedTaskLogs(t *testing.T) {
	t.Run("Claim holder appends output", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", "go test ./..."))
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "reasoning", "Checking the failing package first."))
		items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items()
		if len(items) != 2 || num(items[0]["log_offset"]) != 1 || num(items[1]["log_offset"]) != 2 {
			t.Fatalf("logs %v, want offsets 1 and 2", items)
		}
		frames := taskLogFrames(h, 3)
		if n := len(frames); n != 2 {
			t.Fatalf("task_log frames = %d, want one per chunk", n)
		}
		// The frame_id of a task_log frame is the stored event id of its chunk.
		assertBaselineLog(t, items[0], wantLog{frames[0].FrameID, 1010, 7, 1, "work", "go test ./..."})
		assertBaselineLog(t, items[1], wantLog{frames[1].FrameID, 1010, 7, 2, "reasoning", "Checking the failing package first."})
		assertFrameMatchesLog(t, frames[0], items[0])
		assertFrameMatchesLog(t, frames[1], items[1])
		for _, k := range kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()) {
			if k == "log" {
				t.Fatalf("a log chunk was converted into a task event")
			}
		}
	})

	t.Run("Log chunk arrives before claim_accepted", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", "early output"))
		// The held chunk does not block later events of the task.
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		if items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items(); len(items) != 0 {
			t.Fatalf("held chunk returned before claim_accepted: %v", items)
		}
		if n := len(taskLogFrames(h, 3)); n != 0 {
			t.Fatalf("held chunk streamed before claim_accepted")
		}
		h.clock.Advance(2 * time.Second)
		h.claimAccepted(3, 7, 1010, 1, 1)
		items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items()
		if len(items) != 1 {
			t.Fatalf("chunk not accepted after claim_accepted: %v", items)
		}
		frames := taskLogFrames(h, 3)
		if len(frames) != 1 {
			t.Fatalf("task_log frames after acceptance = %d, want 1", len(frames))
		}
		assertBaselineLog(t, items[0], wantLog{frames[0].FrameID, 1010, 7, 1, "work", "early output"})
		assertFrameMatchesLog(t, frames[0], items[0])
		max, _ := h.eng.MaxFrameID(context.Background(), 3)
		if frames[0].FrameID != max {
			t.Fatalf("accepted chunk frame_id %d is not the team's latest frame %d", frames[0].FrameID, max)
		}
	})

	t.Run("Log chunk without claim_accepted is dropped", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		chunk := h.logChunk(3, 7, 1010, 1, 1, "work", "orphan output")
		h.mustAck(chunk)
		h.clock.Advance(10*time.Minute + time.Second)
		if err := h.eng.ExpireHeld(context.Background()); err != nil {
			t.Fatalf("expire held: %v", err)
		}
		last := h.store.Events()[len(h.store.Events())-1]
		status, ok, _ := h.store.HeldStatus(context.Background(), last.EventID)
		if !ok || status != engine.StatusNotClaimHolder {
			t.Fatalf("chunk status after 10 minutes = %q, want not_claim_holder", status)
		}
		h.claimAccepted(3, 7, 1010, 1, 1)
		if items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items(); len(items) != 0 {
			t.Fatalf("expired chunk returned after a late claim_accepted: %v", items)
		}
		if n := len(taskLogFrames(h, 3)); n != 0 {
			t.Fatalf("expired chunk streamed")
		}
		// Without a sweep, a claim_accepted more than 10 minutes after the
		// chunk was stored still marks it not_claim_holder.
		h.queued(3, 1011, 1)
		h.created(3, 7, 1011, 1, 1)
		h.mustAck(h.logChunk(3, 7, 1011, 1, 1, "work", "late"))
		h.clock.Advance(11 * time.Minute)
		h.claimAccepted(3, 7, 1011, 1, 1)
		if items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1011}).items(); len(items) != 0 {
			t.Fatalf("chunk accepted after the 10 minute window: %v", items)
		}
	})

	t.Run("Operator resumes a task log", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		for i := 1; i <= 5; i++ {
			h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", fmt.Sprintf("line %d", i)))
		}
		var seen []int64
		var cursor any
		for page := 0; page < 5; page++ {
			params := map[string]any{"task_id": 1010, "limit": 2}
			if cursor != nil {
				params["cursor"] = cursor
			}
			r := h.mustOK("task-logs", 3, params)
			for _, it := range r.items() {
				seen = append(seen, num(it["log_offset"]))
			}
			cursor = r.Body["next_cursor"]
			if cursor == nil {
				break
			}
		}
		if fmt.Sprint(seen) != "[1 2 3 4 5]" {
			t.Fatalf("resumed offsets %v, want [1 2 3 4 5] without gaps or duplicates", seen)
		}
	})

	t.Run("Non-holder chunk or foreign read", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.claimTask(3, 7, 1010)
		h.mustAck(h.logChunk(3, 8, 1010, 1, 1, "work", "not mine"))
		last := h.store.Events()[len(h.store.Events())-1]
		if status, _, _ := h.store.HeldStatus(context.Background(), last.EventID); status != engine.StatusNotClaimHolder {
			t.Fatalf("non-holder chunk status %q, want not_claim_holder", status)
		}
		if items := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items(); len(items) != 0 {
			t.Fatalf("non-holder chunk returned: %v", items)
		}
		if n := len(taskLogFrames(h, 3)); n != 0 {
			t.Fatalf("non-holder chunk streamed")
		}
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", "team a secret-free output"))
		foreign := h.ui("task-logs", 4, map[string]any{"task_id": 1010})
		unknown := h.ui("task-logs", 4, map[string]any{"task_id": 424242})
		if foreign.Status != 404 || unknown.Status != 404 {
			t.Fatalf("foreign read status %d, unknown %d, want 404", foreign.Status, unknown.Status)
		}
		if string(foreign.Raw) != string(unknown.Raw) {
			t.Fatalf("foreign 404 differs from unknown 404:\n%s\n%s", foreign.Raw, unknown.Raw)
		}
		validateReply(t, "task-logs", foreign)
		for _, k := range []string{"items", "next_cursor", "count"} {
			if _, ok := foreign.Body[k]; ok {
				t.Fatalf("foreign 404 carries %s", k)
			}
		}
	})
}
