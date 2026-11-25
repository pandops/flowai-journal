package test

import (
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Requirement: journal streams team frames with a resumable cursor.
func TestV0048JournalStreamsTeamFramesWithAResumableCursor(t *testing.T) {
	t.Run("Client reconnects after a gap", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		e := h.edge()
		conn := e.open(t, 3, nil)
		time.Sleep(50 * time.Millisecond)
		h.claimTask(3, 7, 1010)
		first := readFrames(t, conn, 3, 2*time.Second)
		if len(first) != 3 || first[0].FrameType != engine.FrameTaskCommitted || first[1].FrameType != engine.FrameTaskEvent {
			t.Fatalf("first frames %+v, want task_committed then task_event", first)
		}
		last := first[2].FrameID
		conn.Close()
		// 3 events of team 3 and 2 of team 4 are committed while disconnected.
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.svc("task-manager", "queued", 4, map[string]any{"task_id": 1, "task_name": taskName(4, 1), "attempt": 1, "intake_id": 1})
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", "output"))
		h.exec(4, 9, "created", map[string]any{"task_id": 1, "attempt": 1, "claim_seq": 1})
		h.exec(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		again := e.open(t, 3, url.Values{"after_frame_id": {strconv.FormatInt(last, 10)}})
		replayed := readFrames(t, again, 3, 2*time.Second)
		if len(replayed) != 3 {
			t.Fatalf("replayed %d frames, want exactly 3", len(replayed))
		}
		prev := last
		for _, f := range replayed {
			if f.FrameID <= prev || f.TeamID != 3 {
				t.Fatalf("replay out of order or foreign: %+v after %d", f, prev)
			}
			prev = f.FrameID
		}
		if replayed[1].FrameType != engine.FrameTaskLog {
			t.Fatalf("second replayed frame %s, want task_log", replayed[1].FrameType)
		}
		// Then live.
		h.svc("task-manager", "archived", 3, map[string]any{"task_id": 1010, "task_name": taskName(3, 1010)})
		live := readFrames(t, again, 1, 2*time.Second)
		if len(live) != 1 || live[0].FrameID <= prev {
			t.Fatalf("live frame after replay %+v", live)
		}
		if dup := readFrames(t, again, 1, 200*time.Millisecond); len(dup) != 0 {
			t.Fatalf("duplicate frame after live: %+v", dup)
		}
	})

	t.Run("Late event after journal downtime", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		cursor, _ := h.eng.MaxFrameID(t.Context(), 3)
		// Events published during journal's downtime carry earlier
		// publisher times than events already committed.
		earlier := h.clock.Now().Add(-time.Hour)
		h.clock.Set(earlier)
		backlog := []msg{
			h.execMsg(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1}),
			h.logChunk(3, 7, 1010, 1, 1, "work", "late output"),
			h.execMsg(3, 7, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1}),
		}
		for _, m := range backlog {
			h.mustAck(m)
		}
		e := h.edge()
		conn := e.open(t, 3, url.Values{"after_frame_id": {strconv.FormatInt(cursor, 10)}})
		frames := readFrames(t, conn, 3, 2*time.Second)
		if len(frames) != 3 {
			t.Fatalf("client received %d of the 3 drained events", len(frames))
		}
		for _, f := range frames {
			if f.FrameID <= cursor {
				t.Fatalf("drained event frame_id %d not above the cursor %d", f.FrameID, cursor)
			}
		}
	})

	t.Run("Non-integer stream cursor is rejected", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		e := h.edge()
		for _, bad := range []string{"task_event:1040", "0", "3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f", "-1", "01", "1.5", "9223372036854775808", ""} {
			conn, resp, err := e.dial(t, 3, streamHeaders(3), url.Values{"after_frame_id": {bad}})
			if err == nil || conn != nil || statusOf(resp) != 400 || errorCodeOf(t, resp) != "invalid_request" {
				t.Fatalf("after_frame_id %q: status %d, want 400 invalid_request without upgrade", bad, statusOf(resp))
			}
		}
	})
}
