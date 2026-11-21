//go:build integration

package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/pgstore"
)

// pgFrames reads a team's frames through the PostgreSQL store.
func pgFrames(t *testing.T, h *harness, team int64, task *int64) []engine.Frame {
	t.Helper()
	frames, err := h.eng.Frames(context.Background(), team, 0, task, 500)
	if err != nil {
		t.Fatalf("frames: %v", err)
	}
	return frames
}

// journal's read side on a real PostgreSQL: the team predicate and the
// visibility rules hold in the SQL, not only in the in-memory fake
// (service-platform service filters team-scoped reads before shaping;
// journal stores and serves ordered task logs; journal records Executor
// events and capacity; journal returns complete selected dashboard
// calendars; services use integer identifiers from committed sequences).
func TestV0048JournalReadsIntegration(t *testing.T) {
	t.Run("Teams with equal task ids read only their own rows", func(t *testing.T) {
		h, _, _ := pgHarness(t, "journal_r1")
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		for _, team := range []int64{3, 4} {
			executor := map[int64]int64{3: 7, 4: 9}[team]
			h.claimTask(team, executor, 1010)
			h.exec(team, executor, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
			h.mustAck(h.logChunk(team, executor, 1010, 1, 1, "work", "output"))
			status(h, team, executor, "connected", 1, 1)
		}
		requestCancel(h, 3, 1010, 41)
		requestCancel(h, 4, 1010, 42)
		h.exec(4, 9, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})

		for _, op := range []struct {
			name   string
			params map[string]any
		}{
			{"task-events", map[string]any{"task_id": 1010}},
			{"task-logs", map[string]any{"task_id": 1010}},
			{"control-events", map[string]any{"task_id": 1010, "control_id": 41}},
			{"executor-events", map[string]any{"executor_id": 7}},
		} {
			items := h.mustOK(op.name, 3, op.params).items()
			if len(items) == 0 {
				t.Fatalf("%s: team 3 sees nothing", op.name)
			}
			for _, it := range items {
				if v, ok := it["team_id"]; ok && num(v) != 3 {
					t.Fatalf("%s: team 3 read a team %v row: %v", op.name, v, it)
				}
				if v, ok := it["executor_id"]; ok && v != nil && num(v) != 7 {
					t.Fatalf("%s: team 3 read Executor %v's row", op.name, v)
				}
			}
		}
		if n := len(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()); n != len(h.mustOK("task-events", 4, map[string]any{"task_id": 1010}).items())-1 {
			t.Fatalf("team 3 task history (%d events) is not separate from team 4's", n)
		}
		if logs := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items(); len(logs) != 1 {
			t.Fatalf("team 3 logs %v, want its one chunk", logs)
		}
		if r := h.ui("control-events", 3, map[string]any{"task_id": 1010, "control_id": 42}); r.Status != 404 {
			t.Fatalf("team 3 read team 4's control: %d", r.Status)
		}
		if r := h.ui("executor-events", 3, map[string]any{"executor_id": 9}); r.Status != 404 {
			t.Fatalf("team 3 read team 4's Executor: %d", r.Status)
		}
		if st := executorStates(h, 3); len(st) != 1 || st[7] == nil {
			t.Fatalf("team 3 executor-states %v, want only Executor 7", st)
		}
		counts := countsOf(h.mustOK("dashboard", 3, map[string]any{"period": "week"}).Body["counts"])
		var total int64
		for _, v := range counts {
			total += v
		}
		if total != 1 {
			t.Fatalf("team 3 dashboard counts %v, want its one task", counts)
		}
		task := int64(1010)
		for _, filter := range []*int64{nil, &task} {
			frames := pgFrames(t, h, 3, filter)
			foreign := pgFrames(t, h, 4, filter)
			if len(frames) == 0 || len(frames) >= len(foreign) {
				t.Fatalf("team 3 has %d frames, team 4 %d (task filter %v): team 4 published one more event", len(frames), len(foreign), filter)
			}
			for _, f := range frames {
				if f.TeamID != 3 {
					t.Fatalf("team 3 frames carry a team %d frame (task filter %v)", f.TeamID, filter)
				}
				if filter != nil && (f.TaskID == nil || *f.TaskID != task) {
					t.Fatalf("task-filtered frames carry %+v", f)
				}
				// Oracles that do not depend on the store's team label.
				if (f.ExecutorID != nil && *f.ExecutorID == 9) || (f.ControlID != nil && *f.ControlID == 42) {
					t.Fatalf("team 3 frames carry team 4's frame %+v (task filter %v)", f, filter)
				}
				var data map[string]any
				if err := json.Unmarshal(f.Data, &data); err != nil {
					t.Fatalf("frame data: %v", err)
				}
				if num(data["executor_id"]) == 9 || num(data["control_id"]) == 42 {
					t.Fatalf("team 3 frame data is team 4's: %s", f.Data)
				}
			}
		}
		if max3, _ := h.eng.MaxFrameID(context.Background(), 3); max3 == 0 {
			t.Fatalf("team 3 max frame id is 0")
		}
	})

	t.Run("Non-holder and expired held chunks are never returned", func(t *testing.T) {
		h, admin, _ := pgHarness(t, "journal_r2")
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.claimTask(3, 7, 1010)
		h.mustAck(h.logChunk(3, 8, 1010, 1, 1, "work", "not mine"))

		h.queued(3, 1011, 1)
		h.created(3, 7, 1011, 1, 1)
		h.mustAck(h.logChunk(3, 7, 1011, 1, 1, "work", "orphan"))
		h.clock.Advance(10*time.Minute + time.Second)
		if err := h.eng.ExpireHeld(context.Background()); err != nil {
			t.Fatalf("expire held: %v", err)
		}
		h.claimAccepted(3, 7, 1011, 1, 1)

		for _, task := range []int64{1010, 1011} {
			if items := h.mustOK("task-logs", 3, map[string]any{"task_id": task}).items(); len(items) != 0 {
				t.Fatalf("task %d returned a non-holder or expired chunk: %v", task, items)
			}
		}
		for _, f := range pgFrames(t, h, 3, nil) {
			if f.FrameType == engine.FrameTaskLog {
				t.Fatalf("a non-holder or expired chunk was streamed: %+v", f)
			}
		}
		var n int
		if err := admin.QueryRow(`SELECT count(*) FROM journal_r2_schema.logs`).Scan(&n); err != nil {
			t.Fatalf("count logs: %v", err)
		}
		if n != 0 {
			t.Fatalf("%d log rows stored for rejected chunks", n)
		}
	})

	t.Run("Executor views follow the stored history", func(t *testing.T) {
		h, _, _ := pgHarness(t, "journal_r3")
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		status(h, 3, 8, "connected", 1, 1)
		h.claimTask(3, 8, 1010)
		registryEvent(h, 3, 8, "ip_changed", map[string]any{"old_ip": nil, "new_ip": "10.42.0.9"})
		var ipEvents int
		for _, k := range kinds(h.mustOK("executor-events", 3, map[string]any{"executor_id": 8}).items()) {
			if k == "ip_changed" {
				ipEvents++
			}
		}
		if ipEvents != 1 {
			t.Fatalf("Executor history has %d ip_changed, want 1", ipEvents)
		}
		for _, k := range kinds(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()) {
			if k == "ip_changed" {
				t.Fatalf("the task timeline shows ip_changed")
			}
		}
		if st := executorStates(h, 3)[8]; st["reported_ip"] != "10.42.0.9" {
			t.Fatalf("reported_ip %v, want 10.42.0.9", st["reported_ip"])
		}
		registryEvent(h, 3, 7, "executor_removed", nil)
		states := executorStates(h, 3)
		if _, ok := states[7]; ok {
			t.Fatalf("removed Executor 7 still listed")
		}
		if _, ok := states[8]; !ok {
			t.Fatalf("Executor 8 missing after removing 7")
		}
	})

	t.Run("Dashboard range includes its start and excludes its end", func(t *testing.T) {
		h, _, _ := pgHarness(t, "journal_r4") // now: Thursday 2026-10-15, week 2026-W42
		h.queuedAt(3, 1010, time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC))
		h.queuedAt(3, 1011, time.Date(2026, 10, 18, 23, 59, 59, 999_000_000, time.UTC))
		h.queuedAt(3, 1012, time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC))
		h.queuedAt(3, 1013, time.Date(2026, 10, 11, 23, 59, 59, 999_000_000, time.UTC))
		b := buckets(h.mustOK("dashboard", 3, map[string]any{"period": "week", "week": "2026-W42", "time_zone": "UTC"}))
		if len(b) != 7 {
			t.Fatalf("buckets = %d, want 7", len(b))
		}
		if num(b[0]["total"]) != 1 || num(b[6]["total"]) != 1 {
			t.Fatalf("Monday %v Sunday %v, want one task each", b[0], b[6])
		}
		var total int64
		for _, bucket := range b {
			total += num(bucket["total"])
		}
		if total != 2 {
			t.Fatalf("week total %d, want 2: a task queued at the range end or before its start is counted", total)
		}
	})

	t.Run("Aborted write burns its id and leaves no row", func(t *testing.T) {
		h, admin, c := pgHarness(t, "journal_r5")
		store, err := pgstore.New(openAs(t, c), c.Schema)
		if err != nil {
			t.Fatal(err)
		}
		m := h.svcMsg("task-manager", "queued", "3", queuedFields(3, 1010, 1))
		var aborted int64
		abort := errors.New("abort after insert")
		err = store.Write(context.Background(), func(tx engine.Tx) error {
			id, err := tx.NextID()
			if err != nil {
				return err
			}
			aborted = id
			task := int64(1010)
			if err := tx.InsertEvent(engine.Event{EventID: id, TeamID: 3, Kind: "queued", Source: "task-manager", PublisherKey: "task-manager", PublisherSeq: m.PublisherSeq, TaskID: &task, OccurredAt: h.clock.Now(), CommittedAt: h.clock.Now(), Data: m.Data}); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) || aborted == 0 {
			t.Fatalf("aborted write: id %d err %v", aborted, err)
		}
		h.mustAck(m)
		items := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()
		if len(items) != 1 || num(items[0]["event_id"]) <= aborted {
			t.Fatalf("committed redelivery %v, want one event with event_id > %d", items, aborted)
		}
		var n int
		if err := admin.QueryRow(`SELECT count(*) FROM journal_r5_schema.events WHERE event_id = $1`, aborted).Scan(&n); err != nil {
			t.Fatalf("count aborted: %v", err)
		}
		if n != 0 {
			t.Fatalf("the aborted write left %d rows", n)
		}
	})
}
