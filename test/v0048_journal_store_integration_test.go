//go:build integration

package test

import (
	"context"
	"sync"
	"testing"

	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/pgstore"
)

// journal's store on a real PostgreSQL: idempotent storage, the global
// commit sequence under concurrency, held log acceptance, team-bound frames
// and immutable rows (journal consumes ALL_EVENT durably and stores every
// event once; journal stores and serves ordered task logs; journal streams
// team frames with a resumable cursor).
func TestV0048JournalStoreIntegration(t *testing.T) {
	t.Run("Redelivery and restart store each event once", func(t *testing.T) {
		h, admin, c := pgHarness(t, "journal_a")
		h.register(3, 7, "gpu")
		m := h.svcMsg("task-manager", "queued", "3", queuedFields(3, 1010, 1))
		h.mustAck(m)
		h.mustAck(m)
		// A new process over the same schema still recognizes it.
		store, err := pgstore.New(openAs(t, c), c.Schema)
		if err != nil {
			t.Fatal(err)
		}
		h.eng = engine.New(store, engine.Options{Now: h.clock.Now})
		h.rpc.Engine = h.eng
		h.mustAck(m)
		var n int
		if err := admin.QueryRow(`SELECT count(*) FROM journal_a_schema.events WHERE publisher = 'task-manager'`).Scan(&n); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if n != 1 {
			t.Fatalf("(task-manager, seq) stored %d times", n)
		}
		if got := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items(); len(got) != 1 {
			t.Fatalf("task events %v", got)
		}
	})

	t.Run("Concurrent commits get distinct increasing event_ids", func(t *testing.T) {
		h, _, _ := pgHarness(t, "journal_b")
		var msgs []msg
		for task := int64(1); task <= 20; task++ {
			msgs = append(msgs, h.svcMsg("task-manager", "queued", "3", queuedFields(3, task, 1)))
		}
		var wg sync.WaitGroup
		for i, m := range msgs {
			wg.Add(1)
			go func(seq uint64, in engine.Inbound) {
				defer wg.Done()
				in.StreamSeq = seq
				if d := h.eng.Handle(context.Background(), in); d != engine.Ack {
					t.Errorf("concurrent commit: %v", d)
				}
			}(uint64(i+1), m.Inbound)
		}
		wg.Wait()
		frames, err := h.eng.Frames(context.Background(), 3, 0, nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(frames) != 20 {
			t.Fatalf("frames %d, want 20", len(frames))
		}
		seen := map[int64]bool{}
		for i, f := range frames {
			if seen[f.FrameID] || (i > 0 && f.FrameID <= frames[i-1].FrameID) {
				t.Fatalf("frame ids not distinct and increasing: %+v", frames)
			}
			seen[f.FrameID] = true
		}
	})

	t.Run("Held log chunk is accepted after claim_accepted", func(t *testing.T) {
		h, _, _ := pgHarness(t, "journal_c")
		h.register(3, 7, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		h.mustAck(h.logChunk(3, 7, 1010, 1, 1, "work", "early output"))
		if got := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items(); len(got) != 0 {
			t.Fatalf("held chunk served before claim_accepted: %v", got)
		}
		before, _ := h.eng.MaxFrameID(context.Background(), 3)
		h.claimAccepted(3, 7, 1010, 1, 1)
		got := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}).items()
		if len(got) != 1 || num(got[0]["log_offset"]) != 1 || got[0]["content"] != "early output" || num(got[0]["log_chunk_id"]) <= before {
			t.Fatalf("accepted chunk %v (cursor before %d)", got, before)
		}
		replay, _ := h.eng.Frames(context.Background(), 3, before, nil, 10)
		var logs int
		for _, f := range replay {
			if f.FrameType == engine.FrameTaskLog {
				logs++
				if f.FrameID != num(got[0]["log_chunk_id"]) {
					t.Fatalf("log_chunk_id %v is not the stored event id %d", got[0]["log_chunk_id"], f.FrameID)
				}
			}
		}
		if logs != 1 {
			t.Fatalf("task_log frames after acceptance = %d, want 1", logs)
		}
	})

	t.Run("Holder's event acked before claim_accepted applies after a restart", func(t *testing.T) {
		h, _, c := pgHarness(t, "journal_g3b1")
		h.register(3, 7, "gpu")
		h.register(3, 8, "gpu")
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.exec(3, 7, "failed", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "failure_reason": "runtime_error", "retryable": false})
		h.exec(3, 8, "finished", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		// A new process over the same schema receives claim_accepted.
		store, err := pgstore.New(openAs(t, c), c.Schema)
		if err != nil {
			t.Fatal(err)
		}
		h.eng = engine.New(store, engine.Options{Now: h.clock.Now})
		h.rpc.Engine = h.eng
		h.claimAccepted(3, 7, 1010, 1, 1)
		d := h.mustOK("dashboard", 3, map[string]any{"period": "week", "time_zone": "UTC"})
		raw, _ := d.Body["counts"].(map[string]any)
		got := map[string]int64{}
		for k, v := range raw {
			if n := num(v); n != 0 {
				got[k] = n
			}
		}
		if len(got) != 1 || got["failed"] != 1 {
			t.Fatalf("dashboard counts %v, want the one task under failed", got)
		}
		// The replayed failed ended the claim: its later running moves nothing.
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		d = h.mustOK("dashboard", 3, map[string]any{"period": "week", "time_zone": "UTC"})
		if raw, _ := d.Body["counts"].(map[string]any); num(raw["failed"]) != 1 || num(raw["running"]) != 0 {
			t.Fatalf("dashboard counts %v after the ended claim's running, want failed", raw)
		}
	})

	t.Run("Stored events are never updated or deleted", func(t *testing.T) {
		h, _, c := pgHarness(t, "journal_d")
		h.queued(3, 1010, 1)
		h.queued(4, 1010, 1)
		db := openAs(t, c)
		if _, err := db.Exec(`UPDATE events SET team_id = 4`); err == nil {
			t.Fatalf("UPDATE of stored events succeeded")
		}
		if _, err := db.Exec(`DELETE FROM events`); err == nil {
			t.Fatalf("DELETE of stored events succeeded")
		}
		frames, _ := h.eng.Frames(context.Background(), 3, 0, nil, 10)
		if len(frames) != 1 || frames[0].TeamID != 3 {
			t.Fatalf("team 3 frames %+v", frames)
		}
	})
}
