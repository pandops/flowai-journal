package test

import (
	"net/url"
	"sync"
	"testing"
	"time"
)

// Requirement: journal authenticates the Gateway stream edge and binds one team.
func TestV0048JournalAuthenticatesTheGatewayStreamEdgeAndBindsOneTeam(t *testing.T) {
	t.Run("Concurrent teams subscribe to the same event kinds", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		e := h.edge()
		c3 := e.open(t, 3, nil)
		c4 := e.open(t, 4, nil)
		time.Sleep(50 * time.Millisecond)
		var wg sync.WaitGroup
		var mu sync.Mutex
		publish := func(f func()) { mu.Lock(); defer mu.Unlock(); f() }
		for i := int64(0); i < 5; i++ {
			wg.Add(2)
			go func(i int64) { defer wg.Done(); publish(func() { h.claimTask(3, 7, 1010+i) }) }(i)
			go func(i int64) { defer wg.Done(); publish(func() { h.claimTask(4, 9, 1010+i) }) }(i)
		}
		wg.Wait()
		// Each claimTask emits task_committed, task_event (created), task_event (claim_accepted).
		f3 := readFrames(t, c3, 15, 3*time.Second)
		f4 := readFrames(t, c4, 15, 3*time.Second)
		if len(f3) != 15 || len(f4) != 15 {
			t.Fatalf("frames team 3 = %d, team 4 = %d, want 15 each", len(f3), len(f4))
		}
		for _, f := range f3 {
			if f.TeamID != 3 {
				t.Fatalf("team 3 stream received a team %d frame", f.TeamID)
			}
		}
		for _, f := range f4 {
			if f.TeamID != 4 {
				t.Fatalf("team 4 stream received a team %d frame", f.TeamID)
			}
		}
		if extra := readFrames(t, c3, 1, 200*time.Millisecond); len(extra) != 0 {
			t.Fatalf("team 3 stream received an extra frame %+v", extra)
		}
	})

	t.Run("Subscription names a foreign task", func(t *testing.T) {
		h := newHarness(t)
		h.register(4, 9, "gpu")
		e := h.edge()
		c3 := e.open(t, 3, url.Values{"task_id": {"1010"}})
		time.Sleep(50 * time.Millisecond)
		h.claimTask(4, 9, 1010)
		h.exec(4, 9, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		if frames := readFrames(t, c3, 1, 400*time.Millisecond); len(frames) != 0 {
			t.Fatalf("team 3 filter on foreign task 1010 received %+v", frames)
		}
		replay := e.open(t, 3, url.Values{"task_id": {"1010"}, "after_frame_id": {"1"}})
		if frames := readFrames(t, replay, 1, 300*time.Millisecond); len(frames) != 0 {
			t.Fatalf("replay for a foreign task returned %+v", frames)
		}
	})

	t.Run("Caller lacks the Gateway service token", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		e := h.edge()
		noToken := streamHeaders(3)
		noToken.Del("Authorization")
		conn, resp, err := e.dial(t, 3, noToken, url.Values{"after_frame_id": {"1"}})
		if err == nil || conn != nil || statusOf(resp) != 401 {
			t.Fatalf("no token: status %d err %v, want 401 without upgrade", statusOf(resp), err)
		}
		wrong := streamHeaders(3)
		wrong.Set("Authorization", "Bearer "+gatewayToken+"x")
		if _, resp, err := e.dial(t, 3, wrong, nil); err == nil || statusOf(resp) != 401 {
			t.Fatalf("wrong token: status %d, want 401", statusOf(resp))
		}
		mismatch := streamHeaders(4)
		if _, resp, err := e.dial(t, 3, mismatch, nil); err == nil || statusOf(resp) != 404 {
			t.Fatalf("header team 4 on path team 3: status %d, want 404", statusOf(resp))
		}
		missing := streamHeaders(3)
		missing.Del("X-FlowAI-Request-ID")
		if _, resp, err := e.dial(t, 3, missing, nil); err == nil || statusOf(resp) != 400 {
			t.Fatalf("missing context header: status %d, want 400", statusOf(resp))
		}
	})
}
