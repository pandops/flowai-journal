package test

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Requirement: service-platform service filters team-scoped reads before
// shaping.
func TestV0048ServicePlatformServiceFiltersTeamScopedReadsBeforeShaping(t *testing.T) {
	t.Run("Teams share filter values", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		h.claimTask(3, 7, 1010)
		for task := int64(1010); task < 1013; task++ {
			h.claimTask(4, 9, task)
			h.exec(4, 9, "running", map[string]any{"task_id": task, "attempt": 1, "claim_seq": 1})
		}
		d := h.mustOK("dashboard", 3, map[string]any{"period": "week", "time_zone": "UTC"})
		counts, _ := d.Body["counts"].(map[string]any)
		var total int64
		for _, v := range counts {
			total += num(v)
		}
		if total != 1 || num(counts["running"]) != 0 || num(counts["created"]) != 1 {
			t.Fatalf("team 3 dashboard counts %v, want only its one created task", counts)
		}
		if st := executorStates(h, 3); len(st) != 1 {
			t.Fatalf("team 3 executor-states %v, want only Executor 7", st)
		}
		page := h.mustOK("task-events", 3, map[string]any{"task_id": 1010, "limit": 2})
		for _, it := range page.items() {
			if num(it["team_id"]) != 3 {
				t.Fatalf("team 3 page carries %v", it)
			}
		}
	})

	t.Run("Concurrent team streams", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		e := h.edge()
		c3 := e.open(t, 3, nil)
		c4 := e.open(t, 4, nil)
		time.Sleep(50 * time.Millisecond)
		h.claimTask(3, 7, 1010)
		h.claimTask(4, 9, 1010)
		for team, conn := range map[int64]*websocket.Conn{3: c3, 4: c4} {
			for _, f := range readFrames(t, conn, 3, 2*time.Second) {
				if f.TeamID != team {
					t.Fatalf("team %d stream carried a team %d frame", team, f.TeamID)
				}
			}
			if extra := readFrames(t, conn, 1, 200*time.Millisecond); len(extra) != 0 {
				t.Fatalf("team %d stream carried extra frames %+v", team, extra)
			}
		}
	})
}
