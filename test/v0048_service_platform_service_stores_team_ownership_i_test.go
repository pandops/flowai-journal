package test

import (
	"bytes"
	"testing"
)

// Requirement: service-platform service stores team ownership in
// normalized form.
func TestV0048ServicePlatformServiceStoresTeamOwnershipInNormalizedForm(t *testing.T) {
	t.Run("Team-owned parent keeps its team", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		// A later executor_registered naming another team for Executor 7
		// does not move it.
		h.mustAck(h.svcMsg("executor-registry", "executor_registered", "4", map[string]any{"executor_id": 7, "execution_tag": "gpu", "executor_type": "executor_docker_openhands"}))
		if st := executorStates(h, 4); len(st) != 0 {
			t.Fatalf("Executor 7 appears in team 4: %v", st)
		}
		if st := executorStates(h, 3); len(st) != 1 {
			t.Fatalf("Executor 7 left team 3: %v", st)
		}
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		for _, e := range h.store.Events() {
			if e.ExecutorID != nil && *e.ExecutorID == 7 && e.Kind == "created" && e.TeamID != 3 {
				t.Fatalf("Executor 7 event stored under team %d", e.TeamID)
			}
		}
	})

	t.Run("Child references a foreign-team parent", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.register(4, 9, "gpu")
		h.claimTask(3, 7, 1010)
		before := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).Raw
		// Team 4's Executor names team 3's task: it lands in team 4 only.
		h.exec(4, 9, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1})
		h.mustAck(h.logChunk(4, 9, 1010, 1, 1, "work", "foreign output"))
		if after := h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).Raw; !bytes.Equal(before, after) {
			t.Fatalf("team 4 Executor event attached to team 3's task")
		}
		if logs := h.mustOK("task-logs", 3, map[string]any{"task_id": 1010}); len(logs.items()) != 0 {
			t.Fatalf("team 4 log chunk attached to team 3's task: %s", logs.Raw)
		}
	})

	t.Run("Display metadata never authorizes", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		before := len(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items())
		h.svc("team-registry", "team_updated", 3, map[string]any{"display_name": "Team Four"})
		hdr := uiHeaders(4)
		hdr["team_name"] = []string{"Operations"}
		if r := h.call("task-events", hdr, map[string]any{"team_id": 4, "task_id": 1010}); r.Status != 404 {
			t.Fatalf("team 4 with team 3's display name read team 3's task: %d", r.Status)
		}
		// The rename itself is a team event, not one of the task's events.
		if after := len(h.mustOK("task-events", 3, map[string]any{"task_id": 1010}).items()); before == 0 || after != before {
			t.Fatalf("team 3's task history changed after the rename: %d events before, %d after", before, after)
		}
	})
}
