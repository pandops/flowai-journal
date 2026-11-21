package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/pandops/flowai-journal/internal/natsio"
	"github.com/pandops/flowai-journal/internal/rpc"
)

// Requirement: message-broker names subjects from immutable identifiers.
func TestV0048MessageBrokerNamesSubjectsFromImmutableIdentifiers(t *testing.T) {
	t.Run("Payload team disagrees with the team header", func(t *testing.T) {
		h := newHarness(t)
		start := len(h.store.Events())
		m := h.svcMsg("task-manager", "queued", "3", map[string]any{"team_id": 4, "task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "intake_id": 1})
		h.mustAck(m)
		if len(h.store.Events()) != start {
			t.Fatalf("message with payload team 4 under header team 3 was stored")
		}
		rejected := h.store.Rejected()
		if len(rejected) != 1 || rejected[0].PublisherKey != "task-manager" {
			t.Fatalf("rejection not recorded: %+v", rejected)
		}
		if r := h.ui("task-events", 4, map[string]any{"task_id": 1010}); r.Status != 404 {
			t.Fatalf("team 4 sees the rejected message: %d", r.Status)
		}
		if r := h.ui("task-events", 3, map[string]any{"task_id": 1010}); r.Status != 404 {
			t.Fatalf("team 3 sees the rejected message: %d", r.Status)
		}
	})

	t.Run("Team appears as a subject token only in the task queue and Source intake", func(t *testing.T) {
		// journal publishes no subject; it serves and consumes these.
		var subjects []string
		for _, op := range rpc.Operations {
			subjects = append(subjects, rpc.Subject(op))
		}
		raw, err := os.ReadFile(filepath.Join(journalDir(t), "schemas", "consumers.json"))
		if err != nil {
			t.Fatal(err)
		}
		var consumers []struct {
			Durable        string   `json:"durable"`
			Stream         string   `json:"stream"`
			FilterSubjects []string `json:"filter_subjects"`
		}
		if err := json.Unmarshal(raw, &consumers); err != nil {
			t.Fatal(err)
		}
		if len(consumers) != 1 || consumers[0].Durable != natsio.Durable || consumers[0].Stream != natsio.Stream {
			t.Fatalf("consumers.json %+v, want the one durable journal on ALL_EVENT", consumers)
		}
		subjects = append(subjects, consumers[0].FilterSubjects...)
		token := regexp.MustCompile(`^[A-Za-z0-9_*-]{1,128}$`)
		for _, s := range subjects {
			if strings.HasPrefix(s, "flowai.") || !strings.HasSuffix(s, ".sub") {
				t.Fatalf("subject %q: no flowai. prefix and a .sub ending expected", s)
			}
			for _, part := range strings.Split(s, ".") {
				if !token.MatchString(part) {
					t.Fatalf("subject %q has invalid token %q", s, part)
				}
				if regexp.MustCompile(`^[0-9]+$`).MatchString(part) {
					t.Fatalf("subject %q carries an identifier token", s)
				}
			}
		}
		want := map[string]bool{"*.event.*.sub": true, "executor.event.*.*.sub": true}
		for _, f := range consumers[0].FilterSubjects {
			delete(want, f)
		}
		if len(want) != 0 {
			t.Fatalf("journal consumer lacks filters %v", want)
		}
		for _, op := range rpc.Operations {
			if strings.ContainsAny(op, "._") {
				t.Fatalf("operation token %q must be lowercase words joined by -", op)
			}
		}
	})
}
