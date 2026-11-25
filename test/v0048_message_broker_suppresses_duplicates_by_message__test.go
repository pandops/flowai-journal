package test

import (
	"testing"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

// Requirement: message-broker suppresses duplicates by publisher sequence
// (journal's consumer side).
func TestV0048MessageBrokerSuppressesDuplicatesByPublisherSequence(t *testing.T) {
	t.Run("Duplicate outside the window is ignored by the consumer", func(t *testing.T) {
		h := newHarness(t)
		m := h.svcMsg("task-manager", "queued", "3", map[string]any{"publisher_seq": 77, "task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "intake_id": 1})
		h.mustAck(m)
		stored := len(h.store.Events())
		// Two hours later, beyond JetStream's 2 minute duplicate window, the
		// same message arrives again as a new stream message.
		h.clock.Advance(2 * time.Hour)
		if d := h.deliver(m); d != engine.Ack {
			t.Fatalf("duplicate: %v, want ack", d)
		}
		if len(h.store.Events()) != stored {
			t.Fatalf("duplicate (task-manager, 77) stored a second entry")
		}
	})

	t.Run("Equal sequence numbers of two publishers are distinct messages", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		tm := h.svcMsg("task-manager", "queued", "3", map[string]any{"publisher_seq": 40, "task_id": 1010, "task_name": taskName(3, 1010), "attempt": 1, "intake_id": 1})
		ex := h.execMsg(3, 7, "created", map[string]any{"publisher_seq": 40, "task_id": 1010, "attempt": 1, "claim_seq": 1})
		ex.Header["Nats-Msg-Id"] = []string{"executor-3-7-40"}
		if tm.Header["Nats-Msg-Id"][0] != "task-manager-40" {
			t.Fatalf("task-manager Nats-Msg-Id %v", tm.Header["Nats-Msg-Id"])
		}
		start := len(h.store.Events())
		h.mustAck(tm)
		h.mustAck(ex)
		stored := h.store.Events()[start:]
		if len(stored) != 2 {
			t.Fatalf("stored %d, want both messages", len(stored))
		}
		if stored[0].PublisherKey != "task-manager" || stored[1].PublisherKey != "executor-3-7" || stored[0].PublisherSeq != 40 || stored[1].PublisherSeq != 40 {
			t.Fatalf("publisher keys %q/%d and %q/%d", stored[0].PublisherKey, stored[0].PublisherSeq, stored[1].PublisherKey, stored[1].PublisherSeq)
		}
	})
}
