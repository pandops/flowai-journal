// Package engine holds journal's domain logic: ingesting ALL_EVENT
// messages into immutable stored events, attributing logs and control
// results to claim holders, keeping the Executor and task projections,
// and answering the team-scoped reads. Persistence goes through Store,
// implemented by pgstore in production.
package engine

import (
	"context"
	"encoding/json"
	"time"
)

// Disposition tells the consumer loop how to settle one JetStream message.
type Disposition int

const (
	// Ack: the message is committed (or was already committed, or was
	// recorded as rejected).
	Ack Disposition = iota + 1
	// Nak: the message cannot be committed yet (unknown Executor, an
	// earlier message of the same task is still uncommitted, or a
	// storage failure); JetStream redelivers it.
	Nak
)

func (d Disposition) String() string {
	switch d {
	case Ack:
		return "ack"
	case Nak:
		return "nak"
	default:
		return "unknown"
	}
}

// Inbound is one message delivered by the journal durable consumer.
type Inbound struct {
	Subject   string
	Header    map[string][]string
	Data      []byte
	StreamSeq uint64
}

// Attribution states of a log chunk or Executor control result.
const (
	StatusHeld           = "held"
	StatusAccepted       = "accepted"
	StatusNotClaimHolder = "not_claim_holder"
)

// Frame types of the team WebSocket stream.
const (
	FrameTaskCommitted = "task_committed"
	FrameTaskEvent     = "task_event"
	FrameControlEvent  = "control_event"
	FrameExecutorEvent = "executor_event"
	FrameTaskLog       = "task_log"
)

// Connection states served on ui.executor-states.
const (
	StateNeverConnected = "never_connected"
	StateConnected      = "connected"
	StateConnectionLost = "connection_lost"
	StateExecutorLost   = "executor_lost"
)

// Event is one stored, immutable event. EventID is journal's global
// commit sequence value assigned at commit.
type Event struct {
	EventID      int64
	TeamID       int64
	Kind         string
	Source       string // publishing service name, or "executor"
	PublisherKey string // dedupe key: service name or executor-<team>-<executor>
	PublisherSeq int64
	TaskID       *int64
	Attempt      *int64
	ClaimSeq     *int64
	ExecutorID   *int64
	ControlID    *int64
	OccurredAt   time.Time
	CommittedAt  time.Time
	StreamSeq    uint64
	Data         json.RawMessage
}

// Rejected records a message journal acknowledged without storing it as
// an event (malformed, team mismatch, schema violation).
type Rejected struct {
	PublisherKey string
	PublisherSeq int64
	Subject      string
	Reason       string
	At           time.Time
}

// Claim is the claim-holder record taken only from task-manager's
// claim_accepted.
type Claim struct {
	TeamID     int64
	TaskID     int64
	Attempt    int64
	ClaimSeq   int64
	ExecutorID int64
	EventID    int64
	Ended      bool
}

// Held is a log chunk or Executor control result whose attribution is
// decided by claim_accepted.
type Held struct {
	EventID    int64
	TeamID     int64
	TaskID     int64
	Attempt    int64
	ClaimSeq   int64
	ExecutorID int64
	Kind       string // "log", "control_acknowledged", "control_failed"
	StoredAt   time.Time
	Status     string
	LogOffset  *int64
	FrameID    *int64
	// Payload is the stored event's reply view, used when it is accepted.
	Payload json.RawMessage
}

// ExecutorRecord is journal's projection of one Executor.
type ExecutorRecord struct {
	ExecutorID          int64
	TeamID              int64
	Tag                 string
	ExecutorType        string
	Registered          bool
	Removed             bool
	ConnectionState     string
	ConnectionChangedAt *time.Time
	HeldClaims          *int64
	AvailableCapacity   *int64
	MaxParallelTasks    *int64
	ReportedIP          *string
	IPChangedAt         *time.Time
}

// TaskSummary is journal's per-task projection for dashboards and links.
type TaskSummary struct {
	TeamID         int64
	TaskID         int64
	FirstQueuedAt  *time.Time
	FirstQueuedID  *int64
	LatestKind     string
	LatestAttempt  int64
	LatestClaimSeq int64
	LatestEventID  int64
}

// Frame is one team stream frame.
type Frame struct {
	FrameID    int64
	TeamID     int64
	FrameType  string
	TaskID     *int64
	ExecutorID *int64
	ControlID  *int64
	Data       json.RawMessage
}

// LogEntry is one accepted log chunk.
type LogEntry struct {
	EventID      int64
	TeamID       int64
	TaskID       int64
	Attempt      int64
	ClaimSeq     int64
	ExecutorID   int64
	LogOffset    int64
	Stream       string
	Text         string
	PublisherSeq int64
	OccurredAt   time.Time
}

// Tx is one serialized write transaction of the store.
type Tx interface {
	MessageSeen(publisherKey string, publisherSeq int64) (bool, error)
	RecordRejected(Rejected) error
	NextID() (int64, error)
	InsertEvent(Event) error
	InsertFrame(Frame) error
	// LinkTask adds an event without its own task (an Executor connection
	// observation) to a task timeline under the claim it held.
	LinkTask(teamID, taskID, attempt, claimSeq, eventID int64) error

	Claim(teamID, taskID, attempt, claimSeq int64) (Claim, bool, error)
	PutClaim(Claim) error
	OpenClaimsOf(executorID int64) ([]Claim, error)
	// ClaimStateEvents returns, in event_id order, the stored Executor
	// lifecycle events (ExecutorStateKinds) that executorID published on
	// the claim with event_id below beforeID.
	ClaimStateEvents(teamID, taskID, attempt, claimSeq, executorID, beforeID int64) ([]Event, error)

	PutHeld(Held) error
	HeldFor(teamID, taskID, attempt, claimSeq int64) ([]Held, error)
	HeldBefore(t time.Time) ([]Held, error)
	DecideHeld(Held) error
	NextLogOffset(teamID, taskID int64) (int64, error)
	InsertLog(LogEntry) error

	Executor(executorID int64) (ExecutorRecord, bool, error)
	PutExecutor(ExecutorRecord) error

	Task(teamID, taskID int64) (TaskSummary, bool, error)
	PutTask(TaskSummary) error
}

// EventQuery selects a page of events in ascending event_id order.
type EventQuery struct {
	TeamID     int64
	TaskID     *int64
	ExecutorID *int64
	ControlID  *int64
	AfterID    int64
	Limit      int
	// ExecutorHistory selects executor-registry events about ExecutorID
	// and the Executor's own non-task events.
	ExecutorHistory bool
}

// Reader holds the read side of the store. Every method applies the
// team predicate inside the query.
type Reader interface {
	Ping(ctx context.Context) error
	ExecutorByID(ctx context.Context, executorID int64) (ExecutorRecord, bool, error)
	ClaimOf(ctx context.Context, teamID, taskID, attempt, claimSeq int64) (Claim, bool, error)
	TaskExists(ctx context.Context, teamID, taskID int64) (bool, error)
	ControlExists(ctx context.Context, teamID, taskID, controlID int64) (bool, error)
	ListEvents(ctx context.Context, q EventQuery) ([]Event, error)
	ListLogs(ctx context.Context, teamID, taskID, afterOffset int64, limit int) ([]LogEntry, error)
	ListExecutors(ctx context.Context, teamID int64) ([]ExecutorRecord, error)
	ListTasks(ctx context.Context, teamID int64, queuedFrom, queuedTo time.Time) ([]TaskSummary, error)
	ListFrames(ctx context.Context, teamID, afterFrameID int64, taskID *int64, limit int) ([]Frame, error)
	MaxFrameID(ctx context.Context, teamID int64) (int64, error)
	HeldStatus(ctx context.Context, eventID int64) (string, bool, error)
}

// Store is journal's persistence boundary.
type Store interface {
	Reader
	Write(ctx context.Context, fn func(Tx) error) error
}
