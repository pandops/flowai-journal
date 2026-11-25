package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is the non-revealing not-found of every team-scoped read.
var ErrNotFound = errors.New("not found")

// Dashboard selection errors.
var (
	ErrInvalidPeriod   = errors.New("invalid period")
	ErrInvalidCalendar = errors.New("invalid calendar selection")
	ErrInvalidTimeZone = errors.New("invalid time zone")
)

// errUnknownExecutor keeps an Executor event unacknowledged until
// executor_registered for its Executor is stored.
var errUnknownExecutor = errors.New("executor not yet projected")

// DefaultHoldWindow is how long a log chunk or control result of a claim
// without claim_accepted waits before it is marked not_claim_holder.
const DefaultHoldWindow = 10 * time.Minute

// Options configures an Engine. Now and OnCommit are in-process seams:
// production wiring leaves Now unset (real clock).
type Options struct {
	Now        func() time.Time
	HoldWindow time.Duration
	// OnCommit is called after a committed write with the teams that got
	// new frames, so stream connections wake up.
	OnCommit func(teamIDs []int64)
	Logger   *slog.Logger
}

// Engine is journal's domain core.
type Engine struct {
	store Store
	opts  Options

	mu      sync.Mutex
	blocked map[string]uint64 // task key -> lowest unacknowledged stream sequence
}

// New returns an Engine over store.
func New(store Store, opts Options) *Engine {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.HoldWindow <= 0 {
		opts.HoldWindow = DefaultHoldWindow
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{store: store, opts: opts, blocked: map[string]uint64{}}
}

// Ready reports whether the store answers.
func (e *Engine) Ready(ctx context.Context) bool {
	return e.store.Ping(ctx) == nil
}

// secretKeys are field names whose values never enter journal's storage,
// replies, frames or logs, at any depth.
var secretKeys = map[string]bool{
	"claim_token": true, "token": true, "jwt": true, "api_key": true, "secret": true,
	"secrets": true, "value": true, "values": true, "litellm_key": true, "virtual_key": true,
	"password": true, "seed": true, "launch_parameters": true, "credentials": true,
	"provider_key": true, "source_key": true, "authorization": true,
}

func scrub(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if secretKeys[strings.ToLower(k)] {
				delete(t, k)
				continue
			}
			t[k] = scrub(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = scrub(t[i])
		}
		return t
	}
	return v
}

// message is one parsed inbound message.
type message struct {
	subject    string
	publisher  string // service name or "executor"
	key        string // dedupe key: service name or executor-<team>-<executor>
	seq        int64
	streamSeq  uint64 // ALL_EVENT stream sequence of the delivery
	kind       string
	executorID int64 // Executor events: from the subject
	teamHeader string
	payload    map[string]any
	occurredAt time.Time
}

func (m *message) id(field string) *int64 {
	v, ok := m.payload[field]
	if !ok {
		return nil
	}
	n, ok := jsonID(v)
	if !ok {
		return nil
	}
	return &n
}

func (m *message) str(field string) string {
	s, _ := m.payload[field].(string)
	return s
}

func jsonID(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	s := n.String()
	if s == "" || s[0] == '0' || strings.ContainsAny(s, ".eE-+") {
		return 0, false
	}
	i, err := strconv.ParseInt(s, 10, 64)
	return i, err == nil && i > 0
}

// parse validates the envelope; reject is the reason when the message is
// acknowledged without being stored as an event.
func parse(in Inbound) (m message, reject string) {
	m.subject = in.Subject
	parts := strings.Split(in.Subject, ".")
	switch {
	case len(parts) == 5 && parts[0] == "executor" && parts[1] == "event" && parts[4] == "sub":
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || id < 1 || strconv.FormatInt(id, 10) != parts[2] {
			return m, "invalid executor subject"
		}
		m.publisher, m.executorID, m.kind = "executor", id, parts[3]
	case len(parts) == 4 && parts[1] == "event" && parts[3] == "sub" && parts[0] != "executor":
		m.publisher, m.kind = parts[0], parts[2]
	default:
		return m, "subject is not an event subject"
	}
	if h := in.Header["team"]; len(h) == 1 {
		m.teamHeader = h[0]
	}
	dec := json.NewDecoder(bytes.NewReader(in.Data))
	dec.UseNumber()
	if err := dec.Decode(&m.payload); err != nil || m.payload == nil {
		return m, "payload is not a JSON object"
	}
	seq := m.id("publisher_seq")
	if seq == nil {
		return m, "missing publisher_seq"
	}
	m.seq = *seq
	if m.str("publisher") != m.publisher {
		return m, "publisher does not match the subject"
	}
	if m.str("kind") != m.kind {
		return m, "kind does not match the subject"
	}
	at, err := time.Parse(time.RFC3339Nano, m.str("occurred_at"))
	if err != nil {
		return m, "invalid occurred_at"
	}
	m.occurredAt = at.UTC()
	if m.publisher != "executor" {
		m.key = m.publisher
	}
	scrub(m.payload)
	return m, ""
}

// serviceTeam derives a service event's team from its team header.
func (m *message) serviceTeam() (int64, string) {
	payloadTeam := m.id("team_id")
	if _, present := m.payload["team_id"]; present && payloadTeam == nil {
		return 0, "invalid payload team_id"
	}
	if m.teamHeader == "admin" {
		if payloadTeam == nil {
			return 0, "admin message without payload team_id"
		}
		return *payloadTeam, ""
	}
	team, err := strconv.ParseInt(m.teamHeader, 10, 64)
	if err != nil || team < 1 || strconv.FormatInt(team, 10) != m.teamHeader {
		return 0, "missing or invalid team header"
	}
	if payloadTeam != nil && *payloadTeam != team {
		return 0, "payload team_id differs from the team header"
	}
	return team, ""
}

func (e *Engine) taskKeys(m *message, team int64) []string {
	task := m.id("task_id")
	if task == nil {
		return nil
	}
	var keys []string
	if team > 0 {
		keys = append(keys, fmt.Sprintf("t%d:%d", team, *task))
	}
	if m.executorID > 0 {
		keys = append(keys, fmt.Sprintf("x%d:%d", m.executorID, *task))
	}
	return keys
}

// waiting reports whether an earlier message of the same task is still
// unacknowledged.
func (e *Engine) waiting(keys []string, seq uint64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range keys {
		if b, ok := e.blocked[k]; ok && b < seq {
			return true
		}
	}
	return false
}

func (e *Engine) block(keys []string, seq uint64) {
	if seq == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range keys {
		if b, ok := e.blocked[k]; !ok || seq < b {
			e.blocked[k] = seq
		}
	}
}

func (e *Engine) unblock(keys []string, seq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, k := range keys {
		if e.blocked[k] == seq {
			delete(e.blocked, k)
		}
	}
}

// Handle stores one ALL_EVENT message and returns how to settle it.
func (e *Engine) Handle(ctx context.Context, in Inbound) Disposition {
	m, reason := parse(in)
	m.streamSeq = in.StreamSeq
	log := e.opts.Logger.With("subject", in.Subject, "stream_seq", in.StreamSeq)
	var team int64
	if reason == "" && m.publisher != "executor" {
		team, reason = m.serviceTeam()
	}
	if reason != "" {
		rej := Rejected{PublisherKey: m.key, PublisherSeq: m.seq, Subject: in.Subject, Reason: reason, At: e.opts.Now().UTC()}
		if m.publisher == "executor" {
			rej.PublisherKey = ""
		}
		if err := e.store.Write(ctx, func(tx Tx) error { return tx.RecordRejected(rej) }); err != nil {
			log.Warn("record rejected message", "error", err)
			return Nak
		}
		log.Warn("message rejected", "reason", reason)
		return Ack
	}
	if m.publisher == "executor" {
		if rec, ok, err := e.store.ExecutorByID(ctx, m.executorID); err == nil && ok && rec.Registered {
			team = rec.TeamID
		}
	}
	keys := e.taskKeys(&m, team)
	if in.StreamSeq > 0 && e.waiting(keys, in.StreamSeq) {
		e.block(keys, in.StreamSeq)
		return Nak
	}
	var teams []int64
	err := e.store.Write(ctx, func(tx Tx) error {
		var err error
		teams, err = e.apply(tx, &m, team)
		return err
	})
	if err != nil {
		e.block(e.taskKeys(&m, team), in.StreamSeq)
		if errors.Is(err, errUnknownExecutor) {
			log.Info("executor event waits for executor_registered", "executor_id", m.executorID)
		} else {
			log.Warn("store event", "error", err)
		}
		return Nak
	}
	e.unblock(keys, in.StreamSeq)
	e.unblock(e.taskKeys(&m, team), in.StreamSeq)
	if len(teams) > 0 && e.opts.OnCommit != nil {
		e.opts.OnCommit(teams)
	}
	return Ack
}

// apply stores m in tx and returns the teams that got frames.
func (e *Engine) apply(tx Tx, m *message, team int64) ([]int64, error) {
	if m.publisher == "executor" {
		rec, ok, err := tx.Executor(m.executorID)
		if err != nil {
			return nil, err
		}
		if !ok || !rec.Registered {
			return nil, errUnknownExecutor
		}
		team = rec.TeamID
		m.key = fmt.Sprintf("executor-%d-%d", team, m.executorID)
	}
	seen, err := tx.MessageSeen(m.key, m.seq)
	if err != nil || seen {
		return nil, err
	}
	id, err := tx.NextID()
	if err != nil {
		return nil, err
	}
	now := e.opts.Now().UTC()
	ev := Event{
		EventID: id, TeamID: team, Kind: m.kind, Source: m.publisher, PublisherKey: m.key,
		PublisherSeq: m.seq, TaskID: m.id("task_id"), Attempt: m.id("attempt"),
		ClaimSeq: m.id("claim_seq"), ExecutorID: m.id("executor_id"), ControlID: m.id("control_id"),
		OccurredAt: m.occurredAt, CommittedAt: now, StreamSeq: m.streamSeq,
	}
	if m.publisher == "executor" {
		ex := m.executorID
		ev.ExecutorID = &ex
	}
	// The stored payload: the published message without secret-named
	// fields; the envelope fields journal derives are rendered by View.
	for _, k := range []string{"publisher", "publisher_seq", "team_id", "kind", "occurred_at", "event_id"} {
		delete(m.payload, k)
	}
	if ev.Data, err = json.Marshal(m.payload); err != nil {
		return nil, err
	}
	if err := tx.InsertEvent(ev); err != nil {
		return nil, err
	}
	a := applier{e: e, tx: tx, ev: ev, m: m, now: now}
	if m.publisher == "executor" {
		err = a.executorEvent()
	} else {
		err = a.serviceEvent()
	}
	return a.teams, err
}

type applier struct {
	e     *Engine
	tx    Tx
	ev    Event
	m     *message
	now   time.Time
	teams []int64
}

func (a *applier) frame(frameID int64, frameType string, task *int64, data json.RawMessage) error {
	f := Frame{FrameID: frameID, TeamID: a.ev.TeamID, FrameType: frameType, TaskID: task,
		ExecutorID: a.ev.ExecutorID, ControlID: a.ev.ControlID, Data: data}
	if err := a.tx.InsertFrame(f); err != nil {
		return err
	}
	for _, t := range a.teams {
		if t == f.TeamID {
			return nil
		}
	}
	a.teams = append(a.teams, f.TeamID)
	return nil
}

func (a *applier) eventFrame(frameType string) error {
	var task *int64
	if frameType != FrameExecutorEvent {
		task = a.ev.TaskID
	}
	return a.frame(a.ev.EventID, frameType, task, a.ev.View())
}

func (a *applier) serviceEvent() error {
	ev := a.ev
	switch a.m.publisher {
	case "task-manager":
		if ev.TaskID == nil {
			return nil
		}
		switch ev.Kind {
		case "queued":
			return a.queued()
		case "claim_accepted":
			return a.claimAccepted()
		case "control_requested":
			return a.eventFrame(FrameControlEvent)
		case "released", "retry_scheduled":
			if err := a.endClaim(); err != nil {
				return err
			}
			if err := a.taskState(StateQueuedTask, true); err != nil {
				return err
			}
			return a.eventFrame(FrameTaskEvent)
		case "cancelled":
			if err := a.endClaim(); err != nil {
				return err
			}
			if err := a.taskState("cancelled", true); err != nil {
				return err
			}
			return a.eventFrame(FrameTaskEvent)
		default:
			return a.eventFrame(FrameTaskEvent)
		}
	case "executor-registry":
		return a.registryEvent()
	}
	return nil
}

// StateQueuedTask is the dashboard state of a waiting task.
const StateQueuedTask = "queued"

func (a *applier) queued() error {
	ev := a.ev
	s, ok, err := a.tx.Task(ev.TeamID, *ev.TaskID)
	if err != nil {
		return err
	}
	frameType := FrameTaskEvent
	if !ok {
		at, id := ev.OccurredAt, ev.EventID
		s = TaskSummary{TeamID: ev.TeamID, TaskID: *ev.TaskID, FirstQueuedAt: &at, FirstQueuedID: &id}
		frameType = FrameTaskCommitted
	}
	s.LatestKind = StateQueuedTask
	if ev.Attempt != nil {
		s.LatestAttempt = *ev.Attempt
	}
	if ev.ClaimSeq != nil {
		s.LatestClaimSeq = *ev.ClaimSeq
	}
	s.LatestEventID = ev.EventID
	if err := a.tx.PutTask(s); err != nil {
		return err
	}
	return a.eventFrame(frameType)
}

// taskState applies a lifecycle state to the task summary. Service
// decisions apply as published; an Executor's state applies only for the
// task's current attempt and claim, and only from the holder that
// claim_accepted named while that claim is open. A claim without
// claim_accepted has no holder, so its events move nothing when they
// arrive; claim_accepted then replays its holder's own (replayEarlyStates).
func (a *applier) taskState(state string, service bool) error {
	ev := a.ev
	s, ok, err := a.tx.Task(ev.TeamID, *ev.TaskID)
	if err != nil || !ok {
		return err
	}
	if !service {
		if ev.Attempt == nil || ev.ClaimSeq == nil || *ev.Attempt != s.LatestAttempt || *ev.ClaimSeq < s.LatestClaimSeq {
			return nil
		}
		c, held, err := a.tx.Claim(ev.TeamID, *ev.TaskID, *ev.Attempt, *ev.ClaimSeq)
		if err != nil {
			return err
		}
		if !held || c.Ended || c.ExecutorID != *ev.ExecutorID {
			return nil
		}
		s.LatestClaimSeq = *ev.ClaimSeq
	}
	if a.m.kind == "retry_scheduled" {
		if next := a.m.id("next_attempt"); next != nil {
			s.LatestAttempt = *next
		}
	}
	s.LatestKind = state
	s.LatestEventID = ev.EventID
	return a.tx.PutTask(s)
}

func (a *applier) endClaim() error {
	ev := a.ev
	if ev.TaskID == nil || ev.Attempt == nil || ev.ClaimSeq == nil {
		return nil
	}
	c, ok, err := a.tx.Claim(ev.TeamID, *ev.TaskID, *ev.Attempt, *ev.ClaimSeq)
	if err != nil || !ok || c.Ended {
		return err
	}
	if ev.Source == "executor" && c.ExecutorID != *ev.ExecutorID {
		return nil
	}
	c.Ended = true
	return a.tx.PutClaim(c)
}

func (a *applier) claimAccepted() error {
	ev := a.ev
	if ev.Attempt == nil || ev.ClaimSeq == nil || ev.ExecutorID == nil {
		return a.eventFrame(FrameTaskEvent)
	}
	c := Claim{TeamID: ev.TeamID, TaskID: *ev.TaskID, Attempt: *ev.Attempt, ClaimSeq: *ev.ClaimSeq, ExecutorID: *ev.ExecutorID, EventID: ev.EventID}
	if err := a.tx.PutClaim(c); err != nil {
		return err
	}
	// task-manager accepts the holder's created: the task is created on
	// this claim from now on.
	if s, ok, err := a.tx.Task(c.TeamID, c.TaskID); err != nil {
		return err
	} else if ok && c.Attempt == s.LatestAttempt && c.ClaimSeq >= s.LatestClaimSeq {
		s.LatestKind, s.LatestClaimSeq, s.LatestEventID = "created", c.ClaimSeq, ev.EventID
		if err := a.tx.PutTask(s); err != nil {
			return err
		}
	}
	if err := a.replayEarlyStates(c); err != nil {
		return err
	}
	if err := a.eventFrame(FrameTaskEvent); err != nil {
		return err
	}
	held, err := a.tx.HeldFor(c.TeamID, c.TaskID, c.Attempt, c.ClaimSeq)
	if err != nil {
		return err
	}
	for _, h := range held {
		if h.ExecutorID != c.ExecutorID || a.now.Sub(h.StoredAt) > a.e.opts.HoldWindow {
			h.Status = StatusNotClaimHolder
			if err := a.tx.DecideHeld(h); err != nil {
				return err
			}
			continue
		}
		// The accepted item gets the next commit-sequence value, so its
		// frame follows every cursor issued before the acceptance.
		frameID, err := a.tx.NextID()
		if err != nil {
			return err
		}
		if err := a.accept(&h, frameID); err != nil {
			return err
		}
		if err := a.tx.DecideHeld(h); err != nil {
			return err
		}
	}
	return nil
}

// replayEarlyStates applies, in commit order, the lifecycle events that
// the holder c names published on c before claim_accepted reached journal.
// task-manager publishes claim_accepted from its outbox after commit, while
// the holder publishes as soon as it has the claim token, so its events can
// arrive first. They are already stored events, so this survives restarts;
// events of any other Executor or claim_seq stay without effect.
func (a *applier) replayEarlyStates(c Claim) error {
	early, err := a.tx.ClaimStateEvents(c.TeamID, c.TaskID, c.Attempt, c.ClaimSeq, c.ExecutorID, a.ev.EventID)
	if err != nil {
		return err
	}
	saved := a.ev
	defer func() { a.ev = saved }()
	for _, ev := range early {
		a.ev = ev
		if err := a.executorState(); err != nil {
			return err
		}
	}
	return nil
}

// accept turns a held log chunk or control result into served content.
func (a *applier) accept(h *Held, frameID int64) error {
	h.Status = StatusAccepted
	h.FrameID = &frameID
	task := h.TaskID
	if h.Kind != "log" {
		return a.frameFor(h.TeamID, frameID, FrameControlEvent, &task, h.Payload)
	}
	var p struct {
		Stream       string `json:"stream"`
		Text         string `json:"text"`
		PublisherSeq int64  `json:"publisher_seq"`
		OccurredAt   string `json:"occurred_at"`
	}
	if err := json.Unmarshal(h.Payload, &p); err != nil {
		return err
	}
	offset, err := a.tx.NextLogOffset(h.TeamID, h.TaskID)
	if err != nil {
		return err
	}
	h.LogOffset = &offset
	at, _ := time.Parse(time.RFC3339Nano, p.OccurredAt)
	entry := LogEntry{EventID: frameID, TeamID: h.TeamID, TaskID: h.TaskID, Attempt: h.Attempt, ClaimSeq: h.ClaimSeq,
		ExecutorID: h.ExecutorID, LogOffset: offset, Stream: p.Stream, Text: p.Text, PublisherSeq: p.PublisherSeq, OccurredAt: at.UTC()}
	if err := a.tx.InsertLog(entry); err != nil {
		return err
	}
	data, err := json.Marshal(entry.View())
	if err != nil {
		return err
	}
	return a.frameFor(h.TeamID, frameID, FrameTaskLog, &task, data)
}

func (a *applier) frameFor(team, frameID int64, frameType string, task *int64, data json.RawMessage) error {
	saved := a.ev.TeamID
	a.ev.TeamID = team
	defer func() { a.ev.TeamID = saved }()
	return a.frame(frameID, frameType, task, data)
}

// registryEvent keeps the Executor projection from executor-registry.
func (a *applier) registryEvent() error {
	ev := a.ev
	if ev.ExecutorID == nil {
		return nil
	}
	rec, ok, err := a.tx.Executor(*ev.ExecutorID)
	if err != nil {
		return err
	}
	switch ev.Kind {
	case "executor_registered":
		if ok && rec.Registered {
			// Ownership is immutable: a second registration changes nothing.
			return a.eventFrameIfOwner(rec)
		}
		rec = ExecutorRecord{ExecutorID: *ev.ExecutorID, TeamID: ev.TeamID, Registered: true,
			Tag: a.m.str("execution_tag"), ExecutorType: a.m.str("executor_type")}
		if err := a.tx.PutExecutor(rec); err != nil {
			return err
		}
		return a.eventFrame(FrameExecutorEvent)
	}
	if !ok || !rec.Registered || rec.TeamID != ev.TeamID {
		return nil
	}
	at := ev.OccurredAt
	switch ev.Kind {
	case "executor_removed":
		rec.Removed = true
	case "connection_lost":
		rec.ConnectionState, rec.ConnectionChangedAt = StateConnectionLost, &at
	case "connection_restored":
		rec.ConnectionState, rec.ConnectionChangedAt = StateConnected, &at
	case "executor_lost":
		rec.ConnectionState, rec.ConnectionChangedAt = StateExecutorLost, &at
	case "executor_status":
		switch st := a.m.str("connection_state"); st {
		case StateNeverConnected, StateConnected, StateConnectionLost, StateExecutorLost:
			rec.ConnectionState, rec.ConnectionChangedAt = st, &at
		}
		rec.HeldClaims = a.m.count("held_claims")
		rec.AvailableCapacity = a.m.count("available_capacity")
		rec.MaxParallelTasks = a.m.count("max_parallel_tasks")
	case "ip_changed":
		ip := a.m.str("new_ip")
		if ip != "" {
			rec.ReportedIP, rec.IPChangedAt = &ip, &at
		}
	}
	if err := a.tx.PutExecutor(rec); err != nil {
		return err
	}
	switch ev.Kind {
	case "connection_lost", "connection_restored", "executor_lost":
		if err := a.linkOpenClaims(*ev.ExecutorID); err != nil {
			return err
		}
	}
	return a.eventFrame(FrameExecutorEvent)
}

func (a *applier) eventFrameIfOwner(rec ExecutorRecord) error {
	if rec.TeamID != a.ev.TeamID {
		return nil
	}
	return a.eventFrame(FrameExecutorEvent)
}

func (m *message) count(field string) *int64 {
	n, ok := m.payload[field].(json.Number)
	if !ok {
		return nil
	}
	i, err := n.Int64()
	if err != nil || i < 0 {
		return nil
	}
	return &i
}

// linkOpenClaims adds the event to the timelines of the tasks whose open
// claims the Executor holds.
func (a *applier) linkOpenClaims(executorID int64) error {
	claims, err := a.tx.OpenClaimsOf(executorID)
	if err != nil {
		return err
	}
	for _, c := range claims {
		if c.TeamID != a.ev.TeamID {
			continue
		}
		if err := a.tx.LinkTask(c.TeamID, c.TaskID, c.Attempt, c.ClaimSeq, a.ev.EventID); err != nil {
			return err
		}
	}
	return nil
}

func (a *applier) executorEvent() error {
	ev := a.ev
	if ev.TaskID == nil {
		switch ev.Kind {
		case "executor_connection_lost", "executor_connection_restored":
			if err := a.linkOpenClaims(*ev.ExecutorID); err != nil {
				return err
			}
		}
		return a.eventFrame(FrameExecutorEvent)
	}
	switch ev.Kind {
	case "log", "control_acknowledged", "control_failed":
		return a.attribute()
	case "created", "pending", "running", "finished", "failed", "cancelled", "released":
		if err := a.executorState(); err != nil {
			return err
		}
	}
	return a.eventFrame(FrameTaskEvent)
}

// ExecutorStateKinds are the Executor lifecycle kinds that move a task's
// state on its claim.
var ExecutorStateKinds = []string{"created", "pending", "running", "finished", "failed", "cancelled", "released"}

// executorState applies an Executor lifecycle event to the task state and,
// for a terminal kind, ends its claim.
func (a *applier) executorState() error {
	state := a.ev.Kind
	if state == "released" {
		state = StateQueuedTask
	}
	if err := a.taskState(state, false); err != nil {
		return err
	}
	switch a.ev.Kind {
	case "finished", "failed", "cancelled", "released":
		return a.endClaim()
	}
	return nil
}

// attribute decides a log chunk or control result by claim_accepted.
func (a *applier) attribute() error {
	ev := a.ev
	if ev.Attempt == nil || ev.ClaimSeq == nil {
		return a.tx.PutHeld(Held{EventID: ev.EventID, TeamID: ev.TeamID, TaskID: *ev.TaskID, ExecutorID: *ev.ExecutorID,
			Kind: ev.Kind, StoredAt: a.now, Status: StatusNotClaimHolder})
	}
	h := Held{EventID: ev.EventID, TeamID: ev.TeamID, TaskID: *ev.TaskID, Attempt: *ev.Attempt, ClaimSeq: *ev.ClaimSeq,
		ExecutorID: *ev.ExecutorID, Kind: ev.Kind, StoredAt: a.now, Status: StatusHeld, Payload: ev.View()}
	c, ok, err := a.tx.Claim(ev.TeamID, *ev.TaskID, *ev.Attempt, *ev.ClaimSeq)
	if err != nil {
		return err
	}
	if ok {
		if c.ExecutorID != *ev.ExecutorID {
			h.Status = StatusNotClaimHolder
		} else if err := a.accept(&h, ev.EventID); err != nil {
			return err
		}
	}
	return a.tx.PutHeld(h)
}

// ExpireHeld marks every held item older than the hold window
// not_claim_holder.
func (e *Engine) ExpireHeld(ctx context.Context) error {
	cutoff := e.opts.Now().UTC().Add(-e.opts.HoldWindow)
	return e.store.Write(ctx, func(tx Tx) error {
		held, err := tx.HeldBefore(cutoff)
		if err != nil {
			return err
		}
		for _, h := range held {
			h.Status = StatusNotClaimHolder
			if err := tx.DecideHeld(h); err != nil {
				return err
			}
		}
		return nil
	})
}

// ClaimHolder returns the Executor that claim_accepted named for a claim.
func (e *Engine) ClaimHolder(ctx context.Context, teamID, taskID, attempt, claimSeq int64) (int64, bool, error) {
	c, ok, err := e.store.ClaimOf(ctx, teamID, taskID, attempt, claimSeq)
	if err != nil || !ok {
		return 0, false, err
	}
	return c.ExecutorID, true, nil
}
