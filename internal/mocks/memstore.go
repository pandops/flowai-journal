// Package mocks holds in-process fakes of journal's boundaries for unit
// tests: an in-memory engine.Store and a broker status fake. Production
// wiring never uses them.
package mocks

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

type pubKey struct {
	key string
	seq int64
}

type claimKey struct{ team, task, attempt, claim int64 }

type taskKey struct{ team, task int64 }

type link struct{ attempt, claim int64 }

type memState struct {
	seen      map[pubKey]bool
	rejected  []engine.Rejected
	nextID    int64
	events    []engine.Event
	frames    []engine.Frame
	links     map[taskKey]map[int64]link
	claims    map[claimKey]engine.Claim
	held      map[int64]engine.Held
	offsets   map[taskKey]int64
	logs      []engine.LogEntry
	executors map[int64]engine.ExecutorRecord
	tasks     map[taskKey]engine.TaskSummary
}

func newState() *memState {
	return &memState{
		seen: map[pubKey]bool{}, links: map[taskKey]map[int64]link{},
		claims: map[claimKey]engine.Claim{}, held: map[int64]engine.Held{},
		offsets: map[taskKey]int64{}, executors: map[int64]engine.ExecutorRecord{},
		tasks: map[taskKey]engine.TaskSummary{},
	}
}

func (s *memState) clone() *memState {
	c := newState()
	for k, v := range s.seen {
		c.seen[k] = v
	}
	c.rejected = append([]engine.Rejected(nil), s.rejected...)
	c.nextID = s.nextID
	c.events = append([]engine.Event(nil), s.events...)
	c.frames = append([]engine.Frame(nil), s.frames...)
	for k, v := range s.links {
		m := map[int64]link{}
		for id, l := range v {
			m[id] = l
		}
		c.links[k] = m
	}
	for k, v := range s.claims {
		c.claims[k] = v
	}
	for k, v := range s.held {
		c.held[k] = v
	}
	for k, v := range s.offsets {
		c.offsets[k] = v
	}
	c.logs = append([]engine.LogEntry(nil), s.logs...)
	for k, v := range s.executors {
		c.executors[k] = v
	}
	for k, v := range s.tasks {
		c.tasks[k] = v
	}
	return c
}

// MemStore is an in-memory engine.Store. Write transactions are
// serialized and roll back on error. FailWrites and FailReads make the
// next calls fail, to simulate an unavailable database.
type MemStore struct {
	mu         sync.Mutex
	state      *memState
	FailWrites bool
	FailReads  bool
	// FailCommit runs the next transactions and then rolls them back, as
	// a failed COMMIT would.
	FailCommit bool
	// IDsBurned counts sequence values consumed by rolled-back writes,
	// as a PostgreSQL sequence would.
	IDsBurned int64
}

// NewMemStore returns an empty store.
func NewMemStore() *MemStore { return &MemStore{state: newState()} }

var errUnavailable = errors.New("memstore: unavailable")

// Write runs fn in one transaction.
func (m *MemStore) Write(ctx context.Context, fn func(engine.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailWrites {
		return errUnavailable
	}
	tx := &memTx{s: m.state.clone()}
	err := fn(tx)
	if err == nil && m.FailCommit {
		err = errUnavailable
	}
	if err != nil {
		// A sequence is not transactional: keep the advanced counter.
		m.IDsBurned += tx.s.nextID - m.state.nextID
		m.state.nextID = tx.s.nextID
		return err
	}
	m.state = tx.s
	return nil
}

// Events returns every stored event in commit order.
func (m *MemStore) Events() []engine.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]engine.Event(nil), m.state.events...)
}

// Rejected returns every rejected message record.
func (m *MemStore) Rejected() []engine.Rejected {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]engine.Rejected(nil), m.state.rejected...)
}

// AllFrames returns every frame of every team.
func (m *MemStore) AllFrames() []engine.Frame {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]engine.Frame(nil), m.state.frames...)
}

type memTx struct{ s *memState }

func (t *memTx) MessageSeen(key string, seq int64) (bool, error) {
	return t.s.seen[pubKey{key, seq}], nil
}

func (t *memTx) RecordRejected(r engine.Rejected) error {
	if r.PublisherKey != "" && r.PublisherSeq > 0 {
		t.s.seen[pubKey{r.PublisherKey, r.PublisherSeq}] = true
	}
	t.s.rejected = append(t.s.rejected, r)
	return nil
}

func (t *memTx) NextID() (int64, error) {
	t.s.nextID++
	return t.s.nextID, nil
}

func (t *memTx) InsertEvent(e engine.Event) error {
	k := pubKey{e.PublisherKey, e.PublisherSeq}
	if t.s.seen[k] {
		return fmt.Errorf("duplicate (%s, %d)", e.PublisherKey, e.PublisherSeq)
	}
	t.s.seen[k] = true
	t.s.events = append(t.s.events, e)
	return nil
}

func (t *memTx) InsertFrame(f engine.Frame) error {
	t.s.frames = append(t.s.frames, f)
	return nil
}

func (t *memTx) LinkTask(team, task, attempt, claim, eventID int64) error {
	k := taskKey{team, task}
	if t.s.links[k] == nil {
		t.s.links[k] = map[int64]link{}
	}
	t.s.links[k][eventID] = link{attempt, claim}
	return nil
}

func (t *memTx) Claim(team, task, attempt, claim int64) (engine.Claim, bool, error) {
	c, ok := t.s.claims[claimKey{team, task, attempt, claim}]
	return c, ok, nil
}

func (t *memTx) PutClaim(c engine.Claim) error {
	t.s.claims[claimKey{c.TeamID, c.TaskID, c.Attempt, c.ClaimSeq}] = c
	return nil
}

func (t *memTx) OpenClaimsOf(executorID int64) ([]engine.Claim, error) {
	var out []engine.Claim
	for _, c := range t.s.claims {
		if c.ExecutorID == executorID && !c.Ended {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (t *memTx) ClaimStateEvents(team, task, attempt, claim, executor, before int64) ([]engine.Event, error) {
	var out []engine.Event
	for _, e := range t.s.events {
		if e.TeamID != team || e.Source != "executor" || e.EventID >= before || !slices.Contains(engine.ExecutorStateKinds, e.Kind) ||
			e.TaskID == nil || *e.TaskID != task || e.Attempt == nil || *e.Attempt != attempt ||
			e.ClaimSeq == nil || *e.ClaimSeq != claim || e.ExecutorID == nil || *e.ExecutorID != executor {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (t *memTx) PutHeld(h engine.Held) error {
	t.s.held[h.EventID] = h
	return nil
}

func (t *memTx) HeldFor(team, task, attempt, claim int64) ([]engine.Held, error) {
	var out []engine.Held
	for _, h := range t.s.held {
		if h.Status == engine.StatusHeld && h.TeamID == team && h.TaskID == task && h.Attempt == attempt && h.ClaimSeq == claim {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (t *memTx) HeldBefore(at time.Time) ([]engine.Held, error) {
	var out []engine.Held
	for _, h := range t.s.held {
		if h.Status == engine.StatusHeld && h.StoredAt.Before(at) {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EventID < out[j].EventID })
	return out, nil
}

func (t *memTx) DecideHeld(h engine.Held) error {
	t.s.held[h.EventID] = h
	return nil
}

func (t *memTx) NextLogOffset(team, task int64) (int64, error) {
	k := taskKey{team, task}
	t.s.offsets[k]++
	return t.s.offsets[k], nil
}

func (t *memTx) InsertLog(l engine.LogEntry) error {
	t.s.logs = append(t.s.logs, l)
	return nil
}

func (t *memTx) Executor(id int64) (engine.ExecutorRecord, bool, error) {
	r, ok := t.s.executors[id]
	return r, ok, nil
}

func (t *memTx) PutExecutor(r engine.ExecutorRecord) error {
	t.s.executors[r.ExecutorID] = r
	return nil
}

func (t *memTx) Task(team, task int64) (engine.TaskSummary, bool, error) {
	s, ok := t.s.tasks[taskKey{team, task}]
	return s, ok, nil
}

func (t *memTx) PutTask(s engine.TaskSummary) error {
	t.s.tasks[taskKey{s.TeamID, s.TaskID}] = s
	return nil
}

// ---- Reader ----

func (m *MemStore) read() (*memState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailReads {
		return nil, errUnavailable
	}
	return m.state, nil
}

func (m *MemStore) Ping(ctx context.Context) error {
	if _, err := m.read(); err != nil {
		return err
	}
	return nil
}

func (m *MemStore) ExecutorByID(ctx context.Context, id int64) (engine.ExecutorRecord, bool, error) {
	s, err := m.read()
	if err != nil {
		return engine.ExecutorRecord{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := s.executors[id]
	return r, ok, nil
}

func (m *MemStore) ClaimOf(ctx context.Context, team, task, attempt, claim int64) (engine.Claim, bool, error) {
	s, err := m.read()
	if err != nil {
		return engine.Claim{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := s.claims[claimKey{team, task, attempt, claim}]
	return c, ok, nil
}

func (m *MemStore) TaskExists(ctx context.Context, team, task int64) (bool, error) {
	s, err := m.read()
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range s.events {
		if e.TeamID == team && e.TaskID != nil && *e.TaskID == task {
			return true, nil
		}
	}
	return false, nil
}

func (m *MemStore) ControlExists(ctx context.Context, team, task, control int64) (bool, error) {
	s, err := m.read()
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range s.events {
		if e.TeamID == team && e.TaskID != nil && *e.TaskID == task && e.ControlID != nil && *e.ControlID == control {
			return true, nil
		}
	}
	return false, nil
}

func (s *memState) hidden(e engine.Event) bool {
	if e.Kind == "log" {
		return true
	}
	if h, ok := s.held[e.EventID]; ok && h.Status != engine.StatusAccepted {
		return true
	}
	return false
}

func (m *MemStore) ListEvents(ctx context.Context, q engine.EventQuery) ([]engine.Event, error) {
	s, err := m.read()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []engine.Event
	for _, e := range s.events {
		if e.TeamID != q.TeamID || e.EventID <= q.AfterID || s.hidden(e) {
			continue
		}
		if q.ExecutorHistory {
			if q.ExecutorID == nil || e.ExecutorID == nil || *e.ExecutorID != *q.ExecutorID {
				continue
			}
			if e.Source != "executor-registry" && (e.Source != "executor" || e.TaskID != nil) {
				continue
			}
		}
		if q.TaskID != nil && !q.ExecutorHistory {
			l, linked := s.links[taskKey{q.TeamID, *q.TaskID}][e.EventID]
			own := e.TaskID != nil && *e.TaskID == *q.TaskID
			if !own && !linked {
				continue
			}
			if !own {
				task, attempt, claim := *q.TaskID, l.attempt, l.claim
				e.TaskID, e.Attempt, e.ClaimSeq = &task, &attempt, &claim
			}
			if e.Kind == "ip_changed" {
				continue
			}
		}
		if q.ControlID != nil && (e.ControlID == nil || *e.ControlID != *q.ControlID || !isControlKind(e.Kind)) {
			continue
		}
		out = append(out, e)
		if q.Limit > 0 && len(out) >= q.Limit {
			break
		}
	}
	return out, nil
}

func isControlKind(kind string) bool {
	return kind == "control_requested" || kind == "control_acknowledged" || kind == "control_failed"
}

func (m *MemStore) ListLogs(ctx context.Context, team, task, after int64, limit int) ([]engine.LogEntry, error) {
	s, err := m.read()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []engine.LogEntry
	for _, l := range s.logs {
		if l.TeamID == team && l.TaskID == task && l.LogOffset > after {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LogOffset < out[j].LogOffset })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) ListExecutors(ctx context.Context, team int64) ([]engine.ExecutorRecord, error) {
	s, err := m.read()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []engine.ExecutorRecord
	for _, r := range s.executors {
		if r.TeamID == team && r.Registered && !r.Removed {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecutorID < out[j].ExecutorID })
	return out, nil
}

func (m *MemStore) ListTasks(ctx context.Context, team int64, from, to time.Time) ([]engine.TaskSummary, error) {
	s, err := m.read()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []engine.TaskSummary
	for _, t := range s.tasks {
		if t.TeamID != team || t.FirstQueuedAt == nil {
			continue
		}
		if t.FirstQueuedAt.Before(from) || !t.FirstQueuedAt.Before(to) {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out, nil
}

func (m *MemStore) ListFrames(ctx context.Context, team, after int64, task *int64, limit int) ([]engine.Frame, error) {
	s, err := m.read()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []engine.Frame
	for _, f := range s.frames {
		if f.TeamID != team || f.FrameID <= after {
			continue
		}
		if task != nil && (f.TaskID == nil || *f.TaskID != *task) {
			continue
		}
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FrameID < out[j].FrameID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) MaxFrameID(ctx context.Context, team int64) (int64, error) {
	s, err := m.read()
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var max int64
	for _, f := range s.frames {
		if f.TeamID == team && f.FrameID > max {
			max = f.FrameID
		}
	}
	return max, nil
}

func (m *MemStore) HeldStatus(ctx context.Context, eventID int64) (string, bool, error) {
	s, err := m.read()
	if err != nil {
		return "", false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := s.held[eventID]
	return h.Status, ok, nil
}
