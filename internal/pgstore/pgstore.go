// Package pgstore is journal's PostgreSQL store in its own schema.
package pgstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pandops/flowai-journal/internal/engine"
)

//go:embed migrations/*.sql
var migrations embed.FS

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// writeLock serializes journal's write transactions so the commit
// sequence increases in commit order.
const writeLock = 0x6a6f75726e616c // "journal"

func quote(schema string) (string, error) {
	if !schemaName.MatchString(schema) {
		return "", fmt.Errorf("pgstore: invalid schema name %q", schema)
	}
	return `"` + schema + `"`, nil
}

// Migrate brings schema up to date. It creates the schema only when
// pg_namespace lacks it and never touches another schema.
func Migrate(ctx context.Context, db *sql.DB, schema string) error {
	q, err := quote(schema)
	if err != nil {
		return err
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema).Scan(&exists); err != nil {
		return fmt.Errorf("pgstore: look up schema: %w", err)
	}
	if !exists {
		if _, err := db.ExecContext(ctx, `CREATE SCHEMA `+q); err != nil {
			return fmt.Errorf("pgstore: create schema: %w", err)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO `+q); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, writeLock+1); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version bigint PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("pgstore: migration table: %w", err)
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version, err := strconv.ParseInt(strings.SplitN(strings.TrimPrefix(name, "migrations/"), "_", 2)[0], 10, 64)
		if err != nil {
			return fmt.Errorf("pgstore: migration %s: %w", name, err)
		}
		var applied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("pgstore: apply %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Store is the PostgreSQL engine.Store.
type Store struct {
	db     *sql.DB
	schema string
}

// New returns the store over db in schema. The connection's search_path
// must name schema (config.PostgresDSN sets it); every statement also
// pins it inside its transaction.
func New(db *sql.DB, schema string) (engine.Store, error) {
	if _, err := quote(schema); err != nil {
		return nil, err
	}
	return &Store{db: db, schema: schema}, nil
}

// Ping checks the database.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

type querier interface {
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
	ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
}

// read runs fn in a read-only transaction pinned to the schema.
func (s *Store) read(ctx context.Context, fn func(q querier) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO "`+s.schema+`"`); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Write runs fn in one serialized transaction.
func (s *Store) Write(ctx context.Context, fn func(engine.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO "`+s.schema+`"`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, writeLock); err != nil {
		return err
	}
	if err := fn(&pgTx{ctx: ctx, q: tx}); err != nil {
		return err
	}
	return tx.Commit()
}

type pgTx struct {
	ctx context.Context
	q   querier
}

func (t *pgTx) MessageSeen(key string, seq int64) (bool, error) {
	var seen bool
	err := t.q.QueryRowContext(t.ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE publisher_key = $1 AND publisher_seq = $2)
		OR EXISTS (SELECT 1 FROM rejected WHERE publisher_key = $1 AND publisher_seq = $2 AND $1 <> '')`, key, seq).Scan(&seen)
	return seen, err
}

func (t *pgTx) RecordRejected(r engine.Rejected) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO rejected (publisher_key, publisher_seq, subject, reason, rejected_at)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`, r.PublisherKey, r.PublisherSeq, r.Subject, r.Reason, r.At)
	return err
}

func (t *pgTx) NextID() (int64, error) {
	var id int64
	err := t.q.QueryRowContext(t.ctx, `SELECT nextval('commit_seq')`).Scan(&id)
	return id, err
}

func (t *pgTx) InsertEvent(e engine.Event) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO events (event_id, team_id, kind, publisher, publisher_key, publisher_seq,
		task_id, attempt, claim_seq, executor_id, control_id, occurred_at, committed_at, stream_seq, data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		e.EventID, e.TeamID, e.Kind, e.Source, e.PublisherKey, e.PublisherSeq, e.TaskID, e.Attempt, e.ClaimSeq,
		e.ExecutorID, e.ControlID, e.OccurredAt, e.CommittedAt, int64(e.StreamSeq), jsonOrEmpty(e.Data))
	return err
}

func jsonOrEmpty(b json.RawMessage) []byte {
	if len(b) == 0 {
		return []byte(`{}`)
	}
	return b
}

func (t *pgTx) InsertFrame(f engine.Frame) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO frames (frame_id, team_id, frame_type, task_id, executor_id, control_id, data)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, f.FrameID, f.TeamID, f.FrameType, f.TaskID, f.ExecutorID, f.ControlID, jsonOrEmpty(f.Data))
	return err
}

func (t *pgTx) LinkTask(team, task, attempt, claim, eventID int64) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO task_links (team_id, task_id, event_id, attempt, claim_seq)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, team, task, eventID, attempt, claim)
	return err
}

const claimCols = `team_id, task_id, attempt, claim_seq, executor_id, event_id, ended`

func scanClaim(row interface{ Scan(...any) error }) (engine.Claim, error) {
	var c engine.Claim
	err := row.Scan(&c.TeamID, &c.TaskID, &c.Attempt, &c.ClaimSeq, &c.ExecutorID, &c.EventID, &c.Ended)
	return c, err
}

func claimOf(ctx context.Context, q querier, team, task, attempt, claim int64) (engine.Claim, bool, error) {
	c, err := scanClaim(q.QueryRowContext(ctx, `SELECT `+claimCols+` FROM claims WHERE team_id=$1 AND task_id=$2 AND attempt=$3 AND claim_seq=$4`, team, task, attempt, claim))
	if errors.Is(err, sql.ErrNoRows) {
		return engine.Claim{}, false, nil
	}
	return c, err == nil, err
}

func (t *pgTx) Claim(team, task, attempt, claim int64) (engine.Claim, bool, error) {
	return claimOf(t.ctx, t.q, team, task, attempt, claim)
}

func (t *pgTx) PutClaim(c engine.Claim) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO claims (`+claimCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (team_id, task_id, attempt, claim_seq) DO UPDATE SET executor_id = EXCLUDED.executor_id,
		event_id = EXCLUDED.event_id, ended = EXCLUDED.ended`, c.TeamID, c.TaskID, c.Attempt, c.ClaimSeq, c.ExecutorID, c.EventID, c.Ended)
	return err
}

func (t *pgTx) OpenClaimsOf(executorID int64) ([]engine.Claim, error) {
	rows, err := t.q.QueryContext(t.ctx, `SELECT `+claimCols+` FROM claims WHERE executor_id=$1 AND NOT ended ORDER BY event_id`, executorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []engine.Claim
	for rows.Next() {
		c, err := scanClaim(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

const heldCols = `event_id, team_id, task_id, attempt, claim_seq, executor_id, kind, stored_at, status, log_offset, frame_id, payload`

func (t *pgTx) ClaimStateEvents(team, task, attempt, claim, executor, before int64) ([]engine.Event, error) {
	rows, err := t.q.QueryContext(t.ctx, `SELECT event_id, kind FROM events
		WHERE team_id=$1 AND task_id=$2 AND attempt=$3 AND claim_seq=$4 AND executor_id=$5 AND event_id < $6
		AND publisher='executor' AND kind IN ('`+strings.Join(engine.ExecutorStateKinds, "','")+`') ORDER BY event_id`,
		team, task, attempt, claim, executor, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []engine.Event
	for rows.Next() {
		e := engine.Event{TeamID: team, Source: "executor", TaskID: &task, Attempt: &attempt, ClaimSeq: &claim, ExecutorID: &executor}
		if err := rows.Scan(&e.EventID, &e.Kind); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (t *pgTx) PutHeld(h engine.Held) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO held (`+heldCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		h.EventID, h.TeamID, h.TaskID, h.Attempt, h.ClaimSeq, h.ExecutorID, h.Kind, h.StoredAt, h.Status, h.LogOffset, h.FrameID, nullJSON(h.Payload))
	return err
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}

func (t *pgTx) heldWhere(where string, args ...any) ([]engine.Held, error) {
	rows, err := t.q.QueryContext(t.ctx, `SELECT `+heldCols+` FROM held WHERE `+where+` ORDER BY event_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []engine.Held
	for rows.Next() {
		var h engine.Held
		var payload []byte
		if err := rows.Scan(&h.EventID, &h.TeamID, &h.TaskID, &h.Attempt, &h.ClaimSeq, &h.ExecutorID, &h.Kind, &h.StoredAt,
			&h.Status, &h.LogOffset, &h.FrameID, &payload); err != nil {
			return nil, err
		}
		h.StoredAt = h.StoredAt.UTC()
		h.Payload = payload
		out = append(out, h)
	}
	return out, rows.Err()
}

func (t *pgTx) HeldFor(team, task, attempt, claim int64) ([]engine.Held, error) {
	return t.heldWhere(`status='held' AND team_id=$1 AND task_id=$2 AND attempt=$3 AND claim_seq=$4`, team, task, attempt, claim)
}

func (t *pgTx) HeldBefore(at time.Time) ([]engine.Held, error) {
	return t.heldWhere(`status='held' AND stored_at < $1`, at)
}

func (t *pgTx) DecideHeld(h engine.Held) error {
	_, err := t.q.ExecContext(t.ctx, `UPDATE held SET status=$2, log_offset=$3, frame_id=$4 WHERE event_id=$1`, h.EventID, h.Status, h.LogOffset, h.FrameID)
	return err
}

func (t *pgTx) NextLogOffset(team, task int64) (int64, error) {
	var n int64
	err := t.q.QueryRowContext(t.ctx, `INSERT INTO log_offsets (team_id, task_id, last_offset) VALUES ($1,$2,1)
		ON CONFLICT (team_id, task_id) DO UPDATE SET last_offset = log_offsets.last_offset + 1 RETURNING last_offset`, team, task).Scan(&n)
	return n, err
}

func (t *pgTx) InsertLog(l engine.LogEntry) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO logs (event_id, team_id, task_id, attempt, claim_seq, executor_id, log_offset, stream, text, publisher_seq, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, l.EventID, l.TeamID, l.TaskID, l.Attempt, l.ClaimSeq, l.ExecutorID, l.LogOffset, l.Stream, l.Text, l.PublisherSeq, l.OccurredAt)
	return err
}

const executorCols = `executor_id, team_id, tag, executor_type, registered, removed, connection_state, connection_changed_at,
	held_claims, available_capacity, max_parallel_tasks, reported_ip, ip_changed_at`

func scanExecutor(row interface{ Scan(...any) error }) (engine.ExecutorRecord, error) {
	var r engine.ExecutorRecord
	err := row.Scan(&r.ExecutorID, &r.TeamID, &r.Tag, &r.ExecutorType, &r.Registered, &r.Removed, &r.ConnectionState,
		&r.ConnectionChangedAt, &r.HeldClaims, &r.AvailableCapacity, &r.MaxParallelTasks, &r.ReportedIP, &r.IPChangedAt)
	if r.ConnectionChangedAt != nil {
		u := r.ConnectionChangedAt.UTC()
		r.ConnectionChangedAt = &u
	}
	if r.IPChangedAt != nil {
		u := r.IPChangedAt.UTC()
		r.IPChangedAt = &u
	}
	return r, err
}

func executorByID(ctx context.Context, q querier, id int64) (engine.ExecutorRecord, bool, error) {
	r, err := scanExecutor(q.QueryRowContext(ctx, `SELECT `+executorCols+` FROM executors WHERE executor_id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return engine.ExecutorRecord{}, false, nil
	}
	return r, err == nil, err
}

func (t *pgTx) Executor(id int64) (engine.ExecutorRecord, bool, error) {
	return executorByID(t.ctx, t.q, id)
}

func (t *pgTx) PutExecutor(r engine.ExecutorRecord) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO executors (`+executorCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (executor_id) DO UPDATE SET registered=EXCLUDED.registered, removed=EXCLUDED.removed,
		connection_state=EXCLUDED.connection_state, connection_changed_at=EXCLUDED.connection_changed_at,
		held_claims=EXCLUDED.held_claims, available_capacity=EXCLUDED.available_capacity,
		max_parallel_tasks=EXCLUDED.max_parallel_tasks, reported_ip=EXCLUDED.reported_ip, ip_changed_at=EXCLUDED.ip_changed_at
		WHERE executors.team_id = EXCLUDED.team_id`,
		r.ExecutorID, r.TeamID, r.Tag, r.ExecutorType, r.Registered, r.Removed, r.ConnectionState, r.ConnectionChangedAt,
		r.HeldClaims, r.AvailableCapacity, r.MaxParallelTasks, r.ReportedIP, r.IPChangedAt)
	return err
}

const taskCols = `team_id, task_id, first_queued_at, first_queued_id, latest_kind, latest_attempt, latest_claim_seq, latest_event_id`

func scanTask(row interface{ Scan(...any) error }) (engine.TaskSummary, error) {
	var s engine.TaskSummary
	err := row.Scan(&s.TeamID, &s.TaskID, &s.FirstQueuedAt, &s.FirstQueuedID, &s.LatestKind, &s.LatestAttempt, &s.LatestClaimSeq, &s.LatestEventID)
	if s.FirstQueuedAt != nil {
		u := s.FirstQueuedAt.UTC()
		s.FirstQueuedAt = &u
	}
	return s, err
}

func (t *pgTx) Task(team, task int64) (engine.TaskSummary, bool, error) {
	s, err := scanTask(t.q.QueryRowContext(t.ctx, `SELECT `+taskCols+` FROM tasks WHERE team_id=$1 AND task_id=$2`, team, task))
	if errors.Is(err, sql.ErrNoRows) {
		return engine.TaskSummary{}, false, nil
	}
	return s, err == nil, err
}

func (t *pgTx) PutTask(s engine.TaskSummary) error {
	_, err := t.q.ExecContext(t.ctx, `INSERT INTO tasks (`+taskCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (team_id, task_id) DO UPDATE SET latest_kind=EXCLUDED.latest_kind, latest_attempt=EXCLUDED.latest_attempt,
		latest_claim_seq=EXCLUDED.latest_claim_seq, latest_event_id=EXCLUDED.latest_event_id`,
		s.TeamID, s.TaskID, s.FirstQueuedAt, s.FirstQueuedID, s.LatestKind, s.LatestAttempt, s.LatestClaimSeq, s.LatestEventID)
	return err
}

// ---- reads (every query applies the team predicate) ----

func (s *Store) ExecutorByID(ctx context.Context, id int64) (r engine.ExecutorRecord, ok bool, err error) {
	err = s.read(ctx, func(q querier) error {
		r, ok, err = executorByID(ctx, q, id)
		return err
	})
	return
}

func (s *Store) ClaimOf(ctx context.Context, team, task, attempt, claim int64) (c engine.Claim, ok bool, err error) {
	err = s.read(ctx, func(q querier) error {
		c, ok, err = claimOf(ctx, q, team, task, attempt, claim)
		return err
	})
	return
}

func (s *Store) exists(ctx context.Context, query string, args ...any) (found bool, err error) {
	err = s.read(ctx, func(q querier) error { return q.QueryRowContext(ctx, query, args...).Scan(&found) })
	return
}

func (s *Store) TaskExists(ctx context.Context, team, task int64) (bool, error) {
	return s.exists(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE team_id=$1 AND task_id=$2)`, team, task)
}

func (s *Store) ControlExists(ctx context.Context, team, task, control int64) (bool, error) {
	return s.exists(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE team_id=$1 AND task_id=$2 AND control_id=$3)`, team, task, control)
}

func (s *Store) ListEvents(ctx context.Context, eq engine.EventQuery) ([]engine.Event, error) {
	args := []any{eq.TeamID, eq.AfterID}
	arg := func(v any) string { args = append(args, v); return "$" + strconv.Itoa(len(args)) }
	from := `events e`
	cols := `e.event_id, e.team_id, e.kind, e.publisher, e.publisher_key, e.publisher_seq, e.task_id, e.attempt, e.claim_seq,
		e.executor_id, e.control_id, e.occurred_at, e.committed_at, e.stream_seq, e.data`
	where := []string{`e.team_id = $1`, `e.event_id > $2`, `e.kind <> 'log'`,
		`NOT EXISTS (SELECT 1 FROM held h WHERE h.event_id = e.event_id AND h.status <> 'accepted')`}
	switch {
	case eq.ExecutorHistory && eq.ExecutorID != nil:
		where = append(where, `e.executor_id = `+arg(*eq.ExecutorID),
			`(e.publisher = 'executor-registry' OR (e.publisher = 'executor' AND e.task_id IS NULL))`)
	case eq.TaskID != nil:
		t := arg(*eq.TaskID)
		from += ` LEFT JOIN task_links l ON l.team_id = e.team_id AND l.task_id = ` + t + ` AND l.event_id = e.event_id`
		cols = `e.event_id, e.team_id, e.kind, e.publisher, e.publisher_key, e.publisher_seq,
			COALESCE(e.task_id, l.task_id), COALESCE(e.attempt, l.attempt), COALESCE(e.claim_seq, l.claim_seq),
			e.executor_id, e.control_id, e.occurred_at, e.committed_at, e.stream_seq, e.data`
		where = append(where, `(e.task_id = `+t+` OR l.event_id IS NOT NULL)`, `e.kind <> 'ip_changed'`)
		if eq.ControlID != nil {
			where = append(where, `e.control_id = `+arg(*eq.ControlID),
				`e.kind IN ('control_requested', 'control_acknowledged', 'control_failed')`)
		}
	}
	query := `SELECT ` + cols + ` FROM ` + from + ` WHERE ` + strings.Join(where, " AND ") + ` ORDER BY e.event_id`
	if eq.Limit > 0 {
		query += ` LIMIT ` + arg(eq.Limit)
	}
	var out []engine.Event
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e engine.Event
			var streamSeq int64
			var data []byte
			if err := rows.Scan(&e.EventID, &e.TeamID, &e.Kind, &e.Source, &e.PublisherKey, &e.PublisherSeq, &e.TaskID,
				&e.Attempt, &e.ClaimSeq, &e.ExecutorID, &e.ControlID, &e.OccurredAt, &e.CommittedAt, &streamSeq, &data); err != nil {
				return err
			}
			e.OccurredAt, e.CommittedAt, e.StreamSeq, e.Data = e.OccurredAt.UTC(), e.CommittedAt.UTC(), uint64(streamSeq), data
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) ListLogs(ctx context.Context, team, task, after int64, limit int) ([]engine.LogEntry, error) {
	var out []engine.LogEntry
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `SELECT event_id, team_id, task_id, attempt, claim_seq, executor_id, log_offset, stream, text, publisher_seq, occurred_at
			FROM logs WHERE team_id=$1 AND task_id=$2 AND log_offset > $3 ORDER BY log_offset LIMIT $4`, team, task, after, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var l engine.LogEntry
			if err := rows.Scan(&l.EventID, &l.TeamID, &l.TaskID, &l.Attempt, &l.ClaimSeq, &l.ExecutorID, &l.LogOffset, &l.Stream, &l.Text, &l.PublisherSeq, &l.OccurredAt); err != nil {
				return err
			}
			l.OccurredAt = l.OccurredAt.UTC()
			out = append(out, l)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) ListExecutors(ctx context.Context, team int64) ([]engine.ExecutorRecord, error) {
	var out []engine.ExecutorRecord
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `SELECT `+executorCols+` FROM executors WHERE team_id=$1 AND registered AND NOT removed ORDER BY executor_id`, team)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r, err := scanExecutor(rows)
			if err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) ListTasks(ctx context.Context, team int64, from, to time.Time) ([]engine.TaskSummary, error) {
	var out []engine.TaskSummary
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE team_id=$1 AND first_queued_at >= $2 AND first_queued_at < $3 ORDER BY task_id`, team, from, to)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				return err
			}
			out = append(out, t)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) ListFrames(ctx context.Context, team, after int64, task *int64, limit int) ([]engine.Frame, error) {
	var out []engine.Frame
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `SELECT frame_id, team_id, frame_type, task_id, executor_id, control_id, data FROM frames
			WHERE team_id=$1 AND frame_id > $2 AND ($3::bigint IS NULL OR task_id = $3) ORDER BY frame_id LIMIT $4`, team, after, task, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var f engine.Frame
			var data []byte
			if err := rows.Scan(&f.FrameID, &f.TeamID, &f.FrameType, &f.TaskID, &f.ExecutorID, &f.ControlID, &data); err != nil {
				return err
			}
			f.Data = data
			out = append(out, f)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) MaxFrameID(ctx context.Context, team int64) (max int64, err error) {
	err = s.read(ctx, func(q querier) error {
		return q.QueryRowContext(ctx, `SELECT COALESCE(MAX(frame_id), 0) FROM frames WHERE team_id=$1`, team).Scan(&max)
	})
	return
}

func (s *Store) HeldStatus(ctx context.Context, eventID int64) (status string, ok bool, err error) {
	err = s.read(ctx, func(q querier) error {
		e := q.QueryRowContext(ctx, `SELECT status FROM held WHERE event_id=$1`, eventID).Scan(&status)
		if errors.Is(e, sql.ErrNoRows) {
			return nil
		}
		ok = e == nil
		return e
	})
	return
}
