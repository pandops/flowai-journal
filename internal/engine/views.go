package engine

import (
	"encoding/json"
	"time"
)

// LogView is the reply and frame shape of one accepted log chunk: the
// baseline Log (log_chunk_id, task_id, executor_id, log_offset, stream,
// content, occurred_at) plus journal's claim fields. log_chunk_id is the
// chunk's stored event_id; event_id repeats it as the Web UI's line key.
type LogView struct {
	LogChunkID   int64  `json:"log_chunk_id"`
	EventID      int64  `json:"event_id"`
	TaskID       int64  `json:"task_id"`
	Attempt      int64  `json:"attempt"`
	ClaimSeq     int64  `json:"claim_seq"`
	ExecutorID   int64  `json:"executor_id"`
	LogOffset    int64  `json:"log_offset"`
	Stream       string `json:"stream"`
	Content      string `json:"content"`
	PublisherSeq int64  `json:"publisher_seq"`
	OccurredAt   string `json:"occurred_at"`
}

// ExecutorStateView is one item of ui.executor-states.
type ExecutorStateView struct {
	ExecutorID          int64   `json:"executor_id"`
	ConnectionState     string  `json:"connection_state"`
	ConnectionChangedAt *string `json:"connection_changed_at"`
	HeldClaims          *int64  `json:"held_claims"`
	MaxParallelTasks    *int64  `json:"max_parallel_tasks"`
	AvailableCapacity   *int64  `json:"available_capacity"`
	ReportedIP          *string `json:"reported_ip"`
	IPChangedAt         *string `json:"ip_changed_at"`
}

// FrameView is one WebSocket frame.
type FrameView struct {
	FrameID   int64           `json:"frame_id"`
	FrameType string          `json:"frame_type"`
	TeamID    int64           `json:"team_id"`
	TaskID    *int64          `json:"task_id"`
	Data      json.RawMessage `json:"data"`
}

// FormatTime renders a timestamp as RFC 3339 UTC with milliseconds.
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

func formatOptionalTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := FormatTime(*t)
	return &s
}

// View renders a stored event flat: the stored (secret-free) payload with
// journal's event_id and the derived team, Executor and claim fields.
func (ev Event) View() json.RawMessage {
	fields := map[string]any{}
	if len(ev.Data) > 0 {
		_ = json.Unmarshal(ev.Data, &fields)
	}
	fields["event_id"] = ev.EventID
	fields["publisher"] = ev.Source
	fields["publisher_seq"] = ev.PublisherSeq
	fields["team_id"] = ev.TeamID
	fields["kind"] = ev.Kind
	fields["occurred_at"] = FormatTime(ev.OccurredAt)
	set := func(k string, v *int64) {
		if v != nil {
			fields[k] = *v
		}
	}
	set("task_id", ev.TaskID)
	set("attempt", ev.Attempt)
	set("claim_seq", ev.ClaimSeq)
	set("executor_id", ev.ExecutorID)
	set("control_id", ev.ControlID)
	out, _ := json.Marshal(fields)
	return out
}

// View renders a log chunk.
func (l LogEntry) View() LogView {
	return LogView{
		LogChunkID: l.EventID, EventID: l.EventID, TaskID: l.TaskID, Attempt: l.Attempt, ClaimSeq: l.ClaimSeq,
		ExecutorID: l.ExecutorID, LogOffset: l.LogOffset, Stream: l.Stream, Content: l.Text,
		PublisherSeq: l.PublisherSeq, OccurredAt: FormatTime(l.OccurredAt),
	}
}

// View renders an Executor state item.
func (r ExecutorRecord) View() ExecutorStateView {
	state := r.ConnectionState
	if state == "" {
		state = StateNeverConnected
	}
	return ExecutorStateView{
		ExecutorID: r.ExecutorID, ConnectionState: state,
		ConnectionChangedAt: formatOptionalTime(r.ConnectionChangedAt),
		HeldClaims:          r.HeldClaims, MaxParallelTasks: r.MaxParallelTasks,
		AvailableCapacity: r.AvailableCapacity, ReportedIP: r.ReportedIP,
		IPChangedAt: formatOptionalTime(r.IPChangedAt),
	}
}

// View renders a frame.
func (f Frame) View() FrameView {
	return FrameView{FrameID: f.FrameID, FrameType: f.FrameType, TeamID: f.TeamID, TaskID: f.TaskID, Data: f.Data}
}
