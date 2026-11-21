// Package rpc answers API Gateway's journal.ui.<operation> requests
// (agents.md rule 9 envelope).
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"

	"github.com/pandops/flowai-journal/internal/cursor"
	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/ids"
)

// Operations are the ui operations journal answers.
var Operations = []string{
	"task-events",
	"task-logs",
	"control-events",
	"executor-events",
	"executor-states",
	"dashboard",
}

// Subject is the subject journal subscribes to for op.
func Subject(op string) string { return "journal.ui." + op + ".sub" }

// Reply is one reply: the HTTP status, reply headers and the exact REST body.
type Reply struct {
	Status int
	Header map[string]string
	Body   []byte
}

// Handler serves the operations.
type Handler struct {
	Engine *engine.Engine
	Cursor *cursor.Keyring
	Logger *slog.Logger
}

const (
	defaultLimit = 50
	maxLimit     = 200
	order        = "event_id_asc"
)

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID *int64 `json:"request_id,omitempty"` // absent without a valid header (D12)
}

type ctxInfo struct {
	team, operator int64
	requestID      *int64
}

func fail(status int, code, message string, rid *int64) Reply {
	b, _ := json.Marshal(errorBody{Code: code, Message: message, RequestID: rid})
	return Reply{Status: status, Body: b}
}

func ok(v any) Reply {
	b, err := json.Marshal(v)
	if err != nil {
		return fail(503, "service_unavailable", "journal is unavailable", nil)
	}
	return Reply{Status: 200, Body: b}
}

func header(h map[string][]string, name string) (string, bool) {
	v := h[name]
	if len(v) != 1 {
		return "", false
	}
	return v[0], true
}

// Serve answers one request.
func (h *Handler) Serve(ctx context.Context, op string, hdr map[string][]string, body []byte) Reply {
	var c ctxInfo
	if s, present := header(hdr, "request_id"); present {
		if id, valid := ids.ParseDecimal(s); valid {
			c.requestID = &id
		}
	}
	known := false
	for _, o := range Operations {
		known = known || o == op
	}
	if !known {
		return fail(404, "not_found", "not found", c.requestID)
	}
	team, tok := header(hdr, "team")
	operator, ook := header(hdr, "operator_id")
	if _, rok := header(hdr, "request_id"); !rok || c.requestID == nil || !tok || !ook {
		return fail(400, "invalid_request", "missing or invalid request context", c.requestID)
	}
	var valid bool
	if c.team, valid = ids.ParseDecimal(team); !valid {
		return fail(400, "invalid_request", "missing or invalid request context", c.requestID)
	}
	if c.operator, valid = ids.ParseDecimal(operator); !valid {
		return fail(400, "invalid_request", "missing or invalid request context", c.requestID)
	}
	var req struct {
		Params map[string]any `json:"params"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil || req.Params == nil {
		return fail(400, "invalid_request", "request body must carry params", c.requestID)
	}
	p := params{m: req.Params}
	pathTeam, valid := p.id("team_id")
	if !valid {
		return fail(400, "invalid_request", "invalid team_id", c.requestID)
	}
	if pathTeam != c.team {
		return fail(404, "not_found", "not found", c.requestID)
	}
	r := h.serve(ctx, op, c, p)
	if r.Status == 503 && h.Logger != nil {
		h.Logger.Warn("ui read failed", "operation", op)
	}
	return r
}

type params struct{ m map[string]any }

func (p params) id(name string) (int64, bool) {
	v, present := p.m[name]
	if !present {
		return 0, false
	}
	return ids.FromJSON(v)
}

func (p params) allowed(names ...string) bool {
	for k := range p.m {
		found := false
		for _, n := range names {
			found = found || n == k
		}
		if !found {
			return false
		}
	}
	return true
}

// page reads limit and cursor; ok is false for invalid_pagination.
func (h *Handler) page(p params, b cursor.Binding) (limit int, after int64, valid bool) {
	limit = defaultLimit
	if v, present := p.m["limit"]; present {
		n, isNum := v.(json.Number)
		if !isNum {
			return 0, 0, false
		}
		i, err := strconv.ParseInt(n.String(), 10, 64)
		if err != nil || i < 1 || i > maxLimit {
			return 0, 0, false
		}
		limit = int(i)
	}
	b.Filter["limit"] = int64(limit)
	if v, present := p.m["cursor"]; present && v != nil {
		s, isStr := v.(string)
		if !isStr {
			return 0, 0, false
		}
		pos, err := h.Cursor.Verify(s, b)
		if err != nil {
			return 0, 0, false
		}
		after = pos
	}
	return limit, after, true
}

type eventPage struct {
	Items      []json.RawMessage `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

type logPage struct {
	Items      []engine.LogView `json:"items"`
	NextCursor *string          `json:"next_cursor"`
}

func (h *Handler) readErr(err error, c ctxInfo) Reply {
	if errors.Is(err, engine.ErrNotFound) {
		return fail(404, "not_found", "not found", c.requestID)
	}
	return fail(503, "service_unavailable", "journal is unavailable", c.requestID)
}

func (h *Handler) serve(ctx context.Context, op string, c ctxInfo, p params) Reply {
	invalid := func() Reply { return fail(400, "invalid_request", "invalid parameters", c.requestID) }
	badPage := func() Reply { return fail(400, "invalid_pagination", "invalid cursor or limit", c.requestID) }
	binding := cursor.Binding{Operation: op, TeamID: c.team, OperatorID: c.operator, Filter: map[string]int64{}, Order: order}
	switch op {
	case "task-events", "task-logs", "control-events":
		names := []string{"team_id", "task_id", "cursor", "limit"}
		if op == "control-events" {
			names = append(names, "control_id")
		}
		task, valid := p.id("task_id")
		if !valid || !p.allowed(names...) {
			return invalid()
		}
		binding.Filter["task_id"] = task
		var control int64
		if op == "control-events" {
			if control, valid = p.id("control_id"); !valid {
				return invalid()
			}
			binding.Filter["control_id"] = control
		}
		limit, after, valid := h.page(p, binding)
		if !valid {
			return badPage()
		}
		if op == "task-logs" {
			logs, err := h.Engine.TaskLogs(ctx, c.team, task, after, limit+1)
			if err != nil {
				return h.readErr(err, c)
			}
			out := logPage{Items: []engine.LogView{}}
			for i, l := range logs {
				if i == limit {
					next := h.Cursor.Sign(binding, logs[limit-1].LogOffset)
					out.NextCursor = &next
					break
				}
				out.Items = append(out.Items, l.View())
			}
			return ok(out)
		}
		var events []engine.Event
		var err error
		if op == "task-events" {
			events, err = h.Engine.TaskEvents(ctx, c.team, task, after, limit+1)
		} else {
			events, err = h.Engine.ControlEvents(ctx, c.team, task, control, after, limit+1)
		}
		if err != nil {
			return h.readErr(err, c)
		}
		return ok(h.events(events, limit, binding))
	case "executor-events":
		executor, valid := p.id("executor_id")
		if !valid || !p.allowed("team_id", "executor_id", "cursor", "limit") {
			return invalid()
		}
		binding.Filter["executor_id"] = executor
		limit, after, valid := h.page(p, binding)
		if !valid {
			return badPage()
		}
		events, err := h.Engine.ExecutorEvents(ctx, c.team, executor, after, limit+1)
		if err != nil {
			return h.readErr(err, c)
		}
		return ok(h.events(events, limit, binding))
	case "executor-states":
		if !p.allowed("team_id") {
			return invalid()
		}
		recs, err := h.Engine.ExecutorStates(ctx, c.team)
		if err != nil {
			return h.readErr(err, c)
		}
		out := struct {
			Items []engine.ExecutorStateView `json:"items"`
		}{Items: []engine.ExecutorStateView{}}
		for _, r := range recs {
			out.Items = append(out.Items, r.View())
		}
		return ok(out)
	case "dashboard":
		return h.dashboard(ctx, c, p)
	}
	return fail(404, "not_found", "not found", c.requestID)
}

func (h *Handler) events(events []engine.Event, limit int, b cursor.Binding) eventPage {
	out := eventPage{Items: []json.RawMessage{}}
	for i, ev := range events {
		if i == limit {
			next := h.Cursor.Sign(b, events[limit-1].EventID)
			out.NextCursor = &next
			break
		}
		out.Items = append(out.Items, ev.View())
	}
	return out
}

func (h *Handler) dashboard(ctx context.Context, c ctxInfo, p params) Reply {
	if !p.allowed("team_id", "period", "week", "month", "time_zone") {
		return fail(400, "invalid_request", "invalid parameters", c.requestID)
	}
	var q engine.DashboardQuery
	if v, present := p.m["time_zone"]; present {
		q.HasZone = true
		switch t := v.(type) {
		case string:
			q.TimeZones = []string{t}
		case []any:
			for _, z := range t {
				s, _ := z.(string)
				q.TimeZones = append(q.TimeZones, s)
			}
		default:
			q.TimeZones = nil
		}
	}
	if _, _, err := engine.ResolveZone(q); err != nil {
		return fail(400, "invalid_time_zone", "invalid time zone", c.requestID)
	}
	str := func(name string) (string, bool) {
		v, present := p.m[name]
		if !present {
			return "", true
		}
		s, isStr := v.(string)
		return s, isStr && s != ""
	}
	var valid bool
	if q.Period, valid = str("period"); !valid || q.Period == "" {
		return fail(400, "invalid_period", "invalid period", c.requestID)
	}
	if q.Week, valid = str("week"); !valid {
		return fail(400, "invalid_calendar", "invalid calendar selection", c.requestID)
	}
	if q.Month, valid = str("month"); !valid {
		return fail(400, "invalid_calendar", "invalid calendar selection", c.requestID)
	}
	d, err := h.Engine.Dashboard(ctx, c.team, q)
	switch {
	case errors.Is(err, engine.ErrInvalidTimeZone):
		return fail(400, "invalid_time_zone", "invalid time zone", c.requestID)
	case errors.Is(err, engine.ErrInvalidPeriod):
		return fail(400, "invalid_period", "invalid period", c.requestID)
	case errors.Is(err, engine.ErrInvalidCalendar):
		return fail(400, "invalid_calendar", "invalid calendar selection", c.requestID)
	case err != nil:
		return h.readErr(err, c)
	}
	return ok(d)
}
