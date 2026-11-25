// Package httpedge is journal's one HTTP listener: the probes and API
// Gateway's team WebSocket stream. Every other path answers 404.
package httpedge

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/ids"
)

// Readiness reports the dependencies /v1/readyz waits for.
type Readiness interface {
	BrokerConnected() bool
	ConsumerBound() bool
	StoreReady(ctx context.Context) bool
}

// Options configures the handler.
type Options struct {
	Engine       *engine.Engine
	GatewayToken string
	Readiness    Readiness
	// Wake subscribes to commit notifications of one team.
	Wake         func(teamID int64) (<-chan struct{}, func())
	PollInterval time.Duration
	Logger       *slog.Logger
}

const replayBatch = 200

type handler struct {
	opts     Options
	upgrader websocket.Upgrader
}

// New returns the HTTP handler.
func New(opts Options) http.Handler {
	if opts.PollInterval <= 0 {
		opts.PollInterval = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &handler{opts: opts, upgrader: websocket.Upgrader{
		// API Gateway is the only caller and authenticates with its token;
		// the browser Origin belongs to the Gateway's own edge.
		CheckOrigin: func(*http.Request) bool { return true },
	}}
}

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID *int64 `json:"request_id"`
}

func writeError(w http.ResponseWriter, status int, code, message string, rid *int64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Code: code, Message: message, RequestID: rid})
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/livez" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	case r.URL.Path == "/v1/readyz" && r.Method == http.MethodGet:
		rd := h.opts.Readiness
		if rd == nil || !rd.BrokerConnected() || !rd.ConsumerBound() || !rd.StoreReady(r.Context()) {
			writeError(w, http.StatusServiceUnavailable, "service_unavailable", "journal is not ready", nil)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/teams/") && strings.HasSuffix(r.URL.Path, "/stream"):
		team, valid := ids.ParseDecimal(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/teams/"), "/stream"))
		if !valid {
			writeError(w, http.StatusNotFound, "not_found", "not found", nil)
			return
		}
		h.stream(w, r, team)
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found", nil)
	}
}

func (h *handler) authorized(r *http.Request) bool {
	want := "Bearer " + h.opts.GatewayToken
	got := r.Header.Get("Authorization")
	return h.opts.GatewayToken != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (h *handler) stream(w http.ResponseWriter, r *http.Request, pathTeam int64) {
	if !h.authorized(r) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid service token", nil)
		return
	}
	rid, ridOK := ids.ParseDecimal(r.Header.Get("X-FlowAI-Request-ID"))
	var ridp *int64
	if ridOK {
		ridp = &rid
	}
	_, opOK := ids.ParseDecimal(r.Header.Get("X-FlowAI-Operator-ID"))
	team, teamOK := ids.ParseDecimal(r.Header.Get("X-FlowAI-Team-ID"))
	if !ridOK || !opOK || !teamOK {
		writeError(w, http.StatusBadRequest, "invalid_request", "missing or invalid request context", ridp)
		return
	}
	if team != pathTeam {
		writeError(w, http.StatusNotFound, "not_found", "not found", ridp)
		return
	}
	q := r.URL.Query()
	for k := range q {
		if k != "after_frame_id" && k != "task_id" {
			writeError(w, http.StatusBadRequest, "invalid_request", "unknown query parameter", ridp)
			return
		}
	}
	var taskFilter *int64
	if vals, present := q["task_id"]; present {
		id, valid := ids.ParseDecimal(firstOnly(vals))
		if !valid {
			writeError(w, http.StatusBadRequest, "invalid_request", "invalid task_id", ridp)
			return
		}
		taskFilter = &id
	}
	var after int64
	_, hasAfter := q["after_frame_id"]
	if hasAfter {
		id, valid := ids.ParseDecimal(firstOnly(q["after_frame_id"]))
		if !valid {
			writeError(w, http.StatusBadRequest, "invalid_request", "after_frame_id must be a positive integer", ridp)
			return
		}
		after = id
	}
	if h.opts.Readiness != nil && !h.opts.Readiness.BrokerConnected() {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "journal is unavailable", ridp)
		return
	}
	wake, cancel := h.wake(team)
	defer cancel()
	if !hasAfter {
		max, err := h.opts.Engine.MaxFrameID(r.Context(), team)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "service_unavailable", "journal is unavailable", ridp)
			return
		}
		after = max
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ctx, stop := context.WithCancel(r.Context())
	defer stop()
	go func() {
		// The client sends nothing; a read error means it went away.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				stop()
				return
			}
		}
	}()
	ticker := time.NewTicker(h.opts.PollInterval)
	defer ticker.Stop()
	for {
		frames, err := h.opts.Engine.Frames(ctx, team, after, taskFilter, replayBatch)
		if err != nil {
			if ctx.Err() == nil {
				h.opts.Logger.Warn("stream read failed", "team_id", team)
			}
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "unavailable"), time.Now().Add(time.Second))
			return
		}
		for _, f := range frames {
			if f.TeamID != team {
				continue
			}
			raw, err := json.Marshal(f.View())
			if err != nil {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
				return
			}
			after = f.FrameID
		}
		if len(frames) == replayBatch {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func firstOnly(vals []string) string {
	if len(vals) != 1 {
		return ""
	}
	return vals[0]
}

func (h *handler) wake(team int64) (<-chan struct{}, func()) {
	if h.opts.Wake == nil {
		return nil, func() {}
	}
	return h.opts.Wake(team)
}

// Hub fans commit notifications out to the team's stream connections.
type Hub struct {
	mu   sync.Mutex
	subs map[int64]map[chan struct{}]struct{}
}

// NewHub returns an empty hub.
func NewHub() *Hub { return &Hub{subs: map[int64]map[chan struct{}]struct{}{}} }

// Notify wakes every connection of the given teams.
func (h *Hub) Notify(teamIDs []int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, t := range teamIDs {
		for ch := range h.subs[t] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// Subscribe registers one connection of team.
func (h *Hub) Subscribe(teamID int64) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs[teamID] == nil {
		h.subs[teamID] = map[chan struct{}]struct{}{}
	}
	h.subs[teamID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs[teamID], ch)
		if len(h.subs[teamID]) == 0 {
			delete(h.subs, teamID)
		}
		h.mu.Unlock()
	}
}
