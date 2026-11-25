package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/httpedge"
	"github.com/pandops/flowai-journal/internal/mocks"
)

const gatewayToken = "gateway-stream-token-for-tests-0001"

type edge struct {
	srv   *httptest.Server
	ready *mocks.FakeReadiness
}

func (h *harness) edge() *edge {
	h.t.Helper()
	ready := &mocks.FakeReadiness{Broker: true, Consumer: true, Store: true}
	e := h.edgeWith(ready)
	e.ready = ready
	return e
}

// edgeWith serves the edge with the given readiness.
func (h *harness) edgeWith(ready httpedge.Readiness) *edge {
	h.t.Helper()
	handler := httpedge.New(httpedge.Options{Engine: h.eng, GatewayToken: gatewayToken, Readiness: ready, Wake: h.hub.Subscribe, PollInterval: 20 * time.Millisecond})
	srv := httptest.NewServer(handler)
	h.t.Cleanup(srv.Close)
	return &edge{srv: srv}
}

// streamHeaders are the headers API Gateway sends on the stream edge.
func streamHeaders(team int64) http.Header {
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+gatewayToken)
	hdr.Set("X-FlowAI-Operator-ID", "501")
	hdr.Set("X-FlowAI-Team-ID", strconv.FormatInt(team, 10))
	hdr.Set("X-FlowAI-Request-ID", "9001")
	hdr.Set("X-FlowAI-Team-Name", "Operations")
	return hdr
}

func (e *edge) streamURL(team int64, query url.Values) string {
	u := "ws" + strings.TrimPrefix(e.srv.URL, "http") + fmt.Sprintf("/v1/teams/%d/stream", team)
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return u
}

func (e *edge) dial(t *testing.T, team int64, hdr http.Header, query url.Values) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, resp, err := d.Dial(e.streamURL(team, query), hdr)
	if conn != nil {
		t.Cleanup(func() { conn.Close() })
	}
	return conn, resp, err
}

func (e *edge) open(t *testing.T, team int64, query url.Values) *websocket.Conn {
	t.Helper()
	conn, resp, err := e.dial(t, team, streamHeaders(team), query)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("open team %d stream: %v (status %d)", team, err, status)
	}
	return conn
}

// readFrames reads frames until n arrive or the timeout passes.
func readFrames(t *testing.T, conn *websocket.Conn, n int, timeout time.Duration) []engine.FrameView {
	t.Helper()
	var out []engine.FrameView
	deadline := time.Now().Add(timeout)
	for len(out) < n {
		_ = conn.SetReadDeadline(deadline)
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return out
		}
		var f engine.FrameView
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("frame is not JSON: %v: %s", err, raw)
		}
		if err := validateJSON(t, journalDir(t)+"/schemas/frame.schema.json", raw); err != nil {
			t.Fatalf("frame violates frame.schema.json: %v: %s", err, raw)
		}
		out = append(out, f)
	}
	return out
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func errorCodeOf(t *testing.T, resp *http.Response) string {
	t.Helper()
	if resp == nil || resp.Body == nil {
		return ""
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	c, _ := body["code"].(string)
	return c
}
