package test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Requirement: message-broker allows HTTP only on the named edges
// (journal's one edge is the Gateway stream).
func TestV0048MessageBrokerAllowsHTTPOnlyOnTheNamedEdges(t *testing.T) {
	t.Run("Service offers no internal HTTP API", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		e := h.edge()
		before := len(h.store.Events())
		for _, c := range []struct{ method, path string }{
			{"GET", "/v1/teams/3/events"},
			{"GET", "/v1/teams/3/tasks/1010/events"},
			{"GET", "/ui/v1/teams/3/tasks/1010/logs"},
			{"POST", "/v1/events"},
			{"GET", "/v1/_test/reset"},
			{"GET", "/healthz"},
			{"DELETE", "/v1/teams/3/stream"},
			{"GET", "/"},
		} {
			req, _ := http.NewRequest(c.method, e.srv.URL+c.path, strings.NewReader("{}"))
			for k, v := range streamHeaders(3) {
				req.Header[k] = v
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := http.StatusNotFound
			if c.method == "DELETE" {
				want = http.StatusMethodNotAllowed
			}
			if resp.StatusCode != want && resp.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s: %d, want %d", c.method, c.path, resp.StatusCode, want)
			}
		}
		if len(h.store.Events()) != before {
			t.Fatalf("an HTTP request changed journal's data")
		}
	})

	t.Run("Journal stream edge requires the Gateway token", func(t *testing.T) {
		h := newHarness(t)
		e := h.edge()
		for _, auth := range []string{"", "Bearer ", "Bearer wrong", "Basic " + gatewayToken, gatewayToken} {
			hdr := streamHeaders(3)
			if auth == "" {
				hdr.Del("Authorization")
			} else {
				hdr.Set("Authorization", auth)
			}
			conn, resp, err := e.dial(t, 3, hdr, url.Values{"after_frame_id": {"1"}})
			if err == nil || conn != nil || statusOf(resp) != http.StatusUnauthorized {
				t.Fatalf("Authorization %q: status %d, want 401 without upgrade", auth, statusOf(resp))
			}
		}
	})
}
