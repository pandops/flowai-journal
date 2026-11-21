package test

// Survivors of deleted State Registry cases that had no journal test
// (ledger docs/testing/v0048-test-decisions.csv, rows named per subtest).

import (
	"net/url"
	"testing"
	"time"
)

func TestV0048JournalRehomedRestCases(t *testing.T) {
	// M342 (TestEventsStreamValidatesGatewayContextShape): with a valid
	// Gateway token, a missing, blank or non-integer team or operator
	// header is a non-auth 400 without upgrade or frame; the complete
	// context upgrades.
	t.Run("M342 stream edge validates the team context headers", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		h.claimTask(3, 7, 1010)
		e := h.edge()
		replay := url.Values{"after_frame_id": {"1"}}
		cases := map[string]func(hdr map[string][]string){
			"no team":           func(hdr map[string][]string) { delete(hdr, "X-Flowai-Team-Id") },
			"no operator":       func(hdr map[string][]string) { delete(hdr, "X-Flowai-Operator-Id") },
			"blank team":        func(hdr map[string][]string) { hdr["X-Flowai-Team-Id"] = []string{"   "} },
			"empty team":        func(hdr map[string][]string) { hdr["X-Flowai-Team-Id"] = []string{""} },
			"blank operator":    func(hdr map[string][]string) { hdr["X-Flowai-Operator-Id"] = []string{"   "} },
			"empty operator":    func(hdr map[string][]string) { hdr["X-Flowai-Operator-Id"] = []string{""} },
			"text team":         func(hdr map[string][]string) { hdr["X-Flowai-Team-Id"] = []string{"t"} },
			"text operator":     func(hdr map[string][]string) { hdr["X-Flowai-Operator-Id"] = []string{"o"} },
			"zero operator":     func(hdr map[string][]string) { hdr["X-Flowai-Operator-Id"] = []string{"0"} },
			"no team, operator": func(hdr map[string][]string) { delete(hdr, "X-Flowai-Team-Id"); delete(hdr, "X-Flowai-Operator-Id") },
		}
		for name, mutate := range cases {
			hdr := streamHeaders(3)
			mutate(hdr)
			conn, resp, err := e.dial(t, 3, hdr, replay)
			if err == nil || conn != nil {
				t.Fatalf("%s: upgraded, want 400 without upgrade", name)
			}
			if got := statusOf(resp); got != 400 {
				t.Fatalf("%s: status %d, want non-auth 400", name, got)
			}
			if code := errorCodeOf(t, resp); code != "invalid_request" {
				t.Fatalf("%s: code %q, want invalid_request", name, code)
			}
		}
		ok := e.open(t, 3, replay)
		if frames := readFrames(t, ok, 1, 2*time.Second); len(frames) != 1 || frames[0].TeamID != 3 {
			t.Fatalf("complete context: frames %+v, want a team 3 replay frame", frames)
		}
	})
}
