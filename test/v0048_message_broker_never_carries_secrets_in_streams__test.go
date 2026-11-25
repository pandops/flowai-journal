package test

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Requirement: message-broker never carries secrets in streams or logs
// (journal stores, serves, streams and logs none).
func TestV0048MessageBrokerNeverCarriesSecretsInStreamsOrLogs(t *testing.T) {
	t.Run("Stream content inspection", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		e := h.edge()
		conn := e.open(t, 3, nil)
		time.Sleep(50 * time.Millisecond)
		secrets := []string{"claim-token-SECRET-1", "eyJhbGciOiJFZERTQSJ9.SECRET-2", "sk-litellm-SECRET-3", "secret-value-SECRET-4", "launch-param-SECRET-5"} // gitleaks:allow (fake markers)
		// A misbehaving publisher puts secret-named fields into events.
		h.svc("task-manager", "queued", 3, merge(queuedFields(3, 1010, 1), map[string]any{"claim_token": secrets[0], "jwt": secrets[1]}))
		h.exec(3, 7, "created", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "litellm_key": secrets[2]})
		h.claimAccepted(3, 7, 1010, 1, 1)
		h.exec(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1, "secret": secrets[3], "launch_parameters": map[string]any{"API_TOKEN": secrets[4]}, "value": secrets[3], "api_key": secrets[2]})
		frames := readFrames(t, conn, 4, 2*time.Second)
		var seen strings.Builder
		for _, f := range frames {
			raw, _ := json.Marshal(f)
			seen.Write(raw)
		}
		for _, op := range []string{"task-events", "task-logs"} {
			seen.Write(h.mustOK(op, 3, map[string]any{"task_id": 1010}).Raw)
		}
		seen.Write(h.mustOK("executor-events", 3, map[string]any{"executor_id": 7}).Raw)
		replay := e.open(t, 3, url.Values{"after_frame_id": {strconv.Itoa(1)}})
		for _, f := range readFrames(t, replay, 3, time.Second) {
			raw, _ := json.Marshal(f)
			seen.Write(raw)
		}
		for _, ev := range h.store.Events() {
			seen.Write(ev.Data)
		}
		seen.WriteString(h.logs.String())
		for _, s := range secrets {
			if strings.Contains(seen.String(), s) {
				t.Fatalf("secret %q appears in stored events, replies, frames or logs", s)
			}
		}
		for _, k := range []string{`"claim_token"`, `"jwt"`, `"litellm_key"`, `"api_key"`, `"launch_parameters"`, `"secret"`} {
			if strings.Contains(seen.String(), k) {
				t.Fatalf("secret-named field %s kept", k)
			}
		}
		if len(h.store.Events()) < 4 {
			t.Fatalf("events with secret fields were dropped instead of stored without them")
		}
	})
}

func merge(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
