package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-journal/internal/cursor"
	"github.com/pandops/flowai-journal/internal/engine"
	"github.com/pandops/flowai-journal/internal/httpedge"
	"github.com/pandops/flowai-journal/internal/mocks"
	"github.com/pandops/flowai-journal/internal/rpc"
)

// absentField removes an envelope field from a built message.
type absentField struct{}

var absent = absentField{}

// journalDir is the standalone root, found from this file's location.
func journalDir(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Dir(filepath.Dir(file))
}

// contractFixtureDir points to publisher inputs staged outside the checkout.
func contractFixtureDir(t testing.TB) string {
	t.Helper()
	const variable = "FLOWAI_CONTRACT_FIXTURE_DIR"
	dir := os.Getenv(variable)
	if !filepath.IsAbs(dir) {
		t.Fatalf("%s must be an absolute directory outside the journal checkout", variable)
	}
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve %s: %v", variable, err)
	}
	checkout, err := filepath.EvalSymlinks(journalDir(t))
	if err != nil {
		t.Fatalf("resolve journal checkout: %v", err)
	}
	if rel, err := filepath.Rel(checkout, root); err == nil && filepath.IsLocal(rel) {
		t.Fatalf("%s must be outside the journal checkout", variable)
	}
	return root
}

// clock is a settable test clock.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// harness wires an engine over the in-memory store with a fixed clock.
type harness struct {
	t         *testing.T
	store     *mocks.MemStore
	eng       *engine.Engine
	clock     *clock
	rpc       *rpc.Handler
	keys      *cursor.Keyring
	streamSeq uint64
	pubSeq    map[string]int64
	committed []int64
	mu        sync.Mutex
	hub       *httpedge.Hub
	logs      *syncBuffer
}

// syncBuffer collects log output from concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var testCursorKeys = []cursor.Key{{ID: 1, Secret: []byte("journal-test-cursor-key-0000000001")}, {ID: 2, Secret: []byte("journal-test-cursor-key-0000000002")}}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, store: mocks.NewMemStore(), clock: &clock{now: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)}, pubSeq: map[string]int64{}, hub: httpedge.NewHub(), logs: &syncBuffer{}}
	h.eng = engine.New(h.store, engine.Options{Now: h.clock.Now, Logger: slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})), OnCommit: func(teams []int64) {
		h.mu.Lock()
		h.committed = append(h.committed, teams...)
		h.mu.Unlock()
		h.hub.Notify(teams)
	}})
	keys, err := cursor.NewKeyring(testCursorKeys)
	if err != nil {
		t.Fatalf("keyring: %v", err)
	}
	h.keys = keys
	h.rpc = &rpc.Handler{Engine: h.eng, Cursor: keys}
	return h
}

func (h *harness) nextPubSeq(key string) int64 {
	h.pubSeq[key]++
	return h.pubSeq[key]
}

func (h *harness) nextStreamSeq() uint64 {
	h.streamSeq++
	return h.streamSeq
}

// msg is one built message; deliver it with h.deliver.
type msg struct {
	engine.Inbound
	PublisherSeq int64
}

// svcMsg builds a service event on <svc>.event.<kind>.sub. teamHeader is
// the decimal team or "admin"; payload fields override the envelope.
func (h *harness) svcMsg(svc, kind, teamHeader string, fields map[string]any) msg {
	h.t.Helper()
	payload := map[string]any{
		"publisher":     svc,
		"publisher_seq": h.nextPubSeq(svc),
		"occurred_at":   engine.FormatTime(h.clock.Now()),
		"kind":          kind,
	}
	if team, err := strconv.ParseInt(teamHeader, 10, 64); err == nil {
		payload["team_id"] = team
	}
	for k, v := range fields {
		if v == absent {
			delete(payload, k)
			continue
		}
		payload[k] = v
	}
	data, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	seq, _ := payload["publisher_seq"].(int64)
	if n, ok := payload["publisher_seq"].(int); ok {
		seq = int64(n)
	}
	hdr := map[string][]string{"Nats-Msg-Id": {fmt.Sprintf("%s-%d", svc, seq)}}
	if teamHeader != "" {
		hdr["team"] = []string{teamHeader}
	}
	return msg{Inbound: engine.Inbound{Subject: svc + ".event." + kind + ".sub", Header: hdr, Data: data}, PublisherSeq: seq}
}

// execMsg builds an Executor event on executor.event.<executor>.<kind>.sub.
// team is the Executor's JWT team (payload team_id and team header).
func (h *harness) execMsg(team, executor int64, kind string, fields map[string]any) msg {
	h.t.Helper()
	key := fmt.Sprintf("executor-%d-%d", team, executor)
	seq := h.nextPubSeq(key)
	payload := map[string]any{
		"publisher":     "executor",
		"publisher_seq": seq,
		"team_id":       team,
		"executor_id":   executor,
		"occurred_at":   engine.FormatTime(h.clock.Now()),
		"kind":          kind,
	}
	for k, v := range fields {
		if v == absent {
			delete(payload, k)
			continue
		}
		payload[k] = v
	}
	data, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	hdr := map[string][]string{"Nats-Msg-Id": {fmt.Sprintf("%s-%d", key, seq)}, "team": {strconv.FormatInt(team, 10)}}
	return msg{Inbound: engine.Inbound{Subject: fmt.Sprintf("executor.event.%d.%s.sub", executor, kind), Header: hdr, Data: data}, PublisherSeq: seq}
}

// deliver hands m to the engine with the next stream sequence.
func (h *harness) deliver(m msg) engine.Disposition {
	h.t.Helper()
	in := m.Inbound
	if in.StreamSeq == 0 {
		in.StreamSeq = h.nextStreamSeq()
	}
	return h.eng.Handle(context.Background(), in)
}

// redeliver hands m to the engine again with the same stream sequence.
func (h *harness) redeliver(m msg, streamSeq uint64) engine.Disposition {
	h.t.Helper()
	in := m.Inbound
	in.StreamSeq = streamSeq
	return h.eng.Handle(context.Background(), in)
}

func (h *harness) mustAck(m msg) {
	h.t.Helper()
	if d := h.deliver(m); d != engine.Ack {
		h.t.Fatalf("deliver %s: disposition %v, want ack", m.Subject, d)
	}
}

// svc delivers a service event and requires an ack.
func (h *harness) svc(svc, kind string, team int64, fields map[string]any) {
	h.t.Helper()
	h.mustAck(h.svcMsg(svc, kind, strconv.FormatInt(team, 10), fields))
}

// exec delivers an Executor event and requires an ack.
func (h *harness) exec(team, executor int64, kind string, fields map[string]any) {
	h.t.Helper()
	h.mustAck(h.execMsg(team, executor, kind, fields))
}

// register publishes executor-registry's executor_registered.
func (h *harness) register(team, executor int64, tag string) {
	h.t.Helper()
	h.svc("executor-registry", "executor_registered", team, map[string]any{"executor_id": executor, "execution_tag": tag, "executor_type": "executor_docker_openhands"})
}

func (h *harness) queued(team, task, attempt int64) {
	h.t.Helper()
	h.svc("task-manager", "queued", team, queuedFields(team, task, attempt))
}

// queuedFields are the fields of a task-manager queued event for one
// attempt, valid under task-manager's event.queued schema.
func queuedFields(team, task, attempt int64) map[string]any {
	return map[string]any{"task_id": task, "task_name": taskName(team, task), "attempt": attempt, "claim_seq": attempt, "intake_id": task, "flow_id": 1, "source_id": 1, "required_tag": "gpu", "task_publisher_seq": task}
}

// taskName is the task_name task-manager puts on every task event:
// <team_name>-<task_id>, where the fixture team's name is team<team_id>
// (a valid team name of at most 10 characters).
func taskName(team, task int64) string {
	return "team" + strconv.FormatInt(team, 10) + "-" + strconv.FormatInt(task, 10)
}

func (h *harness) created(team, executor, task, attempt, claim int64) {
	h.t.Helper()
	h.exec(team, executor, "created", map[string]any{"task_id": task, "attempt": attempt, "claim_seq": claim})
}

func (h *harness) claimAccepted(team, executor, task, attempt, claim int64) {
	h.t.Helper()
	h.svc("task-manager", "claim_accepted", team, map[string]any{"task_id": task, "task_name": taskName(team, task), "attempt": attempt, "claim_seq": claim, "executor_id": executor, "created_publisher_seq": 1})
}

func (h *harness) logChunk(team, executor, task, attempt, claim int64, stream, content string) msg {
	h.t.Helper()
	return h.execMsg(team, executor, "log", map[string]any{"task_id": task, "attempt": attempt, "claim_seq": claim, "stream": stream, "text": content})
}

// claimTask registers nothing; it queues a task and lets executor win
// claim (attempt, claim) of it.
func (h *harness) claimTask(team, executor, task int64) {
	h.t.Helper()
	h.queued(team, task, 1)
	h.created(team, executor, task, 1, 1)
	h.claimAccepted(team, executor, task, 1, 1)
}

// uiHeaders are the verified headers API Gateway sets.
func uiHeaders(team int64) map[string][]string {
	return map[string][]string{"team": {strconv.FormatInt(team, 10)}, "operator_id": {"501"}, "request_id": {"9001"}, "team_name": {"Operations"}}
}

// uiReply is a decoded reply.
type uiReply struct {
	Status int
	Header map[string]string
	Raw    []byte
	Body   map[string]any
}

func (r uiReply) items() []map[string]any {
	raw, _ := r.Body["items"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, it := range raw {
		m, _ := it.(map[string]any)
		out = append(out, m)
	}
	return out
}

func (r uiReply) code() string {
	c, _ := r.Body["code"].(string)
	return c
}

// call sends one ui request with the given headers and params.
func (h *harness) call(op string, headers map[string][]string, params map[string]any) uiReply {
	h.t.Helper()
	body, err := json.Marshal(map[string]any{"params": params})
	if err != nil {
		h.t.Fatalf("marshal: %v", err)
	}
	return h.callRaw(op, headers, body)
}

func (h *harness) callRaw(op string, headers map[string][]string, body []byte) uiReply {
	h.t.Helper()
	rep := h.rpc.Serve(context.Background(), op, headers, body)
	out := uiReply{Status: rep.Status, Header: rep.Header, Raw: rep.Body}
	if len(rep.Body) > 0 {
		if err := json.Unmarshal(rep.Body, &out.Body); err != nil {
			h.t.Fatalf("reply of %s is not a JSON object: %v: %s", op, err, rep.Body)
		}
	}
	return out
}

// ui sends a request for team with params (team_id added).
func (h *harness) ui(op string, team int64, params map[string]any) uiReply {
	h.t.Helper()
	p := map[string]any{"team_id": team}
	for k, v := range params {
		p[k] = v
	}
	return h.call(op, uiHeaders(team), p)
}

func (h *harness) mustOK(op string, team int64, params map[string]any) uiReply {
	h.t.Helper()
	r := h.ui(op, team, params)
	if r.Status != 200 {
		h.t.Fatalf("%s for team %d: status %d body %s", op, team, r.Status, r.Raw)
	}
	validateReply(h.t, op, r)
	return r
}

func kinds(items []map[string]any) []string {
	out := []string{}
	for _, it := range items {
		k, _ := it["kind"].(string)
		out = append(out, k)
	}
	return out
}

func num(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case int64:
		return n
	case int:
		return int64(n)
	}
	return -1
}

// ---- JSON Schema validation ----

var (
	schemaOnce     sync.Mutex
	schemaCompiled = map[string]*jsonschema.Schema{}
)

// compileSchema compiles the schema file at an absolute path.
func compileSchema(t testing.TB, path string) *jsonschema.Schema {
	t.Helper()
	schemaOnce.Lock()
	defer schemaOnce.Unlock()
	if s, ok := schemaCompiled[path]; ok {
		return s
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("schema file %s: %v", path, err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	// Sibling schemas reference each other by their $id; register every
	// schema of the directory under it.
	target := path
	siblings, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "*.json"))
	for _, f := range siblings {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read schema %s: %v", f, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			continue // not a JSON document (consumers.json still parses)
		}
		if m, ok := doc.(map[string]any); ok {
			if id, ok := m["$id"].(string); ok && id != "" {
				if err := c.AddResource(id, doc); err != nil {
					t.Fatalf("register schema %s: %v", f, err)
				}
				if f == path {
					target = id
				}
			}
		}
	}
	s, err := c.Compile(target)
	if err != nil {
		t.Fatalf("compile schema %s: %v", path, err)
	}
	schemaCompiled[path] = s
	return s
}

func validateJSON(t testing.TB, schemaPath string, raw []byte) error {
	t.Helper()
	s := compileSchema(t, schemaPath)
	inst, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	return s.Validate(inst)
}

// validateReply checks a reply against journal's own reply schema.
func validateReply(t testing.TB, op string, r uiReply) {
	t.Helper()
	name := "ui." + op + ".reply.schema.json"
	if r.Status != 200 {
		name = "error.reply.schema.json"
	}
	if err := validateJSON(t, filepath.Join(journalDir(t), "schemas", name), r.Raw); err != nil {
		t.Fatalf("%s reply violates %s: %v\n%s", op, name, err, r.Raw)
	}
}

// publisherSchema returns the path of the publisher's schema file for
// one message kind under dir (for example "svc/executor-registry/schemas"),
// failing when it does not exist or qa-e2e/scripts/shared-test-inputs.json
// does not declare it as an input of journal's tests.
func publisherSchema(t testing.TB, dir, kind string) string {
	t.Helper()
	root := contractFixtureDir(t)
	if strings.HasPrefix(dir, "executor/") {
		// Executors keep one schema file for every kind (agents.md rule 15).
		return declared(t, root, filepath.Join(root, dir, "executor-event.schema.json"))
	}
	matches, _ := filepath.Glob(filepath.Join(root, dir, "*"+kind+".*json"))
	var path string
	for _, m := range matches {
		base := filepath.Base(m)
		if strings.Contains(base, "."+kind+".") || strings.HasPrefix(base, kind+".") {
			path = m
			break
		}
	}
	if path == "" {
		t.Fatalf("publisher schema for %s under %s not written", kind, dir)
	}
	return declared(t, root, path)
}

// declared fails unless the schema file exists and
// qa-e2e/scripts/shared-test-inputs.json declares it as a test input.
func declared(t testing.TB, root, path string) string {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("publisher schema %s not written: %v", path, err)
	}
	rel, _ := filepath.Rel(root, path)
	raw, err := os.ReadFile(filepath.Join(root, "qa-e2e", "scripts", "shared-test-inputs.json"))
	if err != nil {
		t.Fatalf("read shared-test-inputs.json: %v", err)
	}
	if !strings.Contains(string(raw), `"`+filepath.ToSlash(rel)+`"`) {
		t.Fatalf("shared-test-inputs.json does not declare %s as a journal test input", rel)
	}
	return path
}

// contract validates m's payload against the publisher's schema for its
// kind before it is delivered, so the input is a message the publisher
// may send.
func contract(t testing.TB, dir, kind string, m msg) msg {
	t.Helper()
	path := publisherSchema(t, dir, kind)
	if err := validateJSON(t, path, m.Data); err != nil {
		t.Fatalf("test input is not valid under %s: %v\n%s", path, err, m.Data)
	}
	return m
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
