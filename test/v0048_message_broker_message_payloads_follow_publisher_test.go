package test

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Requirement: message-broker message payloads follow publisher-owned JSON
// Schemas (journal is a consumer and the owner of its reply schemas).
func TestV0048MessageBrokerMessagePayloadsFollowPublisherOwnedSchemas(t *testing.T) {
	t.Run("Publisher message violates its own schema", func(t *testing.T) {
		// journal publishes no stream message; its own published contract
		// is its reply schemas. A reply without a required field fails.
		h := newHarness(t)
		h.register(3, 7, "gpu")
		r := h.mustOK("executor-states", 3, map[string]any{})
		var body map[string]any
		if err := json.Unmarshal(r.Raw, &body); err != nil {
			t.Fatal(err)
		}
		items, _ := body["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("executor-states items %v", items)
		}
		delete(items[0].(map[string]any), "connection_state")
		broken, _ := json.Marshal(body)
		path := filepath.Join(journalDir(t), "schemas", "ui.executor-states.reply.schema.json")
		if err := validateJSON(t, path, broken); err == nil {
			t.Fatalf("reply without connection_state passed journal's own schema")
		}
	})

	t.Run("Consumer contract test reads the publisher schema", func(t *testing.T) {
		h := newHarness(t)
		h.register(3, 7, "gpu")
		m := contract(t, "svc/executor-registry/schemas", "executor_status",
			h.svcMsg("executor-registry", "executor_status", "3", map[string]any{
				"executor_id": 7, "connection_state": "connected", "max_parallel_tasks": 4, "held_claims": 1, "available_capacity": 3,
			}))
		h.mustAck(m)
		st := executorStates(h, 3)[7]
		if st["connection_state"] != "connected" || num(st["held_claims"]) != 1 || num(st["available_capacity"]) != 3 || num(st["max_parallel_tasks"]) != 4 {
			t.Fatalf("executor_status not applied: %v", st)
		}
		// Executor messages: one schema file for every kind.
		run := contract(t, "executor/docker_openhands/schemas", "running",
			h.execMsg(3, 7, "running", map[string]any{"task_id": 1010, "attempt": 1, "claim_seq": 1}))
		h.queued(3, 1010, 1)
		h.created(3, 7, 1010, 1, 1)
		h.claimAccepted(3, 7, 1010, 1, 1)
		h.mustAck(run)
		// journal imports no package of another service.
		assertOwnImports(t)
	})

	t.Run("Schema change selects its consumers", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join(contractFixtureDir(t), "qa-e2e", "scripts", "shared-test-inputs.json"))
		if err != nil {
			t.Fatal(err)
		}
		var inputs struct {
			Schemas map[string][]string `json:"schemas"`
		}
		if err := json.Unmarshal(raw, &inputs); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{
			"svc/executor-registry/schemas/event.executor_status.schema.json",
			"svc/task-manager/schemas/event.queued.schema.json",
			"executor/docker_openhands/schemas/executor-event.schema.json",
		} {
			if !contains(inputs.Schemas[path], "journal") {
				t.Fatalf("shared-test-inputs.json does not declare journal as a consumer of %s", path)
			}
		}
		own, _ := filepath.Glob(filepath.Join(journalDir(t), "schemas", "*.schema.json"))
		if len(own) == 0 {
			t.Fatalf("journal has no schema files")
		}
		for _, p := range own {
			rel := "svc/journal/schemas/" + filepath.Base(p)
			if users := inputs.Schemas[rel]; !contains(users, "journal") || !contains(users, "api-gateway") {
				t.Fatalf("shared-test-inputs.json declares %v for %s, want journal and api-gateway", users, rel)
			}
		}
	})
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// assertOwnImports rejects imports from other FlowAI services.
func assertOwnImports(t *testing.T) {
	t.Helper()
	const module = "github.com/pandops/flowai-journal"
	err := filepath.Walk(journalDir(t), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			legacy := p == "github.com/flowai/platform" || strings.HasPrefix(p, "github.com/flowai/platform/")
			otherService := strings.HasPrefix(p, "github.com/pandops/flowai-") &&
				p != module && !strings.HasPrefix(p, module+"/")
			if legacy || otherService {
				t.Fatalf("%s imports %s from another service", path, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
