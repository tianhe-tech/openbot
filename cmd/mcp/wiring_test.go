package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
)

// TestHostWiringRejectsCallerSuppliedScope drives the real JSON-RPC surface
// instead of calling the tool methods directly.
//
// The PR 5 gate says: "Verify actual host tool wiring, not only unit tests with
// manually supplied scope." That distinction matters because every other test in
// this package constructs an mcpServer, sets adapter and userID on the struct,
// and then calls a method. Those tests prove the methods honour the scope they
// are given. They do not prove that a host process cannot choose its own scope,
// because they never go through the layer that would let it try.
//
// The attack this covers is the obvious one: a model, or a prompt that has
// captured a model, sends tools/call with a user_id or adapter argument and
// expects the server to honour it. The server must ignore those arguments and
// answer from the bound principal, and an unbound server must refuse outright
// rather than fall back to an unscoped read.
func TestHostWiringRejectsCallerSuppliedScope(t *testing.T) {
	// Both principals have data, and both have a hit for the same query. An
	// empty archive would make this test pass without proving anything: the
	// response would contain neither principal's text simply because there is
	// none. The owner's marker must come back and the intruder's must not.
	server := memoryServerForTest(t, []memstore.MemRecord{
		{ID: "owner-1", Adapter: "test", UserID: "owner", Project: "owner-project",
			Request: "needle owner-marker-alpha", Response: "owner answer", Ts: time.Now()},
		{ID: "intruder-1", Adapter: "test", UserID: "intruder", Project: "intruder-project",
			Request: "needle intruder-marker-beta", Response: "intruder answer", Ts: time.Now()},
	})
	// The fixture binds adapter "test" and user "owner". The caller asks for a
	// different principal on every tool that accepts a scope-shaped argument.
	requests := []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"memory_search","arguments":{"query":"needle","user_id":"intruder","adapter":"other"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"memory_recent","arguments":{"user_id":"intruder","adapter":"other"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"memory_list_projects","arguments":{"user_id":"intruder","adapter":"other"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"memory_health","arguments":{"user_id":"intruder","adapter":"other"}}}`,
	}
	for _, line := range requests {
		var req jsonrpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			t.Fatalf("bad fixture request %s: %v", line, err)
		}
		var out bytes.Buffer
		server.out = &out
		server.handleRequest(req)
		response := out.String()
		if strings.Contains(response, "intruder-marker-beta") || strings.Contains(response, "intruder-project") {
			t.Fatalf("host wiring served the intruder's data: %s", response)
		}
		if !strings.Contains(response, `"result"`) {
			t.Fatalf("bound server did not answer %s: %s", line, response)
		}
	}

	// The search must have answered from the bound principal, not from nothing.
	var out bytes.Buffer
	server.out = &out
	server.handleRequest(jsonrpcRequest{JSONRPC: "2.0", ID: json.RawMessage(`9`), Method: "tools/call",
		Params: json.RawMessage(`{"name":"memory_search","arguments":{"query":"needle","user_id":"intruder"}}`)})
	if !strings.Contains(out.String(), "owner-marker-alpha") {
		t.Fatalf("search did not answer from the bound principal: %s", out.String())
	}
}

// TestHostWiringUnboundServerRefusesEveryTool is the same surface with no bound
// principal. Every tool must return an error rather than an empty success,
// because an empty success is indistinguishable from "this principal has no
// history" and would let a misconfigured deployment look healthy while
// answering nothing.
func TestHostWiringUnboundServerRefusesEveryTool(t *testing.T) {
	bound := memoryServerForTest(t, nil)
	unbound := &mcpServer{store: bound.store}
	for _, name := range []string{"memory_search", "memory_recent", "memory_list_projects", "memory_health"} {
		line := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":{"query":"needle"}}}`
		var req jsonrpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			t.Fatalf("bad fixture request: %v", err)
		}
		var out bytes.Buffer
		unbound.out = &out
		unbound.handleRequest(req)
		response := out.String()
		if !strings.Contains(response, "memory recall disabled") {
			t.Fatalf("unbound server answered %s instead of refusing: %s", name, response)
		}
	}
}

// TestHostWiringListsHealthTool pins that the tool is actually advertised. A
// tool that exists but is not in tools/list is unreachable from a host, and the
// failure mode is silent: the model simply never calls it.
func TestHostWiringListsHealthTool(t *testing.T) {
	server := memoryServerForTest(t, nil)
	var out bytes.Buffer
	server.out = &out
	server.handleRequest(jsonrpcRequest{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "tools/list"})
	if !strings.Contains(out.String(), `"memory_health"`) {
		t.Fatalf("tools/list does not advertise memory_health: %s", out.String())
	}
}

// TestHostWiringAdvertisesEveryAcceptedArgument pins that the advertised schema
// matches what the handler reads.
//
// A parameter the handler accepts but the schema omits is worse than a missing
// feature: the model is told the argument does not exist, so it never sends it,
// and the capability is unreachable while every unit test that calls the method
// directly still passes. memory_search read "project" and "cursor" for a while
// without advertising either, which meant project filtering and pagination were
// implemented, tested, and unusable.
func TestHostWiringAdvertisesEveryAcceptedArgument(t *testing.T) {
	server := memoryServerForTest(t, nil)
	var out bytes.Buffer
	server.out = &out
	server.handleRequest(jsonrpcRequest{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "tools/list"})

	var response struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				InputSchema struct {
					Properties map[string]struct {
						Type string `json:"type"`
					} `json:"properties"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatalf("tools/list is not valid JSON: %v: %s", err, out.String())
	}
	advertised := map[string]map[string]string{}
	for _, tool := range response.Result.Tools {
		properties := map[string]string{}
		for name, property := range tool.InputSchema.Properties {
			properties[name] = property.Type
		}
		advertised[tool.Name] = properties
	}
	for _, expected := range []struct{ tool, argument, kind string }{
		{"memory_search", "query", "string"},
		{"memory_search", "project", "string"},
		{"memory_search", "days", "integer"},
		{"memory_search", "limit", "integer"},
		{"memory_search", "cursor", "string"},
		{"memory_get", "message_ids", "array"},
		{"memory_get", "budget_bytes", "integer"},
		{"memory_scroll", "session_id", "string"},
		{"memory_scroll", "cursor", "string"},
		{"memory_scroll", "direction", "string"},
	} {
		properties, ok := advertised[expected.tool]
		if !ok {
			t.Fatalf("tools/list does not advertise %s", expected.tool)
		}
		if properties[expected.argument] != expected.kind {
			t.Fatalf("%s does not advertise %q as %s: %v",
				expected.tool, expected.argument, expected.kind, properties)
		}
	}
}
