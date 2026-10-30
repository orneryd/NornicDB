package server

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestMCPEnabled_Default(t *testing.T) {
	config := DefaultConfig()
	if !config.MCPEnabled {
		t.Error("MCPEnabled should be true by default")
	}
}

func TestMCPServer_EnabledByDefault(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Default config should have MCP enabled
	config := DefaultConfig()
	config.EmbeddingEnabled = false // Disable embeddings to skip health check

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	if server.mcpServer == nil {
		t.Fatal("MCP server should be created when MCPEnabled is true (default)")
	}

	t.Log("✓ MCP server enabled by default")
}

func TestMCPServer_DisabledByConfig(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Explicitly disable MCP
	config := DefaultConfig()
	config.MCPEnabled = false
	config.EmbeddingEnabled = false

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	if server == nil {
		t.Fatal("Server should not be nil")
	}

	// MCP server should be nil when disabled
	if server.mcpServer != nil {
		t.Fatal("MCP server should be nil when MCPEnabled is false")
	}

	t.Log("✓ MCP server correctly disabled by config")
}

func TestMCPServer_DisabledStillAllowsHTTP(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Disable MCP but keep server functional
	config := DefaultConfig()
	config.MCPEnabled = false
	config.EmbeddingEnabled = false

	server, err := New(db, nil, config)
	if err != nil {
		t.Fatalf("Server creation failed: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	// Server should still be functional for HTTP API
	if server.db == nil {
		t.Fatal("Database reference should be set")
	}

	if server.config == nil {
		t.Fatal("Config should be set")
	}

	t.Log("✓ HTTP server still functional with MCP disabled")
}

// TestMCPToolRunnerAdapter_NilAllowlist verifies that when allowlist is nil,
// ToolNames() and ToolDefinitions() return all MCP tools (configuration: nil = all tools).
func TestMCPToolRunnerAdapter_NilAllowlist(t *testing.T) {
	mcpSrv := mcp.NewServer(nil, nil)
	adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: nil}

	names := adapter.ToolNames()
	allTools := mcp.AllTools()
	if len(names) != len(allTools) {
		t.Errorf("ToolNames(): want %d tools, got %d", len(allTools), len(names))
	}
	for i, n := range allTools {
		if i >= len(names) || names[i] != n {
			t.Errorf("ToolNames()[%d]: want %q, got %v", i, n, names[i])
			break
		}
	}

	defs := adapter.ToolDefinitions()
	if len(defs) != len(allTools) {
		t.Errorf("ToolDefinitions(): want %d tools, got %d", len(allTools), len(defs))
	}
	defNames := make(map[string]struct{})
	for _, d := range defs {
		defNames[d.Name] = struct{}{}
	}
	for _, n := range allTools {
		if _, ok := defNames[n]; !ok {
			t.Errorf("ToolDefinitions(): missing tool %q", n)
		}
	}
}

// TestMCPToolRunnerAdapter_EmptyAllowlist verifies that when allowlist is empty,
// ToolNames() and ToolDefinitions() return no tools (configuration: [] = no tools).
func TestMCPToolRunnerAdapter_EmptyAllowlist(t *testing.T) {
	mcpSrv := mcp.NewServer(nil, nil)
	adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: []string{}}

	names := adapter.ToolNames()
	if len(names) != 0 {
		t.Errorf("ToolNames(): want 0 tools, got %d: %v", len(names), names)
	}

	defs := adapter.ToolDefinitions()
	if len(defs) != 0 {
		t.Errorf("ToolDefinitions(): want 0 tools, got %d", len(defs))
	}
}

// TestMCPToolRunnerAdapter_SpecificAllowlist verifies that when allowlist is
// a subset of tool names, only those tools are returned.
func TestMCPToolRunnerAdapter_SpecificAllowlist(t *testing.T) {
	mcpSrv := mcp.NewServer(nil, nil)
	allowlist := []string{"store", "recall", "discover"}
	adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: allowlist}

	names := adapter.ToolNames()
	if len(names) != len(allowlist) {
		t.Errorf("ToolNames(): want %d tools, got %d: %v", len(allowlist), len(names), names)
	}
	for i, w := range allowlist {
		if i >= len(names) || names[i] != w {
			t.Errorf("ToolNames()[%d]: want %q, got %v", i, w, names[i])
			break
		}
	}

	defs := adapter.ToolDefinitions()
	if len(defs) != len(allowlist) {
		t.Errorf("ToolDefinitions(): want %d tools, got %d", len(allowlist), len(defs))
	}
	for _, d := range defs {
		found := false
		for _, w := range allowlist {
			if d.Name == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ToolDefinitions(): unexpected tool %q", d.Name)
		}
	}
}

// TestMCPToolRunnerAdapter_CallTool_RejectsWhenNotInAllowlist verifies that
// CallTool returns an error for tools not in the allowlist.
func TestMCPToolRunnerAdapter_CallTool_RejectsWhenNotInAllowlist(t *testing.T) {
	mcpSrv := mcp.NewServer(nil, nil)
	ctx := context.Background()

	t.Run("empty_allowlist_rejects_any_tool", func(t *testing.T) {
		adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: []string{}}
		_, err := adapter.CallTool(ctx, "store", map[string]interface{}{}, "")
		if err == nil {
			t.Fatal("CallTool(store) with empty allowlist should fail")
		}
		if !strings.Contains(err.Error(), "not in the MCP allowlist") {
			t.Errorf("expected allowlist error, got: %v", err)
		}
	})

	t.Run("specific_allowlist_rejects_other_tools", func(t *testing.T) {
		adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: []string{"store", "recall"}}
		_, err := adapter.CallTool(ctx, "discover", map[string]interface{}{}, "")
		if err == nil {
			t.Fatal("CallTool(discover) with allowlist [store,recall] should fail")
		}
		if !strings.Contains(err.Error(), "not in the MCP allowlist") {
			t.Errorf("expected allowlist error, got: %v", err)
		}
	})
}

// TestMCPToolRunnerAdapter_CallTool_AllowsWhenInAllowlist verifies that
// CallTool does not return an allowlist error when the tool is in the allowlist.
// It may still error for other reasons (e.g. missing args, nil DB).
func TestMCPToolRunnerAdapter_CallTool_AllowsWhenInAllowlist(t *testing.T) {
	mcpSrv := mcp.NewServer(nil, nil)
	ctx := context.Background()
	adapter := &mcpToolRunnerAdapter{s: mcpSrv, allowlist: []string{"store", "recall"}}

	_, err := adapter.CallTool(ctx, "store", map[string]interface{}{}, "")
	if err != nil && strings.Contains(err.Error(), "not in the MCP allowlist") {
		t.Errorf("store is in allowlist; should not get allowlist error: %v", err)
	}
}
