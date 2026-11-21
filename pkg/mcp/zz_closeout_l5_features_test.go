package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseoutL5MCPSurface(t *testing.T) {
	server := NewServer(nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	require.Equal(t, http.StatusOK, response.Code)
	var listing struct {
		Result ListToolsResponse `json:"result"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &listing))
	require.NotEmpty(t, listing.Result.Tools)
	inputs := make(map[string]map[string]json.RawMessage)
	for _, tool := range listing.Result.Tools {
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(tool.InputSchema, &schema))
		inputs[tool.Name] = schema.Properties
	}
	require.Contains(t, inputs["recall"], "id")
	require.Contains(t, inputs["discover"], "query")
	require.Contains(t, inputs["discover"], "depth")
	require.Contains(t, inputs["discover"], "limit")
	t.Logf("L5 MCP tools=%v", AllTools())

	for _, tc := range []struct {
		id     string
		tool   string
		fields []string
	}{
		{"P-N152-101", "discover", []string{"require_semantic", "search_policy", "rerank"}},
		{"P-N152-100", "recall", []string{"fields", "projection", "excerpt", "ids", "keys", "application_key"}},
		{"P-N152-99", "recall", []string{"cursor", "continue", "discard"}},
		{"P-N152-99", "discover", []string{"cursor", "continue", "discard"}},
		{"P-N152-98", "recall", []string{"filter", "filters", "predicate", "where"}},
		{"P-N152-98", "discover", []string{"filter", "filters", "predicate", "where"}},
		{"P-N152-97", "discover", []string{"start_id", "start_key", "target_id", "shortest_path"}},
		{"P-N152-105", "recall", []string{"view", "graph_view"}},
		{"P-N152-105", "discover", []string{"view", "graph_view"}},
	} {
		t.Run(tc.id+"/"+tc.tool, func(t *testing.T) {
			for _, field := range tc.fields {
				require.NotContains(t, inputs[tc.tool], field, "published input changed")
			}
			t.Logf("L5 %s %s has no advertised fields %v", tc.id, tc.tool, tc.fields)
		})
	}

	for _, tc := range []struct {
		id    string
		terms []string
	}{
		{"P-N152-97", []string{"neighborhood", "neighbourhood", "shortest_path", "traverse"}},
		{"P-N152-99", []string{"cursor", "continue", "discard"}},
		{"P-N152-103", []string{"register", "operation", "template"}},
		{"P-N152-104", []string{"history", "diff", "as_of"}},
		{"P-N151-30", []string{"view", "binding"}},
		{"P-N151-29", []string{"repository", "capture"}},
		{"P-N151-27", []string{"batch", "lifecycle"}},
		{"P-N151-26", []string{"export", "upgrade"}},
		{"P-N151-25", []string{"review", "record"}},
		{"P-N151-24", []string{"summary", "summaries"}},
		{"P-N151-23", []string{"blob", "ingest"}},
		{"P-N151-22", []string{"publish", "source"}},
		{"P-N151-21", []string{"correction", "conflict"}},
		{"P-N151-20", []string{"identity", "entity_merge"}},
		{"P-N151-19", []string{"job", "lease", "workflow"}},
		{"H-243", []string{"citation", "source_span"}},
	} {
		t.Run(tc.id+"/tools", func(t *testing.T) {
			for tool := range inputs {
				for _, term := range tc.terms {
					require.NotContains(t, strings.ToLower(tool), term)
				}
			}
			t.Logf("L5 %s shipped MCP catalog offers none of %v", tc.id, tc.terms)
		})
	}

	t.Run("P-N152-103/host-plugin-control", func(t *testing.T) {
		require.NoError(t, server.RegisterTool(Tool{
			Name: "closeout_echo", InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`),
		}, func(_ context.Context, args map[string]interface{}) (interface{}, error) {
			return args["name"], nil
		}))
		got, err := server.CallTool(t.Context(), "closeout_echo", map[string]interface{}{"name": "control"})
		require.NoError(t, err)
		require.Equal(t, "control", got)
		t.Log("L5 P-N152-103 host registration works")
	})
}
