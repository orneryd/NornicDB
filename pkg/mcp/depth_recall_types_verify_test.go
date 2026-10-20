package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise the published MCP contracts through every shared dispatch route.
func verifyMCP144Call(t *testing.T, server *Server, route, name string, args map[string]interface{}, result interface{}) {
	t.Helper()
	var response CallToolResponse
	if route == "in_process" {
		raw, err := server.CallTool(context.Background(), name, args)
		require.NoError(t, err)
		encoded, err := json.Marshal(raw)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(encoded, result))
		return
	}
	payload := map[string]interface{}{"name": name, "arguments": args}
	path := "/mcp/tools/call"
	if route == "jsonrpc" {
		payload = map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": payload}
		path = "/mcp"
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	if route == "jsonrpc" {
		var envelope struct {
			Result CallToolResponse `json:"result"`
			Error  json.RawMessage  `json:"error"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
		require.Empty(t, envelope.Error, rec.Body.String())
		response = envelope.Result
	} else {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	}
	require.False(t, response.IsError, rec.Body.String())
	require.Len(t, response.Content, 1)
	require.NoError(t, json.Unmarshal([]byte(response.Content[0].Text), result))
}

func verifyMCP144Schema(t *testing.T, server *Server, tool, parameter string) map[string]interface{} {
	t.Helper()
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp/tools/list", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var listed ListToolsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	for _, def := range listed.Tools {
		if def.Name == tool {
			var schema map[string]interface{}
			require.NoError(t, json.Unmarshal(def.InputSchema, &schema))
			return schema["properties"].(map[string]interface{})[parameter].(map[string]interface{})
		}
	}
	t.Fatalf("tool %q not listed", tool)
	return nil
}

func TestVerifyMCP144DiscoverDepthSchema(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	engine := db.GetStorage()
	for i := 0; i < 4; i++ {
		content := "supporting context"
		if i == 0 {
			content = "depthverifyanchor"
		}
		if i == 3 {
			content = "depthverifyreverse"
		}
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("depth-%d", i)), Labels: []string{"DepthProbe"},
			Properties: map[string]interface{}{"title": fmt.Sprintf("chain-%d", i), "content": content},
		})
		require.NoError(t, err)
		if i > 0 {
			require.NoError(t, engine.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("depth-edge-%d", i)),
				StartNode: storage.NodeID(fmt.Sprintf("depth-%d", i-1)),
				EndNode:   storage.NodeID(fmt.Sprintf("depth-%d", i)), Type: "NEXT",
			}))
		}
	}
	require.NoError(t, db.BuildSearchIndexes(context.Background()))
	server := NewServer(db, nil)
	schema := verifyMCP144Schema(t, server, "discover", "depth")
	t.Logf("published discover.depth schema: %v", schema)
	for _, meaning := range []string{"1=matches only", "2=1 hop", "3=2 hops"} {
		require.Contains(t, schema["description"], meaning)
	}
	require.Equal(t, float64(1), schema["default"])
	require.Equal(t, float64(1), schema["minimum"])
	require.Equal(t, float64(3), schema["maximum"])
	for _, route := range []string{"jsonrpc", "http_alias", "in_process"} {
		for _, direction := range []string{"outgoing", "incoming"} {
			for depth := 1; depth <= 3; depth++ {
				t.Run(fmt.Sprintf("%s/%s/depth_%d", route, direction, depth), func(t *testing.T) {
					query, root := "depthverifyanchor", "chain-0"
					if direction == "incoming" {
						query, root = "depthverifyreverse", "chain-3"
					}
					var result DiscoverResult
					verifyMCP144Call(t, server, route, "discover", map[string]interface{}{
						"query": query, "depth": depth, "limit": 1,
					}, &result)
					require.Len(t, result.Results, 1)
					require.Equal(t, root, result.Results[0].Title)
					gotHops := make([]int, 0)
					wantHops := make([]int, 0)
					for hop := 1; hop < depth; hop++ {
						wantHops = append(wantHops, hop)
					}
					for _, related := range result.Results[0].Related {
						gotHops = append(gotHops, related.Distance)
						assert.Equal(t, direction, related.Direction)
						assert.Len(t, related.Path, related.Distance+1)
					}
					sort.Ints(gotHops)
					t.Logf("context depth=%d; returned neighbour hops=%v; want=%v", depth, gotHops, wantHops)
					assert.Equal(t, wantHops, gotHops, "context depth must expand up to depth-1 hops")
				})
			}
		}
	}
}

func TestVerifyMCP144RecallAllRequestedTypes(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, fixture := range []struct {
		title  string
		labels []string
	}{
		{"only-x", []string{"VerifyX"}},
		{"only-y", []string{"VerifyY"}},
		{"both-xy", []string{"VerifyX", "VerifyY"}},
		{"excluded-z", []string{"VerifyZ"}},
	} {
		_, err := db.GetStorage().CreateNode(&storage.Node{
			ID: storage.NodeID(fixture.title), Labels: fixture.labels,
			Properties: map[string]interface{}{"title": fixture.title, "content": "type filter verification"},
		})
		require.NoError(t, err)
	}
	server := NewServer(db, nil)
	schema := verifyMCP144Schema(t, server, "recall", "type")
	t.Logf("published recall.type schema: %v", schema)
	require.Equal(t, "array", schema["type"])
	require.Contains(t, schema["description"], "any listed node type")
	for _, route := range []string{"jsonrpc", "http_alias", "in_process"} {
		for _, tc := range []struct {
			name  string
			types []string
			want  []string
		}{
			{"empty", []string{}, []string{"only-x", "only-y", "both-xy", "excluded-z"}},
			{"x", []string{"VerifyX"}, []string{"only-x", "both-xy"}},
			{"y", []string{"VerifyY"}, []string{"only-y", "both-xy"}},
			{"xy", []string{"VerifyX", "VerifyY"}, []string{"only-x", "only-y", "both-xy"}},
			{"yx", []string{"VerifyY", "VerifyX"}, []string{"only-x", "only-y", "both-xy"}},
			{"missing_y", []string{"MissingType", "VerifyY"}, []string{"only-y", "both-xy"}},
			{"duplicate_x", []string{"VerifyX", "VerifyX"}, []string{"only-x", "both-xy"}},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				var result RecallResult
				verifyMCP144Call(t, server, route, "recall", map[string]interface{}{"type": tc.types, "limit": 100}, &result)
				got := make([]string, 0, len(result.Nodes))
				for _, node := range result.Nodes {
					got = append(got, node.Title)
				}
				sort.Strings(got)
				t.Logf("recall.type=%v returned titles=%v count=%d", tc.types, got, result.Count)
				assert.Equal(t, len(result.Nodes), result.Count)
				assert.ElementsMatch(t, tc.want, got, "all requested types must participate in the OR filter")
			})
		}
	}
}
