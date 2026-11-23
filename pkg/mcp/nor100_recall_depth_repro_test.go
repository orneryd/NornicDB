package mcp

// NOR-100 reproduction coverage.
//
// It pins the two reported contracts through the public MCP surfaces (the
// JSON-RPC /mcp endpoint and the REST-style /mcp/tools/call routes served by
// Server.ServeHTTP):
//
//   - recall must filter on every requested type (union of the requested
//     labels), not only the first one;
//   - discover must clamp depth to the advertised 1-3 range and traverse
//     depth-1 hops (depth 1 publishes no related nodes, depth 2 one hop,
//     depth 3 two hops).
//
// The union cases were red before N152-44 because handleRecall consulted
// nodeTypes[0] only; the canonical handler now matches every requested label.
// Everything else (single/empty/unknown type behavior, limit-after-eligibility,
// depth clamps and the advertised tools/list schema bounds) is asserted against
// the current implementation. The depth description now states the hop
// semantics and is logged rather than pinned as exact prose.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOR-100 recall fixture seed positions. Every union case leaves the Noise node
// unrequested, so a limit spent before the type filter would surface an
// ineligible node; assertions never depend on storage scan order.
const (
	nor100Noise = iota
	nor100MemoryAlpha
	nor100TaskAlpha
	nor100MemoryBeta
	nor100DecisionAlpha
)

// seedNOR100RecallFixture builds the small real-database population shared by
// the recall cases: one unrequested Noise node, two Memory nodes, one Task and
// one Decision. It reuses the package's canonical MCP database fixture.
func seedNOR100RecallFixture(t *testing.T) (*nornicdb.DB, []string) {
	t.Helper()
	db, ids := seedMCPDiscoverGraph(t, []mcpDiscoverNodeSeed{
		{label: "Noise", props: map[string]interface{}{"title": "Noise Note", "content": "unrequested noise document"}},
		{label: "Memory", props: map[string]interface{}{"title": "Memory Alpha", "content": "first memory document"}},
		{label: "Task", props: map[string]interface{}{"title": "Task Alpha", "content": "task document"}},
		{label: "Memory", props: map[string]interface{}{"title": "Memory Beta", "content": "second memory document"}},
		{label: "Decision", props: map[string]interface{}{"title": "Decision Alpha", "content": "decision document"}},
	}, nil)
	require.Len(t, ids, 5, "the recall fixture keeps its own five-node population")
	return db, ids
}

// nor100MCPJSONRPC posts one JSON-RPC request through the public MCP router and
// returns the decoded result member.
func nor100MCPJSONRPC(t *testing.T, server *Server, path, method string, params map[string]interface{}) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope), "body: %s", rec.Body.String())
	require.Empty(t, envelope.Error, "JSON-RPC error: %s", string(envelope.Error))
	return envelope.Result
}

// nor100JSONRPCToolCall invokes one tool over the JSON-RPC tools/call method.
func nor100JSONRPCToolCall(t *testing.T, server *Server, path, tool string, args map[string]interface{}) CallToolResponse {
	t.Helper()
	raw := nor100MCPJSONRPC(t, server, path, "tools/call", map[string]interface{}{
		"name":      tool,
		"arguments": args,
	})
	var response CallToolResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	return response
}

// nor100RESTToolCall invokes one tool over the REST-style tools/call route,
// including the database-pinned form when path carries a pin.
func nor100RESTToolCall(t *testing.T, server *Server, path, tool string, args map[string]interface{}) CallToolResponse {
	t.Helper()
	body, err := json.Marshal(CallToolRequest{Name: tool, Arguments: args})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var response CallToolResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	return response
}

// nor100RESTToolsList fetches tools/list over the REST-style route.
func nor100RESTToolsList(t *testing.T, server *Server) ListToolsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/mcp/tools/list", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var listed ListToolsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listed))
	return listed
}

// nor100JSONRPCToolsList fetches tools/list over the JSON-RPC endpoint.
func nor100JSONRPCToolsList(t *testing.T, server *Server) ListToolsResponse {
	t.Helper()
	var listed ListToolsResponse
	require.NoError(t, json.Unmarshal(nor100MCPJSONRPC(t, server, "/mcp", "tools/list", nil), &listed))
	return listed
}

func nor100ContentText(response CallToolResponse) string {
	if len(response.Content) == 0 {
		return "<no content>"
	}
	return response.Content[0].Text
}

func nor100DecodeRecall(t *testing.T, response CallToolResponse) RecallResult {
	t.Helper()
	require.False(t, response.IsError, "recall reported a tool error: %s", nor100ContentText(response))
	require.Len(t, response.Content, 1, "recall returns one text content entry")
	var recall RecallResult
	require.NoError(t, json.Unmarshal([]byte(nor100ContentText(response)), &recall))
	return recall
}

func nor100RecallIDs(recall RecallResult) []string {
	ids := make([]string, 0, len(recall.Nodes))
	for _, node := range recall.Nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func nor100RecallTypes(recall RecallResult) []string {
	types := make([]string, 0, len(recall.Nodes))
	for _, node := range recall.Nodes {
		types = append(types, node.Type)
	}
	return types
}

func nor100RelatedIDs(related []RelatedNode) []string {
	ids := make([]string, 0, len(related))
	for _, node := range related {
		ids = append(ids, node.ID)
	}
	return ids
}

// nor100RecallTransport is one public surface that reaches handleRecall.
type nor100RecallTransport struct {
	name string
	call func(t *testing.T, server *Server, args map[string]interface{}) CallToolResponse
}

func nor100RecallTransports() []nor100RecallTransport {
	return []nor100RecallTransport{
		{name: "jsonrpc", call: func(t *testing.T, server *Server, args map[string]interface{}) CallToolResponse {
			return nor100JSONRPCToolCall(t, server, "/mcp", ToolRecall, args)
		}},
		{name: "rest", call: func(t *testing.T, server *Server, args map[string]interface{}) CallToolResponse {
			return nor100RESTToolCall(t, server, "/mcp/tools/call", ToolRecall, args)
		}},
		{name: "rest pinned database", call: func(t *testing.T, server *Server, args map[string]interface{}) CallToolResponse {
			return nor100RESTToolCall(t, server, "/mcp/nornic/tools/call", ToolRecall, args)
		}},
	}
}

// TestNOR100RecallFiltersOnEveryRequestedType is the N152-44 regression: a
// recall that requests two or three types must return the union of the
// requested labels, in any request order, and an unknown first type must not
// suppress a later valid type. It reproduced the nodeTypes[0]-only behavior
// before the handler fix.
func TestNOR100RecallFiltersOnEveryRequestedType(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			t.Cleanup(func() { config.SetParserType(previous) })

			db, ids := seedNOR100RecallFixture(t)
			server := NewServer(db, nil)

			cases := []struct {
				name      string
				types     []interface{}
				wantIDs   []string
				wantTypes []string
			}{
				{
					name:      "two requested types return the union",
					types:     []interface{}{"Memory", "Task"},
					wantIDs:   []string{ids[nor100MemoryAlpha], ids[nor100TaskAlpha], ids[nor100MemoryBeta]},
					wantTypes: []string{"Memory", "Task"},
				},
				{
					name:      "three requested types return the union",
					types:     []interface{}{"Memory", "Task", "Decision"},
					wantIDs:   []string{ids[nor100MemoryAlpha], ids[nor100TaskAlpha], ids[nor100MemoryBeta], ids[nor100DecisionAlpha]},
					wantTypes: []string{"Memory", "Task", "Decision"},
				},
				{
					name:      "requested type order does not change the union",
					types:     []interface{}{"Decision", "Task", "Memory"},
					wantIDs:   []string{ids[nor100MemoryAlpha], ids[nor100TaskAlpha], ids[nor100MemoryBeta], ids[nor100DecisionAlpha]},
					wantTypes: []string{"Memory", "Task", "Decision"},
				},
				{
					name:      "unknown first type still returns the later valid type",
					types:     []interface{}{"Bogus", "Memory"},
					wantIDs:   []string{ids[nor100MemoryAlpha], ids[nor100MemoryBeta]},
					wantTypes: []string{"Memory"},
				},
			}

			for _, transport := range nor100RecallTransports() {
				t.Run(transport.name, func(t *testing.T) {
					for _, tc := range cases {
						t.Run(tc.name, func(t *testing.T) {
							recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
								"type":  tc.types,
								"limit": 10,
							}))

							assert.Equal(t, len(tc.wantIDs), recall.Count)
							assert.Len(t, recall.Nodes, len(tc.wantIDs))
							assert.ElementsMatch(t, tc.wantIDs, nor100RecallIDs(recall))
							for _, node := range recall.Nodes {
								require.Contains(t, tc.wantTypes, node.Type)
							}
							require.NotContains(t, nor100RecallIDs(recall), ids[nor100Noise],
								"the unrequested Noise node stays excluded")
						})
					}
				})
			}
		})
	}
}

// TestNOR100RecallSingleEmptyUnknownAndLimitSemantics keeps the existing
// single-type, empty-type, unknown-type and limit behavior visible on the same
// fixture and public surfaces as the union reproduction.
func TestNOR100RecallSingleEmptyUnknownAndLimitSemantics(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			db, ids := seedNOR100RecallFixture(t)
			server := NewServer(db, nil)
			allIDs := []string{
				ids[nor100Noise],
				ids[nor100MemoryAlpha],
				ids[nor100TaskAlpha],
				ids[nor100MemoryBeta],
				ids[nor100DecisionAlpha],
			}

			for _, transport := range nor100RecallTransports() {
				t.Run(transport.name, func(t *testing.T) {
					t.Run("single requested type keeps its existing filter", func(t *testing.T) {
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{"Task"},
							"limit": 10,
						}))
						require.Equal(t, 1, recall.Count)
						require.Equal(t, []string{ids[nor100TaskAlpha]}, nor100RecallIDs(recall))
						require.Equal(t, []string{"Task"}, nor100RecallTypes(recall))
					})

					t.Run("empty requested type array keeps the unfiltered contract", func(t *testing.T) {
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{},
							"limit": 10,
						}))
						require.Equal(t, len(allIDs), recall.Count)
						require.ElementsMatch(t, allIDs, nor100RecallIDs(recall))
					})

					t.Run("absent requested type keeps the unfiltered contract", func(t *testing.T) {
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"limit": 10,
						}))
						require.Equal(t, len(allIDs), recall.Count)
						require.ElementsMatch(t, allIDs, nor100RecallIDs(recall))
					})

					t.Run("unknown type alone returns no nodes without an error", func(t *testing.T) {
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{"Bogus"},
							"limit": 10,
						}))
						require.Zero(t, recall.Count)
						require.Empty(t, recall.Nodes)
					})

					t.Run("single-type limit applies after eligibility", func(t *testing.T) {
						// Storage scan order is not guaranteed, so the assertion is
						// order-insensitive: the only Task node must occupy the single
						// available slot, and no slot may be spent on an ineligible node.
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{"Task"},
							"limit": 1,
						}))
						require.Equal(t, 1, recall.Count)
						require.Equal(t, []string{ids[nor100TaskAlpha]}, nor100RecallIDs(recall))

						recall = nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{"Decision"},
							"limit": 1,
						}))
						require.Equal(t, 1, recall.Count)
						require.Equal(t, []string{ids[nor100DecisionAlpha]}, nor100RecallIDs(recall))
					})

					t.Run("union limit caps only eligible nodes", func(t *testing.T) {
						recall := nor100DecodeRecall(t, transport.call(t, server, map[string]interface{}{
							"type":  []interface{}{"Memory", "Task"},
							"limit": 2,
						}))
						require.Equal(t, 2, recall.Count)
						require.Len(t, recall.Nodes, 2)
						require.NotContains(t, nor100RecallIDs(recall), ids[nor100Noise],
							"an eligible-only limit never spends a slot on the Noise node")
						for _, node := range recall.Nodes {
							require.Contains(t, []string{"Memory", "Task"}, node.Type)
						}
					})
				})
			}
		})
	}
}

// TestNOR100RecallInvalidTypeInputObservations records how the current argument
// decoder treats malformed type values. NOR-100 does not define a new
// validation contract for them, so the observations are logged rather than
// asserted.
func TestNOR100RecallInvalidTypeInputObservations(t *testing.T) {
	observations := []struct {
		name  string
		value interface{}
	}{
		{name: "scalar string", value: "Memory"},
		{name: "scalar number", value: 7},
		{name: "mixed string and number array", value: []interface{}{"Memory", 7}},
		{name: "object", value: map[string]interface{}{"type": "Memory"}},
		{name: "nil", value: nil},
	}
	for _, observation := range observations {
		t.Run(observation.name, func(t *testing.T) {
			decoded := getStringSlice(map[string]interface{}{"type": observation.value}, "type")
			t.Logf("getStringSlice(type=%#v) = %#v", observation.value, decoded)

			db, _ := seedNOR100RecallFixture(t)
			server := NewServer(db, nil)
			response := nor100RESTToolCall(t, server, "/mcp/tools/call", ToolRecall, map[string]interface{}{
				"type": observation.value,
			})
			require.NotEmpty(t, response.Content, "the public path answers with content")
			if response.IsError {
				t.Logf("recall(type=%#v) observed tool error: %s", observation.value, nor100ContentText(response))
				return
			}
			var recall RecallResult
			require.NoError(t, json.Unmarshal([]byte(nor100ContentText(response)), &recall))
			t.Logf("recall(type=%#v) observed count=%d types=%v", observation.value, recall.Count, nor100RecallTypes(recall))
		})
	}
}

// TestNOR100DiscoverDepthClampsThroughPublicHandler drives discover through
// tools/call with the clamping inputs (0 and 4) and the in-range depths, and
// asserts the observed hop semantics: depth 1 publishes no related nodes, 2
// publishes one hop and 3 publishes two. Each case is cross-checked against the
// package's canonical handler helper, runMCPDiscoverDepth, so the public path
// and the existing depth owner observe the same traversal.
func TestNOR100DiscoverDepthClampsThroughPublicHandler(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			db, ids := seedMCPDiscoverGraph(t, []mcpDiscoverNodeSeed{
				{label: "Memory", props: map[string]interface{}{"title": "Canonical Root", "content": "canonical related root document"}},
				{label: "Task", props: map[string]interface{}{"title": "Canonical Child", "content": "supporting neighbor"}},
				{label: "Task", props: map[string]interface{}{"name": "Canonical Grandchild", "content": "second hop"}},
			}, []mcpDiscoverEdgeSeed{
				{from: 0, to: 1, relType: "RELATES_TO"},
				{from: 1, to: 2, relType: "NEXT"},
			})
			require.Len(t, ids, 3, "the depth fixture keeps its own three-node, two-hop population")
			server := NewServer(db, nil)

			decodeDiscover := func(response CallToolResponse) DiscoverResult {
				t.Helper()
				require.False(t, response.IsError, "discover reported a tool error: %s", nor100ContentText(response))
				var discover DiscoverResult
				require.NoError(t, json.Unmarshal([]byte(nor100ContentText(response)), &discover))
				require.NotEmpty(t, discover.Results)
				require.Equal(t, "Canonical Root", discover.Results[0].Title)
				return discover
			}

			cases := []struct {
				name        string
				depth       int
				wantRelated int
			}{
				{name: "depth 0 clamps to depth 1", depth: 0, wantRelated: 0},
				{name: "depth 1 publishes no related nodes", depth: 1, wantRelated: 0},
				{name: "depth 2 publishes one hop", depth: 2, wantRelated: 1},
				{name: "depth 3 publishes two hops", depth: 3, wantRelated: 2},
				{name: "depth 4 clamps to depth 3", depth: 4, wantRelated: 2},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					discover := decodeDiscover(nor100JSONRPCToolCall(t, server, "/mcp", ToolDiscover, map[string]interface{}{
						"query": "canonical related root",
						"depth": tc.depth,
						"limit": 5,
					}))
					require.Len(t, discover.Results[0].Related, tc.wantRelated)

					direct := runMCPDiscoverDepth(t, db, "canonical related root", tc.depth)
					require.NotEmpty(t, direct.Results)
					require.Equal(t, "Canonical Root", direct.Results[0].Title)
					require.Equal(t, nor100RelatedIDs(direct.Results[0].Related), nor100RelatedIDs(discover.Results[0].Related),
						"the public route publishes the same traversal as the handler helper")
				})
			}

			t.Run("rest pinned route clamps depth 4 to two hops", func(t *testing.T) {
				discover := decodeDiscover(nor100RESTToolCall(t, server, "/mcp/nornic/tools/call", ToolDiscover, map[string]interface{}{
					"query": "canonical related root",
					"depth": 4,
					"limit": 5,
				}))
				require.Len(t, discover.Results[0].Related, 2)
			})
		})
	}
}

// TestNOR100DiscoverDepthSchemaAdvertisedBounds checks the tools/list schema
// the public surfaces advertise: depth is an integer with default 1 and the
// 1-3 bounds the handler clamps to. The description text is logged rather than
// asserted, so the test does not pin exact prose.
func TestNOR100DiscoverDepthSchemaAdvertisedBounds(t *testing.T) {
	server := NewServer(nil, nil)

	listings := []struct {
		name string
		list func(t *testing.T) ListToolsResponse
	}{
		{name: "jsonrpc", list: func(t *testing.T) ListToolsResponse {
			return nor100JSONRPCToolsList(t, server)
		}},
		{name: "rest", list: func(t *testing.T) ListToolsResponse {
			return nor100RESTToolsList(t, server)
		}},
	}

	for _, listing := range listings {
		t.Run(listing.name, func(t *testing.T) {
			listed := listing.list(t)
			var discover *Tool
			for index := range listed.Tools {
				if listed.Tools[index].Name == ToolDiscover {
					discover = &listed.Tools[index]
					break
				}
			}
			require.NotNil(t, discover, "tools/list advertises the discover tool")

			var schema struct {
				Properties map[string]struct {
					Type        string  `json:"type"`
					Description string  `json:"description"`
					Default     int     `json:"default"`
					Minimum     float64 `json:"minimum"`
					Maximum     float64 `json:"maximum"`
				} `json:"properties"`
			}
			require.NoError(t, json.Unmarshal(discover.InputSchema, &schema))
			depth, ok := schema.Properties["depth"]
			require.True(t, ok, "the discover schema documents the depth parameter")
			require.Equal(t, "integer", depth.Type)
			require.Equal(t, 1, depth.Default)
			require.Equal(t, 1, int(depth.Minimum))
			require.Equal(t, 3, int(depth.Maximum))
			require.NotEmpty(t, depth.Description)

			// NOR-100 schema bounds stay 1-3; the advertised description now
			// states that depth 1 publishes no related nodes while depth 2 and 3
			// publish one and two hops. Logged rather than asserted so the
			// regression does not pin exact prose.
			t.Logf("advertised discover depth description: %q", depth.Description)
		})
	}
}
