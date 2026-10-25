package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestRelationshipEmitterProperty(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	server := NewServer(db, nil)
	exec := db.GetCypherExecutor()
	ctx := context.Background()
	stored, err := server.handleStore(ctx, map[string]any{"content": "emission subject", "labels": []string{"StoredName"}})
	require.NoError(t, err)
	require.NotNil(t, stored)
	nodes, err := exec.Execute(ctx, "MATCH (n:StoredName) RETURN labels(n), n.content", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{[]any{"StoredName"}, "emission subject"}}, nodes.Rows)
	_, err = exec.Execute(ctx, "CREATE (:Memory {id:'a'}), (:Memory {id:'b'})", nil)
	require.NoError(t, err)
	admitted, declined := 0, 0
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			result, err := server.handleLink(ctx, map[string]any{"from": "a", "to": "b", "relation": name})
			if !IsValidRelation(name) {
				require.Error(t, err)
				declined++
				return
			}
			require.NoError(t, err)
			link := result.(LinkResult)
			rows, err := exec.Execute(ctx, "MATCH ()-[r]->() WHERE elementId(r) = $id RETURN type(r)", map[string]any{"id": link.EdgeID})
			require.NoError(t, err)
			// This protocol deliberately admits ASCII names and uppercases
			// them before emission. Quoting must preserve that existing value.
			require.Equal(t, [][]any{{strings.ToUpper(name)}}, rows.Rows)
			admitted++
		})
	}
	require.Positive(t, admitted)
	require.Positive(t, declined)
}

// The MCP label vocabulary rejects unsupported names; it must not turn one
// caller-supplied identity into another identity before checking admission.
func TestMCPStoreLabelAdmissionIdentity(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	server := NewServer(db, nil)
	ctx := context.Background()
	for _, name := range []string{"ordinary", "a`b", "`ordinary`", " ", "has-dash"} {
		t.Run(name, func(t *testing.T) {
			_, err := server.CallTool(ctx, ToolStore, map[string]any{"content": "admission subject", "labels": []string{name}})
			if !IsValidNodeType(name) {
				require.Error(t, err, "unsupported input must not become a different admitted label")
				return
			}
			require.NoError(t, err)
			rows, err := db.GetCypherExecutor().Execute(ctx, "MATCH (n) WHERE $label IN labels(n) RETURN labels(n)", map[string]any{"label": name})
			require.NoError(t, err)
			require.Equal(t, [][]any{{[]any{name}}}, rows.Rows)
		})
	}
}

func TestMCPDefaultDatabaseIdentity(t *testing.T) {
	for _, name := range []string{"db", " db ", "DB", " ", "\t"} {
		t.Run(name, func(t *testing.T) {
			server, resolved := urlPinSpyServer(t)
			server.SetDefaultDatabase(name)
			require.Equal(t, name, server.DefaultDatabaseName())
			for _, tool := range server.ToolDefinitions() {
				var schema map[string]interface{}
				require.NoError(t, json.Unmarshal(tool.InputSchema, &schema))
				properties := schema["properties"].(map[string]interface{})
				database := properties["database"].(map[string]interface{})
				require.Equal(t, name, database["default"])
			}
			// The resolver intentionally has no executor. Its error follows
			// the routing boundary, whose exact argument is the observation.
			_, err := server.CallTool(context.Background(), ToolStore, map[string]any{"content": "database subject"})
			require.Error(t, err)
			require.Equal(t, []string{name}, *resolved)
		})
	}
}

func TestMCPDatabaseRouteIdentity(t *testing.T) {
	for _, name := range []string{"db", " db ", "DB", " ", "\t"} {
		for _, mode := range []string{"database", "db", "URL"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				server, resolved := urlPinSpyServer(t)
				server.SetDefaultDatabase("fallback")
				if mode == "URL" {
					body := bytes.NewBufferString(`{"name":"store","arguments":{"content":"routing subject"}}`)
					request := httptest.NewRequest(http.MethodPost, "/mcp/"+url.PathEscape(name)+"/tools/call", body)
					server.ServeHTTP(httptest.NewRecorder(), request)
				} else {
					_, err := server.CallTool(context.Background(), ToolStore, map[string]any{"content": "routing subject", mode: name})
					require.Error(t, err)
				}
				require.Equal(t, []string{name}, *resolved, "the actual resolver must receive the decoded database unchanged")
			})
		}
	}
}
