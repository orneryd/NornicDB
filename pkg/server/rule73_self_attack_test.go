package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type rule73HTTPClass string

const (
	rule73HTTPRead      rule73HTTPClass = "read"
	rule73HTTPWrite     rule73HTTPClass = "write"
	rule73HTTPReadWrite rule73HTTPClass = "read_write"
rule73HTTPReadAdmin rule73HTTPClass = "read_admin"
	rule73HTTPAdmin     rule73HTTPClass = "admin"
	rule73HTTPAllAdmin  rule73HTTPClass = "all_admin"
)

type rule73HTTPCase struct {
	name, method, target string
	class                rule73HTTPClass
	path                 func(string) string
	body                 func(string) any
}

func rule73HTTPInventory() []rule73HTTPCase {
	constantBody := func(value any) func(string) any { return func(string) any { return value } }
	return []rule73HTTPCase{
		{"db.info.missing", http.MethodGet, "missing", rule73HTTPRead, func(db string) string { return "/db/" + db + "_missing" }, nil},
		{"db.info", http.MethodGet, "selected", rule73HTTPRead, func(db string) string { return "/db/" + db }, nil},
		{"db.tx.read", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/db/" + db + "/tx/commit" }, constantBody(map[string]any{"statements": []map[string]any{{"statement": "MATCH (n) RETURN n LIMIT 1"}}})},
		{"search", http.MethodPost, "selected", rule73HTTPRead, func(string) string { return "/nornicdb/search" }, func(db string) any { return map[string]any{"database": db, "query": "rule73"} }},
		{"similar", http.MethodPost, "selected", rule73HTTPRead, func(string) string { return "/nornicdb/similar" }, func(db string) any { return map[string]any{"database": db, "node_id": "missing"} }},
		{"graph.neighborhood", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/nornicdb/graph/" + db + "/neighborhood" }, constantBody(map[string]any{"node_ids": []string{"missing"}, "depth": 1})},
		{"graph.expand", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/nornicdb/graph/" + db + "/expand" }, constantBody(map[string]any{"node_ids": []string{"missing"}, "depth": 1})},
		{"graph.path", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/nornicdb/graph/" + db + "/path" }, constantBody(map[string]any{"start_node_id": "missing-a", "end_node_id": "missing-b"})},
		{"graph.temporal", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/nornicdb/graph/" + db + "/temporal" }, constantBody(map[string]any{"node_ids": []string{"missing"}})},
		{"graph.diff", http.MethodPost, "selected", rule73HTTPRead, func(db string) string { return "/nornicdb/graph/" + db + "/diff" }, constantBody(map[string]any{"node_ids": []string{"missing"}})},
		{"decay.info", http.MethodGet, "default", rule73HTTPRead, func(string) string { return "/nornicdb/decay" }, nil},
		{"embed.trigger", http.MethodPost, "all", rule73HTTPReadWrite, func(db string) string { return "/nornicdb/embed/trigger?database=" + db }, nil},
		{"embed.stats", http.MethodGet, "all", rule73HTTPRead, func(string) string { return "/nornicdb/embed/stats" }, nil},
		{"embed.failures", http.MethodGet, "all", rule73HTTPRead, func(string) string { return "/nornicdb/embed/failures" }, nil},
		{"embed.retry-failures", http.MethodPost, "all", rule73HTTPReadWrite, func(string) string { return "/nornicdb/embed/retry-failures" }, constantBody(map[string]any{})},
		{"embed.clear", http.MethodPost, "default", rule73HTTPAdmin, func(db string) string { return "/nornicdb/embed/clear?database=" + db }, constantBody(map[string]any{})},
		{"search.rebuild", http.MethodPost, "selected", rule73HTTPReadWrite, func(string) string { return "/nornicdb/search/rebuild" }, func(db string) any { return map[string]any{"database": db} }},
		{"gdpr.export", http.MethodPost, "default", rule73HTTPRead, func(string) string { return "/gdpr/export" }, constantBody(map[string]any{"user_id": "nobody", "format": "json"})},
		{"gdpr.delete", http.MethodPost, "default", rule73HTTPReadWrite, func(string) string { return "/gdpr/delete" }, constantBody(map[string]any{"user_id": "nobody", "confirm": true})},
		{"admin.backup", http.MethodPost, "all", rule73HTTPAllAdmin, func(string) string { return "/admin/backup" }, constantBody(map[string]any{"path": "/tmp/rule73-probe-backup"})},
		{"admin.restore", http.MethodPost, "all", rule73HTTPAllAdmin, func(string) string { return "/admin/restore" }, constantBody(map[string]any{"path": "/tmp/rule73-probe-missing"})},
		{"admin.database.config", http.MethodGet, "selected", rule73HTTPReadAdmin, func(db string) string { return "/admin/databases/" + db + "/config" }, nil},
		{"admin.database.mvcc", http.MethodGet, "selected", rule73HTTPReadAdmin, func(db string) string { return "/admin/databases/" + db + "/mvcc" }, nil},
		{"retention.policies", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/policies" }, nil},
		{"retention.policy", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/policies/missing" }, nil},
		{"retention.defaults", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/policies/defaults" }, nil},
		{"retention.holds", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/holds" }, nil},
		{"retention.hold", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/holds/missing" }, nil},
		{"retention.erasures", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/erasures" }, nil},
		{"retention.process-erasure", http.MethodPost, "default", rule73HTTPAdmin, func(string) string { return "/admin/retention/erasures/missing/process" }, nil},
		{"retention.sweep", http.MethodPost, "all", rule73HTTPAllAdmin, func(string) string { return "/admin/retention/sweep" }, nil},
		{"retention.status", http.MethodGet, "default", rule73HTTPReadAdmin, func(string) string { return "/admin/retention/status" }, nil},
	}
}

func rule73HTTPDenied(responseBody string, status int) bool {
	return status == http.StatusForbidden || strings.Contains(responseBody, "Neo.ClientError.Security.Forbidden")
}

func TestRule73SelfAttackEveryHTTPDatabaseDataRoute(t *testing.T) {
	server, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, server.dbManager.CreateDatabase("rule73_named"))
	for database, id := range map[string]string{"nornic": "default-secret", "rule73_named": "named-secret"} {
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Rule73Secret"}, Properties: map[string]any{"secret": id}})
		require.NoError(t, err)
	}

	type profile struct {
		name, token        string
		read, write, admin bool
		allowedDatabases   map[string]bool
	}
	var profiles []profile
	for _, spec := range []struct {
		name      string
		databases []string
		read      bool
		write     bool
	}{
		{"default-only", []string{"nornic"}, true, true},
		{"named-only", []string{"rule73_named"}, true, true},
		{"write-only", []string{"nornic", "rule73_named"}, false, true},
{"read-only", []string{"nornic", "rule73_named"}, true, false},
		{"no-access", []string{"nornic", "rule73_named"}, false, false},
	} {
		require.NoError(t, server.roleStore.CreateRole(ctx, spec.name))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, spec.name, []string{"read", "write", "create", "delete", "admin", "schema", "user_manage"}))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, spec.name, spec.databases))
		for _, database := range []string{"nornic", "rule73_named"} {
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, spec.name, database, spec.read && containsString(spec.databases, database), spec.write && containsString(spec.databases, database)))
		}
		_, err := users.CreateUser(spec.name, "password123", []auth.Role{auth.Role(spec.name)})
		require.NoError(t, err)
		allowed := map[string]bool{}
		for _, database := range spec.databases {
			allowed[database] = true
		}
		profiles = append(profiles, profile{spec.name, "Bearer " + getAuthToken(t, users, spec.name), spec.read, spec.write, true, allowed})
	}
	profiles = append(profiles, profile{"admin", "Bearer " + getAuthToken(t, users, "admin"), true, true, true, map[string]bool{"nornic": true, "rule73_named": true}})

	allowed := func(p profile, tc rule73HTTPCase, database string) bool {
		if p.name == "admin" {
			return true
		}
		if tc.target == "missing" {
			return false
		}
		if tc.target == "all" && (!p.allowedDatabases["nornic"] || !p.allowedDatabases["rule73_named"]) {
			return false
		}
		if !p.allowedDatabases[database] {
			return false
		}
		switch tc.class {
		case rule73HTTPRead:
			return p.read
		case rule73HTTPWrite:
			return p.write
		case rule73HTTPReadWrite:
			return p.read && p.write
		case rule73HTTPReadAdmin:
 return p.read && p.admin
 case rule73HTTPAdmin:
			return p.read && p.write && p.admin
		default:
			return false
		}
	}

	for _, tc := range rule73HTTPInventory() {
		databases := []string{"nornic", "rule73_named"}
		if tc.target == "default" || tc.target == "all" {
			databases = []string{"nornic"}
		}
		for _, database := range databases {
			for _, p := range profiles {
				t.Run(tc.name+"/"+database+"/"+p.name, func(t *testing.T) {
					var body any
					if tc.body != nil {
						body = tc.body(database)
					}
					response := makeRequest(t, server, tc.method, tc.path(database), body, p.token)
					denied := rule73HTTPDenied(response.Body.String(), response.Code)
					if allowed(p, tc, database) {
						require.False(t, denied, "legitimate caller over-denied: %d %s", response.Code, response.Body.String())
					} else {
						require.True(t, denied, "route reached behavior without database admission: %d %s", response.Code, response.Body.String())
					}
				})
			}
		}
	}

	// Aggregate readers must expose only admitted databases, not merely return 200.
	for _, endpoint := range []string{"/status", "/admin/stats"} {
		for _, p := range profiles {
			t.Run("aggregate"+endpoint+"/"+p.name, func(t *testing.T) {
				response := makeRequest(t, server, http.MethodGet, endpoint, nil, p.token)
				if endpoint == "/status" {
					var payload map[string]any
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
					database, ok := payload["database"].(map[string]any)
					require.True(t, ok, response.Body.String())
					wantNodes := float64(0)
					switch p.name {
					case "default-only", "named-only":
						wantNodes = 1
					case "admin", "read-only":
						wantNodes = 2
					}
					require.Equal(t, wantNodes, database["nodes"], response.Body.String())
				}
				if p.name == "write-only" || p.name == "no-access" {
					require.NotContains(t, response.Body.String(), "default-secret")
					require.NotContains(t, response.Body.String(), "named-secret")
				}
				if p.name == "default-only" {
					require.NotContains(t, response.Body.String(), "rule73_named")
				}
				if p.name == "named-only" {
					require.NotContains(t, response.Body.String(), "\"nornic\"")
				}
			})
		}
	}

	// Authentication-disabled compatibility is checked across every member last.
	savedAuth := server.auth
	server.auth = nil
	defer func() { server.auth = savedAuth }()
	for _, tc := range rule73HTTPInventory() {
		database := "nornic"
		if tc.target == "selected" {
			database = "rule73_named"
		}
		var body any
		if tc.body != nil {
			body = tc.body(database)
		}
		response := makeRequest(t, server, tc.method, tc.path(database), body, "")
		require.False(t, rule73HTTPDenied(response.Body.String(), response.Code), "%s auth-off changed: %d %s", tc.name, response.Code, response.Body.String())
	}

	// Keep the aggregate response decodable so an empty/error page cannot masquerade as filtering.
	response := makeRequest(t, server, http.MethodGet, "/status", nil, "")
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
}

func TestRule73SelfAttackEveryMCPTool(t *testing.T) {
	server, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, server.dbManager.CreateDatabase("rule73_named"))

	type profile struct {
		name, token string
		read, write bool
		databases   map[string]bool
	}
	var profiles []profile
	for _, spec := range []struct {
		name        string
		dbs         []string
		read, write bool
	}{
		{"default-only", []string{"nornic"}, true, true},
		{"named-only", []string{"rule73_named"}, true, true},
		{"write-only", []string{"nornic", "rule73_named"}, false, true},
{"read-only", []string{"nornic", "rule73_named"}, true, false},
		{"no-access", []string{"nornic", "rule73_named"}, false, false},
	} {
		require.NoError(t, server.roleStore.CreateRole(ctx, spec.name))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, spec.name, []string{"read", "write", "create", "delete", "admin"}))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, spec.name, spec.dbs))
		allowed := map[string]bool{}
		for _, database := range spec.dbs {
			allowed[database] = true
		}
		for _, database := range []string{"nornic", "rule73_named"} {
			require.NoError(t, server.privilegesStore.SavePrivilege(ctx, spec.name, database, spec.read && allowed[database], spec.write && allowed[database]))
		}
		_, err := users.CreateUser(spec.name, "password123", []auth.Role{auth.Role(spec.name)})
		require.NoError(t, err)
		profiles = append(profiles, profile{spec.name, "Bearer " + getAuthToken(t, users, spec.name), spec.read, spec.write, allowed})
	}
	profiles = append(profiles, profile{"admin", "Bearer " + getAuthToken(t, users, "admin"), true, true, map[string]bool{"nornic": true, "rule73_named": true}})

	tools := []struct {
		name      string
		write     bool
		arguments func(string) map[string]any
	}{
		{"store", true, func(db string) map[string]any {
			return map[string]any{"database": db, "content": "rule73 content", "type": "Rule73"}
		}},
		{"recall", false, func(db string) map[string]any { return map[string]any{"database": db, "id": "missing"} }},
		{"discover", false, func(db string) map[string]any { return map[string]any{"database": db, "query": "rule73", "limit": 1} }},
		{"link", true, func(db string) map[string]any {
			return map[string]any{"database": db, "source_id": "missing-a", "target_id": "missing-b", "relationship_type": "RELATED"}
		}},
		{"tasks.list", false, func(db string) map[string]any { return map[string]any{"database": db, "limit": 1} }},
		{"tasks.mutate", true, func(db string) map[string]any { return map[string]any{"database": db, "title": "rule73 task"} }},
	}
	denied := func(body string) bool {
		body = strings.ToLower(body)
		return strings.Contains(body, "access denied") || strings.Contains(body, "not allowed") || strings.Contains(body, "authentication context") || strings.Contains(body, "permission")
	}
	for _, tool := range tools {
		wireName := strings.TrimSuffix(tool.name, ".list")
		wireName = strings.TrimSuffix(wireName, ".mutate")
		for _, database := range []string{"nornic", "rule73_named"} {
			for _, p := range profiles {
				t.Run(tool.name+"/"+database+"/"+p.name, func(t *testing.T) {
					response := makeRequest(t, server, http.MethodPost, "/mcp/tools/call", map[string]any{"name": wireName, "arguments": tool.arguments(database)}, p.token)
					wantAllowed := p.databases[database] && p.read && (!tool.write || p.write)
					if p.name == "admin" {
						wantAllowed = true
					}
					if wantAllowed {
						require.False(t, denied(response.Body.String()), response.Body.String())
					} else {
						require.True(t, denied(response.Body.String()), "tool bypassed admission: %s", response.Body.String())
					}
				})
			}
		}
	}

	savedAuth := server.auth
	server.auth = nil
	defer func() { server.auth = savedAuth }()
	for _, tool := range tools {
		wireName := strings.TrimSuffix(strings.TrimSuffix(tool.name, ".list"), ".mutate")
		response := makeRequest(t, server, http.MethodPost, "/mcp/tools/call", map[string]any{"name": wireName, "arguments": tool.arguments("rule73_named")}, "")
		require.False(t, denied(response.Body.String()), "%s auth-off changed: %s", tool.name, response.Body.String())
	}
}
