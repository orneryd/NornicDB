package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Extract registrations from the current source rather than treating a manual
// list of data routes as complete. Every registration must have a disposition:
// an exercised database reader, a protocol with its own public inventory, or a
// reviewed endpoint that does not read application databases. This is test
// coverage metadata; it grants no authority and is not a runtime route manifest.
func TestHTTPReadInventoryCoversRegisteredRoutes(t *testing.T) {
	metadata := strings.Fields(`
/
/assets/ /favicon.ico /nornicdb.svg /login /security /security/knowledge-policies
/auth/config /health /mcp/health
/auth/token /auth/logout /auth/me /auth/password /auth/profile /auth/api-token
/auth/oauth/redirect /auth/oauth/callback /auth/users /auth/users/
/auth/roles /auth/roles/ /auth/access/databases /auth/access/privileges
/auth/entitlements /auth/role-entitlements
/admin/config /admin/gpu/status /admin/gpu/enable /admin/gpu/disable /admin/gpu/test
/admin/databases/config/keys
`)
	protocols := strings.Fields(`
/mcp /mcp/initialize /mcp/tools/list /mcp/tools/call
/mcp/{database} /mcp/{database}/initialize /mcp/{database}/tools/list /mcp/{database}/tools/call
/graphql /graphql/playground
/api/bifrost/status /api/bifrost/chat/completions /api/bifrost/autocomplete /api/bifrost/events
/v1/models /v1/chat/completions
`)
	disposition := map[string]string{}
	for _, path := range metadata {
		disposition[path] = "UI, identity, health, configuration or GPU control; no application database reader"
	}
	for _, path := range protocols {
		disposition[path] = "delegated public MCP / GraphQL / Heimdall inventories"
	}
	for _, tc := range rule73HTTPInventory() {
		disposition[tc.path("{database}")] = "public HTTP admission/population regression"
	}
	registered := map[string]bool{}
	add := func(expr ast.Expr) {
		t.Helper()
		literal, ok := expr.(*ast.BasicLit)
		require.True(t, ok, "registration path needs an explicit inventory extraction: %T", expr)
		path, err := strconv.Unquote(literal.Value)
		require.NoError(t, err)
		registered[path] = true
	}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.CallExpr:
				selector, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "HandleFunc" && selector.Sel.Name != "Handle") || len(node.Args) != 2 {
					break
				}
				if selector, ok := node.Args[0].(*ast.SelectorExpr); ok && selector.Sel.Name == "path" {
					break // canonical registerRouteTable; its literals are extracted below
				}
				add(node.Args[0])
			case *ast.CompositeLit:
				array, ok := node.Type.(*ast.ArrayType)
				if !ok { break }
				kind, ok := array.Elt.(*ast.Ident)
				if !ok || kind.Name != "routeSpec" { break }
				for _, element := range node.Elts {
					route, ok := element.(*ast.CompositeLit)
					require.True(t, ok)
					require.NotEmpty(t, route.Elts)
					add(route.Elts[0])
				}
			}
			return true
		})
	}
	require.NotEmpty(t, registered)
	for path := range registered {
		covered := disposition[path] != ""
		pattern := strings.Split(path, "/")
		for exercised := range disposition {
			parts := strings.Split(exercised, "/")
			if len(parts) != len(pattern) { continue }
			matches := true
			for i, segment := range pattern {
				if segment != parts[i] && !(strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") && parts[i] != "") {
					matches = false
					break
				}
			}
			covered = covered || matches
		}
		// ServeMux prefix registrations dispatch the concrete paths exercised
		// by the database and retention inventories.
		if !covered && strings.HasSuffix(path, "/") {
			for exercised := range disposition {
				covered = covered || strings.HasPrefix(exercised, path)
			}
		}
		require.True(t, covered, "registered route %s is missing a read-family disposition and public coverage", path)
	}
	t.Logf("classified %d extracted HTTP registrations", len(registered))
}
