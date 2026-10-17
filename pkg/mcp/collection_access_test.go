package mcp

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// The MCP tools search and read storage around a principal's graph view
// (#935), so a principal restricted by collection rules, or one whose
// visibility can't be resolved, is refused before the tool runs; a
// principal without collection rules is served as before.
func TestCallToolRefusesCollectionRestrictedPrincipal(t *testing.T) {
	server := NewServer(nil, nil)
	var handlerCalls int
	server.handlers[ToolRecall] = func(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
		handlerCalls++
		return DatabaseFromContext(ctx), nil
	}
	principal := func(visibility func(string) (*cypher.GraphVisibility, error)) context.Context {
		ctx := auth.WithRequestDatabaseAccessMode(context.Background(), auth.FullDatabaseAccessMode)
		ctx = withResolvedAccess(ctx, func(string) auth.ResolvedAccess { return auth.ResolvedAccess{Read: true, Write: true} })
		ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("tenant_a", map[string]string{"tenant_a": "tenant_a"}))
		return cypher.WithRequestPrincipalAccess(ctx, cypher.PrincipalAccess{Visibility: visibility})
	}
	args := map[string]interface{}{"database": "tenant_a", "id": "node-1"}

	_, err := server.CallTool(principal(func(string) (*cypher.GraphVisibility, error) {
		return nil, errors.New("visibility read failed")
	}), ToolRecall, args)
	require.Error(t, err)
	require.Zero(t, handlerCalls)

	result, err := server.CallTool(principal(func(string) (*cypher.GraphVisibility, error) { return nil, nil }), ToolRecall, args)
	require.NoError(t, err)
	require.Equal(t, "tenant_a", result)
	require.Equal(t, 1, handlerCalls)
}
