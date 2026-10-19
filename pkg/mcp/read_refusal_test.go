package mcp

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestCallToolReadRefusalPreservesAdmissionError(t *testing.T) {
	server := NewServer(nil, nil)
	ctx := auth.WithRequestDatabaseScope(context.Background(), auth.NewRequestDatabaseScope("tenant", map[string]string{"tenant": "tenant"}))
	ctx = auth.WithRequestEntitlementResolver(ctx, func(string, auth.Permission) bool { return false })
	for name := range toolDatabaseAccessByName {
		t.Run(name, func(t *testing.T) {
			called := false
			server.handlers[name] = func(context.Context, map[string]interface{}) (interface{}, error) {
				called = true
				return nil, nil
			}
			_, err := server.CallTool(ctx, name, map[string]interface{}{"database": "tenant"})
			require.EqualError(t, err, (&cypher.PermissionDeniedError{Permission: "read"}).Error())
			require.False(t, called)
		})
	}
}
