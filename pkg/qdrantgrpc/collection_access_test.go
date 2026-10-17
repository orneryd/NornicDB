package qdrantgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gRPC reads storage and search around a principal's graph view (#935), so
// a principal restricted by collection rules on a database is refused there,
// and so is one whose visibility can't be resolved; a principal without
// collection rules is admitted as before.
func TestGRPCRefusesCollectionRestrictedPrincipal(t *testing.T) {
	ctx := context.Background()
	systemBase := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = systemBase.Close() })
	system := storage.NewNamespacedEngine(systemBase, "system")
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "agent"))
	admin := cypher.NewStorageExecutor(system)
	for _, statement := range []string{
		"CREATE ACCESS SCOPE collections ON GRAPH nornic MEMBERSHIP (d:Document)-[:MEMBER_OF]->(c:Collection) KEY d.id, c.id",
		"GRANT MATCH {*} ON GRAPH nornic FOR (n:Document) WHERE (n)-[:MEMBER_OF]->(:Collection {id: 'A'}) TO agent",
	} {
		_, err := admin.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}

	s := &Server{
		config: DefaultConfig(),
		resolvedAccessResolver: func([]string, string) auth.ResolvedAccess {
			return auth.ResolvedAccess{Read: true, Write: true}
		},
		graphVisibility: func(roles []string, database string) (*cypher.GraphVisibility, error) {
			for _, role := range roles {
				if role == "unreadable" {
					return nil, errors.New("visibility read failed")
				}
			}
			return cypher.ResolveGraphVisibility(ctx, system, roles, database)
		},
	}
	as := func(roles ...string) context.Context {
		return context.WithValue(ctx, contextKeyClaims{}, &auth.JWTClaims{Roles: roles})
	}

	require.NoError(t, s.AllowDatabaseOperation(as("editor"), "nornic", cypher.PrivilegeRead))
	for _, roles := range [][]string{{"agent"}, {"agent", "editor"}, {"unreadable"}} {
		err := s.AllowDatabaseOperation(as(roles...), "nornic", cypher.PrivilegeRead)
		require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", roles)
	}
	require.NoError(t, s.AllowDatabaseOperation(as("agent"), "other", cypher.PrivilegeRead), "the rules are on nornic only")
}
