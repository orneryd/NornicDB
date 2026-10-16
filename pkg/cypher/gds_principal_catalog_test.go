package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSCatalogUsesExecutingPrincipal(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "catalog_principal"))
	_, err := executor.Execute(context.Background(), "CREATE (a:P {v: 1})-[:R]->(b:P {v: 2})", nil)
	require.NoError(t, err)
	identity := func(name string, impersonated bool, admin bool) context.Context {
		ctx := WithAuthenticatedPrincipal(context.Background(), "sub:"+name)
		request := &RequestIdentity{User: &AuthenticatedUser{Name: name}}
		if impersonated {
			request.ImpersonatedBy = &AuthenticatedUser{Name: "service", Roles: []string{"admin"}}
		}
		ctx = WithRequestIdentity(ctx, request)
		return WithPermissionChecker(ctx, func(permission string) bool {
			return permission == "read" || permission == "write" || permission == "admin" && admin
		})
	}
	alice, bob := identity("alice", false, false), identity("bob", false, false)
	target := identity("bob", true, false)
	run := func(ctx context.Context, query string) *ExecuteResult {
		t.Helper()
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}
	t.Cleanup(func() {
		projectionsMu.Lock()
		defer projectionsMu.Unlock()
		for key := range graphProjections {
			if key.Database == "catalog_principal" {
				delete(graphProjections, key)
			}
		}
	})
	for _, ctx := range []context.Context{alice, bob} {
		run(ctx, "CALL gds.graph.project('same', '*', '*')")
		run(ctx, "MATCH (a:P)-[:R]->(b:P) RETURN gds.graph.project('aggregate', a, b) AS graph")
		for _, query := range []string{
			"RETURN gds.graph.exists('same') AS exists",
			"WITH 1 AS n RETURN gds.graph.exists('aggregate') AS exists",
			"MATCH (n:P) RETURN gds.graph.exists('same') AS exists",
			"RETURN CASE WHEN true THEN gds.graph.exists('same') ELSE false END AS exists",
			"RETURN [x IN [1] | gds.graph.exists('same')][0] AS exists",
			"RETURN coalesce(gds.graph.exists('same'), false) AS exists",
			"MATCH (n:P) WHERE gds.graph.exists('same') RETURN true AS exists",
		} {
			result := run(ctx, query)
			require.NotEmpty(t, result.Rows)
			for _, row := range result.Rows {
				require.Equal(t, true, row[0], query)
			}
		}
		run(ctx, "CALL gds.fastRP.stream('same', {embeddingDimension: 4, randomSeed: 1})")
		run(ctx, "CALL gds.louvain.stream('same')")
		require.Len(t, run(ctx, "CALL gds.graph.list()").Rows, 2)
	}
	// Impersonation selects the target's existing catalog, with no union of
	// the service login's administrator rights or namespace.
	run(target, "CALL gds.graph.drop('same')")
	require.Equal(t, false, run(bob, "RETURN gds.graph.exists('same')").Rows[0][0])
	require.Equal(t, true, run(alice, "RETURN gds.graph.exists('same')").Rows[0][0])
	_, err = executor.Execute(target, "CALL gds.graph.drop('same', true, '', 'alice')", nil)
	require.Error(t, err)
	require.Equal(t, true, run(alice, "RETURN gds.graph.exists('same')").Rows[0][0])
	run(identity("operator", false, true), "CALL gds.graph.drop('same', true, '', 'alice')")
	require.Equal(t, false, run(alice, "RETURN gds.graph.exists('same')").Rows[0][0])
}

func TestGDSCatalogCacheUsesSemanticCallableNames(t *testing.T) {
	executor := NewStatementExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "catalog_cache_principal"))
	as := func(name string) context.Context {
		ctx := WithAuthenticatedPrincipal(context.Background(), "sub:"+name)
		ctx = WithRequestIdentity(ctx, &RequestIdentity{User: &AuthenticatedUser{Name: name}})
		return WithPermissionChecker(ctx, func(permission string) bool { return permission == "read" || permission == "write" })
	}
	alice, bob := as("alice"), as("bob")
	_, err := executor.Execute(alice, "CREATE (:P)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(alice, "CALL gds.graph.project('private', '*', '*')", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = executor.Execute(alice, "CALL gds.graph.drop('private')", nil)
	})
	for _, query := range []string{
		"RETURN `gds`.`graph`.`exists`('private') AS exists",
		"CALL `gds`.`graph`.`exists`('private') YIELD exists RETURN exists",
	} {
		t.Run(query, func(t *testing.T) {
			for _, principal := range []struct {
				ctx context.Context
				expected bool
			}{{alice, true}, {bob, false}, {alice, true}} {
				result, err := executor.Execute(principal.ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{principal.expected}}, result.Rows, "a cache entry cannot replace this principal's catalog lookup")
			}
		})
	}
}
