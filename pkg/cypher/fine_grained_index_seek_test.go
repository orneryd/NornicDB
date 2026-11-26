package cypher

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedReadableIndexedLookupUsesSeek(t *testing.T) {
	ctx := t.Context()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "fgac_index")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(ctx, "CREATE INDEX person_email_fgac IF NOT EXISTS FOR (p:Person) ON (p.email)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX person_secret_fgac IF NOT EXISTS FOR (p:Person) ON (p.secret)", nil)
	require.NoError(t, err)
	for _, node := range []*storage.Node{
		{ID: "found", Labels: []string{"Person"}, Properties: map[string]any{"email": "found@example.com", "name": "Found", "secret": "private"}},
		{ID: "other", Labels: []string{"Person"}, Properties: map[string]any{"email": "other@example.com", "name": "Other"}},
		{ID: "hidden", Labels: []string{"Secret"}, Properties: map[string]any{"email": "found@example.com", "name": "Hidden"}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "fgac-index-id"}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}}},
			{ID: "email", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"email"}}},
			{ID: "name", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"name"}}},
		},
		Assignments: map[string][]string{"reader": {"database", "traverse", "email", "name"}},
	})
	require.NoError(t, err)
	scoped := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reader"}),
		DatabaseID: func(name string) string {
			if name == "fgac_index" {
				return "fgac-index-id"
			}
			return ""
		},
	})
	result, err := exec.Execute(scoped, "PROFILE MATCH (p:Person {email: $e}) RETURN p.name", map[string]any{"e": "found@example.com"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{"Found"}}, result.Rows)
	plan := executionPlanFromResult(t, result)
	require.NotNil(t, planOperatorOfType(plan.Root, "NodeIndexSeek"), "a readable indexed lookup should not scan the Person label")

	where, err := exec.Execute(scoped, "PROFILE MATCH (p:Person) WHERE p.email = $e RETURN p.name", map[string]any{"e": "found@example.com"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{"Found"}}, where.Rows)
	require.NotNil(t, planOperatorOfType(executionPlanFromResult(t, where).Root, "NodeIndexSeek"))

	hidden, err := exec.Execute(scoped, "PROFILE MATCH (p:Person {secret: 'private'}) RETURN p.name", nil)
	require.NoError(t, err)
	require.Empty(t, hidden.Rows)
	require.Nil(t, planOperatorOfType(executionPlanFromResult(t, hidden).Root, "NodeIndexSeek"), "a hidden property cannot select index candidates")

	_, err = store.CreateNode(ctx, &storage.Node{ID: "denied", Labels: []string{"Person", "Archived"}, Properties: map[string]any{"email": "found@example.com", "name": "Denied"}})
	require.NoError(t, err)
	deniedPolicy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "fgac-index-id"}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}}},
			{ID: "deny-archived", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Archived"}}},
			{ID: "email", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"email"}}},
			{ID: "name", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"name"}}},
		},
		Assignments: map[string][]string{"reader": {"database", "traverse", "deny-archived", "email", "name"}},
	})
	require.NoError(t, err)
	deniedCtx := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
		Access: deniedPolicy.ForRoles([]string{"reader"}),
		DatabaseID: func(name string) string {
			if name == "fgac_index" {
				return "fgac-index-id"
			}
			return ""
		},
	})
	denied, err := exec.Execute(deniedCtx, "PROFILE MATCH (p:Person {email: $e}) RETURN p.name", map[string]any{"e": "found@example.com"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{"Found"}}, denied.Rows)
	require.Nil(t, planOperatorOfType(executionPlanFromResult(t, denied).Root, "NodeIndexSeek"), "a denied subpopulation must not be exposed through index counts")

	conditionalPolicy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "fgac-index-id"}},
			{ID: "traverse-found", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Predicate: &auth.FineGrainedPredicate{Property: "name", Operator: "=", Value: "Found"}}},
			{ID: "email", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"email"}}},
			{ID: "name", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"name"}}},
		},
		Assignments: map[string][]string{"reader": {"database", "traverse-found", "email", "name"}},
	})
	require.NoError(t, err)
	conditionalCtx := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
		Access:     conditionalPolicy.ForRoles([]string{"reader"}),
		DatabaseID: func(string) string { return "fgac-index-id" },
	})
	conditional, err := exec.Execute(conditionalCtx, "PROFILE MATCH (p:Person {email: $e}) RETURN p.name", map[string]any{"e": "found@example.com"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{"Found"}}, conditional.Rows)
	require.Nil(t, planOperatorOfType(executionPlanFromResult(t, conditional).Root, "NodeIndexSeek"), "conditional traversal must retain scoped population checks")
}

func BenchmarkFineGrainedIndexedLookup(b *testing.B) {
	ctx := b.Context()
	base := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "fgac_bench")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err := exec.Execute(ctx, "CREATE INDEX person_email_fgac_bench IF NOT EXISTS FOR (p:Person) ON (p.email)", nil)
	if err != nil {
		b.Fatal(err)
	}
	for i := range 10000 {
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("p-%d", i)), Labels: []string{"Person"}, Properties: map[string]any{"email": fmt.Sprintf("person-%d@example.com", i), "name": "Person"}})
		if err != nil {
			b.Fatal(err)
		}
	}
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "database", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "bench-id"}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}}},
			{ID: "email", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"email"}}},
			{ID: "name", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Person"}, Properties: []string{"name"}}},
		},
		Assignments: map[string][]string{"reader": {"database", "traverse", "email", "name"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	scoped := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{Access: policy.ForRoles([]string{"reader"}), DatabaseID: func(string) string { return "bench-id" }})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(scoped, "MATCH (p:Person {email: $e}) RETURN p.name", map[string]any{"e": fmt.Sprintf("person-%d@example.com", i%10000)})
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("lookup result: %v, %v", result, err)
		}
	}
}
