package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedCypherVisibilityAndWrites(t *testing.T) {
	ctx := t.Context()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "work")
	executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 50})
	for _, node := range []*storage.Node{
		{ID: "a", Labels: []string{"Article"}, Properties: map[string]any{"title": "visible", "draft": true}},
		{ID: "b", Labels: []string{"Article", "Archived"}, Properties: map[string]any{"title": "archived"}},
		{ID: "c", Labels: []string{"Other"}, Properties: map[string]any{"title": "other"}},
	} {
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "e", Type: "CITES", StartNode: "a", EndNode: "b"}))
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "work-access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "work-id"}},
			{ID: "article", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Article"}}},
			{ID: "archived", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Archived"}}},
			{ID: "title", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"title"}}},
			{ID: "title-edit", Action: "graph.node.property.set", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"title"}}},
			{ID: "links", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
		},
		Assignments: map[string][]string{"reviewer": {"work-access", "article", "archived", "title", "title-edit", "links"}},
	})
	require.NoError(t, err)
	scoped := auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"reviewer"}),
		DatabaseID: func(name string) string {
			if name == "work" {
				return "work-id"
			}
			return ""
		},
	})

	const nodeQuery = "MATCH (n:Article) RETURN n.title, n.draft, keys(n) ORDER BY n.title"
	unscoped, err := executor.Execute(ctx, nodeQuery, nil)
	require.NoError(t, err)
	require.Len(t, unscoped.Rows, 2)
	visible, err := executor.Execute(scoped, nodeQuery, nil)
	require.NoError(t, err)
	require.Len(t, visible.Rows, 1)
	require.Equal(t, "visible", visible.Rows[0][0])
	require.Nil(t, visible.Rows[0][1])
	require.EqualValues(t, []any{"title"}, visible.Rows[0][2])

	for _, query := range []string{
		"MATCH (n) RETURN count(n)",
		"MATCH (n:Article) RETURN count(n)",
	} {
		count, err := executor.Execute(scoped, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]any{{int64(1)}}, count.Rows, query)
	}
	relationships, err := executor.Execute(scoped, "MATCH ()-[r:CITES]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(0)}}, relationships.Rows)
	predicate, err := executor.Execute(scoped, "MATCH (n:Article {draft: true}) RETURN n", nil)
	require.NoError(t, err)
	require.Empty(t, predicate.Rows)

	updated, err := executor.Execute(scoped, "MATCH (n:Article) SET n.title = 'new' RETURN n.title", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"new"}}, updated.Rows)
	stored, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, true, stored.Properties["draft"])
	require.Equal(t, "new", stored.Properties["title"])
}
