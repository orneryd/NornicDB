package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/search"
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

func TestFineGrainedCypherExactPropertyDenial(t *testing.T) {
	ctx := t.Context()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "work")
	executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 50})
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Article"},
			Properties: map[string]any{"key": string(id), "title": "Original", "note": "Unchanged"}})
		require.NoError(t, err)
	}
	protected, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	protectedHandle := auth.FineGrainedEntityID(protected.Identity().LocalID, protected.CreatedAt)
	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "traverse", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "read", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "set", Action: "graph.node.property.set", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "deny-read-title", Action: "graph.node.property.read", Deny: true, Scope: auth.FineGrainedScope{
				Kind: auth.FineGrainedNode, DatabaseID: "work-id", EntityIDs: []string{protectedHandle}, Properties: []string{"title"},
			}},
			{ID: "deny-set-title", Action: "graph.node.property.set", Deny: true, Scope: auth.FineGrainedScope{
				Kind: auth.FineGrainedNode, DatabaseID: "work-id", EntityIDs: []string{protectedHandle}, Properties: []string{"title"},
			}},
		},
		Assignments: map[string][]string{
			"reader":     {"access", "traverse", "read", "set"},
			"restricted": {"deny-read-title", "deny-set-title"},
		},
	})
	require.NoError(t, err)
	scope := func(roles ...string) context.Context {
		return auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
			Access: policy.ForRoles(roles), DatabaseID: func(string) string { return "work-id" },
		})
	}
	const project = "MATCH (n:Article) RETURN n.key AS key, n.title AS title, n.note AS note ORDER BY key"
	visible := [][]any{{"a", "Original", "Unchanged"}, {"b", "Original", "Unchanged"}}
	masked := [][]any{{"a", nil, "Unchanged"}, {"b", "Original", "Unchanged"}}
	unrestricted := scope("reader")
	restricted := scope("reader", "restricted")
	for _, testCase := range []struct {
		ctx  context.Context
		want [][]any
	}{{unrestricted, visible}, {restricted, masked}, {unrestricted, visible}} {
		result, err := executor.Execute(testCase.ctx, project, nil)
		require.NoError(t, err)
		require.Equal(t, testCase.want, result.Rows)
	}

	_, err = executor.Execute(restricted, "MATCH (n:Article {key: 'a'}) SET n.note = 'Updated', n.title = 'Updated'", nil)
	require.Error(t, err)
	stored, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "Original", stored.Properties["title"])
	require.Equal(t, "Unchanged", stored.Properties["note"])
	_, err = executor.Execute(restricted, "MATCH (n:Article {key: 'b'}) SET n.title = 'Updated'", nil)
	require.NoError(t, err)
	other, err := store.GetNode(ctx, "b")
	require.NoError(t, err)
	require.Equal(t, "Updated", other.Properties["title"])
}

func TestFineGrainedCypherPropertyNameConfidentiality(t *testing.T) {
	ctx := t.Context()
	store := newTestBadgerNamespacedStore(t)
	executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 50})
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Article"},
			Properties: map[string]any{"name": string(id), "secret": "original"}})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "link", Type: "CITES", StartNode: "a", EndNode: "b",
		Properties: map[string]any{"name": "link", "secret": "original"}}))
	policy, err := auth.CompileFineGrainedPolicy(auth.FineGrainedCoreActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "traverse-node", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "traverse-edge", Action: "graph.relationship.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "read-node", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"name", "unregisteredReadable"}}},
			{ID: "read-edge", Action: "graph.relationship.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship, Properties: []string{"name", "unregisteredReadable"}}},
			{ID: "set-node", Action: "graph.node.property.set", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "set-edge", Action: "graph.relationship.property.set", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "create-node", Action: "graph.node.create", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "create-edge", Action: "graph.relationship.create", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedRelationship}},
			{ID: "create-key", Action: "database.token.property.create", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
		},
		Assignments: map[string][]string{
			"writer":        {"access", "traverse-node", "traverse-edge", "read-node", "read-edge", "set-node", "set-edge", "create-node", "create-edge"},
			"token-creator": {"create-key"},
		},
	})
	require.NoError(t, err)
	scope := func(roles ...string) context.Context {
		return auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
			Access: policy.ForRoles(roles), DatabaseID: func(string) string { return "work-id" },
		})
	}
	restricted := scope("writer")
	for _, key := range []string{"secret", "unknown"} {
		for _, query := range []string{
			"MATCH (n:Article {name: 'a'}) SET n." + key + " = 'changed'",
			"MATCH ()-[r:CITES]->() SET r += {" + key + ": 'changed'}",
			"MERGE (n:Article {name: 'a'}) ON MATCH SET n." + key + " = 'changed'",
			"MATCH (n:Article {name: 'a'}) REMOVE n." + key,
			"MATCH ()-[r:CITES]->() SET r." + key + " = null",
			"CREATE (:Article {" + key + ": 'changed'})",
			"MATCH (a:Article {name: 'a'}), (b:Article {name: 'b'}) CREATE (a)-[:CITES {" + key + ": 'changed'}]->(b)",
		} {
			_, err := executor.Execute(restricted, query, nil)
			require.ErrorIs(t, err, auth.ErrFineGrainedForbidden, query)
		}
	}
	_, err = executor.Execute(restricted, "MATCH (n:Article {name: 'a'}) SET n.unregisteredReadable = true", nil)
	require.ErrorIs(t, err, auth.ErrFineGrainedForbidden)
	_, err = executor.Execute(restricted, "MATCH (n:Article {name: 'a'}) SET n = {name: 'updated'}", nil)
	require.NoError(t, err)
	_, err = executor.Execute(restricted, "MATCH ()-[r:CITES]->() SET r = {name: 'updated link'}", nil)
	require.NoError(t, err)
	node, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "updated", "secret": "original"}, node.Properties)
	link, err := store.GetEdge(ctx, "link")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "updated link", "secret": "original"}, link.Properties)

	privileged := scope("writer", "token-creator")
	_, err = executor.Execute(privileged, "MATCH (n:Article {name: 'updated'}) SET n.secret = 'changed', n.newKey = 'created'", nil)
	require.NoError(t, err)
	node, err = store.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, "changed", node.Properties["secret"])
	require.Equal(t, "created", node.Properties["newKey"])
	result, err := executor.Execute(privileged, "MATCH (n:Article {name: 'updated'}) RETURN n.secret, n.newKey", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{nil, nil}}, result.Rows)
}

func TestFineGrainedRevealPreservesGraphProjection(t *testing.T) {
	ctx := t.Context()
	store := newTestBadgerNamespacedStore(t)
	engine := unwrapBadgerEngine(store)
	require.NotNil(t, engine)
	database := storage.DatabaseOf(store)
	schema := engine.GetSchemaForNamespace(database)
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "incident-decay", Scope: knowledgepolicy.ScopeNode,
		Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600,
		VisibilityThreshold: .10, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "incidents", ProfileRef: "incident-decay", TargetLabels: []string{"Incident"},
	}))
	engine.SetDecayEnabled(true)

	now := time.Now()
	old := now.Add(-720 * time.Hour)
	for _, item := range []struct {
		node   *storage.Node
		vector []any
	}{
		{&storage.Node{ID: "visible", Labels: []string{"Incident"}, CreatedAt: now}, []any{1.0, .5, 0.0}},
		{&storage.Node{ID: "suppressed", Labels: []string{"Incident"}, CreatedAt: old}, []any{1.0, .1, 0.0}},
		{&storage.Node{ID: "restricted", Labels: []string{"Incident", "Confidential"}, CreatedAt: old}, []any{1.0, 0.0, 0.0}},
	} {
		node := item.node
		node.UpdatedAt = node.CreatedAt
		node.Properties = map[string]any{"summary": string(node.ID), "details": "reviewer only", "embedding": item.vector}
		_, err := store.CreateNode(ctx, node)
		require.NoError(t, err)
	}

	policy, err := auth.CompileFineGrainedPolicy(auth.GraphFineGrainedActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "work-id"}},
			{ID: "reveal", Action: "database.reveal", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: "work-id"}},
			{ID: "incidents", Action: "graph.node.traverse", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Incident"}}},
			{ID: "confidential", Action: "graph.node.traverse", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Types: []string{"Confidential"}}},
			{ID: "read", Action: "graph.node.property.read", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode}},
			{ID: "vector-procedure", Action: "database.procedure.execute", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, Properties: []string{"db.index.vector.queryNodes"}}},
			{ID: "details", Action: "graph.node.property.read", Deny: true, Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, Properties: []string{"details"}}},
		},
		Assignments: map[string][]string{
			"reader":       {"access", "incidents", "confidential", "read", "details", "vector-procedure"},
			"investigator": {"reveal"},
		},
	})
	require.NoError(t, err)
	scoped := func(roles ...string) context.Context {
		return auth.WithFineGrainedRequest(ctx, &auth.FineGrainedRequest{
			Access: policy.ForRoles(roles), DatabaseID: func(string) string { return "work-id" },
		})
	}

	executor := NewStorageExecutor(store)
	_, err = executor.Execute(ctx, "CALL db.index.vector.createNodeIndex('incidents', 'Incident', 'embedding', 3, 'cosine')", nil)
	require.NoError(t, err)
	service := search.NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(ctx))
	executor.SetSearchService(service)
	const normal = "MATCH (n:Incident) RETURN n.summary, n.details ORDER BY n.summary"
	const vector = "CALL db.index.vector.queryNodes('incidents', 1, [1.0, 0.0, 0.0]) YIELD node"
	const vectorNormal = vector + " RETURN node.summary, node.details"
	const vectorReveal = vector + " RETURN reveal(node).summary AS summary, node.details"
	const reveal = "MATCH (n:Incident) RETURN reveal(n).summary AS summary, n.details ORDER BY summary"
	reader := scoped("reader")
	investigator := scoped("reader", "investigator")
	for _, query := range []string{reveal, vectorReveal} {
		_, err = executor.Execute(reader, query, nil)
		var denied *PermissionDeniedError
		require.ErrorAs(t, err, &denied)
		require.Equal(t, "database.reveal", denied.Permission)
	}

	for _, tc := range []struct {
		ctx   context.Context
		query string
		rows  [][]any
	}{
		{investigator, normal, [][]any{{"visible", nil}}},
		{investigator, reveal, [][]any{{"suppressed", nil}, {"visible", nil}}},
		{reader, normal, [][]any{{"visible", nil}}},
		{investigator, vectorNormal, [][]any{{"visible", nil}}},
		{investigator, vectorReveal, [][]any{{"suppressed", nil}}},
		{reader, vectorNormal, [][]any{{"visible", nil}}},
	} {
		result, err := executor.Execute(tc.ctx, tc.query, nil)
		require.NoError(t, err)
		require.Equal(t, tc.rows, result.Rows)
	}
}

func TestFineGrainedVectorIndexPublishesOnCommit(t *testing.T) {
	fixture := newDDLPublicationFixture(t, "badger")
	executor := fixture.executor()
	policy, err := auth.CompileFineGrainedPolicy(auth.FineGrainedCoreActions(), auth.FineGrainedCatalog{
		Definitions: []auth.FineGrainedEntitlement{
			{ID: "access", Action: "database.access", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "index-create", Action: "database.index.create", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "index-show", Action: "database.index.show", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
			{ID: "index-drop", Action: "database.index.drop", Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase}},
		},
		Assignments: map[string][]string{"schema-writer": {"access", "index-create", "index-show", "index-drop"}},
	})
	require.NoError(t, err)
	ctx := auth.WithFineGrainedRequest(t.Context(), &auth.FineGrainedRequest{
		Access: policy.ForRoles([]string{"schema-writer"}), DatabaseID: func(string) string { return "ddl-id" },
	})
	execute := func(query string) *ExecuteResult {
		t.Helper()
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}

	execute("CREATE VECTOR INDEX fgac_nodes FOR (n:Article) ON (n.embedding)")
	require.Equal(t, [][]any{{"fgac_nodes"}},
		execute("SHOW VECTOR INDEXES YIELD name RETURN name ORDER BY name").Rows)

	execute("BEGIN")
	execute("CREATE VECTOR INDEX fgac_links FOR ()-[r:LINK]-() ON (r.embedding)")
	_, published := fixture.store.GetSchema().GetVectorIndex("fgac_links")
	require.False(t, published, "the transaction has not committed")
	execute("COMMIT")
	require.Equal(t, [][]any{{"fgac_links"}, {"fgac_nodes"}},
		execute("SHOW VECTOR INDEXES YIELD name RETURN name ORDER BY name").Rows)

	execute("BEGIN")
	execute("DROP INDEX fgac_nodes")
	execute("ROLLBACK")
	require.Equal(t, [][]any{{"fgac_links"}, {"fgac_nodes"}},
		execute("SHOW VECTOR INDEXES YIELD name RETURN name ORDER BY name").Rows)
}
