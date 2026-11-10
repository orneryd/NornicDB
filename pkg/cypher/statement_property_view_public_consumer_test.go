package cypher

// NOR-310 probe: can a supported public consumer mutate the statement-local
// cached property view (statement_property_views.go) before another read of the
// same statement, and have that read observe the mutation?
//
// The view is memoized per statement (withStatementPropertyViews) and reused by
// every internal property read of one statement (queryVisibleValue,
// queryEntityProperties, decayVisibleNodeProperties/decayVisibleEdgeProperties,
// properties(n)). These probes establish the ownership boundary that decides
// the issue:
//
//   - The memo is released at every published row: publishQueryRow defers
//     releaseRow, and both public emitters (materialized result and
//     ResultStream.emit) route rows through it. So the only property maps a
//     consumer ever holds are released (orphaned) views.
//   - The memo view is always a detached top-level map: a decay-enabled
//     executor's QueryResultProjection has a non-nil engine, so NodeProperties
//     and EdgeProperties allocate, and the memo never aliases a stored map.
//
// These probes mutate the maps a public consumer receives (Execute rows and
// streamed rows) and then observe the statement's continuation, a later
// statement read, and the stored body. They are the reachability tests NOR-310
// asks for; they do not exercise the cached ExecuteResult leak (NOR-307).
//
// Distinct from NOR-307: nothing here reuses an ExecuteResult. Every probe uses
// its own executor with the query result cache disabled (MaxEntries: 0), so a
// later read really re-executes the statement and its own statement view.
//
// TestStatementPropertyViewMutationBeforePublicationReachesTheNextRead is the
// counterfactual: the same mutation of a live view is observed by the next
// read, so the public probes above fail if a published row ever leaves the
// statement view live.

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// publicViewMapIdentity is the identity of a property map. Two published cells
// share a statement view only when they hold the same map.
func publicViewMapIdentity(properties map[string]interface{}) uintptr {
	return uintptr(reflect.ValueOf(properties).UnsafePointer())
}

// newPublicConsumerViewExecutor builds a decay-enabled executor so that a
// deterministic read statement owns a statement property view, with the query
// result cache off so later reads re-execute (NOR-307 is not involved).
// linkCount relationships of one Doc to one Target repeat the same node in
// several rows.
func newPublicConsumerViewExecutor(t *testing.T, linkCount int) (*StorageExecutor, *storage.BadgerEngine) {
	t.Helper()
	be, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { be.Close() })
	schema := be.GetSchemaForNamespace("nor310")
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "nor310_doc", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "nor310_doc_binding", ProfileRef: "nor310_doc", TargetLabels: []string{"Doc"},
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "body", HalfLifeSeconds: 1}},
	}))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "nor310_link", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "nor310_link_binding", ProfileRef: "nor310_link", TargetEdgeType: "LINK", IsEdge: true,
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "summary", HalfLifeSeconds: 1}},
	}))
	be.SetDecayEnabled(true)
	old := time.Now().Add(-time.Hour)
	for _, node := range []*storage.Node{
		{ID: "nor310:a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "a", "body": "hidden"}, CreatedAt: old, UpdatedAt: old},
		{ID: "nor310:b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "b", "body": "hidden"}, CreatedAt: old, UpdatedAt: old},
		{ID: "nor310:target", Labels: []string{"Target"}, Properties: map[string]interface{}{"title": "target"}, CreatedAt: old, UpdatedAt: old},
	} {
		_, err = be.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	for i := 0; i < linkCount; i++ {
		require.NoError(t, be.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("nor310:link%c", 'a'+i)),
			StartNode: "nor310:a", EndNode: "nor310:target", Type: "LINK",
			Properties: map[string]interface{}{"summary": "hidden", "title": "link"}, CreatedAt: old, UpdatedAt: old,
		}))
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(be, "nor310"), QueryCachePolicy{MaxEntries: 0, TTL: time.Minute}), be
}

// requireStatementViewAdmits proves the probe statement really owns the
// statement view it is meant to test: the same admission Execute uses.
func requireStatementViewAdmits(t *testing.T, exec *StorageExecutor, query string) {
	t.Helper()
	require.NotNil(t, exec.withStatementPropertyViews(context.Background(), query),
		"the probe statement must own a statement property view, or it proves nothing about the view")
}

func requireUnmutatedStoredBody(t *testing.T, be *storage.BadgerEngine) {
	t.Helper()
	stored, err := be.GetNode(t.Context(), "nor310:a")
	require.NoError(t, err)
	require.Equal(t, "a", stored.Properties["title"], "a consumer mutation must not reach the stored node")
	require.Equal(t, "hidden", stored.Properties["body"], "a consumer mutation must not reach the stored body")
}

// publicPathParts reads the authoritative path carrier a published row holds:
// the path map's "_pathResult" value, whose Nodes and Relationships are the
// entities the statement view projected.
func publicPathParts(t *testing.T, value interface{}) ([]*storage.Node, []*storage.Edge) {
	t.Helper()
	path, ok := value.(map[string]interface{})
	require.True(t, ok, "a RETURN p row carries the path map")
	var result *PathResult
	switch typed := path["_pathResult"].(type) {
	case PathResult:
		result = &typed
	case *PathResult:
		result = typed
	}
	require.NotNil(t, result, "the path map carries its authoritative PathResult")
	return result.Nodes, result.Relationships
}

// TestPublicConsumerNodeMutationDoesNotReachAnotherStatementRead mutates the
// node map of an Execute row (the same Doc repeated by UNWIND), then reads the
// statement again on the same executor. It also pins per-row map ownership: no
// two published rows share a property map, so one row's mutation cannot be
// another row's read.
func TestPublicConsumerNodeMutationDoesNotReachAnotherStatementRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH (d:Doc {title:'a'}) UNWIND [1,2,3] AS i RETURN d"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 3)
	published := make(map[uintptr]bool, len(first.Rows))
	for _, row := range first.Rows {
		node, ok := row[0].(*storage.Node)
		require.True(t, ok)
		require.Equal(t, "a", node.Properties["title"])
		require.NotContains(t, node.Properties, "body", "decay hides the body in every published row")
		published[publicViewMapIdentity(node.Properties)] = true
	}
	require.Len(t, published, len(first.Rows), "each published row owns its property map: no statement view outlives a row")

	mutated := first.Rows[0][0].(*storage.Node).Properties
	mutated["title"] = "consumer-mutated"
	mutated["body"] = "consumer-injected"
	mutated["injected"] = true

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, second.Rows, 3)
	for _, row := range second.Rows {
		node := row[0].(*storage.Node)
		require.Equal(t, "a", node.Properties["title"], "a later read must not observe a consumer mutation of an earlier row")
		require.NotContains(t, node.Properties, "body")
		require.NotContains(t, node.Properties, "injected")
		require.NotEqual(t, publicViewMapIdentity(mutated), publicViewMapIdentity(node.Properties))
	}
	requireUnmutatedStoredBody(t, be)
}

// TestPublicConsumerStreamedNodeMutationDoesNotReachStatementContinuation is
// the direct "mutate before another read" probe: the consumer mutates a
// streamed row while the statement keeps producing rows of the same Doc, and
// the continuation reads that Doc's body again. A live shared view would make
// the later rows report the injected body.
func TestPublicConsumerStreamedNodeMutationDoesNotReachStatementContinuation(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const query = "MATCH (d:Doc {title:'a'}) UNWIND [1,2,3,4,5,6] AS i RETURN d, d.body AS body"
	requireStatementViewAdmits(t, exec, query)

	run := executeStreamed(ctx, exec, query, 1)
	defer func() { cancel(); <-run.stream.Done() }()
	require.True(t, run.stream.streamed(), "the probe needs the statement to keep producing rows after the first published row")
	require.NoError(t, run.errIfDone())

	first, exhausted := run.stream.Next(1)
	require.False(t, exhausted)
	require.Len(t, first, 1)
	require.Nil(t, first[0][1], "the view hides the body before the mutation")
	mutated := first[0][0].(*storage.Node).Properties
	require.Equal(t, "a", mutated["title"])
	require.NotContains(t, mutated, "body")
	mutated["title"] = "consumer-mutated"
	mutated["body"] = "consumer-injected"
	mutated["injected"] = true

	rest, exhausted := run.stream.Next(-1)
	require.True(t, exhausted)
	require.Len(t, rest, 5, "the statement had rows left to produce after the mutation")
	<-run.stream.Done()
	require.NoError(t, run.err)
	for _, row := range rest {
		node := row[0].(*storage.Node)
		require.Equal(t, "a", node.Properties["title"], "a statement continuation must not read a consumer-mutated view")
		require.NotContains(t, node.Properties, "body")
		require.NotContains(t, node.Properties, "injected")
		require.Nil(t, row[1], "the continuation's own property read must project the stored body, not the mutated view")
		require.NotEqual(t, publicViewMapIdentity(mutated), publicViewMapIdentity(node.Properties))
	}
	requireUnmutatedStoredBody(t, be)
}

// TestPublicConsumerEdgeMutationDoesNotReachAnotherStatementRead is the
// relationship side of the same contract (statementPropertyViews.edge).
func TestPublicConsumerEdgeMutationDoesNotReachAnotherStatementRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH (:Doc {title:'a'})-[r:LINK]->() UNWIND [1,2,3] AS i RETURN r"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 3)
	for _, row := range first.Rows {
		edge, ok := row[0].(*storage.Edge)
		require.True(t, ok)
		require.Equal(t, "link", edge.Properties["title"])
		require.NotContains(t, edge.Properties, "summary", "decay hides the summary in every published row")
	}

	mutated := first.Rows[0][0].(*storage.Edge).Properties
	mutated["summary"] = "consumer-injected"
	mutated["title"] = "consumer-mutated"

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, second.Rows, 3)
	for _, row := range second.Rows {
		edge := row[0].(*storage.Edge)
		require.Equal(t, "link", edge.Properties["title"])
		require.NotContains(t, edge.Properties, "summary")
		require.NotEqual(t, publicViewMapIdentity(mutated), publicViewMapIdentity(edge.Properties))
	}
	stored, err := be.GetEdge(t.Context(), "nor310:linka")
	require.NoError(t, err)
	require.Equal(t, "hidden", stored.Properties["summary"])
	require.Equal(t, "link", stored.Properties["title"])
}

// TestPublicConsumerPropertiesMapMutationDoesNotReachAnotherStatementRead uses
// properties(n): the RETURN assembles the statement view, so the published map
// must be detached before the consumer holds it.
func TestPublicConsumerPropertiesMapMutationDoesNotReachAnotherStatementRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH (d:Doc {title:'a'}) RETURN properties(d) AS props"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	properties, ok := first.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "a", properties["title"])
	require.NotContains(t, properties, "body")
	properties["title"] = "consumer-mutated"
	properties["body"] = "consumer-injected"
	properties["injected"] = true

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"title": "a"}, second.Rows[0][0])
	requireUnmutatedStoredBody(t, be)
}

// TestPublicConsumerPathMutationDoesNotReachAnotherStatementRead covers the path
// carrier: projectVisibleQueryValue projects each path node and relationship
// through the same statement view before publication.
func TestPublicConsumerPathMutationDoesNotReachAnotherStatementRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH p=(d:Doc {title:'a'})-[r:LINK]->() UNWIND [1,2,3] AS i RETURN p"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 3)
	nodes, relationships := publicPathParts(t, first.Rows[0][0])
	require.NotEmpty(t, nodes)
	require.NotEmpty(t, relationships)
	var doc *storage.Node
	for _, node := range nodes {
		if node.Properties["title"] == "a" {
			doc = node
		}
	}
	require.NotNil(t, doc, "the path carries the Doc the probe reads")
	require.NotContains(t, doc.Properties, "body")
	doc.Properties["title"] = "consumer-mutated"
	doc.Properties["body"] = "consumer-injected"
	mutatedRelationship := relationships[0].Properties
	require.NotContains(t, mutatedRelationship, "summary")
	mutatedRelationship["summary"] = "consumer-injected"

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, second.Rows, 3)
	for _, row := range second.Rows {
		nodes, relationships := publicPathParts(t, row[0])
		for _, node := range nodes {
			require.NotContains(t, node.Properties, "injected")
			require.NotContains(t, node.Properties, "body")
			if node.ID == doc.ID {
				require.Equal(t, "a", node.Properties["title"])
			}
		}
		for _, relationship := range relationships {
			require.NotContains(t, relationship.Properties, "summary")
		}
	}
	requireUnmutatedStoredBody(t, be)
}

// TestPublicConsumerLimitFastPathMutationDoesNotReachAnotherStatementRead covers
// the bounded simple-projection result assembly (projectVisiblePipelineNodeRead,
// reached by a LIMIT read): the fast path projects the scanned nodes through the
// statement view, and each row is still released before the consumer holds it.
func TestPublicConsumerLimitFastPathMutationDoesNotReachAnotherStatementRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH (d:Doc) RETURN d LIMIT 2"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	firstNode := first.Rows[0][0].(*storage.Node)
	secondNode := first.Rows[1][0].(*storage.Node)
	require.Equal(t, "a", firstNode.Properties["title"])
	require.NotContains(t, firstNode.Properties, "body")
	require.NotContains(t, secondNode.Properties, "body")
	require.NotEqual(t, publicViewMapIdentity(firstNode.Properties), publicViewMapIdentity(secondNode.Properties),
		"the fast path still publishes one detached map per row")
	mutated := firstNode.Properties
	mutated["title"] = "consumer-mutated"
	mutated["body"] = "consumer-injected"
	require.Equal(t, "b", secondNode.Properties["title"], "the other published row is unaffected")

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, second.Rows, 2)
	for _, row := range second.Rows {
		node := row[0].(*storage.Node)
		require.NotEqual(t, "consumer-mutated", node.Properties["title"], "a later LIMIT read must not observe the mutation")
		require.NotContains(t, node.Properties, "body")
	}
	require.Equal(t, "a", second.Rows[0][0].(*storage.Node).Properties["title"])
	requireUnmutatedStoredBody(t, be)
}

// TestPublicConsumerSameRowCellsShareOnlyTheReleasedView records the lab
// report's "changing the returned property map affects other cells" case and
// bounds it: one published row repeats the same entity value, so its two cells
// share that row's released view. The sharing does not cross the row boundary,
// so it cannot change another read, and it is not the stored map. This is the
// current entity identity of a row (RETURN d, d is one entity twice), not a
// live statement cache.
func TestPublicConsumerSameRowCellsShareOnlyTheReleasedView(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	const query = "MATCH (d:Doc {title:'a'}) UNWIND [1,2] AS i RETURN d, d AS again"
	requireStatementViewAdmits(t, exec, query)

	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	firstCell := first.Rows[0][0].(*storage.Node)
	require.Same(t, firstCell, first.Rows[0][1].(*storage.Node),
		"one row repeats one entity value; it does not alias a live statement view")
	require.NotContains(t, firstCell.Properties, "body")
	require.NotEqual(t, publicViewMapIdentity(first.Rows[1][0].(*storage.Node).Properties), publicViewMapIdentity(firstCell.Properties),
		"the next published row owns a different map")
	firstCell.Properties["title"] = "consumer-mutated"
	require.Equal(t, "consumer-mutated", first.Rows[0][1].(*storage.Node).Properties["title"],
		"the repeated cell of the same published row sees the mutation (lab observation), bounded to this row")
	require.Equal(t, "a", first.Rows[1][0].(*storage.Node).Properties["title"],
		"no other published row shares that map")
	requireUnmutatedStoredBody(t, be)

	second, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	for _, row := range second.Rows {
		require.Equal(t, "a", row[0].(*storage.Node).Properties["title"])
		require.Equal(t, "a", row[1].(*storage.Node).Properties["title"])
	}
}

// TestStatementPropertyViewMemoIsReleasedAtEveryPublishedRow pins the ownership
// boundary itself: the memo holds no source, body or byte after a row is
// published, its view is a detached map (never the stored map), and reuse only
// exists before publication.
func TestStatementPropertyViewMemoIsReleasedAtEveryPublishedRow(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	scoped := exec.withStatementPropertyViews(ctx, "MATCH (d:Doc {title:'a'}) RETURN d")
	require.NotNil(t, scoped, "a deterministic decay-enabled read owns a statement view")
	views := scoped.statementPropertyViews

	source, err := be.GetNode(ctx, "nor310:a")
	require.NoError(t, err)
	view := scoped.queryVisibleValue(source).(*storage.Node)
	require.Equal(t, "a", view.Properties["title"])
	require.NotContains(t, view.Properties, "body")
	require.NotEqual(t, publicViewMapIdentity(source.Properties), publicViewMapIdentity(view.Properties),
		"the statement view is a detached projection, never the stored map")
	require.Same(t, view, scoped.queryVisibleValue(source), "the view is reused inside the statement before publication")

	published, _ := scoped.publishQueryRow([]interface{}{source})
	require.Len(t, views.nodes, 0, "publication releases the node view")
	require.Len(t, views.nodeBodies, 0, "publication releases the detached body snapshot")
	require.Len(t, views.edges, 0)
	require.Len(t, views.edgeBodies, 0)
	require.Zero(t, views.bytes)

	row, ok := published[0].(*storage.Node)
	require.True(t, ok)
	require.Same(t, view, row, "the published cell is the statement view the row was projected from")
	require.Equal(t, publicViewMapIdentity(view.Properties), publicViewMapIdentity(row.Properties),
		"the consumer holds the statement view map itself, so only the row release can detach it")
	require.NotContains(t, row.Properties, "body")
	row.Properties["title"] = "consumer-mutated"
	row.Properties["body"] = "consumer-injected"

	next := scoped.queryVisibleValue(source).(*storage.Node)
	require.Equal(t, "a", next.Properties["title"], "the next read projects the stored body, not the released view")
	require.NotContains(t, next.Properties, "body")
	require.NotEqual(t, publicViewMapIdentity(row.Properties), publicViewMapIdentity(next.Properties))
	requireUnmutatedStoredBody(t, be)
}

// TestStatementPropertyViewMutationBeforePublicationReachesTheNextRead is the
// counterfactual the public probes above depend on: mutating a view that is
// still live is observed by the next read of the same statement. It reproduces
// the lab report's "same cell view" observation for internal reuse only, and it
// keeps the public reachability probes honest: they would fail if a published
// row left a view live, and they pass because publication releases it.
func TestStatementPropertyViewMutationBeforePublicationReachesTheNextRead(t *testing.T) {
	exec, be := newPublicConsumerViewExecutor(t, 1)
	ctx := context.Background()
	scoped := exec.withStatementPropertyViews(ctx, "MATCH (d:Doc {title:'a'}) RETURN d")
	require.NotNil(t, scoped)

	source, err := be.GetNode(ctx, "nor310:a")
	require.NoError(t, err)
	live := scoped.queryVisibleValue(source).(*storage.Node)
	live.Properties["title"] = "view-mutated"
	live.Properties["injected"] = true
	again := scoped.queryVisibleValue(source).(*storage.Node)
	require.Same(t, live, again, "the live statement view is the same value, so the next read sees its mutation")
	require.Equal(t, "view-mutated", again.Properties["title"])
	require.Equal(t, true, again.Properties["injected"])

	// The view is still detached: mutating it never reaches the stored body.
	requireUnmutatedStoredBody(t, be)
}
