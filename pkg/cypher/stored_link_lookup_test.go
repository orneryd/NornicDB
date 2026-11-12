package cypher

// NOR-414 STORED_LINK shape C: a stored-link lookup constrains both endpoint
// nodes and the relationship by inline unique keys. The target node owns N
// incoming PD_RELATION relationships while the source owns exactly three
// outgoing ones, so the lookup must use its declared relationship key without expanding either endpoint.
// A control without the relationship key still expands the cheaper source.
//
// The fixture preserves the reported laboratory population: in-memory native
// Badger behind the personal_documents namespace, populated by sequential
// CreateNode/CreateEdge calls (history), with the two unique constraints
// installed only after every node and relationship exists, and read through the
// public Execute path. Each population is built once; each parser reads it
// through its own executor so neither parser reuses the other's cached plan.
// The probe preserves native optional capabilities and reuses the existing
// adjacencyReadSpy accounting owner. It must not hide the equality index the
// production reader uses.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	storedLinkSourceID = "synthetic-from"
	storedLinkTargetID = "synthetic-to"
	storedLinkEdgeID   = "synthetic-stored-link"

	// storedLinkStatement is the reported lookup. Every endpoint and the
	// relationship carry an inline unique key; the projection is the six
	// relationship properties the stored claim needs.
	storedLinkStatement = `MATCH (:PDGraphNode {id: $from_id})-[r:PD_RELATION {id: $id}]->(:PDGraphNode {id: $to_id})
RETURN r.kind AS kind, r.statement AS statement, r.source_id AS source_id,
       r.cite_start AS cite_start, r.cite_end AS cite_end, r.citation_claim AS citation_claim`
)

// storedLinkParams is the reported parameter set exactly.
func storedLinkParams() map[string]interface{} {
	return map[string]interface{}{
		"id":      storedLinkEdgeID,
		"from_id": storedLinkSourceID,
		"to_id":   storedLinkTargetID,
	}
}

// storedLinkExpectedRows is the exact six-column stored claim the lookup
// must return on every population size and parser.
func storedLinkExpectedRows() [][]interface{} {
	return [][]interface{}{{
		"supports",
		"synthetic stored claim",
		"synthetic-source",
		[]int64{5},
		[]int64{15},
		"synthetic-citation-claim",
	}}
}

// storedLinkFixture is shape C behind the canonical adjacency read probe:
// N+3 PDGraphNode nodes and N+2 PD_RELATION relationships, with the source's
// outgoing degree 3 and the target's incoming degree N.
type storedLinkFixture struct {
	*traversalLabSeedFixture
	spy  *storedLinkAdjacencyProbe
	exec *StorageExecutor
}

// newStoredLinkFixture builds the committed population through sequential
// CreateNode/CreateEdge calls, then installs both unique constraints after
// loading, so the read observes the reported fixture history and configuration.
func newStoredLinkFixture(tb testing.TB, degree int) *storedLinkFixture {
	tb.Helper()
	require.GreaterOrEqual(tb, degree, 2, "shape C needs the stored link and at least one extra incoming relationship")
	population := newTraversalLabSeedFixture(tb)
	store := population.store
	createNode := func(id string) {
		population.createNode(tb, id, []string{"PDGraphNode"}, map[string]interface{}{"id": id})
	}
	createEdge := func(id, start, end string, properties map[string]interface{}) {
		population.createEdge(tb, id, start, end, "PD_RELATION", properties)
	}

	for _, id := range []string{storedLinkSourceID, storedLinkTargetID, "synthetic-spare-a", "synthetic-spare-b"} {
		createNode(id)
	}
	createEdge("edge-wanted", storedLinkSourceID, storedLinkTargetID, map[string]interface{}{
		"id":             storedLinkEdgeID,
		"kind":           "supports",
		"statement":      "synthetic stored claim",
		"source_id":      "synthetic-source",
		"cite_start":     []interface{}{int64(5)},
		"cite_end":       []interface{}{int64(15)},
		"citation_claim": "synthetic-citation-claim",
	})
	for i, id := range []string{"synthetic-spare-a", "synthetic-spare-b"} {
		createEdge(fmt.Sprintf("spare-edge-%d", i), storedLinkSourceID, id,
			map[string]interface{}{"id": fmt.Sprintf("spare-link-%d", i), "kind": "supports"})
	}
	for i := 0; i < degree-1; i++ {
		nodeID := fmt.Sprintf("synthetic-noise-%d", i)
		createNode(nodeID)
		createEdge(fmt.Sprintf("noise-edge-%d", i), nodeID, storedLinkTargetID,
			map[string]interface{}{"id": fmt.Sprintf("noise-link-%d", i), "kind": "supports", "statement": "synthetic noise"})
	}

	// Preserve the native harness's key names and distinct physical/logical
	// relationship IDs; the text reproduction calls the node key stored_node_id.
	population.createConstraints(tb,
		"CREATE CONSTRAINT pd_graph_node_id IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT pd_relation_id IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE",
	)
	ctx := context.Background()
	nodes, err := store.NodeCount(ctx)
	require.NoError(tb, err)
	require.Equal(tb, int64(degree+3), nodes)
	edges, err := store.EdgeCount(ctx)
	require.NoError(tb, err)
	require.Equal(tb, int64(degree+2), edges)
	byType, err := store.EdgeCountByType(ctx, "PD_RELATION")
	require.NoError(tb, err)
	require.Equal(tb, edges, byType)
	outDegree, err := store.GetOutDegree(ctx, storedLinkSourceID)
	require.NoError(tb, err)
	require.Equal(tb, 3, outDegree)
	inDegree, err := store.GetInDegree(ctx, storedLinkTargetID)
	require.NoError(tb, err)
	require.Equal(tb, degree, inDegree)
	population.requireConstraints(tb, []traversalLabConstraint{
		{name: "pd_graph_node_id", target: "PDGraphNode", property: "id"},
		{name: "pd_relation_id", target: "PD_RELATION", property: "id"},
	})
	require.Implements(tb, (*storage.RelationshipPropertyEqualityLookup)(nil), store)

	spy := &storedLinkAdjacencyProbe{NamespacedEngine: store, counts: adjacencyReadSpy{Engine: store}}
	return &storedLinkFixture{
		traversalLabSeedFixture: population,
		spy:                     spy,
		exec:                    population.executor(),
	}
}

// storedLinkAdjacencyProbe keeps every native namespace capability available.
// Its one override delegates metering to the shared adjacency test owner.
type storedLinkAdjacencyProbe struct {
	*storage.NamespacedEngine
	counts                     adjacencyReadSpy
	lookupCalls, lookupHandled int
}

func (p *storedLinkAdjacencyProbe) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	return p.counts.ReadAdjacentEdges(ctx, request)
}

func (p *storedLinkAdjacencyProbe) LookupEdgesByPropertyEquality(ctx context.Context, kind string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	p.lookupCalls++
	edges, handled, err := p.NamespacedEngine.LookupEdgesByPropertyEquality(ctx, kind, properties, values)
	if handled {
		p.lookupHandled++
	}
	return edges, handled, err
}

func TestStoredLinkLookupPreservesClaimWithoutAdjacencyExpansion(t *testing.T) {
	for _, degree := range []int{1000, 16000} {
		t.Run(fmt.Sprintf("degree=%d", degree), func(t *testing.T) {
			fixture := newStoredLinkFixture(t, degree)
			for _, parser := range relationshipEqualityParsers {
				t.Run(parser.name, func(t *testing.T) {
					t.Cleanup(parser.setup())
					require.Implements(t, (*storage.RelationshipPropertyEqualityLookup)(nil), fixture.spy)
					exec := NewStorageExecutor(fixture.spy, QueryCachePolicy{MaxEntries: 0})
					// Prove that the meter sees real expansion when no key pins
					// the relationship. The complete stored claim is unchanged.
					control := strings.Replace(storedLinkStatement, " {id: $id}", "", 1)
					fixture.spy.counts.reset()
					result, err := exec.Execute(t.Context(), control, storedLinkParams())
					require.NoError(t, err)
					require.Equal(t, storedLinkExpectedRows(), result.Rows)
					require.Equal(t, 3, fixture.spy.counts.entries)
					fixture.spy.counts.reset()
					fixture.spy.lookupCalls, fixture.spy.lookupHandled = 0, 0
					result, err = exec.Execute(t.Context(), storedLinkStatement, storedLinkParams())
					require.NoError(t, err)
					require.Equal(t, storedLinkExpectedRows(), result.Rows)
					require.Zero(t, fixture.spy.counts.entries, "the declared relationship key must avoid endpoint expansion")
					require.Zero(t, fixture.spy.counts.streams, "the keyed lookup must not scan the relationship type")
					require.Positive(t, fixture.spy.lookupCalls)
					require.Positive(t, fixture.spy.lookupHandled, "the native equality owner must answer")
					fixture.spy.counts.reset()
					fixture.spy.lookupCalls, fixture.spy.lookupHandled = 0, 0
					// Read the unwrapped object used by the benchmark, too, with
					// its fresh explicit transaction and the same complete claim.
					for _, store := range []storage.Engine{fixture.store, fixture.spy} {
						func() {
							native := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
							_, err := native.Execute(t.Context(), "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, err := native.Execute(t.Context(), "ROLLBACK", nil); require.NoError(t, err) }()
							result, err := native.Execute(t.Context(), storedLinkStatement, storedLinkParams())
							require.NoError(t, err)
							require.Equal(t, storedLinkExpectedRows(), result.Rows)
							// Explicit reads use transactionStorageWrapper ->
							// BadgerTransaction's index, not the live namespace
							// probe. Verify that same transaction's lookup owner.
							lookup, ok := native.storage.(storage.RelationshipPropertyEqualityLookup)
							require.True(t, ok)
							edges, handled, err := lookup.LookupEdgesByPropertyEquality(t.Context(), "PD_RELATION", []string{"id"}, []interface{}{storedLinkEdgeID})
							require.NoError(t, err)
							require.True(t, handled)
							require.Len(t, edges, 1)
							require.Equal(t, storedLinkEdgeID, edges[0].Properties["id"])
						}()
					}
				})
			}
		})
	}
}
