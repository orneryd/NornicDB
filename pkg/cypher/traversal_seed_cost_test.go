package cypher

// NOR-414 traversal seed cost: an indexed end node is not automatically the
// cheaper end of a one-hop traversal. The reported population is one
// PDCollection (no property index, small adjacency) reached by MEMBER_OF from
// 351 PDDocument nodes indexed on id, every document also carrying a large
// unrelated outgoing adjacency. Preferring the indexed end as the traversal
// start reverses the walk onto the documents and lists their whole adjacency
// (the reported read visited about 290,000 entries instead of 351): the
// start-side label scan reads one collection and lists its 351 incoming
// MEMBER_OF relationships.
//
// The same seed choice must still start from a small indexed end population
// when the start label holds many nodes (many collections, two indexed
// documents), and an inline unique key on the start node must be evaluated
// before the end is chosen, whichever end the index would answer.
//
// These tests use the public Execute path (autocommit and an explicit
// transaction) on both parser implementations, and count the relationship
// listings the traversal makes through adjacencyReadSpy
// (relationship_id_pinned_edge_test.go). An explicit transaction serves its
// adjacency reads itself, so the transaction case bounds the statement's own
// allocations instead; both are deterministic for the same store.

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalSeedCostStatement is the reported lookup: the documents are
// constrained by the WHERE's indexed id list, the single collection is not
// constrained at all.
const traversalSeedCostStatement = "MATCH (k:PDCollection)<-[:MEMBER_OF]-(d:PDDocument) WHERE d.id IN $ids RETURN d.id, k.archived"

// traversalSeedCostInlineStatement constrains both ends by their own inline
// unique key, so the start key must be read before the end is chosen.
const traversalSeedCostInlineStatement = "MATCH (k:PDCollection {id: $k})<-[:MEMBER_OF]-(d:PDDocument {id: $d}) RETURN d.id, k.id, k.archived"

// traversalSeedCostPopulation is a whole in-memory graph:
//
//	(:PDCollection {id: 'col-N'}) <- [:MEMBER_OF] - (:PDDocument {id: 'doc-N'})
//
// every document also owns unrelatedFanout outgoing UNRELATED relationships to
// a shared pool of PDUnrelated nodes, and every document is MEMBER_OF
// membershipsPerDocument collections (consecutive ones, wrapping at the
// collection count).
type traversalSeedCostPopulation struct {
	collections            int
	documents              int
	membershipsPerDocument int
	unrelatedFanout        int
}

// traversalSeedCostFixture is a traversal population behind an
// adjacencyReadSpy. Writes go through the same namespaced engine the read
// executor observes, so index maintenance is the production one.
type traversalSeedCostFixture struct {
	spy       *adjacencyReadSpy
	exec      *StorageExecutor
	ctx       context.Context
	documents []string
}

// traversalSeedCostWork is the adjacency work one read did: how many listings
// it asked for, how many relationship entries those listings returned, and
// how many whole-type streams it opened.
type traversalSeedCostWork struct {
	listings int
	entries  int
	streams  int
}

func newTraversalSeedCostFixture(t testing.TB, population traversalSeedCostPopulation) *traversalSeedCostFixture {
	t.Helper()
	require.Positive(t, population.collections, "population")
	require.Positive(t, population.documents, "population")
	if population.membershipsPerDocument < 1 {
		population.membershipsPerDocument = 1
	}
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_cost")
	writer := NewStorageExecutor(store)
	// Unique endpoint ids: the reported lookup is an id/index point read, and
	// the inline form addresses either end by its own unique key.
	for _, statement := range []string{
		"CREATE CONSTRAINT seed_cost_collection_id_unique FOR (n:PDCollection) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT seed_cost_document_id_unique FOR (n:PDDocument) REQUIRE n.id IS UNIQUE",
	} {
		if _, err := writer.Execute(ctx, statement, nil); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// One shared pool of unrelated targets: an UNRELATED relationship is
	// type-filtered away by the traversal, but a listing that starts at the
	// document still hands every entry over.
	nodes := make([]*storage.Node, 0, population.collections+population.documents+population.unrelatedFanout)
	for index := 0; index < population.unrelatedFanout; index++ {
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("unrelated-%d", index)),
			Labels:     []string{"PDUnrelated"},
			Properties: map[string]interface{}{"slot": int64(index)},
		})
	}
	for index := 0; index < population.collections; index++ {
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("col-%d", index)),
			Labels:     []string{"PDCollection"},
			Properties: map[string]interface{}{"id": fmt.Sprintf("col-%d", index), "archived": false},
		})
	}
	for index := 0; index < population.documents; index++ {
		id := fmt.Sprintf("doc-%d", index)
		nodes = append(nodes, &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"PDDocument"},
			Properties: map[string]interface{}{"id": id},
		})
	}
	if err := store.BulkCreateNodes(ctx, nodes); err != nil {
		t.Fatalf("create traversal seed cost population: %v", err)
	}

	edges := make([]*storage.Edge, 0, population.documents*(population.membershipsPerDocument+population.unrelatedFanout))
	for index := 0; index < population.documents; index++ {
		document := fmt.Sprintf("doc-%d", index)
		for membership := 0; membership < population.membershipsPerDocument; membership++ {
			collection := fmt.Sprintf("col-%d", (index*population.membershipsPerDocument+membership)%population.collections)
			edges = append(edges, &storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("member-%d-%d", index, membership)),
				StartNode: storage.NodeID(document),
				EndNode:   storage.NodeID(collection),
				Type:      "MEMBER_OF",
			})
		}
		for slot := 0; slot < population.unrelatedFanout; slot++ {
			edges = append(edges, &storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("unrelated-edge-%d-%d", index, slot)),
				StartNode: storage.NodeID(document),
				EndNode:   storage.NodeID(fmt.Sprintf("unrelated-%d", slot)),
				Type:      "UNRELATED",
			})
		}
	}
	if err := store.BulkCreateEdges(ctx, edges); err != nil {
		t.Fatalf("create traversal seed cost relationships: %v", err)
	}

	documents := make([]string, 0, population.documents)
	for index := 0; index < population.documents; index++ {
		documents = append(documents, fmt.Sprintf("doc-%d", index))
	}
	spy := &adjacencyReadSpy{Engine: store}
	return &traversalSeedCostFixture{
		spy:       spy,
		exec:      NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0}),
		ctx:       ctx,
		documents: documents,
	}
}

// idParams is the WHERE's $ids list: every document id.
func (f *traversalSeedCostFixture) idParams() map[string]interface{} {
	ids := make([]interface{}, len(f.documents))
	for index, document := range f.documents {
		ids[index] = document
	}
	return map[string]interface{}{"ids": ids}
}

// measure runs statement through the public executor and returns its sorted
// rows together with the adjacency work the read did.
func (f *traversalSeedCostFixture) measure(t *testing.T, statement string, params map[string]interface{}) ([]string, traversalSeedCostWork) {
	t.Helper()
	f.spy.reset()
	result, err := f.exec.Execute(f.ctx, statement, params)
	require.NoError(t, err, statement)
	return seedRowKeys(result.Rows), traversalSeedCostWork{
		listings: f.spy.calls,
		entries:  f.spy.entries,
		streams:  f.spy.streams,
	}
}

// seedCostRows is the expected row set for documents whose collections are not
// projected: one row per (document, collection) pair, k.archived false.
func seedCostRows(documents []string, membershipsPerDocument int) []string {
	rows := make([]string, 0, len(documents)*membershipsPerDocument)
	for _, document := range documents {
		for membership := 0; membership < membershipsPerDocument; membership++ {
			rows = append(rows, fmt.Sprint(document, false))
		}
	}
	sort.Strings(rows)
	return rows
}

// The reported shape: one unindexed PDCollection, the indexed PDDocuments in
// the WHERE's id list, and a wide unrelated adjacency on every document. The
// traversal must start from the collection label (one node, the collection's
// MEMBER_OF entries), not from the indexed end (one listing of every
// document's whole adjacency per document).
func TestTraversalSeedCostSingleCollectionBeatsIndexedDocumentFanout(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const documents = 12
		const unrelatedFanout = 64
		fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
			collections:            1,
			documents:              documents,
			membershipsPerDocument: 1,
			unrelatedFanout:        unrelatedFanout,
		})
		rows, work := fixture.measure(t, traversalSeedCostStatement, fixture.idParams())
		t.Logf("adjacency work: %+v", work)
		require.Equal(t, seedCostRows(fixture.documents, 1), rows)
		require.Equal(t, 0, work.streams)
		require.LessOrEqual(t, work.listings, 2,
			"adjacency listings: the single collection's MEMBER_OF list, not one listing per indexed document")
		require.LessOrEqual(t, work.entries, 2*documents,
			"adjacency entries: the collection's %d MEMBER_OF relationships, not the documents' %d-entry unrelated adjacency", documents, unrelatedFanout)
	})
}

// Both ends carry an inline unique key. The start key must be read before the
// end is chosen, so a start key that names no collection matches nothing
// without listing a document's adjacency, and a start key that names one
// matches by the collection's own relationship list.
func TestTraversalSeedCostInlineStartKeyBeatsEndChoice(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const documents = 12
		const unrelatedFanout = 160
		fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
			collections:            1,
			documents:              documents,
			membershipsPerDocument: 1,
			unrelatedFanout:        unrelatedFanout,
		})
		rows, work := fixture.measure(t, traversalSeedCostInlineStatement, map[string]interface{}{
			"k": "col-0", "d": "doc-3",
		})
		t.Logf("adjacency work: %+v", work)
		require.Equal(t, []string{fmt.Sprint("doc-3", "col-0", false)}, rows)
		require.Equal(t, 0, work.streams)
		require.LessOrEqual(t, work.listings, 2,
			"adjacency listings: the collection's own MEMBER_OF list, not the document's whole adjacency")
		require.LessOrEqual(t, work.entries, 2*documents,
			"adjacency entries: the collection's %d MEMBER_OF relationships, not the document's %d-entry unrelated adjacency", documents, unrelatedFanout)

		// The start key is a decision, not a residual filter: no such
		// collection means no path, and nothing is listed to find that out.
		rows, work = fixture.measure(t, traversalSeedCostInlineStatement, map[string]interface{}{
			"k": "col-missing", "d": "doc-3",
		})
		require.Empty(t, rows, "an absent start key matches nothing")
		require.Zero(t, work.entries, "an absent start key lists no adjacency: %+v", work)
	})
}

// The opposite case keeps its improvement: many unindexed collections and a
// tiny indexed end population. Starting from the two indexed documents costs
// two listings; starting from the 200 collections would list every one of
// them.
func TestTraversalSeedCostKeepsEndSeedForTinyIndexedPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const collections = 200
		const documents = 2
		const membershipsPerDocument = 3
		fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
			collections:            collections,
			documents:              documents,
			membershipsPerDocument: membershipsPerDocument,
			unrelatedFanout:        1,
		})
		rows, work := fixture.measure(t,
			"MATCH (k:PDCollection)<-[:MEMBER_OF]-(d:PDDocument) WHERE d.id IN $ids RETURN d.id, k.id, k.archived",
			fixture.idParams())
		t.Logf("adjacency work: %+v", work)
		expected := make([]string, 0, documents*membershipsPerDocument)
		for index := 0; index < documents; index++ {
			for membership := 0; membership < membershipsPerDocument; membership++ {
				expected = append(expected, fmt.Sprint(
					fmt.Sprintf("doc-%d", index),
					fmt.Sprintf("col-%d", (index*membershipsPerDocument+membership)%collections),
					false))
			}
		}
		sort.Strings(expected)
		require.Equal(t, expected, rows)
		require.Equal(t, 0, work.streams)
		require.LessOrEqual(t, work.listings, documents+2,
			"adjacency listings: the %d indexed documents, not the %d collections", documents, collections)
		require.LessOrEqual(t, work.entries, documents*(membershipsPerDocument+1),
			"adjacency entries: the documents' own relationships")
	})
}

// The same read inside an explicit transaction. The transaction serves its
// adjacency listings itself, so the spy sees none of them; the statement's own
// allocations are the deterministic cost, and they must not grow with the
// documents' unrelated adjacency.
func TestTraversalSeedCostExplicitTransaction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		const documents = 12
		measure := func(fanout int) (*traversalSeedCostFixture, []string, float64) {
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            1,
				documents:              documents,
				membershipsPerDocument: 1,
				unrelatedFanout:        fanout,
			})
			_, err := fixture.exec.Execute(fixture.ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = fixture.exec.Execute(fixture.ctx, "ROLLBACK", nil) })
			// One read warms the statement's own caches; the counted runs then
			// repeat the same plan.
			result, err := fixture.exec.Execute(fixture.ctx, traversalSeedCostStatement, fixture.idParams())
			require.NoError(t, err)
			rows := seedRowKeys(result.Rows)
			allocations := testing.AllocsPerRun(5, func() {
				_, err := fixture.exec.Execute(fixture.ctx, traversalSeedCostStatement, fixture.idParams())
				require.NoError(t, err)
			})
			return fixture, rows, allocations
		}
		narrowFixture, narrowRows, narrowAllocations := measure(1)
		_, wideRows, wideAllocations := measure(64)
		require.Equal(t, seedCostRows(narrowFixture.documents, 1), narrowRows)
		require.Equal(t, narrowRows, wideRows)
		t.Logf("explicit transaction allocations per statement: %.0f with an unrelated fanout of 1, %.0f with 64",
			narrowAllocations, wideAllocations)
		require.Less(t, wideAllocations, narrowAllocations+500,
			"the explicit transaction's allocations grew with the documents' unrelated adjacency")
	})
}

// More than one start node exercises adjacency cost, rather than the single-
// candidate shortcut: twelve indexed documents are still much more expensive.
func TestTraversalSeedCostComparesAdjacencyAndPreservesInlineAdmission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
			collections: 2, documents: 12, membershipsPerDocument: 1, unrelatedFanout: 64,
		})
		rows, work := fixture.measure(t, traversalSeedCostStatement, fixture.idParams())
		require.Equal(t, seedCostRows(fixture.documents, 1), rows)
		require.LessOrEqual(t, work.listings, 2)
		require.Equal(t, 12, work.entries)
		for _, statement := range []string{
			"MATCH (k:PDCollection:MissingLabel {id: $k})<-[:MEMBER_OF]-(d:PDDocument {id: $d}) RETURN d.id",
			"MATCH (k:PDCollection {id: $k, archived: true})<-[:MEMBER_OF]-(d:PDDocument {id: $d}) RETURN d.id",
		} {
			rows, work := fixture.measure(t, statement, map[string]interface{}{"k": "col-0", "d": "doc-0"})
			require.Empty(t, rows, statement)
			require.Zero(t, work.entries, statement)
		}
	})
}

func TestTraversalSeedCostBoundEndKeepsIntermediateNodes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newSeedFixture(t,
			"CREATE CONSTRAINT anchor_id_unique FOR (n:Anchor) REQUIRE n.id IS UNIQUE",
			"CREATE (:Anchor {id: 'a'})-[:NEXT]->(:Anchor {id: 'b'})-[:NEXT]->(:Anchor {id: 'c'})",
		)
		for _, hops := range []string{"2", "1..2"} {
			result, err := fixture.exec.Execute(context.Background(),
				"MATCH (a:Anchor {id: 'a'})-[:NEXT*"+hops+"]->(c:Anchor {id: 'c'}) RETURN a.id, c.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"a", "c"}}, result.Rows)
		}
	})
}

func TestTraversalSeedCostPreservesCounterAndCancellationErrors(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{collections: 2, documents: 2})
		failed := NewStorageExecutor(failingCountEngine{Engine: fixture.spy.Engine})
		_, err := failed.Execute(fixture.ctx, traversalSeedCostStatement, fixture.idParams())
		require.ErrorIs(t, err, errCountStore)
		ctx, cancel := context.WithCancel(fixture.ctx)
		cancel()
		_, err = fixture.exec.Execute(ctx, traversalSeedCostStatement, fixture.idParams())
		require.ErrorIs(t, err, context.Canceled)
	})
}

// BenchmarkTraversalSeedCost times the reported population (one PDCollection,
// 351 indexed PDDocuments, 800 unrelated relationships per document), the
// both-inline-unique-keys form, and the many-collections/tiny-end form whose
// end seed must be kept. The read's adjacency work is recorded as
// listings/op and entries/op so a profile run shows the plan the query took.
func BenchmarkTraversalSeedCost(b *testing.B) {
	for _, name := range []string{"single_collection_indexed_end", "inline_keys_both_ends", "many_collections_tiny_end"} {
		b.Run(name, func(b *testing.B) {
			population := traversalSeedCostPopulation{collections: 1, documents: 351, membershipsPerDocument: 1, unrelatedFanout: 800}
			if name == "many_collections_tiny_end" {
				population = traversalSeedCostPopulation{collections: 200, documents: 2, membershipsPerDocument: 3, unrelatedFanout: 1}
			}
			fixture := newTraversalSeedCostFixture(b, population)
			statement, params := traversalSeedCostStatement, fixture.idParams()
			expected := seedCostRows(fixture.documents, population.membershipsPerDocument)
			if name == "inline_keys_both_ends" {
				statement, params = traversalSeedCostInlineStatement, map[string]interface{}{"k": "col-0", "d": "doc-7"}
				expected = []string{fmt.Sprint("doc-7", "col-0", false)}
			}
			for _, mode := range []string{"autocommit", "transaction"} {
				b.Run(mode, func(b *testing.B) {
					if mode == "transaction" {
						_, err := fixture.exec.Execute(fixture.ctx, "BEGIN", nil)
						require.NoError(b, err)
						b.Cleanup(func() { _, _ = fixture.exec.Execute(fixture.ctx, "ROLLBACK", nil) })
					}
					fixture.spy.reset()
					probe, err := fixture.exec.Execute(fixture.ctx, statement, params)
					require.NoError(b, err)
					require.Equal(b, expected, seedRowKeys(probe.Rows))
					work := traversalSeedCostWork{listings: fixture.spy.calls, entries: fixture.spy.entries}
					b.ReportAllocs()
					b.ResetTimer()
					if mode == "autocommit" {
						b.ReportMetric(float64(work.listings), "listings/op")
						b.ReportMetric(float64(work.entries), "entries/op")
					}
					b.ReportMetric(float64(len(probe.Rows)), "rows/op")
					for run := 0; run < b.N; run++ {
						result, err := fixture.exec.Execute(fixture.ctx, statement, params)
						if err != nil || len(result.Rows) != len(expected) {
							b.Fatalf("read: rows or error mismatch: %v", err)
						}
					}
				})
			}
		})
	}
}
