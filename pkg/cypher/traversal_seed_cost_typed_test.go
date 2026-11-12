package cypher

// NOR-414 typed seed statistics: a traversal step selects the pattern's
// relationship types at the adjacency read (TraversalContext.relTypes), so a
// node's first-hop work is the positional (label, type) counters of those
// types, not of every type its endpoints hold. traversalSeedAdjacencyWork asks
// the count store for the same types the walk reads, so its estimate counts the
// relationships the traversal really lists, and a namespaced store answers each
// (namespace, label, type) from that counter instead of walking the label's
// retained type history. A pattern that names no type asks for every type, as
// its untyped read does.
//
// These cases run on the NOR-414 seed cost fixture (traversal_seed_cost_test.go)
// through the public Execute path: adjacencyReadSpy counts the adjacency work
// the read chose, and seedCountStoreSpy records every count-store request it
// made, so both the requested types and the work they predicted are visible.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// seedCountStoreSpy records the count-store requests a read makes and forwards
// them unchanged, so a case sees the request the seed estimate built and the
// answer the real counters gave it.
type seedCountStoreSpy struct {
	storage.Engine
	requests []storage.CountStoreReadRequest
}

// GetInnerEngine is the canonical EngineUnwrapper accessor.
func (s *seedCountStoreSpy) GetInnerEngine() storage.Engine { return s.Engine }

// Namespace preserves the namespace boundary of the observed store.
func (s *seedCountStoreSpy) Namespace() string {
	if scoped, ok := s.Engine.(interface{ Namespace() string }); ok {
		return scoped.Namespace()
	}
	return ""
}

func (s *seedCountStoreSpy) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	s.requests = append(s.requests, request)
	if reader, ok := s.Engine.(storage.CountStoreReadEngine); ok {
		return reader.ReadCountStore(ctx, request)
	}
	return storage.CountStoreRead{}, nil
}

func TestTraversalSeedCostAsksForTheTypesItWalks(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		// measure runs statement through the public executor over a recorder of
		// the fixture's spy, and returns the sorted rows, the adjacency work the
		// read chose, and every count-store request it made.
		measure := func(t *testing.T, fixture *traversalSeedCostFixture, statement string, params map[string]interface{}) ([]string, traversalSeedCostWork, []storage.CountStoreReadRequest) {
			t.Helper()
			counter := &seedCountStoreSpy{Engine: fixture.spy}
			exec := NewStorageExecutor(counter, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			fixture.spy.reset()
			result, err := exec.Execute(fixture.ctx, statement, params)
			require.NoError(t, err, statement)
			return seedRowKeys(result.Rows), traversalSeedCostWork{
				listings: fixture.spy.calls,
				entries:  fixture.spy.entries,
				streams:  fixture.spy.streams,
			}, counter.requests
		}

		// The reported shape, read as a type union: both positional estimates
		// are counted for the pattern's types, in the pattern's order, and the
		// collection's typed adjacency is what the start seed lists.
		t.Run("positional estimates carry the pattern's types", func(t *testing.T) {
			const documents = 12
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            1,
				documents:              documents,
				membershipsPerDocument: 1,
				unrelatedFanout:        64,
			})
			rows, work, requests := measure(t, fixture,
				"MATCH (k:PDCollection)<-[:MEMBER_OF|ARCHIVED]-(d:PDDocument) WHERE d.id IN $ids RETURN d.id, k.archived",
				fixture.idParams())
			require.Equal(t, seedCostRows(fixture.documents, 1), rows)
			require.Equal(t, []storage.CountStoreReadRequest{
				{Kind: storage.CountStoreNodesByLabel, Name: "PDCollection"},
				{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"},
				{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF", "ARCHIVED"}},
				{Kind: storage.CountStoreRelationshipsByEndLabel, Name: "PDCollection", Types: []string{"MEMBER_OF", "ARCHIVED"}},
			}, requests)
			require.Equal(t, 0, work.streams)
			require.LessOrEqual(t, work.listings, 2,
				"adjacency listings: the single collection's MEMBER_OF list, not one listing per indexed document")
			require.Equal(t, documents, work.entries,
				"adjacency entries: the collection's %d MEMBER_OF relationships", documents)
		})

		// Typed work makes the two indexed documents' one-hop adjacency (2
		// listings, 2 entries) cheaper than a scan of the six collections (6
		// listings). The every-type estimate counts the documents' 64 unrelated
		// relationships each and starts from the collections instead, so the
		// chosen work follows the typed counters.
		t.Run("typed adjacency chooses the cheaper typed end walk", func(t *testing.T) {
			const collections = 6
			const documents = 2
			const unrelatedFanout = 64
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            collections,
				documents:              documents,
				membershipsPerDocument: 1,
				unrelatedFanout:        unrelatedFanout,
			})
			rows, work, requests := measure(t, fixture, traversalSeedCostStatement, fixture.idParams())
			require.Equal(t, seedCostRows(fixture.documents, 1), rows)
			require.Equal(t, []storage.CountStoreReadRequest{
				{Kind: storage.CountStoreNodesByLabel, Name: "PDCollection"},
				{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"},
				{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF"}},
			}, requests,
				"the typed end walk is cheaper than the collection scan, so no start-side estimate is asked for")
			require.Equal(t, 0, work.streams)
			require.Equal(t, documents, work.entries,
				"adjacency entries: each document's %d MEMBER_OF relationships, not its %d unrelated ones", 1, unrelatedFanout)
			require.LessOrEqual(t, work.listings, documents+2)
			require.Less(t, work.listings, collections,
				"adjacency listings: the two documents' typed adjacency, not one listing per collection")
		})

		// The opposite case keeps its answer: many collections, a tiny indexed
		// end. The end walk is cheaper whatever its types, so the end seed is
		// kept and its typed degree is the recorded work.
		t.Run("many collections keep the tiny typed end", func(t *testing.T) {
			const documents = 2
			const membershipsPerDocument = 3
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            200,
				documents:              documents,
				membershipsPerDocument: membershipsPerDocument,
				unrelatedFanout:        1,
			})
			rows, work, requests := measure(t, fixture,
				"MATCH (k:PDCollection)<-[:MEMBER_OF]-(d:PDDocument) WHERE d.id IN $ids RETURN d.id, k.archived",
				fixture.idParams())
			require.Equal(t, seedCostRows(fixture.documents, membershipsPerDocument), rows)
			require.Equal(t, []storage.CountStoreReadRequest{
				{Kind: storage.CountStoreNodesByLabel, Name: "PDCollection"},
				{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"},
				{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument", Types: []string{"MEMBER_OF"}},
			}, requests)
			require.Equal(t, 0, work.streams)
			require.Equal(t, documents*membershipsPerDocument, work.entries,
				"adjacency entries: the documents' typed MEMBER_OF degree")
			require.LessOrEqual(t, work.listings, documents+2,
				"adjacency listings: the %d indexed documents, not the 200 collections", documents)
		})

		// An untyped pattern names no type: the estimate keeps asking for every
		// type the endpoint label holds, exactly as the untyped read lists them.
		t.Run("an untyped pattern asks for every type", func(t *testing.T) {
			const documents = 12
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            1,
				documents:              documents,
				membershipsPerDocument: 1,
				unrelatedFanout:        64,
			})
			rows, work, requests := measure(t, fixture,
				"MATCH (k:PDCollection)<--(d:PDDocument) WHERE d.id IN $ids RETURN d.id, k.archived",
				fixture.idParams())
			require.Equal(t, seedCostRows(fixture.documents, 1), rows)
			require.Equal(t, []storage.CountStoreReadRequest{
				{Kind: storage.CountStoreNodesByLabel, Name: "PDCollection"},
				{Kind: storage.CountStoreNodesByLabel, Name: "PDDocument"},
				{Kind: storage.CountStoreRelationshipsByStartLabel, Name: "PDDocument"},
				{Kind: storage.CountStoreRelationshipsByEndLabel, Name: "PDCollection"},
			}, requests)
			require.Equal(t, 0, work.streams)
			require.LessOrEqual(t, work.listings, 2)
			require.Equal(t, documents, work.entries)
		})

		// A relationship the WHERE pins by elementId is the only relationship
		// the pattern walks, and it is walked from its own endpoints: the end
		// seed choice never runs, so it asks for no statistics and lists no
		// adjacency.
		t.Run("a pinned relationship read asks for no statistics", func(t *testing.T) {
			fixture := newTraversalSeedCostFixture(t, traversalSeedCostPopulation{
				collections:            1,
				documents:              12,
				membershipsPerDocument: 1,
				unrelatedFanout:        64,
			})
			pinned := singleQueryValue[string](t, fixture.exec,
				"MATCH (:PDCollection {id: 'col-0'})<-[r:MEMBER_OF]-(:PDDocument {id: 'doc-0'}) RETURN elementId(r)")
			rows, work, requests := measure(t, fixture,
				"MATCH (k:PDCollection)<-[r:MEMBER_OF]-(d:PDDocument) WHERE elementId(r) = $e RETURN d.id, k.archived",
				map[string]interface{}{"e": pinned})
			require.Equal(t, []string{"doc-0false"}, rows)
			require.Empty(t, requests, "a pinned relationship walk reads no statistics")
			require.Zero(t, work.entries)
			require.Zero(t, work.streams)
		})
	})
}
