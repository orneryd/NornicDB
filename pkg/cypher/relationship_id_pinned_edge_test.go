package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// adjacencyReadSpy counts every relationship listing a query makes
// through the public executor: per-node adjacency (headers or full bodies) and
// whole-type edge streams. calls/entries are the work a one-hop match spends
// listing a start node's adjacency instead of walking one pinned relationship.
type adjacencyReadSpy struct {
	storage.Engine
	calls   int
	entries int
	streams int
	fail    bool
}

// ReadCountStore preserves the real planning statistics while observing adjacency.
func (s *adjacencyReadSpy) ReadCountStore(ctx context.Context, request storage.CountStoreReadRequest) (storage.CountStoreRead, error) {
	if reader, ok := s.Engine.(storage.CountStoreReadEngine); ok {
		return reader.ReadCountStore(ctx, request)
	}
	return storage.CountStoreRead{}, nil
}

// Namespace keeps the observer transparent to transaction counter eligibility.
func (s *adjacencyReadSpy) Namespace() string {
	if scoped, ok := s.Engine.(interface{ Namespace() string }); ok {
		return scoped.Namespace()
	}
	return ""
}

// GetInnerEngine preserves namespace and source identity while observing reads.
func (s *adjacencyReadSpy) GetInnerEngine() storage.Engine { return s.Engine }

func (s *adjacencyReadSpy) count(edges []*storage.Edge) {
	s.calls++
	s.entries += len(edges)
}

func (s *adjacencyReadSpy) countStream() {
	s.streams++
}

func (s *adjacencyReadSpy) reset() {
	s.calls, s.entries, s.streams = 0, 0, 0
}

// ReadAdjacentEdges counts every relationship listing a query makes: one call
// per adjacent-edge request and one entry per relationship it lists.
func (s *adjacencyReadSpy) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if s.fail {
		return nil, errEdgeRead
	}
	edges, err := s.Engine.ReadAdjacentEdges(ctx, request)
	if err == nil {
		s.count(edges)
	}
	return edges, err
}

func (s *adjacencyReadSpy) StreamEdgesByType(ctx context.Context, kind string, visit func(*storage.Edge) error) error {
	s.countStream()
	return storage.StreamEdgesByType(ctx, s.Engine, kind, visit)
}

// pinnedEdgeFixture is a graph whose pinned relationship starts at a hub with
// a wide adjacency: the pinned edge is one of the hub's 300 SPOKE
// relationships, so listing the hub's adjacency lists 300 relationships while
// walking the pinned edge lists none.
type pinnedEdgeFixture struct {
	spy    *adjacencyReadSpy
	exec   *StorageExecutor
	writer *StorageExecutor

	spokeID       string
	spokeNativeID interface{}
	loopID        string
}

func newPinnedEdgeFixture(t *testing.T) *pinnedEdgeFixture {
	t.Helper()
	namespaced := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	spy := &adjacencyReadSpy{Engine: namespaced}
	writer := NewStorageExecutor(namespaced)
	ctx := context.Background()
	_, err := writer.Execute(ctx, `
		CREATE (a:X {id: 1})-[:R {id: 1}]->(b:Y {id: 1})
		CREATE (hub:X {id: 2})
		CREATE (loop:L {id: 1})-[:LOOP]->(loop)
	`, nil)
	require.NoError(t, err)
	_, err = writer.Execute(ctx, "MATCH (hub:X {id: 2}) UNWIND range(0, 299) AS i CREATE (hub)-[:SPOKE {i: i}]->(:Z {id: i})", nil)
	require.NoError(t, err)

	exec := NewStorageExecutor(spy)
	fixture := &pinnedEdgeFixture{spy: spy, exec: exec, writer: writer}
	fixture.spokeID = singleQueryValue[string](t, exec, "MATCH (:X {id: 2})-[r:SPOKE]->(z:Z {id: 7}) RETURN elementId(r)")
	fixture.spokeNativeID = rowsOf(t, exec, "MATCH (:X {id: 2})-[r:SPOKE]->(z:Z {id: 7}) RETURN id(r)", nil)[0][0]
	fixture.loopID = singleQueryValue[string](t, exec, "MATCH (loop:L {id: 1})-[r:LOOP]->(again) RETURN elementId(r)")
	return fixture
}

// A one-hop match whose relationship is pinned by elementId(r) or id(r) walks
// that relationship from its own endpoints, and answers what the general,
// adjacency-listing path answers (the same WHERE with an extra true conjunct
// keeps the pinned lookup out of it). It must not list a start node's
// adjacency: the pinned hub edge is one of 300 SPOKE relationships, so a single
// adjacency listing is 300 entries of avoidable work.
func TestRelationshipIDPinnedEdgeWalksOnlyThatRelationship(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	ctx := context.Background()
	params := map[string]interface{}{"e": fixture.spokeID, "id": fixture.spokeNativeID}

	for _, shape := range []struct {
		name    string
		pattern string
		clause  string
		tail    string
		want    [][]interface{}
	}{
		{"outgoing parameter", "(a)-[r]->(b)", "elementId(r) = $e", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"outgoing literal", "(a)-[r]->(b)", fmt.Sprintf("elementId(r) = '%s'", fixture.spokeID), "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"native id", "(a)-[r]->(b)", "id(r) = $id", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"incoming parameter", "(b)<-[r]-(a)", "elementId(r) = $e", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"undirected parameter", "(a)-[r]-(b)", "elementId(r) = $e", "RETURN labels(a)[0], labels(b)[0] ORDER BY labels(a)[0]", [][]interface{}{{"X", "Z"}, {"Z", "X"}}},
		{"typed parameter", "(a)-[r:SPOKE]->(b)", "elementId(r) = $e", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"labelled endpoints", "(a:X)-[r]->(b:Z)", "elementId(r) = $e", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"inline node properties", "(a:X {id: 2})-[r]->(b:Z {id: 7})", "elementId(r) = $e", "RETURN a.id, b.id", [][]interface{}{{int64(2), int64(7)}}},
		{"path variable", "p = (a)-[r]->(b)", "elementId(r) = $e", "RETURN length(p), a.id, b.id", [][]interface{}{{int64(1), int64(2), int64(7)}}},
		{"relationship properties", "(a)-[r]->(b)", "elementId(r) = $e", "RETURN r.i", [][]interface{}{{int64(7)}}},
	} {
		pinned := fmt.Sprintf("MATCH %s WHERE %s %s", shape.pattern, shape.clause, shape.tail)
		fixture.spy.reset()
		result, err := fixture.exec.Execute(ctx, pinned, params)
		require.NoError(t, err, shape.name)
		require.Equal(t, shape.want, result.Rows, shape.name)
		require.Zero(t, fixture.spy.calls, "%s: adjacency listings %q", shape.name, pinned)
		require.Zero(t, fixture.spy.entries, "%s: adjacency entries %q", shape.name, pinned)
		require.Zero(t, fixture.spy.streams, "%s: whole-type edge streams %q", shape.name, pinned)

		general := fmt.Sprintf("MATCH %s WHERE %s AND true %s", shape.pattern, shape.clause, shape.tail)
		require.Equal(t, rowsOf(t, fixture.exec, general, params), result.Rows,
			"%s: the general path answers the same rows", shape.name)
	}
}

// Missing, foreign-database and wrong-kind IDs match nothing. The pinned
// lookup and general predicate must agree without listing adjacency.
func TestRelationshipIDPinnedEdgeUnresolvedMatchesNothing(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	ctx := context.Background()
	_, _, local, ok := storage.ParseElementID(fixture.spokeID)
	require.True(t, ok)

	deletedID := singleQueryValue[string](t, fixture.exec, "MATCH (:X {id: 1})-[r:R]->() RETURN elementId(r)")
	_, err := fixture.writer.Execute(ctx, "MATCH (:X {id: 1})-[r:R]->() DELETE r", nil)
	require.NoError(t, err)
	nodeID := singleQueryValue[string](t, fixture.exec, "MATCH (a:X {id: 1}) RETURN elementId(a)")

	for _, test := range []struct {
		name  string
		value interface{}
	}{
		{"missing local id", "5:test:no-such-relationship"},
		{"deleted relationship", deletedID},
		{"foreign database", "5:otherdb:" + local},
		{"malformed value", "bogus"},
		{"node element id", nodeID},
		{"null value", nil},
	} {
		params := map[string]interface{}{"e": test.value}
		// The general path's answer: an extra true conjunct keeps the pinned
		// lookup out of it, so the pattern is filtered by WHERE alone.
		general := rowsOf(t, fixture.exec,
			"MATCH (a)-[r]->(b) WHERE elementId(r) = $e AND true RETURN count(r)", params)
		fixture.spy.reset()
		pinned := rowsOf(t, fixture.exec,
			"MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN count(r)", params)
		require.Equal(t, general, pinned, test.name)
		require.Zero(t, fixture.spy.calls, "%s: adjacency listings", test.name)
		require.Zero(t, fixture.spy.entries, "%s: adjacency entries", test.name)
		require.Equal(t, [][]interface{}{{int64(0)}}, pinned, test.name)
	}
}

// An UNWIND row map supplies the pinned id per row: the relationship is read
// per row and its adjacency is never listed, and another relationship type is
// rejected before any adjacency read. A bound endpoint keeps its own seed
// owner and its answer.
func TestRelationshipIDPinnedEdgeFromUnwindRowMap(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	rows := []interface{}{
		map[string]interface{}{"name": "spoke", "edge": fixture.spokeID},
		map[string]interface{}{"name": "missing", "edge": "5:test:no-such-relationship"},
		map[string]interface{}{"name": "loop", "edge": fixture.loopID},
	}
	fixture.spy.reset()
	result, err := fixture.exec.Execute(context.Background(),
		"UNWIND $rows AS row MATCH (a)-[r]->(b) WHERE elementId(r) = row.edge RETURN row.name, a.id, b.id ORDER BY row.name",
		map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"loop", int64(1), int64(1)}, {"spoke", int64(2), int64(7)}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "row-map pinned ids must not list adjacency")
	require.Zero(t, fixture.spy.entries)

	fixture.spy.reset()
	result, err = fixture.exec.Execute(context.Background(),
		"UNWIND $rows AS row MATCH ()-[r:OTHER]->() WHERE elementId(r) = row.edge RETURN count(*)",
		map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "another relationship type is rejected before adjacency")

	// A row that binds the pattern's start node keeps that endpoint's seed.
	result, err = fixture.exec.Execute(context.Background(),
		"UNWIND [{id: 2, edge: $e}] AS row MATCH (a:X {id: row.id})-[r]->(b) WHERE elementId(r) = row.edge RETURN a.id, b.id",
		map[string]interface{}{"e": fixture.spokeID})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(7)}}, result.Rows)
}

// Shapes the pinned lookup does not own keep their existing owner and answers:
// IN and OR are not a single equality, so they fall back to the pattern owners
// and list adjacency. Answers are the graph's: the hub's pinned row, the two
// relationships leaving an id-1 node (R and the LOOP).
func TestRelationshipIDPinnedEdgeFallbackShapes(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	ctx := context.Background()
	params := map[string]interface{}{"e": fixture.spokeID, "rows": []interface{}{fixture.spokeID}}

	for query, want := range map[string][][]interface{}{
		"MATCH (a)-[r]->(b) WHERE elementId(r) IN $rows RETURN a.id, b.id":           {{int64(2), int64(7)}},
		"MATCH (a)-[r]->(b) WHERE elementId(r) = $e OR a.id = 1 RETURN count(*)":     {{int64(3)}},
		"MATCH (a)-[r]->(b) WHERE elementId(r) = $e AND b.id = 7 RETURN count(*)":    {{int64(1)}},
		"MATCH (a)-[r]->(b) WHERE elementId(r) = $e AND b.id = 8 RETURN count(*)":    {{int64(0)}},
		"MATCH (a)-[r]->(b) WHERE elementId(r) IN $rows OR a.id = 1 RETURN count(*)": {{int64(3)}},
	} {
		result, err := fixture.exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}

	// IN and OR cannot use the pinned lookup, so they keep listing adjacency.
	fixture.spy.reset()
	_, err := fixture.exec.Execute(ctx, "MATCH (a)-[r]->(b) WHERE elementId(r) IN $rows RETURN count(*)", params)
	require.NoError(t, err)
	require.NotZero(t, fixture.spy.calls, "the IN fallback keeps its own access path")
}

// Undirected, self-loop and OPTIONAL MATCH shapes keep the traversal's own
// answer while walking only the pinned relationship.
func TestRelationshipIDPinnedEdgeUndirectedAndOptional(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	ctx := context.Background()
	params := map[string]interface{}{"e": fixture.spokeID, "loop": fixture.loopID, "missing": "5:test:no-such"}

	fixture.spy.reset()
	result, err := fixture.exec.Execute(ctx,
		"MATCH (a)-[r]-(b) WHERE elementId(r) = $e RETURN a.id, b.id ORDER BY a.id", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(7)}, {int64(7), int64(2)}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "an undirected pinned match lists no adjacency")

	fixture.spy.reset()
	result, err = fixture.exec.Execute(ctx,
		"MATCH (a)-[r:LOOP]-(b) WHERE elementId(r) = $loop RETURN a.id, b.id", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "a pinned self-loop lists no adjacency")

	fixture.spy.reset()
	result, err = fixture.exec.Execute(ctx,
		"UNWIND [1, 2] AS seed OPTIONAL MATCH (a)-[r]->(b) WHERE elementId(r) = $missing RETURN seed, r IS NULL ORDER BY seed", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), true}, {int64(2), true}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "an unmatched optional pin lists no adjacency")

	fixture.spy.reset()
	result, err = fixture.exec.Execute(ctx,
		"UNWIND [1, 2] AS seed OPTIONAL MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN seed, r IS NULL ORDER BY seed", params)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), false}, {int64(2), false}}, result.Rows)
	require.Zero(t, fixture.spy.calls, "a matched optional pin lists no adjacency")
}

// An element id names its database: a constituent's id selects that
// constituent's relationship and an id naming another database matches none of
// this one's. The pinned lookup answers what the general path answers, and the
// IN fallback (which never pins) agrees.
func TestRelationshipIDPinnedEdgeResolvesElementIDDatabase(t *testing.T) {
	exec, _ := newCompositeIdentityExecutor(t, false)
	ctx := context.Background()
	for id, want := range map[string][][]interface{}{
		"5:one:same-rel":     {{int64(1)}},
		"5:two:same-rel":     {{int64(1)}},
		"5:otherdb:same-rel": {{int64(0)}},
		"5:otherdb:no-such":  {{int64(0)}},
	} {
		for _, query := range []string{
			"USE comp MATCH (n)-[r]->(m) WHERE elementId(r) = '%s' RETURN count(r)",
			"USE comp MATCH (n)-[r]->(m) WHERE elementId(r) = '%s' AND true RETURN count(r)",
		} {
			result, err := exec.Execute(ctx, fmt.Sprintf(query, id), nil)
			require.NoError(t, err, query)
			require.Equal(t, want, result.Rows, query, id)
		}
	}
	result, err := exec.Execute(ctx,
		"USE comp MATCH (n)-[r]->(m) WHERE elementId(r) IN ['5:otherdb:same-rel'] RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "the IN fallback agrees")
}

// The pinned relationship resolves through the transaction's local writes and
// still lists no adjacency.
func TestRelationshipIDPinnedEdgeUsesTransactionLocalWrites(t *testing.T) {
	base := newTestMemoryEngine(t)
	namespaced := storage.NewNamespacedEngine(base, "pinned")
	ctx := context.Background()

	tx, err := base.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	txStore := &transactionStorageWrapper{tx: tx, underlying: namespaced, namespace: "pinned", separator: ":"}

	_, err = txStore.CreateNode(ctx, &storage.Node{ID: "pinned:x", Labels: []string{"X"}, Properties: map[string]interface{}{"id": int64(2)}})
	require.NoError(t, err)
	for index := 0; index < 100; index++ {
		zID, createErr := txStore.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("pinned:z-%d", index)),
			Labels:     []string{"Z"},
			Properties: map[string]interface{}{"id": int64(index)},
		})
		require.NoError(t, createErr)
		require.NoError(t, txStore.CreateEdge(ctx, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("pinned:spoke-%d", index)),
			StartNode: "pinned:x",
			EndNode:   zID,
			Type:      "SPOKE",
		}))
	}

	txSpy := &adjacencyReadSpy{Engine: txStore}
	txExec := NewStorageExecutor(txSpy)
	edgeID := singleQueryValue[string](t, txExec, "MATCH (:X {id: 2})-[r:SPOKE]->(z:Z {id: 3}) RETURN elementId(r)")

	txSpy.reset()
	result, err := txExec.Execute(ctx, "MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN a.id, b.id",
		map[string]interface{}{"e": edgeID})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3)}}, result.Rows)
	require.Zero(t, txSpy.calls, "a transaction's pinned relationship lists no adjacency")
	require.Zero(t, txSpy.entries)

	// The committed reader does not see the uncommitted relationship.
	readerSpy := &adjacencyReadSpy{Engine: namespaced}
	reader := NewStorageExecutor(readerSpy)
	result, err = reader.Execute(ctx, "MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN count(r)",
		map[string]interface{}{"e": edgeID})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	require.Zero(t, readerSpy.calls)
}

// BenchmarkRelationshipIDPinnedEdgeTraversal measures a pinned one-hop match
// against a hub of 5,000 relationships. Run it on its own (the implementation
// must already be in place): the pinned relationship is walked from its own
// endpoints, so the hub's adjacency must not be listed.
func BenchmarkRelationshipIDPinnedEdgeTraversal(b *testing.B) {
	namespaced := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
	writer := NewStorageExecutor(namespaced)
	ctx := context.Background()
	if _, err := writer.Execute(ctx, "CREATE (hub:X {id: 1})", nil); err != nil {
		b.Fatal(err)
	}
	if _, err := writer.Execute(ctx, "MATCH (hub:X {id: 1}) UNWIND range(0, 4999) AS i CREATE (hub)-[:SPOKE {i: i}]->(:Z {id: i})", nil); err != nil {
		b.Fatal(err)
	}
	spy := &adjacencyReadSpy{Engine: namespaced}
	exec := NewStorageExecutor(spy)
	result, err := exec.Execute(ctx, "MATCH (:X {id: 1})-[r:SPOKE]->(z:Z {id: 12}) RETURN elementId(r)", nil)
	if err != nil {
		b.Fatal(err)
	}
	if len(result.Rows) != 1 {
		b.Fatalf("pinned relationship not found: %v", result.Rows)
	}
	params := map[string]interface{}{"e": result.Rows[0][0]}
	spy.reset()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "MATCH (a)-[r]->(b) WHERE elementId(r) = $e RETURN a.id, b.id", params); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if spy.calls != 0 || spy.entries != 0 {
		b.Fatalf("pinned lookup listed adjacency: %d calls, %d entries", spy.calls, spy.entries)
	}
}

func TestRelationshipIDPinRetainsBoundEndpointAndConjunct(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	for _, tc := range []struct {
		query string
		want  int64
	}{
		{"MATCH (a:X {id:2}) WITH a MATCH (a)-[r]->(b) WHERE elementId(r)=$e RETURN count(r)", 1},
		{"MATCH (a:X {id:1}) WITH a MATCH (a)-[r]->(b) WHERE elementId(r)=$e RETURN count(r)", 0},
		{"MATCH (b:Z {id:7}) WITH b MATCH (a)-[r]->(b) WHERE elementId(r)=$e RETURN count(r)", 1},
		{"UNWIND $rows AS row MATCH (a)-[r]->(b) WHERE row.gate=1 AND elementId(r)=row.edge RETURN count(r)", 1},
	} {
		fixture.spy.reset()
		result, err := fixture.exec.Execute(t.Context(), tc.query, map[string]interface{}{"e": fixture.spokeID, "rows": []interface{}{map[string]interface{}{"gate": int64(1), "edge": fixture.spokeID}}})
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
		require.Zero(t, fixture.spy.calls, tc.query)
		require.Zero(t, fixture.spy.entries, tc.query)
	}
	exec, _ := newCompositeIdentityExecutor(t, false)
	result, err := exec.Execute(t.Context(), "USE comp MATCH (n)-[r]->(m) WHERE elementId(r)='5:one:same-rel' RETURN n.db", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"one"}}, result.Rows)
}

func TestRelationshipIDPinIsRequestOwned(t *testing.T) {
	fixture := newPinnedEdgeFixture(t)
	exec := NewStorageExecutor(fixture.writer.storage)
	// Observing adjacency must not change the database identity emitted by
	// the wrapped store or make the result unusable by an unwrapped executor.
	require.Equal(t, fixture.spokeID, singleQueryValue[string](t, exec, "MATCH (:X {id: 2})-[r:SPOKE]->(z:Z {id: 7}) RETURN elementId(r)"))
	require.Equal(t, fixture.loopID, singleQueryValue[string](t, exec, "MATCH (loop:L {id: 1})-[r:LOOP]->(again) RETURN elementId(r)"))
	failures := make(chan error, 2)
	var workers sync.WaitGroup
	for _, tc := range []struct {
		id            string
		first, second int64
	}{{fixture.spokeID, 2, 7}, {fixture.loopID, 1, 1}} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 40; i++ {
				result, err := exec.Execute(t.Context(), "MATCH (a)-[r]->(b) WHERE elementId(r)=$edge RETURN a.id,b.id", map[string]interface{}{"edge": tc.id})
				if err != nil {
					failures <- err
					return
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 2 || result.Rows[0][0] != tc.first || result.Rows[0][1] != tc.second {
					failures <- fmt.Errorf("pin %q returned %v", tc.id, result.Rows)
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}
