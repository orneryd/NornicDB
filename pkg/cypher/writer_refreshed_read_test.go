package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-213 refreshed read images, public consumer regressions: a no-op write
// that takes an entity's write lock after a peer committed it must read that
// entity as last committed through every query route that returns it, exactly
// as its point reads do, without staging a write and without advancing the
// rest of the pinned snapshot.
func TestWriterNoOpRetainsRefreshedImageForCollectionReads(t *testing.T) {
	for _, shape := range []struct {
		name string
		seed []string
		peer string
		noop string
		read []string
	}{
		{
			name: "node",
			seed: []string{"CREATE (:Doc {id:'n', value:1, peer:1})"},
			peer: "MATCH (n:Doc) SET n.peer=2, n:New",
			noop: "MATCH (n:Doc) SET n.value=1",
			read: []string{
				"MATCH (n:Doc) RETURN n.peer",
				"MATCH (n) RETURN n.peer",
				"MATCH (n:Doc {peer:2}) RETURN n.peer",
				"MATCH (n:New) RETURN n.peer",
				"MATCH (n:Doc {id:'n'}) RETURN n.peer",
			},
		},
		{
			name: "relationship",
			seed: []string{"CREATE (:Doc)-[:HAS {value:1, peer:1}]->(:Part)"},
			peer: "MATCH ()-[r:HAS]->() SET r.peer=2",
			noop: "MATCH ()-[r:HAS]->() SET r.value=1",
			read: []string{
				"MATCH ()-[r:HAS]->() RETURN r.peer",
				"MATCH ()-[r]->() RETURN r.peer",
				"MATCH (:Doc)-[r]->() RETURN r.peer",
				"MATCH (:Part)<-[r]-(:Doc) RETURN r.peer",
				"MATCH (:Doc)-[r]-(:Part) RETURN r.peer",
				"MATCH (:Doc)-[r:HAS]->(:Part) RETURN r.peer",
			},
		},
	} {
		t.Run(shape.name, func(t *testing.T) {
			ctx := context.Background()
			store, setup := newMergeRaceStore(t)
			runRefreshedReadStatements(t, setup, ctx, shape.seed...)
			writer := NewStorageExecutor(store)
			_, err := writer.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer writer.Execute(ctx, "ROLLBACK", nil)
			// Prime every population/index before the peer changes the target.
			runRefreshedReadStatements(t, writer, ctx, shape.read...)
			runRefreshedReadStatements(t, setup, ctx, shape.peer)
			runRefreshedReadStatements(t, writer, ctx, shape.noop)
			native := writer.txContext.tx.(*storage.BadgerTransaction)
			require.Zero(t, native.OperationCount(), "refreshing a no-op must not manufacture a write")
			for _, query := range shape.read {
				require.Equal(t, [][]interface{}{{int64(2)}}, refreshedReadRows(t, writer, ctx, query), query)
			}
			require.Zero(t, native.OperationCount(), "read composition must not stage mutations")
		})
	}
}

// TestWriterRefreshedReadsAgreeWithPointReadsAndKeepTheSnapshotPinned covers
// the remaining public read routes (full label and all-node streams, property
// filters, counts, equality-cache MERGEs, type/all/adjacency/between edge
// reads) and proves that only the entities the writer locked advance while
// unrelated committed entities stay at the transaction's snapshot.
func TestWriterRefreshedReadsAgreeWithPointReadsAndKeepTheSnapshotPinned(t *testing.T) {
	ctx := context.Background()
	store, setup := newMergeRaceStore(t)
	runRefreshedReadStatements(t, setup, ctx,
		"CREATE (:Doc {id:'n', value:1, peer:1})",
		"CREATE (:Doc {id:'peer', value:1})",
		"CREATE (:Doc {id:'bystander', value:1, peer:1})",
		"CREATE (:Part {id:'p'})",
		"CREATE (:Part {id:'p2'})",
		"MATCH (a:Doc {id:'n'}), (b:Part {id:'p'}) CREATE (a)-[:HAS {value:1, peer:1}]->(b)",
		"MATCH (a:Doc {id:'n'}), (b:Part {id:'p2'}) CREATE (a)-[:LINK {value:1, peer:1}]->(b)",
	)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer writer.Execute(ctx, "ROLLBACK", nil)

	warm := []string{
		"MATCH (n:Doc) RETURN n.id, n.peer ORDER BY n.id",
		"MATCH (n:Doc) RETURN n",
		"MATCH (n) RETURN n.id, n.peer ORDER BY n.id",
		"MATCH (n) RETURN n",
		"MATCH (n:Doc {peer:1}) RETURN n.id ORDER BY n.id",
		"MATCH (n:New) RETURN n.id",
		"MATCH (u:Doc {id:'peer'}) RETURN u.value",
		"MATCH (c:Changed) RETURN c.id",
		"MATCH ()-[r:HAS]->() RETURN r.peer",
		"MATCH ()-[r]->() RETURN type(r), r.peer ORDER BY type(r)",
		"MATCH (:Doc {id:'n'})-[r]->() RETURN type(r), r.peer ORDER BY type(r)",
		"MATCH (:Part {id:'p'})<-[r]-(:Doc) RETURN r.peer",
		"MATCH (:Doc {id:'n'})-[r]-(:Part {id:'p'}) RETURN r.peer",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) RETURN r.peer",
		"MATCH (n:Doc) RETURN count(n)",
		"MATCH (n) RETURN count(n)",
	}
	runRefreshedReadStatements(t, writer, ctx, warm...)
	require.Equal(t, [][]interface{}{{"HAS", int64(1)}, {"LINK", int64(1)}},
		refreshedReadRows(t, writer, ctx, "MATCH ()-[r]->() RETURN type(r), r.peer ORDER BY type(r)"))

	runRefreshedReadStatements(t, setup, ctx,
		"MATCH (n:Doc {id:'n'}) SET n.peer=2, n.fresh=7, n:New",
		"MATCH (u:Doc {id:'peer'}) SET u.peer=9, u:Changed",
		"MATCH (b:Doc {id:'bystander'}) SET b.peer=9",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) SET r.peer=2, r.fresh=7",
		"MATCH (:Doc {id:'n'})-[r:LINK]->(:Part {id:'p2'}) SET r.peer=9",
		"CREATE (:Doc {id:'added', peer:2, fresh:2})",
	)
	runRefreshedReadStatements(t, writer, ctx,
		"MATCH (n:Doc {id:'n'}) SET n.value=1",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) SET r.value=1",
	)
	native := writer.txContext.tx.(*storage.BadgerTransaction)
	require.Zero(t, native.OperationCount(), "refreshing a no-op must not manufacture a write")

	t.Run("point reads agree with the collections", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{int64(2), int64(7)}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {id:'n'}) RETURN n.peer, n.fresh"))
		require.Equal(t, [][]interface{}{{int64(2), int64(7)}},
			refreshedReadRows(t, writer, ctx, "MATCH ()-[r:HAS]->() RETURN r.peer, r.fresh"))
	})

	t.Run("node label and all-node collections", func(t *testing.T) {
		require.Equal(t, [][]interface{}{
			{"bystander", int64(1)},
			{"n", int64(2)},
			{"peer", nil},
		}, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc) RETURN n.id, n.peer ORDER BY n.id"))
		require.Equal(t, [][]interface{}{
			{"bystander", int64(1)},
			{"n", int64(2)},
			{"p", nil},
			{"p2", nil},
			{"peer", nil},
		}, refreshedReadRows(t, writer, ctx, "MATCH (n) RETURN n.id, n.peer ORDER BY n.id"))

		labelNodes := refreshedReadNodes(t, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc) RETURN n"))
		require.Len(t, labelNodes, 3)
		require.Equal(t, int64(2), labelNodes["n"].Properties["peer"])
		require.Equal(t, int64(7), labelNodes["n"].Properties["fresh"])
		require.Contains(t, labelNodes["n"].Labels, "New")
		require.Equal(t, int64(1), labelNodes["bystander"].Properties["peer"])
		require.Equal(t, int64(1), labelNodes["peer"].Properties["value"])
		require.NotContains(t, labelNodes, "added")

		allNodes := refreshedReadNodes(t, refreshedReadRows(t, writer, ctx, "MATCH (n) RETURN n"))
		require.Len(t, allNodes, 5)
		require.Equal(t, int64(2), allNodes["n"].Properties["peer"])
		require.NotContains(t, allNodes, "added")
	})

	t.Run("property filters and the equality cache", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {peer:2}) RETURN n.id"))
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {fresh:7}) RETURN n.id"))
		require.Equal(t, [][]interface{}{{"bystander"}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {peer:1}) RETURN n.id"))
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:New) RETURN n.id"))
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MATCH (n:New {fresh:7}) RETURN n.id"))
		require.Empty(t, refreshedReadRows(t, writer, ctx, "MATCH (c:Changed) RETURN c.id"))

		// MERGE reads the transaction's complete equality population: the
		// changed and new values match the refreshed image, never a duplicate.
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MERGE (n:Doc {peer:2}) RETURN n.id"))
		require.Equal(t, [][]interface{}{{"n"}},
			refreshedReadRows(t, writer, ctx, "MERGE (n:New {fresh:7}) RETURN n.id"))
		require.Zero(t, native.OperationCount(), "matched MERGEs must not manufacture writes")
	})

	t.Run("unrelated entities stay pinned", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{nil}},
			refreshedReadRows(t, writer, ctx, "MATCH (u:Doc {id:'peer'}) RETURN u.peer"))
		require.Equal(t, [][]interface{}{{int64(1)}},
			refreshedReadRows(t, writer, ctx, "MATCH (b:Doc {id:'bystander'}) RETURN b.peer"))
		require.Empty(t, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {fresh:2}) RETURN n.id"),
			"a node committed after the snapshot is not part of it")
	})

	t.Run("relationship collections and adjacency", func(t *testing.T) {
		require.Equal(t, [][]interface{}{{"HAS", int64(2)}, {"LINK", int64(1)}},
			refreshedReadRows(t, writer, ctx, "MATCH ()-[r]->() RETURN type(r), r.peer ORDER BY type(r)"))
		require.Equal(t, [][]interface{}{{int64(2)}},
			refreshedReadRows(t, writer, ctx, "MATCH ()-[r:HAS]->() RETURN r.peer"))
		require.Equal(t, [][]interface{}{{"HAS", int64(2)}, {"LINK", int64(1)}},
			refreshedReadRows(t, writer, ctx, "MATCH (:Doc {id:'n'})-[r]->() RETURN type(r), r.peer ORDER BY type(r)"))
		require.Equal(t, [][]interface{}{{int64(2)}},
			refreshedReadRows(t, writer, ctx, "MATCH (:Part {id:'p'})<-[r]-(:Doc) RETURN r.peer"))
		require.Equal(t, [][]interface{}{{int64(2)}},
			refreshedReadRows(t, writer, ctx, "MATCH (:Doc {id:'n'})-[r]-(:Part {id:'p'}) RETURN r.peer"))
		require.Equal(t, [][]interface{}{{int64(2)}},
			refreshedReadRows(t, writer, ctx, "MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) RETURN r.peer"))
		require.Equal(t, [][]interface{}{{int64(2)}},
			refreshedReadRows(t, writer, ctx,
				"MATCH (a:Doc {id:'n'}), (b:Part {id:'p'}) MERGE (a)-[r:HAS]->(b) RETURN r.peer"),
			"a bound-endpoint relationship MERGE reads the refreshed MatchEdgesBetween image")
		require.Zero(t, native.OperationCount(), "a matched relationship MERGE must not manufacture writes")
	})
}

// TestWriterRefreshedReadsYieldToStagedWritesAndDeletes proves the writer's
// own staged images keep precedence over the refreshed committed images, and
// that its staged deletes remove them from every route.
func TestWriterRefreshedReadsYieldToStagedWritesAndDeletes(t *testing.T) {
	ctx := context.Background()
	store, setup := newMergeRaceStore(t)
	runRefreshedReadStatements(t, setup, ctx,
		"CREATE (:Doc {id:'n', value:1, peer:1})",
		"CREATE (:Doc {id:'doomed', value:1, peer:1})",
		"CREATE (:Doc {id:'peer', value:1})",
		"CREATE (:Part {id:'p'})",
		"CREATE (:Part {id:'p2'})",
		"MATCH (a:Doc {id:'n'}), (b:Part {id:'p'}) CREATE (a)-[:HAS {value:1, peer:1}]->(b)",
		"MATCH (a:Doc {id:'n'}), (b:Part {id:'p2'}) CREATE (a)-[:LINK {value:1, peer:1}]->(b)",
	)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer writer.Execute(ctx, "ROLLBACK", nil)
	runRefreshedReadStatements(t, writer, ctx,
		"MATCH (n:Doc) RETURN n.peer",
		"MATCH (n) RETURN n.peer",
		"MATCH (n:Doc {peer:1}) RETURN n.id",
		"MATCH ()-[r]->() RETURN r.peer",
		"MATCH (:Doc {id:'n'})-[r]->() RETURN r.peer",
	)

	runRefreshedReadStatements(t, setup, ctx,
		"MATCH (n:Doc {id:'n'}) SET n.peer=2, n.fresh=7",
		"MATCH (n:Doc {id:'doomed'}) SET n.peer=8",
		"MATCH (u:Doc {id:'peer'}) SET u.peer=9",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) SET r.peer=2",
		"MATCH (:Doc {id:'n'})-[r:LINK]->(:Part {id:'p2'}) SET r.peer=9",
	)
	// No-op writes take the write locks and refresh each image; the following
	// statements then stage changes and deletes on top of them.
	runRefreshedReadStatements(t, writer, ctx,
		"MATCH (n:Doc {id:'n'}) SET n.value=1",
		"MATCH (n:Doc {id:'doomed'}) SET n.value=1",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) SET r.value=1",
		"MATCH (:Doc {id:'n'})-[r:LINK]->(:Part {id:'p2'}) SET r.value=1",
		"MATCH (n:Doc {id:'n'}) SET n.value=2",
		"MATCH (:Doc {id:'n'})-[r:HAS]->(:Part {id:'p'}) SET r.value=2",
		"MATCH (n:Doc {id:'doomed'}) DETACH DELETE n",
		"MATCH (:Doc {id:'n'})-[r:LINK]->(:Part {id:'p2'}) DELETE r",
	)

	require.Equal(t, [][]interface{}{
		{"n", int64(2), int64(2)},
		{"peer", nil, int64(1)},
	}, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc) RETURN n.id, n.peer, n.value ORDER BY n.id"))
	require.Equal(t, [][]interface{}{{int64(2), int64(2)}},
		refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {id:'n'}) RETURN n.peer, n.value"),
		"the point read returns the same staged image as the collection")
	require.Empty(t, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {id:'doomed'}) RETURN n.id"))
	require.Empty(t, refreshedReadRows(t, writer, ctx, "MATCH (n:Doc {peer:8}) RETURN n.id"),
		"a staged delete removes the refreshed image from the equality population")
	require.Equal(t, [][]interface{}{{"HAS", int64(2), int64(2)}},
		refreshedReadRows(t, writer, ctx, "MATCH ()-[r]->() RETURN type(r), r.peer, r.value"))
	require.Equal(t, [][]interface{}{{"HAS"}},
		refreshedReadRows(t, writer, ctx, "MATCH (:Doc {id:'n'})-[r]->() RETURN type(r)"))
}

// TestWriterRefreshedReadCancellationPropagates proves a canceled collection
// read reports the cancellation through the public query surface rather than
// returning a partial population.
func TestWriterRefreshedReadCancellationPropagates(t *testing.T) {
	ctx := context.Background()
	store, setup := newMergeRaceStore(t)
	runRefreshedReadStatements(t, setup, ctx, "CREATE (:Doc {id:'n', value:1, peer:1})")
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer writer.Execute(ctx, "ROLLBACK", nil)
	require.Equal(t, [][]interface{}{{int64(1)}},
		refreshedReadRows(t, writer, ctx, "MATCH (n:Doc) RETURN n.peer"))
	runRefreshedReadStatements(t, setup, ctx, "MATCH (n:Doc) SET n.peer=2")
	runRefreshedReadStatements(t, writer, ctx, "MATCH (n:Doc) SET n.value=1")

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = writer.Execute(canceled, "MATCH (n:Doc) RETURN n.peer", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), context.Canceled.Error())
}

func runRefreshedReadStatements(t *testing.T, exec *StorageExecutor, ctx context.Context, queries ...string) {
	t.Helper()
	for _, query := range queries {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func refreshedReadRows(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, query)
	return result.Rows
}

func refreshedReadNodes(t *testing.T, rows [][]interface{}) map[string]*storage.Node {
	t.Helper()
	nodes := make(map[string]*storage.Node, len(rows))
	for _, row := range rows {
		require.Len(t, row, 1)
		node, ok := row[0].(*storage.Node)
		require.True(t, ok, "a RETURN node row carries *storage.Node")
		key := string(node.ID)
		if id, ok := node.Properties["id"].(string); ok {
			key = id
		}
		nodes[key] = node
	}
	return nodes
}
