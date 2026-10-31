package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestWriteBehind_IndexedMatchSeesBufferedNodes reproduces monster's Pokec
// batched-load loss: with write-behind on, UNWIND batches of friendships
// that MATCH their endpoints through a property index silently produced
// nothing while the user writes were still buffered. The index seek must
// merge acknowledged-but-unflushed nodes.
func TestWriteBehind_IndexedMatchSeesBufferedNodes(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour, // nothing flushes during the test
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX user_id IF NOT EXISTS FOR (n:User) ON (n.id)", nil)
	require.NoError(t, err)

	const users = 50
	rows := make([]map[string]any, 0, users)
	for i := 1; i <= users; i++ {
		rows = append(rows, map[string]any{"id": int64(i)})
	}
	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:User {id: r.id})", map[string]any{"rows": rows})
	require.NoError(t, err)

	// Nothing has flushed: the friendships below must find their endpoints
	// through the User(id) index merged with the buffer.
	pairs := make([][]int64, 0, users-1)
	for i := 1; i < users; i++ {
		pairs = append(pairs, []int64{int64(i), int64(i + 1)})
	}
	_, err = exec.Execute(ctx,
		"UNWIND $pairs AS p MATCH (n:User {id: p[0]}), (m:User {id: p[1]}) CREATE (n)-[:Friend]->(m)",
		map[string]any{"pairs": pairs})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH ()-[r:Friend]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(len(pairs)), res.Rows[0][0],
		"every friendship must be created while the user writes are still buffered")

	// The same visibility must hold after the buffer drains.
	require.NoError(t, engine.FlushWriteBehind(nil))
	res, err = exec.Execute(ctx, "MATCH ()-[r:Friend]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(pairs)), res.Rows[0][0])
}

// TestWriteBehind_IndexedMatchReusesBufferedIndexAfterFlush runs the seek
// against the committed index after a flush, then again against a mix of
// committed and buffered nodes.
func TestWriteBehind_IndexedMatchReusesBufferedIndexAfterFlush(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE INDEX user_id IF NOT EXISTS FOR (n:User) ON (n.id)", nil)
	require.NoError(t, err)

	// Seed committed users, then buffer more on top.
	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:User {id: r.id})",
		map[string]any{"rows": []map[string]any{{"id": int64(1)}}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(nil))

	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:User {id: r.id})",
		map[string]any{"rows": []map[string]any{{"id": int64(2)}}})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:User) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Rows[0][0])

	// A friendship from the committed user to the buffered one must work.
	_, err = exec.Execute(ctx,
		"MATCH (n:User {id: 1}), (m:User {id: 2}) CREATE (n)-[:Friend]->(m)", nil)
	require.NoError(t, err)

	res, err = exec.Execute(ctx, "MATCH ()-[r:Friend]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows[0][0], fmt.Sprintf("rows: %v", res.Rows))
}

// TestWriteBehind_MergeAndDetachDeleteOverBufferedEdges reproduces the rest of
// monster's part-1 matrix at the Cypher level: MERGE must not duplicate a
// buffered relationship, and DETACH DELETE must leave no orphan edges while
// everything is still buffered.
func TestWriteBehind_MergeAndDetachDeleteOverBufferedEdges(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE (:User {id: 1}), (:User {id: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:User {id: 1}), (m:User {id: 2}) MERGE (n)-[r:Friend]->(m)", nil)
	require.NoError(t, err)

	// MERGE again while the first edge is still buffered: no duplicate.
	_, err = exec.Execute(ctx, "MATCH (n:User {id: 1}), (m:User {id: 2}) MERGE (n)-[r:Friend]->(m)", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (:User {id: 1})-[r:Friend]->(:User {id: 2}) RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows[0][0], "MERGE must not duplicate a buffered relationship")

	// DETACH DELETE one endpoint while both node and edge writes are buffered.
	_, err = exec.Execute(ctx, "MATCH (n:User {id: 1}) DETACH DELETE n", nil)
	require.NoError(t, err)
	res, err = exec.Execute(ctx, "MATCH ()-[r:Friend]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.Rows[0][0], "DETACH DELETE must remove buffered relationships")

	require.NoError(t, engine.FlushWriteBehind(nil))
	res, err = exec.Execute(ctx, "MATCH ()-[r:Friend]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), res.Rows[0][0], "no orphan may reappear after the flush")
}
