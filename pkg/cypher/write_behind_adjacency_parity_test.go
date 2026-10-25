package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// firstInt returns the first column of the first result row as int64, and 0
// when there is no row. It exists to compare a query's answer before and
// after a flush.
func firstInt(t *testing.T, res *ExecuteResult, err error) int64 {
	t.Helper()
	require.NoError(t, err)
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0
	}
	switch v := res.Rows[0][0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		t.Fatalf("unexpected first-column type %T (%v)", res.Rows[0][0], res.Rows[0][0])
		return 0
	}
}

func runInt(t *testing.T, exec *StorageExecutor, ctx context.Context, q string) int64 {
	t.Helper()
	res, err := exec.Execute(ctx, q, nil)
	return firstInt(t, res, err)
}

// TestWriteBehind_AdjacencyParity mirrors monster's retest part 1: every read
// shape must give the same answer while the buffer is held as after
// FlushWriteBehind, including the shapes that previously missed buffered edges
// (unnamed-relationship expansion, relationship-property reads in type scans).
func TestWriteBehind_AdjacencyParity(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour, // buffer never drains mid-test
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE INDEX user_id IF NOT EXISTS FOR (n:User) ON (n.id)", nil)
	require.NoError(t, err)

	// 50 :User nodes.
	users := make([]map[string]any, 0, 50)
	for i := 1; i <= 50; i++ {
		users = append(users, map[string]any{"id": int64(i)})
	}
	_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:User {id: r.id})", map[string]any{"rows": users})
	require.NoError(t, err)

	// 40 :Friend {w} from users 1..10 to 11..50 (4 each), then 10 more from
	// 11..20 to 21..30 (1 each).
	pairs := make([][]int64, 0, 50)
	for i := 1; i <= 10; i++ {
		for j := 0; j < 4; j++ {
			pairs = append(pairs, []int64{int64(i), int64(11 + (i-1)*4 + j)})
		}
	}
	for i := 11; i <= 20; i++ {
		pairs = append(pairs, []int64{int64(i), int64(10 + i)})
	}
	_, err = exec.Execute(ctx,
		"UNWIND $pairs AS p MATCH (n:User {id: p[0]}), (m:User {id: p[1]}) CREATE (n)-[:Friend {w: p[0] + p[1]}]->(m)",
		map[string]any{"pairs": pairs})
	require.NoError(t, err)

	queries := []string{
		"MATCH (a:User {id: 1})-[:Friend]->(b) RETURN count(b)",
		"MATCH (a:User {id: 1})-[:Friend*1..2]->(b) RETURN count(b)",
		"MATCH (a:User {id: 1}) OPTIONAL MATCH (a)-[:Friend]->(b) RETURN count(b)",
		"MATCH (a:User {id: 1}) RETURN size([(a)-[:Friend]->(b) | b])",
		"MATCH (a:User {id: 1}) RETURN size([(a)--() | 1])",
		"MATCH (a:User) WHERE (a)-[:Friend]->() RETURN count(a)",
		"MATCH ()-[r:Friend]->() RETURN sum(r.w)",
		"MATCH ()-[r:Friend]->() WHERE r.w > 20 RETURN count(r)",
		// Controls that already agreed before the fix.
		"MATCH ()-[r:Friend]->() RETURN count(r)",
		"MATCH (a:User {id: 1})-[r:Friend]->(b) RETURN count(b)",
		"MATCH (:User)-[r:Friend]->(:User) RETURN count(r)",
		"MATCH (a:User {id: 1})-[r]-(b) RETURN count(r)",
	}

	buffered := make(map[string]int64, len(queries))
	for _, q := range queries {
		buffered[q] = runInt(t, exec, ctx, q)
	}

	require.NoError(t, engine.FlushWriteBehind())

	for _, q := range queries {
		require.Equal(t, buffered[q], runInt(t, exec, ctx, q), "query differs after flush: %s", q)
	}

	// The endpoint-lookup shapes must see the four edges of user 1.
	require.Equal(t, int64(4), buffered["MATCH (a:User {id: 1})-[:Friend]->(b) RETURN count(b)"])
	require.Equal(t, int64(50), buffered["MATCH ()-[r:Friend]->() RETURN count(r)"])
}

// TestWriteBehind_SetBufferedRelationship pins monster's part-1 SET shape: a
// SET on a buffered relationship must succeed (not report a conflict) and the
// write must survive the flush.
func TestWriteBehind_SetBufferedRelationship(t *testing.T) {
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
	_, err = exec.Execute(ctx, "CREATE (:User {id: 1}), (:User {id: 11})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"MATCH (n:User {id: 1}), (m:User {id: 11}) CREATE (n)-[:Friend {w: 7}]->(m)", nil)
	require.NoError(t, err)

	// SET on the buffered relationship must succeed.
	_, err = exec.Execute(ctx, "MATCH (a:User {id: 1})-[r:Friend]->(b:User {id: 11}) SET r.w = 999", nil)
	require.NoError(t, err)

	require.Equal(t, int64(999), runInt(t, exec, ctx, "MATCH ()-[r:Friend]->() RETURN r.w"))

	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, int64(999), runInt(t, exec, ctx, "MATCH ()-[r:Friend]->() RETURN r.w"))
}

// TestWriteBehind_DetachDeleteCountConsistency pins monster's part-1 count
// drift: right after a DETACH DELETE of a node whose relationships are all
// buffered, the type count must equal the post-flush count (no transient
// under- or over-count).
// The default-cache case also pins buffered GraphMutationVersion invalidation:
// a warmed whole-type relationship count must change before the flush.
func TestWriteBehind_DetachDeleteCountConsistency(t *testing.T) {
	for _, mode := range []struct {
		name   string
		policy []QueryCachePolicy
		cached bool
	}{
		{"storage overlay cache disabled", []QueryCachePolicy{{MaxEntries: 0, TTL: 0}}, false},
		{"query cache invalidation", nil, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
				DataDir:             t.TempDir(),
				WriteBehind:         true,
				WriteBehindInterval: time.Hour,
			}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })

			// Both cache modes must observe the buffered detach before and after flush.
			exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"), mode.policy...)
			ctx := context.Background()

			_, err = exec.Execute(ctx, "CREATE INDEX user_id IF NOT EXISTS FOR (n:User) ON (n.id)", nil)
			require.NoError(t, err)

			users := make([]map[string]any, 0, 50)
			for i := 1; i <= 50; i++ {
				users = append(users, map[string]any{"id": int64(i)})
			}
			_, err = exec.Execute(ctx, "UNWIND $rows AS r CREATE (:User {id: r.id})", map[string]any{"rows": users})
			require.NoError(t, err)

			pairs := make([][]int64, 0, 50)
			for i := 1; i <= 10; i++ {
				for j := 0; j < 4; j++ {
					pairs = append(pairs, []int64{int64(i), int64(11 + (i-1)*4 + j)})
				}
			}
			for i := 11; i <= 20; i++ {
				pairs = append(pairs, []int64{int64(i), int64(10 + i)})
			}
			_, err = exec.Execute(ctx,
				"UNWIND $pairs AS p MATCH (n:User {id: p[0]}), (m:User {id: p[1]}) CREATE (n)-[:Friend {w: 1}]->(m)",
				map[string]any{"pairs": pairs})
			require.NoError(t, err)

			before := runInt(t, exec, ctx, "MATCH ()-[r:Friend]->() RETURN count(r)")
			if mode.cached {
				require.Equal(t, int64(50), before)
			}

			_, err = exec.Execute(ctx, "MATCH (n:User {id: 14}) DETACH DELETE n", nil)
			require.NoError(t, err)

			afterBuffered := runInt(t, exec, ctx, "MATCH ()-[r:Friend]->() RETURN count(r)")
			if mode.cached {
				require.Equal(t, int64(48), afterBuffered, "cached whole-type count must be invalidated by the buffered delete")
			}

			require.NoError(t, engine.FlushWriteBehind())
			afterFlushed := runInt(t, exec, ctx, "MATCH ()-[r:Friend]->() RETURN count(r)")

			require.Equal(t, before-2, afterFlushed, "user 14 has exactly two buffered :Friend edges")
			require.Equal(t, afterFlushed, afterBuffered, "count right after DETACH DELETE must match the post-flush count")

			if mode.cached {
				require.Equal(t, int64(48), afterFlushed)
			}
		})
	}
}
