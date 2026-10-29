package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The round-2 committed-read repro, with the read barrier moved to the new
// canonical reader. No stack-based fixture remains necessary.
func TestSol1482R2CypherCommittedReadCannotResurrectNewerDelete(t *testing.T) {
	ctx := context.Background()
	store, barrier, svc := newConsumerVectorStore(t, consumerContent("currentword"))
	older := NewStorageExecutor(store)
	older.SetSearchService(svc)
	newer := NewStorageExecutor(store)
	newer.SetSearchService(svc)
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := older.Execute(ctx, "MATCH (n:Doc) SET n.content = 'currentword' RETURN n.content", nil)
		done <- err
	}()
	runtimeFixtureOwner.WaitForBarrier(t, barrier.entered, 5*time.Second, barrier.open, "committed refresh did not reach its observation")
	_, err := newer.Execute(ctx, "MATCH (n:Doc) DETACH DELETE n", nil)
	require.NoError(t, err)
	barrier.open()
	require.NoError(t, <-done)
	_, err = store.GetNode("source")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.EmbeddingCount())
}

func TestSol1482R3ExplicitCommitAndRollback(t *testing.T) {
	ctx := WithTransactionControl(context.Background())
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	svc := search.NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	run := func(query string) { t.Helper(); _, err := exec.Execute(ctx, query, nil); require.NoError(t, err) }
	run("BEGIN")
	run("CREATE (:Doc {content:'rollbackword'})-[:CITES {vec:[1.0,0.0]}]->(:Doc)")
	run("ROLLBACK")
	require.Zero(t, svc.FulltextDocCount())
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
	run("BEGIN")
	run("CREATE (:Doc {content:'commitword'})-[:CITES {vec:[1.0,0.0]}]->(:Doc)")
	run("COMMIT")
	require.Equal(t, 1, consumerHits(t, svc, "commitword"))
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
	run("BEGIN")
	run("MATCH (n:Doc) DETACH DELETE n")
	run("ROLLBACK")
	require.Equal(t, 1, consumerHits(t, svc, "commitword"))
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
	run("BEGIN")
	run("MATCH (n:Doc) DETACH DELETE n")
	run("COMMIT")
	require.Zero(t, svc.FulltextDocCount())
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
}

func TestSol1482R3SuppressedEdgeDoesNotConsumeTopK(t *testing.T) {
	ctx := context.Background()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	svc := search.NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	run := func(query string) *ExecuteResult {
		t.Helper()
		r, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		return r
	}
	run("CREATE VECTOR INDEX rel_vec FOR ()-[r:CITES]-() ON (r.vec) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}")
	run("CREATE (:Doc)-[:CITES {name:'near',vec:[1.0,0.0]}]->(:Doc), (:Doc)-[:CITES {name:'far',vec:[0.0,1.0]}]->(:Doc)")
	query := "CALL db.index.vector.queryRelationships('rel_vec', 1, [1.0,0.0]) YIELD relationship RETURN relationship.name"
	require.Equal(t, [][]interface{}{{"near"}}, run(query).Rows)
	run("MATCH ()-[r:CITES {name:'near'}]->() CALL kp.suppress(r)")
	require.Equal(t, [][]interface{}{{"far"}}, run(query).Rows, "the hidden nearest edge consumed k=1 before visibility filtering")
}
