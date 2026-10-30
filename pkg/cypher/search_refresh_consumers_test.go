package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// consumerObservationBarrier pauses the first armed read whose result matches
// observed, after the observation and before the caller acts on it. The
// observation itself is the barrier; no goroutine stacks are inspected.
type consumerObservationBarrier struct {
	storage.Engine
	*runtimeFixtureOwner.PauseGate
	observed func(*storage.Node, error) bool
}

// newerWrite runs the newer statement while the older observation is paused.
// If it is still waiting, release the observation so the caller can verify
// the final storage and search state after both statements complete.
func (b *consumerObservationBarrier) newerWrite(t *testing.T, exec *StorageExecutor, query string) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := exec.Execute(context.Background(), query, nil)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		b.ReleaseWrites()
		t.Log("newer write was still waiting when the older observation was released")
		return <-done
	}
}

func newConsumerObservationBarrier(inner storage.Engine, observed func(*storage.Node, error) bool) *consumerObservationBarrier {
	return &consumerObservationBarrier{Engine: inner, observed: observed, PauseGate: runtimeFixtureOwner.NewPauseGate()}
}

func (b *consumerObservationBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if b.Armed.Load() && b.observed(node, err) {
		b.Pause(b.Armed, b.Entered)
	}
	return node, err
}

// consumerSuppressionBarrier also unwraps for reveal() and suppression. Only the
// unsuppress probe uses it: an unwrappable engine lets the executor bypass
// the barrier for ordinary reads.
type consumerSuppressionBarrier struct{ *consumerObservationBarrier }

// SetManualSuppression arms the barrier only when storage shows a node again.
func (b consumerSuppressionBarrier) SetManualSuppression(kind storage.StoredEntityKind, id string, suppressed bool) error {
	err := b.Engine.(storage.ManualSuppressionEngine).SetManualSuppression(kind, id, suppressed)
	if err == nil && kind == storage.StoredEntityNodeKind && !suppressed {
		b.Armed.Store(true)
	}
	return err
}

func (b consumerSuppressionBarrier) GetInnerEngine() storage.Engine { return b.Engine }

func consumerContent(word string) func(*storage.Node, error) bool {
	return func(node *storage.Node, err error) bool {
		return err == nil && node != nil && node.Properties["content"] == word
	}
}

func consumerHits(t *testing.T, svc *search.Service, word string) int {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.MinRRFScore = 0
	response, err := svc.Search(context.Background(), word, nil, opts)
	require.NoError(t, err)
	return len(response.Results)
}

// newConsumerVectorStore stores one indexed node. The search service reads
// through the returned barrier, as does an executor built on it: the barrier
// pauses whichever read decides the older statement's index change.
func newConsumerVectorStore(t *testing.T, observed func(*storage.Node, error) bool) (storage.Engine, *consumerObservationBarrier, *search.Service) {
	t.Helper()
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "test")
	node := &storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "obsoleteword"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	barrier := newConsumerObservationBarrier(store, observed)
	t.Cleanup(barrier.ReleaseWrites)
	svc := runtimeFixtureOwner.Own(t, search.NewService(barrier, 2, nil))
	require.NoError(t, svc.BuildIndexes(context.Background()))
	return store, barrier, svc
}

// An older SET's committed read must not resurrect a newer DETACH DELETE, and
// must not replace a newer SET's index entry.
func TestCypherDelayedSetCannotOverrideNewerWrite(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		observed string
		olderSet string
		newer    string
		verify   func(t *testing.T, store storage.Engine, svc *search.Service)
	}{
		{
			name:     "resurrect_delete",
			observed: "currentword",
			olderSet: "MATCH (n:Doc) SET n.content = 'currentword'",
			newer:    "MATCH (n:Doc) DETACH DELETE n",
			verify: func(t *testing.T, store storage.Engine, svc *search.Service) {
				_, err := store.GetNode("source")
				require.ErrorIs(t, err, storage.ErrNotFound)
				require.Zero(t, svc.EmbeddingCount(), "an older committed read resurrected a deleted vector")
			},
		},
		{
			name:     "overwrite_newer_set",
			observed: "olderword",
			olderSet: "MATCH (n:Doc) SET n.content = 'olderword'",
			newer:    "MATCH (n:Doc) SET n.content = 'newerword'",
			verify: func(t *testing.T, store storage.Engine, svc *search.Service) {
				stored, err := store.GetNode("source")
				require.NoError(t, err)
				require.Equal(t, "newerword", stored.Properties["content"])
				require.Equal(t, 1, consumerHits(t, svc, "newerword"), "the stored content is not searchable")
				require.Zero(t, consumerHits(t, svc, "olderword"), "an older committed read replaced the newer index entry")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, barrier, svc := newConsumerVectorStore(t, consumerContent(tc.observed))
			older := NewStorageExecutor(barrier)
			older.SetSearchService(svc)
			newer := NewStorageExecutor(store)
			newer.SetSearchService(svc)
			barrier.Armed.Store(true)
			done := make(chan error, 1)
			go func() {
				_, err := older.Execute(ctx, tc.olderSet, nil)
				done <- err
			}()
			runtimeFixtureOwner.WaitForBarrier(t, barrier.Entered, 5*time.Second, barrier.ReleaseWrites, "older SET did not reach its observation")
			err := barrier.newerWrite(t, newer, tc.newer)
			require.NoError(t, err)
			barrier.ReleaseWrites()
			require.NoError(t, <-done)
			tc.verify(t, store, svc)
		})
	}
}

// Cypher vector orphan cleanup must not remove a vector indexed for a
// recreated node after the orphan was observed.
func TestCypherVectorOrphanKeepsNewerIndex(t *testing.T) {
	ctx := context.Background()
	store, barrier, svc := newConsumerVectorStore(t, func(_ *storage.Node, err error) bool { return errors.Is(err, storage.ErrNotFound) })
	require.NoError(t, store.DeleteNode("source"))
	exec := NewStorageExecutor(barrier)
	exec.SetSearchService(svc)
	barrier.Armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('managed', 5, [1.0, 0.0]) YIELD node, score RETURN node, score", nil)
		done <- err
	}()
	runtimeFixtureOwner.WaitForBarrier(t, barrier.Entered, 5*time.Second, barrier.ReleaseWrites, "vector query did not reach its observation")
	newer := &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(newer)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(newer))
	barrier.ReleaseWrites()
	require.NoError(t, <-done)
	require.Equal(t, 1, svc.EmbeddingCount(), "an older orphan observation removed a newer live vector")
}

// The unsuppress read must not resurrect a node deleted after it.
func TestCypherDelayedUnsuppressCannotResurrectDelete(t *testing.T) {
	ctx := context.Background()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "test")
	plain := NewStorageExecutor(store)
	_, err = plain.Execute(ctx, "CREATE (:Doc {name: 'quokkalicious'})", nil)
	require.NoError(t, err)
	// Search reads through the barrier too, which only unsuppress arms.
	barrier := newConsumerObservationBarrier(store, func(node *storage.Node, err error) bool { return err == nil && node != nil })
	t.Cleanup(barrier.ReleaseWrites)
	svc := search.NewService(barrier, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	plain.SetSearchService(svc)
	_, err = plain.Execute(ctx, "MATCH (n:Doc) CALL kp.suppress(n)", nil)
	require.NoError(t, err)
	require.Zero(t, consumerHits(t, svc, "quokkalicious"))
	older := NewStorageExecutor(consumerSuppressionBarrier{barrier})
	older.SetSearchService(svc)
	done := make(chan error, 1)
	go func() {
		_, err := older.Execute(ctx, "MATCH (n:Doc) WHERE reveal(n) IS NOT NULL CALL kp.unsuppress(n)", nil)
		done <- err
	}()
	runtimeFixtureOwner.WaitForBarrier(t, barrier.Entered, 5*time.Second, barrier.ReleaseWrites, "unsuppress did not reach its observation")
	err = barrier.newerWrite(t, plain, "MATCH (n:Doc) DETACH DELETE n")
	require.NoError(t, err)
	barrier.ReleaseWrites()
	require.NoError(t, <-done)
	result, err := plain.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
	require.Zero(t, consumerHits(t, svc, "quokkalicious"), "an older unsuppress read resurrected a deleted node")
}

// Deleting a relationship through Cypher removes its client-supplied vector.
func TestCypherRelationshipDeleteRemovesVector(t *testing.T) {
	ctx := context.Background()
	store, _, svc := newConsumerVectorStore(t, nil)
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	_, err := exec.Execute(ctx, "CREATE (:Doc {name: 'a'})-[:CITES {vec: [1.0, 0.0]}]->(:Doc {name: 'b'})", nil)
	require.NoError(t, err)
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "the relationship vector is indexed")
	_, err = exec.Execute(ctx, "MATCH ()-[r:CITES]->() DELETE r", nil)
	require.NoError(t, err)
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a deleted relationship's vector stayed indexed")
}

// Relationship visibility enters search in both directions and keeps the
// transaction's commit/rollback semantics: a hidden nearest relationship
// doesn't take the k=1 place, a shown one takes it back.
func TestCypherRelationshipVisibilityRefreshesSearch(t *testing.T) {
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
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}
	run("CREATE VECTOR INDEX rel_vec FOR ()-[r:CITES]-() ON (r.vec) OPTIONS {indexConfig: {`vector.dimensions`: 2, `vector.similarity_function`: 'cosine'}}")
	run("CREATE (:Doc)-[:CITES {name:'near',vec:[1.0,0.0]}]->(:Doc), (:Doc)-[:CITES {name:'far',vec:[0.0,1.0]}]->(:Doc)")
	nearest := func() [][]interface{} {
		return run("CALL db.index.vector.queryRelationships('rel_vec', 1, [1.0,0.0]) YIELD relationship RETURN relationship.name").Rows
	}
	near, far := [][]interface{}{{"near"}}, [][]interface{}{{"far"}}
	require.Equal(t, near, nearest())

	run("BEGIN")
	run("MATCH ()-[r:CITES {name:'near'}]->() CALL kp.suppress(r)")
	run("ROLLBACK")
	require.Equal(t, near, nearest(), "a rolled-back suppression changed search")
	run("BEGIN")
	run("MATCH ()-[r:CITES {name:'near'}]->() CALL kp.suppress(r)")
	run("COMMIT")
	require.Equal(t, far, nearest(), "a committed suppression left the relationship in search")
	run("MATCH ()-[r:CITES {name:'near'}]->() WHERE reveal(r) IS NOT NULL CALL kp.unsuppress(r)")
	require.Equal(t, near, nearest(), "a relationship shown again did not return to search")
}
