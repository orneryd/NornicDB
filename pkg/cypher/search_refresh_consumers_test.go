package cypher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	observed func(*storage.Node, error) bool
	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	opened   sync.Once
}

func (b *consumerObservationBarrier) open() { b.opened.Do(func() { close(b.release) }) }

// newerWrite runs the newer statement while the older observation is paused.
// If it cannot finish, the path serializes the two and cannot reorder them:
// the probe is skipped with that finding rather than reported as a failure.
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
		b.open()
		<-done
		t.Skip("SERIALIZED: the newer write waits for the paused older observation")
		return nil
	}
}

func newConsumerObservationBarrier(inner storage.Engine, observed func(*storage.Node, error) bool) *consumerObservationBarrier {
	return &consumerObservationBarrier{Engine: inner, observed: observed, entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *consumerObservationBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if b.armed.Load() && b.observed(node, err) && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return node, err
}

// consumerSuppressionBarrier also unwraps for reveal() and suppression. Only the
// unsuppress probe uses it: an unwrappable engine lets the executor bypass
// the barrier for ordinary reads.
type consumerSuppressionBarrier struct{ *consumerObservationBarrier }

// SetNodeManuallySuppressed arms the barrier once storage shows the node again.
func (b consumerSuppressionBarrier) SetNodeManuallySuppressed(id storage.NodeID, suppressed bool) error {
	err := b.Engine.(storage.ManualSuppressionEngine).SetNodeManuallySuppressed(id, suppressed)
	if err == nil && !suppressed {
		b.armed.Store(true)
	}
	return err
}

func (b consumerSuppressionBarrier) SetEdgeManuallySuppressed(id storage.EdgeID, suppressed bool) error {
	return b.Engine.(storage.ManualSuppressionEngine).SetEdgeManuallySuppressed(id, suppressed)
}

func (b consumerSuppressionBarrier) GetInnerEngine() storage.Engine { return b.Engine }

func (b *consumerObservationBarrier) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		b.open()
		t.Fatal(what + " did not reach its observation")
	}
}

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
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	node := &storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "obsoleteword"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	barrier := newConsumerObservationBarrier(store, observed)
	t.Cleanup(barrier.open)
	svc := search.NewServiceWithDimensions(barrier, 2)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	return store, barrier, svc
}

// An older SET's committed read must not resurrect a newer DETACH DELETE.
func TestCypherDelayedSetCannotResurrectDelete(t *testing.T) {
	ctx := context.Background()
	store, barrier, svc := newConsumerVectorStore(t, consumerContent("currentword"))
	older := NewStorageExecutor(barrier)
	older.SetSearchService(svc)
	newer := NewStorageExecutor(store)
	newer.SetSearchService(svc)
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := older.Execute(ctx, "MATCH (n:Doc) SET n.content = 'currentword'", nil)
		done <- err
	}()
	barrier.wait(t, "older SET")
	err := barrier.newerWrite(t, newer, "MATCH (n:Doc) DETACH DELETE n")
	require.NoError(t, err)
	barrier.open()
	require.NoError(t, <-done)
	_, err = store.GetNode("source")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.EmbeddingCount(), "an older committed read resurrected a deleted vector")
}

// An older SET's committed read must not replace a newer SET's index entry.
func TestCypherDelayedSetCannotOverwriteNewerSet(t *testing.T) {
	ctx := context.Background()
	store, barrier, svc := newConsumerVectorStore(t, consumerContent("olderword"))
	older := NewStorageExecutor(barrier)
	older.SetSearchService(svc)
	newer := NewStorageExecutor(store)
	newer.SetSearchService(svc)
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := older.Execute(ctx, "MATCH (n:Doc) SET n.content = 'olderword'", nil)
		done <- err
	}()
	barrier.wait(t, "older SET")
	err := barrier.newerWrite(t, newer, "MATCH (n:Doc) SET n.content = 'newerword'")
	require.NoError(t, err)
	barrier.open()
	require.NoError(t, <-done)
	stored, err := store.GetNode("source")
	require.NoError(t, err)
	require.Equal(t, "newerword", stored.Properties["content"])
	require.Equal(t, 1, consumerHits(t, svc, "newerword"), "the stored content is not searchable")
	require.Zero(t, consumerHits(t, svc, "olderword"), "an older committed read replaced the newer index entry")
}

// Cypher vector orphan cleanup must not remove a vector indexed for a
// recreated node after the orphan was observed.
func TestCypherVectorOrphanKeepsNewerIndex(t *testing.T) {
	ctx := context.Background()
	store, barrier, svc := newConsumerVectorStore(t, func(_ *storage.Node, err error) bool { return errors.Is(err, storage.ErrNotFound) })
	require.NoError(t, store.DeleteNode("source"))
	exec := NewStorageExecutor(barrier)
	exec.SetSearchService(svc)
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('managed', 5, [1.0, 0.0]) YIELD node, score RETURN node, score", nil)
		done <- err
	}()
	barrier.wait(t, "vector query")
	newer := &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(newer)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(newer))
	barrier.open()
	require.NoError(t, <-done)
	require.Equal(t, 1, svc.EmbeddingCount(), "an older orphan observation removed a newer live vector")
}

// The unsuppress read must not resurrect a node deleted after it.
func TestCypherDelayedUnsuppressCannotResurrectDelete(t *testing.T) {
	ctx := context.Background()
	engine, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "test")
	plain := NewStorageExecutor(store)
	_, err = plain.Execute(ctx, "CREATE (:Doc {name: 'quokkalicious'})", nil)
	require.NoError(t, err)
	// Search reads through the barrier too, which only unsuppress arms.
	barrier := newConsumerObservationBarrier(store, func(node *storage.Node, err error) bool { return err == nil && node != nil })
	t.Cleanup(barrier.open)
	svc := search.NewService(barrier)
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
	barrier.wait(t, "unsuppress")
	err = barrier.newerWrite(t, plain, "MATCH (n:Doc) DETACH DELETE n")
	require.NoError(t, err)
	barrier.open()
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
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	svc := search.NewServiceWithDimensions(store, 2)
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
