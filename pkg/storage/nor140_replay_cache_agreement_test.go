package storage_test

// Author: NOR-140 verification lane, Codex (gpt-6.1-sol), 2026-10-26.
// The controlled schedule preserves TestProbe1655BufferedReplayReadCache's
// buffered-old / durable-null / replay / delayed-publication ordering. UNIQUE
// enforcement remains #1655's responsibility; these tests assert read agreement.

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nor140Executor(b storage.Engine, namespace string) *cypher.StorageExecutor {
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(b, namespace))
}

// StreamNodesWithOptions decodes captured stored bodies, bypassing both node
// body caches. During stress it includes acknowledged overlays; after Flush the
// same public scan is the stored-state oracle. Callers compare only while the
// target has no concurrent logical writer, so sequential reads are comparable.
func nor140ScannedNode(ctx context.Context, b *storage.BadgerEngine, id storage.NodeID) (*storage.Node, error) {
	var found *storage.Node
	count := 0
	err := b.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Prefix: string(id), WithEmbeddings: true}, func(n *storage.Node) error {
		if n.ID == id {
			found = n
			count++
		}
		return nil
	})
	if err == nil && count != 1 {
		err = fmt.Errorf("scan of %s returned %d matching bodies", id, count)
	}
	return found, err
}

func nor140CheckAgreement(ctx context.Context, b *storage.BadgerEngine, exec *cypher.StorageExecutor, id storage.NodeID, note string) (any, error) {
	cached, err := b.GetNode(id)
	if err != nil {
		return nil, err
	}
	queryNode, err := b.GetQueryNode(id)
	if err != nil {
		return nil, err
	}
	// Keep the parser-facing read fresh without evicting either storage node
	// cache; the constructor/cache-policy API differs between the pinned heads.
	exec.ClearQueryCaches()
	result, err := exec.Execute(ctx, "MATCH (n:Doc) WHERE n.note = $note RETURN n.id AS id", map[string]any{"note": note})
	if err != nil {
		return nil, err
	}
	stored, err := nor140ScannedNode(ctx, b, id)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return nil, fmt.Errorf("%s: public query returned %v", id, result.Rows)
	}
	want := stored.Properties["id"]
	if !reflect.DeepEqual(want, cached.Properties["id"]) || !reflect.DeepEqual(want, queryNode.Properties["id"]) || !reflect.DeepEqual(want, result.Rows[0][0]) {
		return want, fmt.Errorf("%s: cached id=%v, query-node id=%v, Cypher id=%v, stored id=%v", id, cached.Properties["id"], queryNode.Properties["id"], result.Rows[0][0], want)
	}
	return want, nil
}

func TestNOR140BufferedReplayCacheAgreement(t *testing.T) {
	for _, lowMemory := range []bool{false, true} {
		t.Run(fmt.Sprintf("low-memory=%t", lowMemory), func(t *testing.T) {
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: lowMemory, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.SetDecayEnabled(false)
			storage.NOR140StopReplayWorkerForTest(b)
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("nor140_doc_id", "Doc", "id"))
			require.NoError(t, storage.RefreshUniqueConstraintValuesForEngine(storage.NewNamespacedEngine(b, "test"), schema))
			_, err = b.CreateNode(&storage.Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "seed", "note": "seed"}})
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			ack, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, ack.SetImplicit(true))
			require.NoError(t, storage.NOR140StageNodeForTest(ack, &storage.Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "old", "note": "seed"}}))
			require.NoError(t, ack.Commit())
			require.True(t, ack.AcknowledgedBuffered())
			require.True(t, storage.NOR140BufferedNodeForTest(b, "test:a"), "the old update must be acknowledged into the replay buffer")
			// Public Cypher BEGIN intentionally drains buffered writes. The
			// recorded storage-transaction schedule must leave them pending.
			peer, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, storage.NOR140StageNodeForTest(peer, &storage.Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"id": nil, "note": "seed"}}))
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			var armed atomic.Bool
			restore := storage.NOR140CommitTailHookForTest(func() {
				if armed.CompareAndSwap(false, true) {
					close(entered)
					select {
					case <-resume:
					case <-ctx.Done():
					}
				}
			})
			committed, flushed := make(chan error, 1), make(chan error, 1)
			var workers sync.WaitGroup
			t.Cleanup(func() { release(); workers.Wait(); restore() })
			workers.Add(1)
			// With the ticker stopped, use the public non-cancellable commit:
			// #1548 deliberately makes cancellable waiters rely on that worker,
			// while Commit itself can drain the pending generation inline.
			go func() { defer workers.Done(); committed <- peer.Commit() }()
			select {
			case <-entered:
			case err := <-committed:
				t.Fatalf("peer missed the existing publication rendezvous: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			workers.Add(1)
			go func() { defer workers.Done(); flushed <- b.FlushWriteBehind() }()
			select {
			case err := <-flushed:
				require.NoError(t, err)
				t.Log("buffer replay completed while the newer commit tail was paused")
			case <-time.After(15 * time.Second):
				// A correct owner may serialize replay with publication. Release the
				// writer and test the resulting public state instead of requiring a
				// correct implementation to preserve the original invalid window.
				t.Log("replay was serialized with the paused commit tail")
				release()
				select {
				case err := <-flushed:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			release()
			select {
			case err := <-committed:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, b.FlushWriteBehind())
			require.False(t, storage.NOR140BufferedNodeForTest(b, "test:a"), "stored-state comparison requires retired replay")
			stored, err := nor140CheckAgreement(ctx, b, nor140Executor(b, "test"), "test:a", "seed")
			t.Logf("post-replay stored id=%v; the acceptance assertion compares public cached and Cypher reads with this body", stored)
			assert.NoError(t, err)
		})
	}
}

func TestNOR140ConcurrentReplayCacheAgreement(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetDecayEnabled(false)
	storage.NOR140StopReplayWorkerForTest(b)
	const writers, rounds = 6, 24
	var locks [writers]sync.Mutex
	want := make([]string, writers)
	for i := range writers {
		want[i] = "seed"
		_, err := b.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("stress:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"id": "seed", "note": fmt.Sprint(i)}})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stop := make(chan struct{})
	var background, writing sync.WaitGroup
	var ackCount, directCount, readCount atomic.Int64
	background.Add(2)
	go func() {
		defer background.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := b.FlushWriteBehind(); err != nil {
					t.Errorf("concurrent flush: %v", err)
					return
				}
			}
		}
	}()
	go func() {
		defer background.Done()
		exec := nor140Executor(b, "stress")
		for i := 0; ; i = (i + 1) % writers {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			locks[i].Lock()
			got, err := nor140CheckAgreement(ctx, b, exec, storage.NodeID(fmt.Sprintf("stress:n%d", i)), fmt.Sprint(i))
			if err != nil || !reflect.DeepEqual(want[i], got) {
				t.Errorf("concurrent read %d: want=%s got=%v error=%v", i, want[i], got, err)
			}
			locks[i].Unlock()
			readCount.Add(1)
		}
	}()
	for i := range writers {
		writing.Add(1)
		go func(i int) {
			defer writing.Done()
			exec := nor140Executor(b, "stress")
			for step := range rounds {
				locks[i].Lock()
				value := fmt.Sprintf("writer-%d-step-%d", i, step)
				var err error
				if step%2 == 0 {
					_, err = exec.Execute(ctx, "MATCH (n:Doc) WHERE n.note = $note SET n.id = $value", map[string]any{"note": fmt.Sprint(i), "value": value})
					ackCount.Add(1)
				} else {
					err = b.UpdateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("stress:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"id": value, "note": fmt.Sprint(i)}})
					directCount.Add(1)
				}
				if err == nil {
					want[i] = value
				} else {
					t.Errorf("writer %d step %d: %v", i, step, err)
				}
				locks[i].Unlock()
				if err != nil {
					return
				}
			}
		}(i)
	}
	writing.Wait()
	close(stop)
	background.Wait()
	require.NoError(t, ctx.Err())
	require.NoError(t, b.FlushWriteBehind())
	for i := range writers {
		id := storage.NodeID(fmt.Sprintf("stress:n%d", i))
		assert.False(t, storage.NOR140BufferedNodeForTest(b, id))
		got, err := nor140CheckAgreement(ctx, b, nor140Executor(b, "stress"), id, fmt.Sprint(i))
		assert.NoError(t, err)
		assert.Equal(t, want[i], got, "last acknowledged write after replay")
	}
	assert.Equal(t, int64(writers*rounds/2), ackCount.Load())
	assert.Equal(t, int64(writers*rounds/2), directCount.Load())
	assert.Positive(t, readCount.Load())
	t.Logf("stress: %d implicit updates, %d direct updates, %d concurrent public read comparisons; 6 final stored-state comparisons", ackCount.Load(), directCount.Load(), readCount.Load())
}
