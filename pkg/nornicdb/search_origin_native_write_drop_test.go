package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nativeWriteBarrier pauses the first armed node write before it reaches
// storage, after the DB write API has admitted it.
type nativeWriteBarrier struct {
	storage.Engine
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	opened  sync.Once
}

func (b *nativeWriteBarrier) open() { b.opened.Do(func() { close(b.release) }) }

func (b *nativeWriteBarrier) pause() {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
}

func (b *nativeWriteBarrier) CreateNode(node *storage.Node) (storage.NodeID, error) {
	b.pause()
	return b.Engine.CreateNode(node)
}

func (b *nativeWriteBarrier) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	b.pause()
	return b.Engine.UpdateNode(updateRequest)
}

// A native DB write admitted before DROP INDEX that reaches storage after the
// DROP must not restore an entry of the dropped index, also when the index is
// recreated meanwhile. The write is paused below the namespace after its
// admission; DROP runs through Cypher; then the write completes.
// TestSearchOriginAdmittedContextAfterDrop is the deterministic variant.
func TestSearchOriginNativeWriteAdmittedBeforeDrop(t *testing.T) {
	for _, write := range []string{"CreateNodeWithID", "UpdateNode"} {
		for _, recreate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recreate=%t", write, recreate), func(t *testing.T) {
				db, svc := openConsumerDB(t, false)
				ctx := context.Background()
				ns := db.defaultDatabaseName()
				ddl := cypher.NewStorageExecutor(storage.NewNamespacedEngine(db.GetBaseStorageForManager(), ns))
				ddl.SetSearchService(svc)
				create := "CREATE VECTOR INDEX nativevec FOR (n:Doc) ON (n.embedding)"
				_, err := ddl.Execute(ctx, create, nil)
				require.NoError(t, err)
				vector := make([]float64, svc.VectorIndexDimensions())
				vector[0] = 1
				if write == "UpdateNode" {
					_, err = db.CreateNodeWithID(ctx, "doc", []string{"Doc"}, map[string]interface{}{"content": "nativeword"})
					require.NoError(t, err)
					settleQueuedRefreshes()
				}

				db.bgWg.Wait()
				db.mu.Lock()
				original := db.storage
				namespaced := original.(*storage.NamespacedEngine)
				barrier := &nativeWriteBarrier{Engine: namespaced.GetInnerEngine(), entered: make(chan struct{}), release: make(chan struct{})}
				db.storage = storage.NewNamespacedEngine(barrier, namespaced.Namespace())
				db.mu.Unlock()
				t.Cleanup(func() {
					db.mu.Lock()
					db.storage = original
					db.mu.Unlock()
				})
				t.Cleanup(barrier.open)
				barrier.armed.Store(true)

				properties := map[string]interface{}{"content": "nativeword", "embedding": vector, "other": vector}
				done := make(chan error, 1)
				go func() {
					var err error
					if write == "CreateNodeWithID" {
						_, err = db.CreateNodeWithID(ctx, "doc", []string{"Doc"}, properties)
					} else {
						_, err = db.UpdateNode(ctx, "doc", properties)
					}
					done <- err
				}()
				select {
				case <-barrier.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("admitted native write did not reach storage")
				}
				_, err = ddl.Execute(ctx, "DROP INDEX nativevec", nil)
				require.NoError(t, err)
				if recreate {
					_, err = ddl.Execute(ctx, create, nil)
					require.NoError(t, err)
				}
				barrier.open()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("native write did not return")
				}
				settleQueuedRefreshes()
				require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "a write admitted before DROP restored a retired entry")
				require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
				require.Equal(t, 1, consumerDBHits(t, svc, "nativeword"))
			})
		}
	}
}
