package search

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type serviceLockQueryContext struct {
	context.Context
	once sync.Once
	gate func()
}

func (c *serviceLockQueryContext) Done() <-chan struct{} {
	c.once.Do(c.gate)
	return c.Context.Done()
}

func serviceLockResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		serviceLockDump(t, "Service operation deadlocked")
	}
}

// The public exact read holds the metadata snapshot while checking its
// context. Queue an ordinary configuration writer at that boundary.
func TestServiceExactVectorReadVersusAdminWriter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		node     storage.Node
		property string
	}{
		{name: "named", node: storage.Node{NamedEmbeddings: map[string][]float32{"v": {1, 0}}}, property: "v"},
		{name: "named-fallback", node: storage.Node{NamedEmbeddings: map[string][]float32{"v": {1, 0}}}},
		{name: "property", node: storage.Node{Properties: map[string]any{"v": []float32{1, 0}}}, property: "v"},
		{name: "chunk", node: storage.Node{ChunkEmbeddings: [][]float32{{1, 0}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
			tc.node.ID = "live"
			require.NoError(t, svc.IndexNode(&tc.node))
			entered, resume := make(chan struct{}), make(chan struct{})
			ctx := &serviceLockQueryContext{Context: context.Background(), gate: func() { close(entered); <-resume }}
			queried := make(chan error, 1)
			go func() {
				hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0}, VectorQuerySpec{Similarity: "dot", Property: tc.property, Limit: 1})
				if err == nil && (len(hits) != 1 || hits[0].ID != "live") {
					err = fmt.Errorf("exact read lost its indexed vector: %v", hits)
				}
				queried <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("exact read did not reach its metadata snapshot")
			}
			written := make(chan error, 1)
			go func() { svc.SetDefaultMinSimilarity(0.25); written <- nil }()
			serviceLockWait(t, "admin writer queued behind exact read", func() bool {
				if svc.mu.TryRLock() {
					svc.mu.RUnlock()
					return false
				}
				return true
			})
			close(resume)
			serviceLockResult(t, queried)
			serviceLockResult(t, written)
			require.NoError(t, svc.Close())
		})
	}
}

// HNSW's file iterator invokes callbacks that look vectors up through Service.
// A pending close or append must not turn that lookup into a recursive lock.
func TestServiceFileIterationCallbacksReleaseLeafLock(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	for _, action := range []string{"append", "close", "compact"} {
		t.Run(action, func(t *testing.T) {
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
			svc.SetVectorIndexPath(t.TempDir() + "/vectors")
			svc.SetPersistenceEnabled(true)
			for _, id := range []string{"a", "b"} {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{{1, 0}}}))
			}
			svc.mu.RLock()
			vfs := svc.vectorFileStore
			svc.mu.RUnlock()
			entered, resume := make(chan struct{}), make(chan struct{})
			iterated := make(chan error, 1)
			go func() {
				count := 0
				err := vfs.IterateChunked(1, func(ids []string, _ [][]float32) error {
					if count == 0 {
						close(entered)
						<-resume
						_, _ = svc.getVectorLookup()(ids[0])
					}
					count += len(ids)
					return nil
				})
				if err == nil && count != 2 {
					err = fmt.Errorf("iteration lost its original snapshot: %d vectors", count)
				}
				iterated <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("iteration did not reach its callback")
			}
			written := make(chan error, 1)
			go func() {
				if action == "close" {
					written <- svc.Close()
				} else if action == "compact" {
					vfs.Remove("a")
					compacted, err := vfs.CompactIfNeeded()
					if err == nil && !compacted {
						err = fmt.Errorf("compaction did not replace the vector file")
					}
					written <- err
				} else {
					written <- vfs.Add("c", []float32{0, 1})
				}
			}()
			serviceLockWait(t, "writer queued or completed", func() bool {
				if len(written) > 0 {
					return true
				}
				if vfs.mu.TryRLock() {
					vfs.mu.RUnlock()
					return false
				}
				return true
			})
			close(resume)
			serviceLockResult(t, iterated)
			serviceLockResult(t, written)
			require.NoError(t, svc.Close())
		})
	}
}

func serviceLockWait(t *testing.T, why string, ready func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if ready() {
			return
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("could not stage " + why)
}
func serviceLockDump(t *testing.T, why string) {
	t.Helper()
	b := make([]byte, 1<<20)
	n := runtime.Stack(b, true)
	t.Fatalf("%s\n%s", why, b[:n])
}
