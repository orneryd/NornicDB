package cypher

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetiredQueryCacheKeysCollectible(t *testing.T) {
	for _, invalidate := range []bool{false, true} {
		t.Run(map[bool]string{false: "evict", true: "invalidate"}[invalidate], func(t *testing.T) {
			owner := NewQueryCache(2)
			reference := func() weak.Pointer[byte] {
				key := strings.Repeat("retired", 64)
				owner.lruList = append(owner.lruList, key)
				ref := weak.Make(unsafe.StringData(key))
				if invalidate {
					owner.Invalidate()
				} else {
					owner.evictOldest()
				}
				return ref
			}()
			runtime.GC()
			require.Nil(t, reference.Value())
			runtime.KeepAlive(owner)
		})
	}
}

func TestRetiredMembershipSnapshotCollectible(t *testing.T) {
	owner := &bindingParamMembershipCache{}
	reference := func() weak.Pointer[storage.Node] {
		node := &storage.Node{ID: "test:retired"}
		owner.getValidated([]any{nil, node})
		ref := weak.Make(node)
		owner.getValidated([]any{nil})
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}

func TestRetiredCreatePlanTailCollectible(t *testing.T) {
	owner := &createPlan{}
	reference := func() weak.Pointer[storage.Node] {
		node := &storage.Node{ID: "test:retired"}
		owner.nodes = []*storage.Node{node}
		ref := weak.Make(node)
		// Template rollback shortened the live prefix before pool release.
		owner.nodes = owner.nodes[:0]
		owner.release()
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}

func TestRetiredResultStreamRowsCollectible(t *testing.T) {
	owner := &ResultStream{}
	reference := func() weak.Pointer[storage.Node] {
		node := &storage.Node{ID: "test:retired"}
		rows := [][]any{{node}, {"pending"}}
		ref := weak.Make(node)
		out := owner.take(nil, rows, 1, &owner.buffered)
		require.Same(t, node, out[0][0], "delivery must preserve the returned row")
		require.Equal(t, "pending", owner.buffered[0][0])
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}
