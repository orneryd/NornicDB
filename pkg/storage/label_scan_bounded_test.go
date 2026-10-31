package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// labelScanTailCount is the record count that forces the point-lookup path and
// the buffered batch plus two more records.
const labelScanTailCount = labelScanPointLookups + labelScanMaxBuffered + 2

// labelTailScan scans every Common node in the test namespace, from a pinned
// snapshot transaction when snapshot is set. The snapshot is rolled back when the
// test finishes.
func labelTailScan(t *testing.T, eng *BadgerEngine, properties []string, snapshot bool) func(func(*Node) error) error {
	t.Helper()
	if !snapshot {
		return func(visit func(*Node) error) error {
			return eng.StreamNodesByLabelProjectedInScope("test:", "Common", properties, visit)
		}
	}
	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })
	return func(visit func(*Node) error) error {
		return tx.StreamNodesByLabelProjected("Common", properties, visit)
	}
}

func TestLabelScanLargeTailStaysStreaming(t *testing.T) {
	const count = labelScanTailCount
	for _, properties := range [][]string{nil, {"i"}} {
		t.Run(fmt.Sprintf("engine/projected=%t", properties != nil), func(t *testing.T) {
			eng, ids := labelScanFixture(t, count)
			visited := 0
			scan := labelTailScan(t, eng, properties, false)
			require.NoError(t, scan(func(node *Node) error {
				require.Equal(t, ids[visited], node.ID)
				visited++
				if visited == labelScanPointLookups+1 {
					// A streaming scan must not have decoded the final node yet.
					eng.cacheStoreNode(&Node{ID: ids[count-1], Labels: []string{"Common"}, Properties: map[string]any{"i": int64(-1)}}, eng.db.oracle.published.Load())
				}
				if visited == count {
					require.Equal(t, int64(-1), node.Properties["i"])
				}
				return nil
			}))
			require.Equal(t, count, visited)
		})
	}
	t.Run("snapshot", func(t *testing.T) {
		eng, ids := labelScanFixture(t, count)
		scan := labelTailScan(t, eng, nil, true)
		stop := errors.New("stop")
		visited := 0
		var readAhead bool
		inOrder := true
		require.ErrorIs(t, scan(func(node *Node) error {
			inOrder = inOrder && visited < len(ids) && ids[visited] == node.ID
			visited++
			if visited == labelScanPointLookups+1 {
				eng.nodeBodyCacheMu.RLock()
				_, readAhead = eng.nodeBodyCache[ids[count-1]]
				eng.nodeBodyCacheMu.RUnlock()
				return stop
			}
			return nil
		}), stop)
		require.False(t, readAhead, "LIMIT must not decode the entire remaining tail")
		require.Equal(t, labelScanPointLookups+1, visited)
		require.True(t, inOrder)
	})
}

func TestLabelScanLargeTailPreservesReadErrors(t *testing.T) {
	const count = labelScanTailCount
	eng, ids := labelScanFixture(t, count)
	require.NoError(t, eng.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		if err := txn.Delete(nodeKey(ids[labelScanPointLookups])); err != nil {
			return err
		}
		return txn.Set(nodeKey(ids[count-1]), []byte{0xff})
	}))
	for _, snapshot := range []bool{false, true} {
		for _, properties := range [][]string{nil, {"i"}} {
			t.Run(fmt.Sprintf("snapshot=%t/projected=%t", snapshot, properties != nil), func(t *testing.T) {
				scan := labelTailScan(t, eng, properties, snapshot)
				stop := errors.New("stop before corruption")
				visited := 0
				require.ErrorIs(t, scan(func(*Node) error {
					visited++
					if visited == labelScanPointLookups+1 {
						return stop
					}
					return nil
				}), stop)
				require.Equal(t, labelScanPointLookups+1, visited)
				visited = 0
				require.Error(t, scan(func(*Node) error {
					visited++
					return nil
				}))
				require.Equal(t, count-2, visited, "skip only the missing record and report later corruption")
			})
		}
	}
}

func BenchmarkLabelScanStreaming(b *testing.B) {
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = eng.Close() })
	const count = 40000
	for offset := 0; offset < count; offset += 250 {
		nodes := make([]*Node, 0, 250)
		for index := offset; index < offset+250; index++ {
			nodes = append(nodes, &Node{
				ID: NodeID(fmt.Sprintf("test:n%05d", count-index-1)), Labels: []string{"Common"},
				Properties: map[string]any{"i": int64(index), "big": "x"},
			})
		}
		require.NoError(b, eng.BulkCreateNodes(nodes))
	}
	stop := errors.New("limit")
	for _, snapshot := range []bool{false, true} {
		for _, projected := range []bool{false, true} {
			for _, limit := range []int{0, labelScanPointLookups + 1} {
				b.Run(fmt.Sprintf("snapshot=%t/projected=%t/limit=%d", snapshot, projected, limit), func(b *testing.B) {
					var properties []string
					if projected {
						properties = []string{"i"}
					}
					scan := func(visit func(*Node) error) error {
						return eng.StreamNodesByLabelProjectedInScope("test:", "Common", properties, visit)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						b.StopTimer()
						eng.nodeCacheMu.Lock()
						clear(eng.nodeCache)
						eng.nodeCacheMu.Unlock()
						eng.nodeBodyCacheMu.Lock()
						clear(eng.nodeBodyCache)
						eng.nodeBodyCacheLRU.Init()
						eng.nodeBodyCacheBytes = 0
						eng.nodeBodyCacheMu.Unlock()
						var tx *BadgerTransaction
						if snapshot {
							tx, err = eng.BeginTransaction()
							require.NoError(b, err)
							require.NoError(b, tx.SetNamespace("test"))
							scan = func(visit func(*Node) error) error {
								return tx.StreamNodesByLabelProjected("Common", properties, visit)
							}
						}
						b.StartTimer()
						visited := 0
						err := scan(func(*Node) error {
							visited++
							if visited == limit {
								return stop
							}
							return nil
						})
						if limit == 0 {
							require.NoError(b, err)
							require.Equal(b, count, visited)
						} else {
							require.ErrorIs(b, err, stop)
							require.Equal(b, limit, visited)
						}
						b.StopTimer()
						if tx != nil {
							require.NoError(b, tx.Rollback())
						}
						b.StartTimer()
					}
				})
			}
		}
	}
}
