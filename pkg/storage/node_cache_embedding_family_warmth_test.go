package storage

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type nodeCacheEmbeddingKeyFixture struct {
	name      string
	key       []byte
	ambiguous bool
	unknown   bool
}

func TestNodeCacheEmbeddingFamilyWriterWarmth(t *testing.T) {
	owner := NodeID("test:family-owner")
	for _, fixture := range nodeCacheEmbeddingKeyFixtures(owner) {
		modes := []string{"transaction", "buffered", "recovered"}
		if fixture.unknown {
			modes = []string{"unknown-managed"}
		}
		for _, mode := range modes {
			for _, deleting := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/delete=%t", fixture.name, mode, deleting), func(t *testing.T) {
					var b *BadgerEngine
					if mode == "buffered" {
						b = newWriteBehindTestEngine(t, time.Hour)
					} else {
						b = NewMemoryEngine().BadgerEngine
						t.Cleanup(func() { _ = b.Close() })
					}
					target := &Node{ID: "test:family-untouched", Properties: map[string]any{"v": int64(7)}}
					for _, node := range []*Node{target, {ID: owner, Properties: map[string]any{"v": int64(1)}}} {
						_, err := b.CreateNode(node)
						require.NoError(t, err)
					}
					require.NoError(t, b.FlushWriteBehind())
					_, err := b.GetNode(target.ID)
					require.NoError(t, err)
					state := func() bool {
						b.nodeCacheMu.RLock()
						defer b.nodeCacheMu.RUnlock()
						_, ok := b.cachedNodeLocked(target.ID, false, 0)
						return ok
					}
					require.True(t, state())
					value, err := encodeValue(map[string]any{"state": "scope-fixture"})
					require.NoError(t, err)
					writes, deletes := map[string][]byte{}, map[string]bool{}
					if deleting {
						deletes[string(fixture.key)] = true
					} else {
						writes[string(fixture.key)] = value
					}
					switch mode {
					case "unknown-managed":
						// This is the explicit arbitrary-raw boundary, not a classified inventory.
						err = b.db.Update(func(txn *badger.Txn) error {
							if deleting {
								return txn.Delete(fixture.key)
							}
							return txn.Set(fixture.key, value)
						})
					case "recovered":
						err = b.applyBufferedCommits([]bufferedCommit{{namespace: "test", recovered: true, writes: writes, deletes: deletes}})
					default:
						tx, beginErr := b.BeginTransaction()
						require.NoError(t, beginErr)
						require.NoError(t, tx.SetNamespace("test"))
						require.NoError(t, tx.SetImplicit(mode == "buffered"))
						for key, data := range writes {
							tx.bufferSet([]byte(key), data)
						}
						for key := range deletes {
							tx.bufferDelete([]byte(key))
						}
						err = tx.Commit()
						require.NoError(t, err)
						err = b.FlushWriteBehind()
					}
					require.NoError(t, err)
					eligible := state()
					misses := atomic.LoadInt64(&b.cacheMisses)
					got, err := b.GetNode(target.ID)
					require.NoError(t, err)
					require.Equal(t, int64(7), got.Properties["v"])
					if fixture.unknown || fixture.ambiguous {
						require.False(t, eligible, "unresolved ownership must retain the full conservative fence")
						require.Equal(t, misses+1, atomic.LoadInt64(&b.cacheMisses))
					} else {
						require.True(t, eligible, "classified embedding family cooled an unrelated node")
						require.Equal(t, misses, atomic.LoadInt64(&b.cacheMisses))
					}
				})
			}
		}
	}
}
