package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropRecoveryPreservesVictimsAndCounts(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, phase := range []string{"partial-dictionary", "before-final-sync"} {
			for _, resolution := range []string{"retry", "reopen", "restore"} {
				t.Run(fmt.Sprintf("buffered=%v/%s/%s", buffered, phase, resolution), func(t *testing.T) {
					dir := t.TempDir()
					open := func() *BadgerEngine {
						b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
						if b != nil {
							fixtureowner.Own(t, b)
							t.Cleanup(func() { _ = b.Close() })
						}
						require.NoError(t, err)
						return b
					}
					b := open()
					require.NoError(t, b.GetSchemaForNamespace("victim").AddUniqueConstraint("uid", "Person", "uid"))
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					defer tx.Rollback()
					require.NoError(t, tx.SetImplicit(buffered))
					for i, id := range []NodeID{"victim:a", "victim:b"} {
						_, err = tx.CreateNode(&Node{ID: id, Labels: []string{"Person"}, Properties: map[string]any{"uid": string(id)}, ChunkEmbeddings: [][]float32{make([]float32, 16384)}})
						require.NoError(t, err, "seed node %d", i)
					}
					require.NoError(t, tx.CreateEdge(&Edge{ID: "victim:e", StartNode: "victim:a", EndNode: "victim:b", Type: "LINK"}))
					require.NoError(t, tx.Commit())
					_, err = b.CreateNode(&Node{ID: "peer:keep"})
					require.NoError(t, err)
					var resets atomic.Int32
					b.OnNamespaceReset(func(prefix string) {
						if prefix == "victim:" {
							resets.Add(1)
						}
					})
					sentinel := errors.New("intrinsic DROP tail failed")
					calls := 0
					b.dropPrefixTailHook = func(prefix string) error {
						if prefix != "victim:" {
							return nil
						}
						calls++
						if phase == "before-final-sync" {
							if calls == 2 {
								return sentinel
							}
							return nil
						}
						// A batch of intrinsic cleanup can remove forward dictionaries
						// before failing, while numeric MVCC/reverse/index keys remain.
						if err := b.db.updateHeld(func(txn *badger.Txn) error {
							for _, key := range [][]byte{nodeIDForwardKey("victim:a"), nodeIDForwardKey("victim:b"), edgeIDForwardKey("victim:e"), schemaKey("victim")} {
								if err := txn.Delete(key); err != nil {
									return err
								}
							}
							return nil
						}); err != nil {
							return err
						}
						return sentinel
					}
					_, _, err = b.DeleteByPrefix("victim:")
					require.ErrorIs(t, err, sentinel)
					require.Zero(t, resets.Load(), "failed intrinsic cleanup must not publish a successful reset")
					intent := b.pendingNamespaceDrop("victim")
					require.NotNil(t, intent)
					require.Len(t, intent.nodes, 2)
					require.Len(t, intent.edges, 1)
					persisted, err := b.loadNamespaceDrops()
					require.NoError(t, err)
					if phase == "before-final-sync" {
						require.Empty(t, persisted, "pre-Sync fault follows visible final metadata; live admission must still stay fenced")
					} else {
						require.Len(t, persisted, 1)
						require.Equal(t, intent.nodes, persisted[0].nodes)
						require.Equal(t, intent.edges, persisted[0].edges)
					}
					_, err = b.CreateNode(&Node{ID: "victim:blocked"})
					require.ErrorIs(t, err, ErrConflict)
					require.ErrorIs(t, b.BulkCreateNodes([]*Node{{ID: "victim:bulk"}}), ErrConflict)
					require.ErrorIs(t, b.GetSchemaForNamespace("victim").AddUniqueConstraint("blocked_schema", "Blocked", "uid"), ErrConflict)
					for _, implicit := range []bool{false, true} {
						writer, err := b.BeginTransaction()
						require.NoError(t, err)
						err = writer.SetImplicit(implicit)
						if err == nil {
							_, err = writer.CreateNode(&Node{ID: "victim:transaction"})
						}
						if err == nil {
							err = writer.Commit()
						}
						_ = writer.Rollback()
						require.ErrorIs(t, err, ErrConflict)
					}
					payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "victim", LogicalID: "pending-accepted", Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "victim:accepted"}}}, Mode: CommitModeExplicit})
					require.NoError(t, err)
					envelope, operations, err := DecodeEntityTransaction(payload)
					require.NoError(t, err)
					receipt, err := SubmitAcceptedEntityTransaction(context.Background(), b, envelope, operations, nil)
					if receipt != nil {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						err = receipt.Wait(ctx)
						cancel()
					}
					require.ErrorIs(t, err, ErrConflict)
					_, err = b.CreateNode(&Node{ID: "peer:after"})
					require.NoError(t, err)
					b.dropPrefixTailHook = nil
					switch resolution {
					case "retry":
						nodes, edges, err := b.DeleteByPrefix("victim:")
						require.NoError(t, err)
						require.EqualValues(t, 2, nodes)
						require.EqualValues(t, 1, edges)
						require.EqualValues(t, 1, resets.Load())
					case "reopen":
						require.NoError(t, b.Close())
						b = open()
					case "restore":
						backup := filepath.Join(t.TempDir(), "pending.backup")
						require.NoError(t, b.Backup(backup))
						require.NoError(t, b.Restore(backup))
					}
					require.NoError(t, b.pendingNamespaceConflict("victim"))
					persisted, err = b.loadNamespaceDrops()
					require.NoError(t, err)
					require.Empty(t, persisted)
					require.NoError(t, b.db.viewHeld(func(txn *badger.Txn) error {
						it := txn.NewIterator(badgerIteratorOptions())
						defer it.Close()
						for it.Rewind(); it.Valid(); it.Next() {
							key := it.Item().Key()
							require.False(t, namespaceOwnsBadgerKey(key, []byte("victim:"), "victim", true, intent.nodes, intent.edges), "victim key remains: %x", key)
							require.False(t, isNamespaceDropIntentKey(key), "completed intent must not remain")
						}
						return nil
					}))
					count, err := b.NodeCountByPrefix("victim:")
					require.NoError(t, err)
					require.Zero(t, count)
					count, err = b.EdgeCountByPrefix("victim:")
					require.NoError(t, err)
					require.Zero(t, count)
					count, err = b.NodeCount()
					require.NoError(t, err)
					require.EqualValues(t, 2, count, "recovery counts actual peer rows, never subtracts old victim counts twice")
					count, err = b.EdgeCount()
					require.NoError(t, err)
					require.Zero(t, count)
					_, err = b.GetNode("peer:keep")
					require.NoError(t, err)
					_, err = b.GetNode("peer:after")
					require.NoError(t, err)
					require.Empty(t, b.GetSchemaForNamespace("victim").ExportDefinition().Constraints)
					_, err = b.CreateNode(&Node{ID: "victim:recreated"})
					require.NoError(t, err)
				})
			}
		}
	}
}

func TestNamespaceDropIntentUsesAtomicLargeWriter(t *testing.T) {
	b := newTestEngine(t)
	defer b.Close()
	lease, err := b.beginLifecycleWrite("")
	require.NoError(t, err)
	defer lease.finish()
	intent := &namespaceDropIntent{namespace: "large", nodes: make(map[uint64]struct{}), edges: make(map[uint64]struct{})}
	for id := uint64(1); id <= 800000; id++ {
		intent.nodes[id] = struct{}{}
	}
	sentinel := errors.New("intent batch interrupted")
	batches := 0
	restore := setLargeCommitBatchHook(func(int) error { batches++; return sentinel })
	defer restore()
	require.ErrorIs(t, b.writeNamespaceDropIntent(intent, &lease), sentinel)
	require.Positive(t, batches)
	persisted, err := b.loadNamespaceDrops()
	require.NoError(t, err)
	require.Empty(t, persisted)
	require.Nil(t, b.pendingNamespaceDrop("large"))
	require.NoError(t, b.db.viewHeld(func(txn *badger.Txn) error {
		prefix := []byte{prefixMVCCMeta, prefixMVCCMetaNamespaceDrop}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		it.Rewind()
		require.False(t, it.ValidForPrefix(prefix), "known-abort leaves neither head nor orphan victim chunks")
		return nil
	}))
	require.NoError(t, b.recoverNamespaceDrops(&lease), "an aborted preparation cannot schedule future deletion")

	restore()
	require.NoError(t, b.writeNamespaceDropIntent(intent, &lease))
	persisted, err = b.loadNamespaceDrops()
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Equal(t, intent.nodes, persisted[0].nodes)
	require.NoError(t, b.completeNamespaceDrop(intent, &lease))
	b.transitionPending(namespaceDropRetire, "large", nil)
	persisted, err = b.loadNamespaceDrops()
	require.NoError(t, err)
	require.Empty(t, persisted)
}
