// Author: NOR-288 implementation lane (Codex), 2026-10-26.
package server

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Interpose after WAL acceptance and native preparation, before physical apply.
// Embedding the actual native owner preserves its capabilities; no fake commit
// receipt or error is returned. The conflicting write uses the production
// access-metadata API that caused the original Badger read/write conflict.
type acceptedConflictNative struct {
	*storage.BadgerEngine
	interpose atomic.Pointer[func() error]
	injected  atomic.Int32
	accepted  atomic.Pointer[[]storage.Operation]
}

func (n *acceptedConflictNative) GetInnerEngine() storage.Engine { return n.BadgerEngine }

func (n *acceptedConflictNative) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if fn := n.interpose.Swap(nil); fn != nil {
		images := prepared.Operations()
		n.accepted.Store(&images)
		if err := (*fn)(); err != nil {
			return nil, err
		}
		n.injected.Add(1)
	}
	return n.BadgerEngine.SubmitEntityCommit(ctx, prepared, settle)
}

func TestAcceptedConflictSettlesThroughPublicCypher(t *testing.T) {
	for _, mutation := range []string{"set", "delete-recreate", "delete-recreate-set"} {
		t.Run(mutation, func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			base, err := storage.NewBadgerEngine(filepath.Join(dir, "data"))
			require.NoError(t, err)
			base.SetDecayEnabled(true)
			native := &acceptedConflictNative{BadgerEngine: base}
			walDir := filepath.Join(dir, "wal")
			wal, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
			require.NoError(t, err)
			owner := storage.NewWALEngine(native, wal)
			manager, err := multidb.NewDatabaseManager(t.Context(), owner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			name := manager.DefaultDatabaseName()
			store, err := manager.GetStorage(name)
			require.NoError(t, err)
			exec := cypher.NewStorageExecutor(store)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = exec.Execute(ctx, "CREATE (:Doc {id: 'n', payload: 'before'})", nil)
			require.NoError(t, err)
			nodes, err := storage.GetStoredNodesByLabel(store, "Doc")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			id := name + ":" + string(nodes[0].ID)
			require.NoError(t, base.PutAccessMeta(id, &knowledgepolicy.AccessMetaEntry{TargetID: id, TargetScope: knowledgepolicy.ScopeNode}))
			require.NoError(t, manager.SetDatabaseLimits(ctx, name, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
			inject := func() error {
				return base.PutAccessMeta(id, &knowledgepolicy.AccessMetaEntry{TargetID: id, TargetScope: knowledgepolicy.ScopeNode, MutationCount: 1, LastMutatedAt: time.Now().UnixNano()})
			}
			native.interpose.Store(&inject)
			acceptedRecords := countAcceptedConflictRecords(t, walDir)
			query := "MATCH (n:Doc {id: 'n'}) SET n.payload = 'after' RETURN n.payload"
			if mutation == "delete-recreate" {
				query = "MATCH (n:Doc {id: 'n'}) DELETE n CREATE (m:Doc {id: 'n', payload: 'after'}) RETURN m.payload"
			}
			if mutation == "delete-recreate-set" {
				query = "MATCH (n:Doc {id: 'n'}) DELETE n CREATE (m:Doc {id: 'n'}) SET m.payload = 'after' RETURN m.payload"
			}
			result, err := exec.Execute(ctx, query, nil)
			assert.NoError(t, err, "accepted command must finish native application before reporting success")
			require.Equal(t, int32(1), native.injected.Load(), "conflict schedule must cross the accepted native boundary once")
			if err == nil {
				assert.Equal(t, [][]interface{}{{"after"}}, result.Rows)
			}
			assert.Equal(t, acceptedRecords+1, countAcceptedConflictRecords(t, walDir), "native retry must not append a second accepted command")
			if mutation == "delete-recreate-set" && err == nil {
				var accepted *storage.Node
				for _, op := range *native.accepted.Load() {
					if op.Type == storage.OpCreateNode {
						accepted = op.Node
					}
				}
				require.NotNil(t, accepted)
				require.True(t, accepted.CreatedAt.IsZero(), "accepted unset timestamp must not be normalized to another instant")
				stored, err := storage.GetStoredNodesByLabel(store, "Doc")
				require.NoError(t, err)
				require.Len(t, stored, 1)
				assert.Equal(t, accepted.CreatedAt, stored[0].CreatedAt, "native retry must not initialize accepted image timestamps")
				assert.Equal(t, accepted.UpdatedAt, stored[0].UpdatedAt)
			}
			// A new public request must own the entity immediately after settlement.
			_, nextErr := exec.Execute(ctx, "MATCH (n:Doc {id: 'n'}) SET n.followup = 'writable'", nil)
			assert.NoError(t, nextErr, "settlement must release entity authority")
			count, countErr := store.NodeCount()
			assert.NoError(t, countErr)
			assert.EqualValues(t, 1, count, "native retry must account the replacement once")
			tracked, trackedNodes, trackedEdges := manager.GetStorageSize(context.Background(), name)
			liveRows, err := storage.GetStoredNodesByLabel(storage.NewNamespacedEngine(base, name), "Doc")
			require.NoError(t, err)
			require.Len(t, liveRows, 1)
			assert.NoError(t, manager.Close())

			// Native success and semantic replay must retain the same timestamp
			// image, including the accepted unset CreatedAt (NOR-303).
			t.Run("cold-replay", func(t *testing.T) {
				// Cold WAL replay has no old native frame: it must reproduce exactly
				// the success returned by the accepted request, with no duplicate rows.
				recovered, replay, err := storage.RecoverFromWALWithResult(walDir, "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = recovered.Close() })
				require.Empty(t, replay.Errors)
				rows, err := storage.GetStoredNodesByLabel(storage.NewNamespacedEngine(recovered, name), "Doc")
				require.NoError(t, err)
				require.Len(t, rows, 1)
				assert.Equal(t, "after", rows[0].Properties["payload"])
				assert.Equal(t, "writable", rows[0].Properties["followup"])
				recount, err := multidb.NewDatabaseManager(t.Context(), recovered, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = recount.Close() })
				exact, exactNodes, exactEdges := recount.GetStorageSize(context.Background(), name)
				assert.Equal(t, liveRows[0], rows[0], "cold replay preserves the identical stored image")
				assert.Equal(t, exact, tracked, "live quota bytes must match cold recovery's stored-image census")
				assert.Equal(t, exactNodes, trackedNodes)
				assert.Equal(t, exactEdges, trackedEdges)
			})
		})
	}
}

func countAcceptedConflictRecords(t *testing.T, dir string) int {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		if entry.Operation == storage.OpEntityCommit {
			count++
		}
	}
	return count
}
