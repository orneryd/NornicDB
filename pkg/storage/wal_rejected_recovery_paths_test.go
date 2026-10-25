package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// These public entrypoints share replay owners but have distinct snapshot,
// transaction and streaming entrances. Every one must exclude rejected work.
func TestWALRejectedWorkRecoveryPaths(t *testing.T) {
	for _, phase := range []string{"before call", "native bulk"} {
		for _, baseline := range []string{"none", "legacy JSON", "materialized", "streaming"} {
			for _, recovery := range []string{"result", "ordinary", "logger", "transactions", "into engine", "entry replay", "native backup"} {
				if (recovery == "entry replay" || recovery == "native backup") && baseline != "none" {
					continue
				}
				t.Run(phase+"/"+baseline+"/"+recovery, func(t *testing.T) {
					t.Cleanup(config.WithWALEnabled())
					root := t.TempDir()
					dir := filepath.Join(root, "wal")
					wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
					require.NoError(t, err)
					b, err := NewBadgerEngine(filepath.Join(root, "source"))
					require.NoError(t, err)
					w := NewWALEngine(b, wal)
					t.Cleanup(func() { _ = w.Close() })
					snapshotPath := ""
					if baseline != "none" {
						snapshotPath = filepath.Join(root, "baseline.snapshot")
						if baseline == "streaming" {
							require.NoError(t, SaveStreamingSnapshot(context.Background(), b, snapshotPath, SnapshotOptions{}))
						} else {
							snapshot, err := wal.CreateSnapshot(b)
							require.NoError(t, err)
							if baseline == "legacy JSON" {
								data, err := json.Marshal(snapshot)
								require.NoError(t, err)
								require.NoError(t, os.WriteFile(snapshotPath, data, 0600))
							} else {
								require.NoError(t, SaveSnapshot(snapshot, snapshotPath))
							}
						}
					}
					if phase == "before call" {
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						_, err = w.CreateNodeWithContext(ctx, &Node{ID: "test:rejected", Properties: map[string]interface{}{"marker": "rejected"}})
						require.ErrorIs(t, err, context.Canceled)
					} else {
						ctx := preparationContext(t, "createBulkEntitiesWithContext", 2)
						ctx.additionalOwner = "submitDecodedEntityTransactionWithPolicy"
						nodes := make([]*Node, 20)
						for i := range nodes {
							nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:rejected%d", i)), Properties: map[string]interface{}{"marker": "rejected"}}
						}
						require.ErrorIs(t, w.BulkCreateNodesWithContext(ctx, nodes), context.Canceled)
						require.GreaterOrEqual(t, ctx.hits, 2)
					}
					// A live cancellable caller exercises the same prepared producer's
					// successful path and its retained WAL submit/receipt owner.
					live, cancel := context.WithCancel(context.Background())
					defer cancel()
					_, err = w.CreateNodeWithContext(live, &Node{ID: "test:healthy", Properties: map[string]interface{}{"marker": "healthy"}})
					require.NoError(t, err)
					backup := filepath.Join(root, "native.backup")
					if recovery == "native backup" {
						require.NoError(t, w.Backup(backup))
					}
					require.NoError(t, w.Close())
					var destination Engine
					switch recovery {
					case "result":
						var replay ReplayResult
						destination, replay, err = RecoverFromWALWithResult(dir, snapshotPath)
						require.NoError(t, err)
						require.Empty(t, replay.Errors)
					case "ordinary":
						destination, err = RecoverFromWAL(dir, snapshotPath)
					case "logger":
						destination, err = RecoverFromWALWithLogger(dir, snapshotPath, discardWALSlog())
					case "transactions":
						var replay *TransactionRecoveryResult
						destination, replay, err = RecoverWithTransactions(dir, snapshotPath)
						require.NoError(t, err)
						require.False(t, replay.HasErrors(), "%+v", replay)
					case "into engine", "entry replay":
						destination = newTestMemoryEngine(t)
						if recovery == "into engine" {
							replay, status, recoverErr := RecoverIntoEngine(destination, dir, snapshotPath)
							require.NoError(t, recoverErr)
							require.Empty(t, replay.Errors)
							require.Equal(t, baseline == "streaming" || baseline == "materialized", status.SnapshotStreaming)
						} else {
							entries, readErr := ReadWALEntriesFromDir(dir)
							require.NoError(t, readErr)
							for _, entry := range entries {
								require.NoError(t, ReplayWALEntry(destination, entry))
							}
						}
					case "native backup":
						var restored *BadgerEngine
						restored, err = NewBadgerEngine(filepath.Join(root, "restored"))
						require.NoError(t, err)
						destination = restored
						err = restored.Restore(backup)
					}
					require.NoError(t, err)
					require.NotNil(t, destination)
					t.Cleanup(func() { _ = destination.Close() })
					nodes, err := destination.AllNodes()
					require.NoError(t, err)
					t.Logf("phase=%s baseline=%s recovery=%s nodes=%d", phase, baseline, recovery, len(nodes))
					require.Len(t, nodes, 1, "rejected work cannot become replayable or restorable")
					require.Equal(t, "healthy", nodes[0].Properties["marker"])
				})
			}
		}
	}
}
