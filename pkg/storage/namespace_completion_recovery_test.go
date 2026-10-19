package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIntrinsicFailureRemainsReplayable(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, failure := range []string{"physical", "catalog", "callback", "none"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
			require.NoError(t, err)
			var wal *WAL
			t.Cleanup(func() {
				if wal != nil {
					require.NoError(t, wal.Close())
				}
				if base != nil {
					require.NoError(t, base.Close())
				}
			})
			wal, err = NewWAL(dir+"/wal", DefaultWALConfig())
			require.NoError(t, err)
			ctx := context.Background()
			record := &Node{ID: "system:databases:metadata", Properties: map[string]any{"type": "databases"}}
			value := json.RawMessage(`{"id":"old","name":"recover_a"}`)
			require.NoError(t, PublishNamespaceCatalogContext(ctx, base, NamespaceCatalogMutation{Namespace: "recover_a", Record: record, Create: true, Value: value}))
			for _, id := range []NodeID{"recover_a:first", "recover_a:remaining", "recover_b:peer"} {
				_, err := base.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			fault := errors.New("intrinsic physical cleanup I/O failure")
			op := NewNamespaceDrop(func() error {
				if failure == "callback" {
					return fault
				}
				return nil
			})
			op.nativeWAL = wal
			op.SetCatalogMutation(NamespaceCatalogMutation{Namespace: "recover_a", Record: record, Expected: value})
			require.NoError(t, base.PrepareNamespaceDrop(ctx, "recover_a:", op))
			release, err := base.beginSchemaWriteContext(ctx)
			require.NoError(t, err)
			err = op.commitLocked(ctx)
			release()
			require.NoError(t, err)
			require.True(t, op.Committed())
			require.NotZero(t, op.nativeSequence)
			sequence := op.nativeSequence
			// Reproduce an interrupted physical cleanup after real WAL admission:
			// one body is gone while another still needs the committed DROP.
			require.NoError(t, base.db.DropPrefix(append([]byte{prefixNode}, []byte("recover_a:first")...)))
			var physicalErr error
			if failure == "physical" {
				physicalErr = fault
			} else {
				require.NoError(t, base.db.DropPrefix(append([]byte{prefixNode}, []byte("recover_a:")...)))
			}
			if failure == "catalog" {
				// Make the intrinsic catalog RMW fail while the completion-key
				// store remains writable. Repair that injected fault before reopen.
				catalog, err := base.GetNode(record.ID)
				require.NoError(t, err)
				catalog.Properties["data"] = "{"
				require.NoError(t, base.UpdateNode(catalog))
			}
			err = op.physicalComplete(ctx, physicalErr)
			if failure == "none" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				var outcome *NamespaceDropError
				require.ErrorAs(t, err, &outcome)
				require.True(t, outcome.Committed)
			}
			intrinsicFailed := failure == "physical" || failure == "catalog"
			done, err := base.namespaceCompleted("recover_a", sequence)
			require.NoError(t, err)
			require.Equal(t, !intrinsicFailed, done, "only successful intrinsic cleanup may be skipped by recovery")
			if failure == "catalog" {
				catalog, err := base.GetNode(record.ID)
				require.NoError(t, err)
				catalog.Properties["data"] = `{"recover_a":{"id":"old","name":"recover_a"}}`
				require.NoError(t, base.UpdateNode(catalog))
			}
			if !intrinsicFailed {
				_, err := base.CreateNode(&Node{ID: "recover_a:fresh"})
				require.NoError(t, err)
			}
			require.NoError(t, wal.Close())
			wal = nil
			require.NoError(t, base.Close())
			base = nil
			base, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
			require.NoError(t, err)
			wal, err = NewWAL(dir+"/wal", DefaultWALConfig())
			require.NoError(t, err)
			_, err = RecoverBufferedCommits(base, wal)
			require.NoError(t, err)
			done, err = base.namespaceCompleted("recover_a", sequence)
			require.NoError(t, err)
			require.True(t, done, "restart must complete the admitted operation")
			_, err = base.GetNode("recover_a:remaining")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = base.GetNode("recover_b:peer")
			require.NoError(t, err)
			catalog, err := base.GetNode(record.ID)
			require.NoError(t, err)
			require.NotContains(t, catalog.Properties["data"], "recover_a")
			if !intrinsicFailed {
				_, err := base.GetNode("recover_a:fresh")
				require.NoError(t, err, "a callback error must not replay an already completed DROP")
			}
		})
	}
}
