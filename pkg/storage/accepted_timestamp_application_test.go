// Author: NOR-288 lane (Codex), 2026-10-29.
package storage_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAcceptedDecodedTimestampsSurviveNativeRestore(t *testing.T) {
	for _, at := range []time.Time{{}, time.Unix(0, 0).UTC(), time.Unix(0, -1).UTC(), time.Unix(0, 1).UTC()} {
		t.Run(at.Format(time.RFC3339Nano), func(t *testing.T) {
			base, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			// Native compact edges cannot represent unset/year1. That case is
			// outside this assertion; every representable edge instant remains
			// exact, and unset node timestamps remain unset rather than 'now'.
			edgeTime := at
			if at.IsZero() {
				edgeTime = time.Unix(1, 0).UTC()
			}
			ops := []storage.Operation{
				{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:n", CreatedAt: at, UpdatedAt: at}},
				{Type: storage.OpCreateEdge, Edge: &storage.Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:n", Type: "SELF", CreatedAt: edgeTime, UpdatedAt: edgeTime}},
			}
			// This is the public decoded accepted-command entry. Its consumer
			// must not repeat client CREATE timestamp initialization. The raw
			// bytes decoder's inherited distinctions are measured separately.
			receipt, err := storage.SubmitAcceptedEntityTransaction(t.Context(), base,
				storage.EntityTransactionEnvelope{Version: storage.EntityTransactionVersion, Namespace: "test", LogicalTxID: "decoded-time", Mode: storage.CommitModeExplicit}, ops, nil)
			require.NoError(t, err)
			require.NoError(t, receipt.Wait(t.Context()))
			check := func(engine storage.Engine) {
				t.Helper()
				node, err := storage.GetStoredNode(engine, "test:n")
				require.NoError(t, err)
				edge, err := storage.GetStoredEdge(engine, "test:e")
				require.NoError(t, err)
				for _, pair := range [][2]time.Time{{at, node.CreatedAt}, {at, node.UpdatedAt}, {edgeTime, edge.CreatedAt}, {edgeTime, edge.UpdatedAt}} {
					require.Equal(t, acceptedTimestampBytes(t, pair[0]), acceptedTimestampBytes(t, pair[1]))
				}
			}
			check(base)
			backup := filepath.Join(t.TempDir(), "timestamps.backup")
			require.NoError(t, base.Backup(backup))
			restored, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = restored.Close() })
			require.NoError(t, restored.Restore(backup))
			check(restored)
			next := fmt.Sprintf("writable-%d", at.UnixNano())
			require.NoError(t, base.UpdateNode(&storage.Node{ID: "test:n", Properties: map[string]any{"v": next}}))
		})
	}
}
