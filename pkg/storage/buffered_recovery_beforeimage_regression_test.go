package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRecoveredUpdateDeleteReleasesIntermediateUniqueValue(t *testing.T) {
	for _, mode := range []string{"startup", "replay"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
			wal := engine.bufferedCommitWAL
			require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "unique_v", Type: ConstraintUnique, Label: "L", Properties: []string{"v"}}))
			_, err := engine.CreateNode(&Node{ID: "test:original", Labels: []string{"L"}, Properties: map[string]interface{}{"v": "old"}})
			require.NoError(t, err)
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.SetNamespace("test"))
			node, err := tx.GetNode("test:original")
			require.NoError(t, err)
			node.Properties["v"] = "transient"
			require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
			require.NoError(t, tx.DeleteGraphNode(node.ID, CascadeEdges))
			require.NoError(t, tx.Commit())
			require.True(t, tx.AcknowledgedBuffered())
			entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
			require.NoError(t, err)
			crashClose(t, engine, wal)
			recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
			wal2 := recovered.bufferedCommitWAL
			t.Cleanup(func() { require.NoError(t, wal2.Close()); require.NoError(t, recovered.Close()) })
			if mode == "startup" {
				n, err := RecoverBufferedCommits(recovered, wal2)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			} else {
				for _, entry := range entries {
					if entry.Operation == OpBufferedCommit {
						require.NoError(t, ReplayWALEntry(recovered, entry))
					}
				}
			}
			_, err = recovered.GetNode("test:original")
			require.ErrorIs(t, err, ErrNotFound)
			for _, value := range []string{"old", "transient"} {
				_, err = recovered.CreateNode(&Node{ID: NodeID("test:reuse-" + value), Labels: []string{"L"}, Properties: map[string]interface{}{"v": value}})
				require.NoError(t, err, "deleted intermediate value %q must be reusable after recovery", value)
			}
		})
	}
}
