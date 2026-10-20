package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestXHighMVCCAppendAdmission exercises the exported low-level MVCC writer
// contract after a committed DROP has failed intrinsic cleanup. Every target
// writer must observe the retained namespace fence until recovery completes.
func TestXHighMVCCAppendAdmission(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	op := NewNamespaceDrop(nil)
	require.NoError(t, engine.PrepareNamespaceDrop(context.Background(), "mvcc_a:", op))
	release, err := engine.beginSchemaWriteContext(context.Background())
	require.NoError(t, err)
	require.NoError(t, op.commitLocked(context.Background()))
	release()
	require.Error(t, op.physicalComplete(context.Background(), errors.New("injected cleanup failure")))

	version := MVCCVersion{CommitTimestamp: time.Now(), CommitSequence: 1}
	appendEngine := MVCCAppendEngine(engine)
	checks := map[string]func() error{
		"append-node-version": func() error {
			return appendEngine.AppendNodeVersion(&Node{ID: "mvcc_a:node"}, version)
		},
		"append-node-tombstone": func() error {
			return appendEngine.AppendNodeTombstone("mvcc_a:node", version)
		},
		"append-edge-version": func() error {
			return appendEngine.AppendEdgeVersion(&Edge{ID: "mvcc_a:edge", StartNode: "mvcc_a:node", EndNode: "mvcc_a:node", Type: "R"}, version)
		},
		"append-edge-tombstone": func() error {
			return appendEngine.AppendEdgeTombstone("mvcc_a:edge", version)
		},
		"update-node-head": func() error {
			return appendEngine.UpdateNodeCurrentHead("mvcc_a:node", version, false)
		},
		"update-edge-head": func() error {
			return appendEngine.UpdateEdgeCurrentHead("mvcc_a:edge", version, false)
		},
	}
	for name, write := range checks {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, write(), ErrConflict)
		})
	}

	// A peer namespace remains writable while the failed target is fenced.
	require.NoError(t, appendEngine.AppendNodeVersion(&Node{ID: "mvcc_b:peer"}, version))
}
