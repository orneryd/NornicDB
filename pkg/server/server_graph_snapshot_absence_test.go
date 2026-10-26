package server

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type snapshotNodeErrorEngine struct {
	*graphSnapshotIndexEngine
	err error
}

func (e *snapshotNodeErrorEngine) GetNodeVisibleAt(storage.NodeID, storage.MVCCVersion) (*storage.Node, error) {
	return nil, e.err
}

// Both graph reconstruction endpoints use this owner. Absence at the requested
// version omits the node; a refusal or a hard read error must reach the caller.
func TestCollectSnapshotInducedSubgraph_NodeAbsenceAndErrors(t *testing.T) {
	server := &Server{}
	for _, tc := range []struct {
		name   string
		err    error
		absent bool
	}{
		{"missing", storage.ErrNotFound, true},
		{"not_yet_visible", storage.ErrNotVisibleAtSnapshot, true},
		{"wrapped_not_yet_visible", fmt.Errorf("snapshot: %w", storage.ErrNotVisibleAtSnapshot), true},
		{"refusal", storage.ErrNotImplemented, false},
		{"hard_error", fmt.Errorf("snapshot read failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &snapshotNodeErrorEngine{graphSnapshotIndexEngine: &graphSnapshotIndexEngine{}, err: tc.err}
			collection, err := server.collectSnapshotInducedSubgraph(engine, []string{"node"}, storage.MVCCVersion{}, newGraphFilterSet(nil, nil))
			if tc.absent {
				require.NoError(t, err)
				require.Empty(t, collection.nodes)
				require.Empty(t, collection.edges)
			} else {
				require.ErrorIs(t, err, tc.err)
			}
			require.Zero(t, engine.getEdgesByTypeCalls)
		})
	}
}
