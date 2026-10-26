package enginecontract

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NodeBatchPoint checks generated ID sets against actual point operations on
// the same issued view. It observes public data and source identity, omits only
// point not-found, and preserves a required point failure as an operation error.
// Author: Lane #1644 (Codex); invariant decided by Nornic2 (orchestrator).
func NodeBatchPoint(t *testing.T, ids []storage.NodeID, batch func([]storage.NodeID) (map[storage.NodeID]*storage.Node, error), point func(storage.NodeID) (*storage.Node, error)) {
	t.Helper()
	type image struct {
		ID         storage.NodeID
		Database   string
		Labels     []string
		Properties map[string]interface{}
		EmbedMeta  map[string]any
		Chunks     [][]float32
		Named      map[string][]float32
		Suppressed bool
	}
	images := func(nodes map[storage.NodeID]*storage.Node) map[storage.NodeID]image {
		out := make(map[storage.NodeID]image, len(nodes))
		for id, node := range nodes {
			require.NotNil(t, node)
			out[id] = image{node.ID, node.Database, node.Labels, node.Properties, node.EmbedMeta, node.ChunkEmbeddings, node.NamedEmbeddings, node.VisibilitySuppressed}
		}
		return out
	}
	// Every current adapter supplies a small independently seeded population.
	// Larger future fixtures still traverse every ID, with bounded subset work.
	sets := 1 << min(len(ids), 8)
	for mask := 0; mask < sets; mask++ {
		for _, reverse := range []bool{false, true} {
			var request []storage.NodeID
			for i, id := range ids {
				if mask&(1<<(i%8)) != 0 {
					request = append(request, id)
				}
			}
			if len(request) > 0 {
				request = append(request, request[0])
			}
			if reverse {
				for l, r := 0, len(request)-1; l < r; l, r = l+1, r-1 {
					request[l], request[r] = request[r], request[l]
				}
			}
			want := map[storage.NodeID]*storage.Node{}
			var wantErr error
			for _, id := range request {
				node, err := point(id)
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if err != nil {
					wantErr = err
					break
				}
				want[id] = node
			}
			got, err := batch(request)
			if wantErr != nil {
				require.ErrorIs(t, err, wantErr)
				continue
			}
			require.NoError(t, err, "ids=%v", request)
			require.Equal(t, images(want), images(got), "ids=%v", request)
		}
	}
}
