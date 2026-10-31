package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDirectWritesOrderAfterBufferedEndpoints: a direct relationship delete,
// a direct relationship move and a direct node delete read and write the
// relationship's endpoints (their labels for the positional counts, their
// adjacency). A relabel of such an endpoint the write-behind buffer
// acknowledged before the write is applied first, so the write sees the new
// labels and no replay runs under it; the counts end exact.
func TestDirectWritesOrderAfterBufferedEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name     string
		relabel  NodeID
		write    func(b *BadgerEngine) error
		start    map[string]int64
		end      map[string]int64
		typeLeft int64
	}{
		{
			name:    "relationship delete over a relabelled start",
			relabel: "test:n0",
			write:   func(b *BadgerEngine) error { return b.DeleteEdge("test:r0") },
			start:   map[string]int64{"A": 0, "C": 0},
			end:     map[string]int64{"B": 0},
		},
		{
			name:    "relationship move off a relabelled start",
			relabel: "test:n0",
			write: func(b *BadgerEngine) error {
				return b.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "test:r0", StartNode: "test:n2", EndNode: "test:n1", Type: "LINK"}})
			},
			start:    map[string]int64{"A": 0, "C": 0, "D": 1},
			end:      map[string]int64{"B": 1},
			typeLeft: 1,
		},
		{
			name:    "node delete cascading to a relabelled far end",
			relabel: "test:n1",
			write:   func(b *BadgerEngine) error { return b.DeleteNode("test:n0") },
			start:   map[string]int64{"A": 0},
			end:     map[string]int64{"B": 0, "C": 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			for id, label := range map[NodeID]string{"test:n0": "A", "test:n1": "B", "test:n2": "D"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{label}})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:r0", StartNode: "test:n0", EndNode: "test:n1", Type: "LINK"}))
			require.NoError(t, b.FlushWriteBehind(nil))

			buffered, err := implicitWrite(b, func(tx *BadgerTransaction) error {
				return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: tc.relabel, Labels: []string{"C"}}, Policy: ClientNodeEdits})
			})
			require.NoError(t, err)
			require.True(t, buffered, "the relabel is acknowledged, not yet applied")

			require.NoError(t, tc.write(b), "a direct write over an endpoint's acknowledged commit is refused")
			require.False(t, b.writeBehind.holdsEntities(writtenEntities{nodes: []NodeID{tc.relabel}}),
				"the direct write ran before the endpoint's acknowledged relabel was applied")
			require.NoError(t, b.FlushWriteBehind(nil))

			for label, want := range tc.start {
				got, err := b.EdgeCountByStartLabelInNamespace("test", label, "LINK")
				require.NoError(t, err)
				require.Equal(t, want, got, "relationships starting at %s", label)
			}
			for label, want := range tc.end {
				got, err := b.EdgeCountByEndLabelInNamespace("test", label, "LINK")
				require.NoError(t, err)
				require.Equal(t, want, got, "relationships ending at %s", label)
			}
			total, err := b.EdgeCountByTypeInNamespace("test", "LINK")
			require.NoError(t, err)
			require.Equal(t, tc.typeLeft, total)
		})
	}
}
