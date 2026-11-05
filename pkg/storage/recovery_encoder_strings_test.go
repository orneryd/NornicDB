package storage

import (
	"bytes"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/stretchr/testify/require"
	"testing"
)

// The same test qualifies current release and candidate through production
// readers. MessagePack map ordering is not a stored-format contract.
func TestRecoveryEncoderLosslessStrings(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	for _, value := range []string{high, low, high + "x" + low, "café_😀", `literal\ud800`, "escaped_\\\"quote\"\t\n"} {
		t.Run(value, func(t *testing.T) {
			node, edge := recoveryFieldFixture()
			node.ID = NodeID("nornic:" + value)
			node.Labels = []string{value}
			node.Properties = map[string]any{value: value, "large": int64(9007199254740993)}
			node.EmbedMeta = map[string]any{value: value}
			edge.ID, edge.StartNode, edge.EndNode, edge.Type = EdgeID(value), node.ID, node.ID, value
			edge.Properties = node.Properties
			for _, input := range []any{node, edge} {
				encoder := getValueEncoder()
				retained, err := encoder.recovery(input)
				require.NoError(t, err)
				encoded := bytes.Clone(retained)
				putValueEncoder(encoder)
				if _, ok := input.(*Node); ok {
					var got Node
					require.NoError(t, decodeRecoveryPayload(encoded, &got))
					assertRecoveryNodeFields(t, node, &got)
				} else {
					var got Edge
					require.NoError(t, decodeRecoveryPayload(encoded, &got))
					assertRecoveryEdgeFields(t, edge, &got)
				}
			}
			var buffer bytes.Buffer
			for _, destination := range []*bytes.Buffer{nil, &buffer} {
				encoded, err := marshalWALRecoveryPayload(destination, WALNodeData{Node: node, OldNode: node})
				require.NoError(t, err)
				var got WALNodeData
				require.NoError(t, decodeRecoveryPayload(encoded, &got))
				assertRecoveryNodeFields(t, node, got.Node)
				assertRecoveryNodeFields(t, node, got.OldNode)
				control := WALTxData{TxID: value}
				encoded, err = marshalWALRecoveryPayload(destination, control)
				require.NoError(t, err)
				var restored WALTxData
				require.NoError(t, jsontext.Unmarshal(encoded, &restored))
				require.Equal(t, control, restored)
			}
			var collection bytes.Buffer
			require.NoError(t, writeSnapshotCollection(&collection, &Snapshot{Sequence: 12, Nodes: []*Node{node}, Edges: []*Edge{edge}}))
			visitor := &collectingSnapshotVisitor{}
			metadata, err := ReadSnapshot(&collection, visitor)
			require.NoError(t, err)
			require.Equal(t, uint64(12), metadata.Sequence)
			require.Len(t, visitor.nodes, 1)
			require.Len(t, visitor.edges, 1)
			assertRecoveryNodeFields(t, node, visitor.nodes[0])
			assertRecoveryEdgeFields(t, edge, visitor.edges[0])
		})
	}
}
