package storage

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/stretchr/testify/require"
)

func TestLosslessLegacyRecoveryEntityPopulation(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	low := string(cyphertext.AppendCodepoint(nil, 0xdc00))
	properties := map[string]any{high: low, low: high, "large": int64(9007199254740993), `literal\ud800`: `literal\ud800`}
	node := &Node{ID: NodeID("node_" + high), Labels: []string{"label_" + high}, Properties: properties}
	edge := &Edge{ID: EdgeID("edge_" + low), StartNode: node.ID, EndNode: node.ID, Type: "type_" + low, Properties: properties}
	cases := []struct {
		name          string
		value, target any
	}{
		{"node", node, &Node{}}, {"edge", edge, &Edge{}},
		{"node-images", &WALNodeData{Node: node, OldNode: node}, &WALNodeData{}}, {"edge-images", &WALEdgeData{Edge: edge, OldEdge: edge}, &WALEdgeData{}},
		{"delete-images", &WALDeleteData{OldNode: node, OldEdge: edge}, &WALDeleteData{}},
		{"bulk-nodes", &WALBulkNodesData{Nodes: []*Node{node, nil}}, &WALBulkNodesData{}}, {"bulk-edges", &WALBulkEdgesData{Edges: []*Edge{edge, nil}}, &WALBulkEdgesData{}},
		{"bulk-delete-nodes", &WALBulkDeleteNodesData{OldNodes: []*Node{node, nil}}, &WALBulkDeleteNodesData{}}, {"bulk-delete-edges", &WALBulkDeleteEdgesData{OldEdges: []*Edge{edge, nil}}, &WALBulkDeleteEdgesData{}},
		{"snapshot", &Snapshot{Nodes: []*Node{node}, Edges: []*Edge{edge}}, &Snapshot{}},
		{"buffered", &WALBufferedCommitData{Namespace: "db_" + high, Ops: []Operation{{Node: node, OldNode: node, Edge: edge, OldEdge: edge}}}, &WALBufferedCommitData{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := jsontext.Marshal(tc.value)
			require.NoError(t, err)
			require.NoError(t, decodeLegacyRecoveryJSON(bytes.NewReader(raw), tc.target))
			seen := 0
			transformRecoveryEntities(tc.target, func(got *Node) *Node {
				if got != nil {
					seen++
					require.Equal(t, node.ID, got.ID)
					require.Equal(t, node.Labels, got.Labels)
					require.Equal(t, properties, got.Properties)
					require.Nil(t, got.EmbedMeta)
				}
				return got
			}, func(got *Edge) *Edge {
				if got != nil {
					seen++
					require.Equal(t, edge.ID, got.ID)
					require.Equal(t, edge.Type, got.Type)
					require.Equal(t, properties, got.Properties)
				}
				return got
			})
			require.Positive(t, seen)
			require.Error(t, decodeLegacyRecoveryJSON(bytes.NewReader(append(raw, []byte(" null")...)), tc.target))
		})
	}
}

func TestLosslessWALHeaderAndPropertyNames(t *testing.T) {
	high := string(cyphertext.AppendCodepoint(nil, 0xd800))
	for _, name := range []string{"ordinary_café", `literal\ud800`, "db_" + high} {
		entry := &WALEntry{Sequence: 17, Database: name, Data: []byte{0, 1, 255}, Operation: OpCreateNode}
		var buffer bytes.Buffer
		payload, err := encodeWALEntryV3(&buffer, entry)
		require.NoError(t, err)
		restored, err := decodeWALEntry(payload, walFormatVersionV3)
		require.NoError(t, err)
		require.Equal(t, name, restored.Database)
		require.Equal(t, entry.Data, restored.Data)
		require.Equal(t, entry.Sequence, restored.Sequence)
	}
	source := WALBufferedCommitData{Namespace: "db_" + high, PropKeys: []WALPropKeyEntry{{Namespace: "db_" + high, Name: high, ID: 7}, {Namespace: "db_" + high, Name: `literal\ud800`, ID: 8}}}
	raw, err := jsontext.Marshal(source)
	require.NoError(t, err)
	var target WALBufferedCommitData
	require.NoError(t, decodeLegacyRecoveryJSON(bytes.NewReader(raw), &target))
	require.Equal(t, source.Namespace, target.Namespace)
	require.Equal(t, source.PropKeys, target.PropKeys)
}

func BenchmarkLosslessWALHeaderJSON(b *testing.B) {
	value := &WALEntry{Sequence: 17, Database: "ordinary_database", Operation: OpCreateNode, Data: nil}
	b.Run("stdlib", func(b *testing.B) {
		var buffer bytes.Buffer
		for i := 0; i < b.N; i++ {
			buffer.Reset()
			if err := json.NewEncoder(&buffer).Encode(value); err != nil {
				b.Fatal(err)
			}
			buffer.Truncate(buffer.Len() - 1)
		}
	})
	b.Run("canonical", func(b *testing.B) {
		var buffer bytes.Buffer
		for i := 0; i < b.N; i++ {
			if _, err := marshalJSONCompact(&buffer, value); err != nil {
				b.Fatal(err)
			}
		}
	})
}
