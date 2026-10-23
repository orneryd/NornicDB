package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Existing change-log records use the general storage container decoder, not
// tokenized node bodies. Their property maps obey the same integer/list policy.
func TestNodeCachePropertyReaderLegacyChangeEvent(t *testing.T) {
	b := createTestBadgerEngine(t)
	properties := map[string]any{"n": int(3), "list": []int{128, 3}, "nested": map[string]any{"n": int8(-1)}}
	event := ChangeEvent{CommitSeq: 1, Seq: 1, Node: true, ID: "legacy", Before: &ChangeState{Properties: properties}, After: &ChangeState{Properties: properties}}
	body, err := encodeValue(event)
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(noNodeCacheWrites(), func(txn *badger.Txn) error {
		return txn.Set(changeEventKey("test", 1, 1), body)
	}))
	events, err := b.ChangeEvents("test", 0, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, map[string]any{"n": int64(3), "list": []int64{128, 3}, "nested": map[string]any{"n": int64(-1)}}, events[0].After.Properties)
	require.Equal(t, events[0].After.Properties, events[0].Before.Properties)
}

func TestNodeCachePropertyReaderLegacyEdge(t *testing.T) {
	body, err := encodeEdgeCompactV1(&Edge{ID: "test:legacy", Properties: map[string]any{"n": int(3), "list": []int{128, 3}}}, 1, 2)
	require.NoError(t, err)
	edge, _, _, err := decodeEdgeCompactV1(body)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"n": int64(3), "list": []int64{128, 3}}, edge.Properties)
}

func TestNodeCachePropertyReaderLegacyJSONSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"nodes":[{"id":"test:legacy","properties":{"n":3,"list":[128,3]}}]}`), 0600))
	snapshot, err := LoadSnapshot(path)
	require.NoError(t, err)
	require.Len(t, snapshot.Nodes, 1)
	require.Equal(t, map[string]any{"n": int64(3), "list": []int64{128, 3}}, snapshot.Nodes[0].Properties)
}

func TestNodeCachePropertyReaderRecoveryContainers(t *testing.T) {
	properties := map[string]any{"n": int(3), "list": []int{128, 3}}
	for name, source := range map[string]any{
		"node": &Node{ID: "test:legacy", Properties: properties},
		"edge": &Edge{ID: "test:legacy", Properties: properties},
	} {
		t.Run(name, func(t *testing.T) {
			body, err := encodeValue(source)
			require.NoError(t, err)
			var got map[string]any
			if name == "node" {
				var value Node
				require.NoError(t, decodeRecoveryPayload(body, &value))
				got = value.Properties
			} else {
				var value Edge
				require.NoError(t, decodeRecoveryPayload(body, &value))
				got = value.Properties
			}
			require.Equal(t, map[string]any{"n": int64(3), "list": []int64{128, 3}}, got)
		})
	}
}
