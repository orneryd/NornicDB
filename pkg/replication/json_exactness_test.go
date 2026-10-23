package replication

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONIngestionReplicationFallback(t *testing.T) {
	node, err := decodeNodePayload([]byte(`{"id":"a","properties":{"v":9007199254740993,"d":2.0}}`))
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": int64(9007199254740993), "d": float64(2)}, node.Properties)
	edge, err := decodeEdgePayload([]byte(`{"id":"r","properties":{"v":-9007199254740993,"d":1.25}}`))
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"v": int64(-9007199254740993), "d": 1.25}, edge.Properties)
}
