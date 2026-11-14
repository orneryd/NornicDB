package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/experimental/columnar"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestExperimentalColumnLayoutUsesSharedFlag(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			experimentalStorageFlag(t, enabled)
			dir := t.TempDir()
			engine, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			ctx := context.Background()
			for i := 0; i < 128; i++ {
				_, err = engine.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:column-%03d", i)), Properties: map[string]any{"count": int64(i), "status": "current"}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.Close())
			file, err := os.Open(filepath.Join(dir, "MANIFEST"))
			require.NoError(t, err)
			manifest, _, err := badger.ReplayManifestFile(file, 0, badger.DefaultOptions(dir))
			require.NoError(t, file.Close())
			require.NoError(t, err)
			require.NotEmpty(t, manifest.Tables)
			for _, table := range manifest.Tables {
				require.Equal(t, enabled, table.Compression == options.NornicColumnar)
			}
			config.SetExperimentalPropertyStorageEnabled(false)
			engine, err = NewBadgerEngine(dir)
			require.NoError(t, err)
			for i := 0; i < 128; i++ {
				node, err := engine.GetNode(ctx, NodeID(fmt.Sprintf("test:column-%03d", i)))
				require.NoError(t, err)
				require.Equal(t, map[string]any{"count": int64(i), "status": "current"}, node.Properties)
			}
		})
	}
}

func TestExperimentalColumnLayoutFields(t *testing.T) {
	experimentalStorageFlag(t, true)
	engine := createTestBadgerEngine(t)
	node := &Node{ID: "test:column", Labels: []string{"Doc"}, Properties: map[string]any{"count": int64(17), "name": "document", "active": true}}
	var body []byte
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(tx *badger.Txn) error {
		var err error
		body, _, err = engine.encodeNodeInTxn(tx, "test", node)
		return err
	}))
	spans := experimentalColumnLayout(nodeKey(node.ID), body)
	require.NotEmpty(t, spans)
	previous := 0
	properties := 0
	for _, span := range spans {
		require.GreaterOrEqual(t, span.Start, previous)
		require.LessOrEqual(t, span.Start+span.Length, len(body))
		previous = span.Start + span.Length
		if span.Kind == prefixNode|0x80 {
			name, ok := engine.propKeyDict.lookup("test", span.Column)
			require.True(t, ok)
			raw, err := msgpack.Marshal(node.Properties[name])
			require.NoError(t, err)
			require.Equal(t, raw, body[span.Start:span.Start+span.Length])
			properties++
		}
	}
	require.Equal(t, len(node.Properties), properties)
	edge := &Edge{Type: "RELATED", Confidence: math.Float64frombits(0x7ff8000000000001)}
	header := encodeEdgeCompactHeader(edgeFormatCompactV2, edge, 123, 456, 0)
	adjacency := experimentalColumnLayout([]byte{prefixOutgoingIndex}, header)
	require.Len(t, adjacency, 7)
	require.Equal(t, uint64(123), binary.BigEndian.Uint64(header[adjacency[1].Start:]))
	require.Equal(t, uint64(456), binary.BigEndian.Uint64(header[adjacency[2].Start:]))
	require.Equal(t, math.Float64bits(edge.Confidence), binary.BigEndian.Uint64(header[adjacency[5].Start:]))
	// The physical codec reconstructs application records without knowing names.
	var original []byte
	var columns []columnar.Span
	for i := 0; i < 16; i++ {
		offset := len(original)
		original = append(original, body...)
		for _, span := range spans {
			span.Start += offset
			columns = append(columns, span)
		}
	}
	transformed, err := columnar.Transpose(original, columns)
	require.NoError(t, err)
	require.NotNil(t, transformed)
	restored, err := columnar.Restore(transformed)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, restored))
}
func TestExperimentalColumnLayoutDeclinesMalformed(t *testing.T) {
	badEdge := make([]byte, 42)
	badEdge[0] = edgeFormatCompactV2
	badEdge = binary.AppendUvarint(badEdge, ^uint64(0))
	for _, test := range []struct{ key, value []byte }{{nil, nil}, {[]byte{prefixNode}, []byte("bad")}, {[]byte{prefixEdge}, badEdge}, {[]byte{prefixSchema}, []byte("opaque")}} {
		require.NotPanics(t, func() { require.Nil(t, experimentalColumnLayout(test.key, test.value)) })
	}
}
