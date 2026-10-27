package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

// Author: #1667 implementation lane (Codex). Focused tests extracted from the
// label/preparation lane; no label-index or census implementation is required.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func labelMetadataBody(t *testing.T, b *BadgerEngine, size int, trap bool) []byte {
	t.Helper()
	var body []byte
	require.NoError(t, b.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		props, err := b.encodeTokenizedProperties(txn, "test", map[string]any{"payload": strings.Repeat("p", size)})
		if err != nil {
			return err
		}
		vector := make([]float32, size/4)
		envelope := map[string]any{
			"ID": "test:metadata", "Labels": []string{"Target"}, "VisibilitySuppressed": true,
			"EmbedMeta":       map[string]any{"payload": strings.Repeat("e", size)},
			"NamedEmbeddings": map[string][]float32{"default": vector},
			"ChunkEmbeddings": [][]float32{vector},
		}
		if trap {
			// Unknown extension: full decoding must fail; excluded values must never
			// reach its decoder. The tokenized properties have the same control.
			envelope["EmbedMeta"] = map[string]any{"trap": msgpack.RawMessage{0xd4, 99, 0}}
			props = []byte{1, 1, 0xc1}
		}
		metadata, err := encodeValue(envelope)
		if err != nil {
			return err
		}
		body = appendLabelMetadataUvarint([]byte{nodeFormatTokenizedV1}, uint64(len(props)))
		body = append(body, props...)
		body = append(body, metadata...)
		return txn.Set(nodeKey("test:metadata"), body)
	}))
	return body
}

func appendLabelMetadataUvarint(dst []byte, value uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	return append(dst, scratch[:n]...)
}

func TestNodeEnvelopeModes(t *testing.T) {
	b, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer b.Close()
	body := labelMetadataBody(t, b, 64, false)
	frame, err := SplitNodeEnvelope(body)
	require.NoError(t, err)
	properties, n := binary.Uvarint(body[1:])
	require.Positive(t, n)
	require.Same(t, &body[1+n], &frame.PropertyPayload()[0])
	require.Same(t, &body[1+n+int(properties)], &frame.MetadataPayload()[0])
	labels, err := decodeStoredNodeLabels(body)
	require.NoError(t, err)
	require.Equal(t, []string{"Target"}, labels)
	for name, read := range map[string]func([]byte) error{
		"metadata": func(data []byte) error { _, err := decodeStoredNodeLabels(data); return err },
		"selected": func(data []byte) error {
			node, err := b.decodeNodeProjected("test", data, map[string]struct{}{"payload": {}})
			if err == nil {
				require.Equal(t, strings.Repeat("p", 64), node.Properties["payload"])
			}
			return err
		},
		"full": func(data []byte) error {
			node, err := b.decodeNode("test", data)
			if err == nil {
				require.Len(t, node.NamedEmbeddings["default"], 16)
				require.Len(t, node.ChunkEmbeddings, 1)
				require.Equal(t, strings.Repeat("e", 64), node.EmbedMeta["payload"])
			}
			return err
		},
		"scan": func(data []byte) error {
			d := newProjectedNodeDecoder(b, StreamNodesOptions{Projection: []string{"payload"}})
			_, err := d.decode(nodeKey("test:metadata"), data)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, read(body))
			for _, data := range [][]byte{nil, {0}, {nodeFormatTokenizedV1, 0xff}, {nodeFormatTokenizedV1, 10, 0}, {nodeFormatTokenizedV1, 0}} {
				require.Error(t, read(data))
			}
		})
	}
	// Required metadata must be owned once the Badger value callback returns.
	clear(body)
	require.Equal(t, []string{"Target"}, labels)
}

func TestNodeEnvelopeWALRebinding(t *testing.T) {
	source := createTestBadgerEngine(t)
	_, err := source.CreateNode(&Node{ID: "test:wal", Properties: map[string]any{"alpha": int64(7)}})
	require.NoError(t, err)
	var body []byte
	require.NoError(t, source.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:wal"))
		if err == nil {
			body, err = item.ValueCopy(nil)
		}
		return err
	}))
	envelope, err := SplitNodeEnvelope(body)
	require.NoError(t, err)
	// This deliberately non-minimal integer encoding must survive foreign token
	// remapping exactly. The metadata is another excluded byte population.
	properties := []byte{1, 1, 0xd1, 0, 7}
	body = append(append([]byte{nodeFormatTokenizedV1, byte(len(properties))}, properties...), envelope.metadata...)
	bindings, err := bufferedWALPropertyBindings(map[string][]byte{string(nodeKey("test:wal")): body}, source.propKeyDict)
	require.NoError(t, err)
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			destination := createTestBadgerEngine(t)
			_, err := destination.CreateNode(&Node{ID: "test:sentinel", Properties: map[string]any{"beta": "keep"}})
			require.NoError(t, err)
			data := &WALBufferedCommitData{Namespace: "test", TxID: "envelope", Writes: map[string][]byte{string(nodeKey("test:wal")): body}}
			if legacy {
				data.namedPropertiesAuthoritative = true
				data.Ops = []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:wal", Properties: map[string]any{"alpha": int64(7)}}}}
			} else {
				data.PropertyBindings = bindings
			}
			commit, err := destination.importBufferedWALRecord(data, walForeignImport)
			require.NoError(t, err)
			got, err := SplitNodeEnvelope(commit.writes[string(nodeKey("test:wal"))])
			require.NoError(t, err)
			require.Equal(t, envelope.metadata, got.metadata, "WAL rebinding must preserve all excluded metadata bytes")
			count, n, err := readUvarint(got.properties)
			require.NoError(t, err)
			require.Equal(t, uint64(1), count)
			token, k, err := readUvarint(got.properties[n:])
			require.NoError(t, err)
			require.NotEqual(t, uint64(1), token, "destination dictionary must force rebinding")
			if !legacy {
				require.Equal(t, properties[2:], got.properties[n+k:], "excluded property value bytes must not be re-encoded")
			}
			require.Equal(t, properties, body[2:2+len(properties)], "source body remains immutable")
		})
	}
	for _, tc := range []struct {
		body []byte
		want string
	}{
		{nil, "wal: invalid tokenized node body"},
		{[]byte{0}, "wal: invalid tokenized node body"},
		{[]byte{nodeFormatTokenizedV1, 0xff}, "varint truncated"},
		{append([]byte{nodeFormatTokenizedV1}, bytes.Repeat([]byte{0xff}, 10)...), "varint truncated"},
		{append(append([]byte{nodeFormatTokenizedV1}, bytes.Repeat([]byte{0xff}, 9)...), 2), "varint overflow"},
		{[]byte{nodeFormatTokenizedV1, 10, 0}, "wal: truncated node properties"},
	} {
		_, splitErr := SplitNodeEnvelope(tc.body)
		require.Error(t, splitErr)
		_, err := rewriteWALPropertyPayloads(map[string][]byte{string(nodeKey("test:wal")): tc.body}, func(_, _ string, p []byte) ([]byte, error) {
			t.Fatal("transform must not see an invalid envelope")
			return p, nil
		})
		require.EqualError(t, err, tc.want)
	}
}
