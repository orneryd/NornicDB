package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Frozen reflection schema of the stored node map before runtime API migration.
// Do not derive it from Node: the wire contract must remain independent.
type historicalStoredNodeImage struct {
	ID                         NodeID
	Labels                     []string
	Properties                 map[string]any
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
	NamedEmbeddings            map[string][]float32
	ChunkEmbeddings            [][]float32
	EmbedMeta                  map[string]any
	EmbeddingsStoredSeparately bool
	VisibilitySuppressed       bool
}

type historicalTransportNodeImage struct {
	Database                   string
	ID                         NodeID
	Labels                     []string
	Properties                 transportPropertyMap
	CreatedAt                  transportTime
	UpdatedAt                  transportTime
	NamedEmbeddings            map[string][]float32
	ChunkEmbeddings            [][]float32
	EmbedMeta                  transportPropertyMap
	EmbeddingsStoredSeparately bool
	VisibilitySuppressed       bool
}

func TestNodeRuntimeFrozenPayloadCompatibility(t *testing.T) {
	dir := filepath.Join("testdata", "node-runtime-v4")
	raw, err := os.ReadFile(filepath.Join(dir, "sha256.json"))
	require.NoError(t, err)
	var hashes map[string]string
	require.NoError(t, json.Unmarshal(raw, &hashes))
	require.Len(t, hashes, 14)
	// Preserve scalar wire bytes; normalize only map-entry ordering.
	encode := func(value any) []byte {
		var output bytes.Buffer
		enc := msgpack.NewEncoder(&output).SetSortMapKeys(true)
		require.NoError(t, enc.Encode(value))
		return canonicalNodeWireMapOrder(t, output.Bytes())
	}
	for name, hash := range hashes {
		t.Run(name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			digest := sha256.Sum256(body)
			require.Equal(t, hash, hex.EncodeToString(digest[:]))
			if filepath.Ext(name) == ".bin" {
				serializer, payload, headered, err := splitSerializationHeader(body)
				require.NoError(t, err)
				require.True(t, headered)
				require.Equal(t, serializerIDMsgpack, serializer)
				var before historicalStoredNodeImage
				require.NoError(t, util.DecodeMsgpackBytes(payload, &before))
				node, err := decodeNodeV1(body)
				require.NoError(t, err)
				metadata := node.RuntimeMetadata()
				require.Empty(t, metadata.Database)
				require.Equal(t, before.EmbeddingsStoredSeparately, metadata.EmbeddingsStoredSeparately)
				require.Equal(t, before.VisibilitySuppressed, metadata.VisibilitySuppressed)
				// This compares all values and their MessagePack types, not just key names.
				require.Equal(t, encode(&before), encode(node))
				require.Equal(t, encode(&before), encode(*node), "value and pointer encoding agree")
				require.Nil(t, node.EmbeddingAttempt())
				require.Zero(t, node.EmbeddingRevision())
			} else {
				var before historicalTransportNodeImage
				require.NoError(t, util.DecodeMsgpackBytes(body, &before))
				node, err := DecodeNodePayload(body)
				require.NoError(t, err)
				metadata := node.RuntimeMetadata()
				require.Equal(t, before.Database, metadata.Database)
				require.Equal(t, before.EmbeddingsStoredSeparately, metadata.EmbeddingsStoredSeparately)
				require.Equal(t, before.VisibilitySuppressed, metadata.VisibilitySuppressed)
				require.Equal(t, encode(&before), encode((*transportNodeImage)(node)))
				require.Nil(t, node.EmbeddingAttempt())
				require.Zero(t, node.EmbeddingRevision())
			}
		})
	}
}

// canonicalNodeWireMapOrder preserves every scalar encoding and container header,
// including numeric widths, extensions, nil and empty values. Only map entries
// are sorted: typed maps and transport maps do not honor SetSortMapKeys.
func canonicalNodeWireMapOrder(t *testing.T, raw []byte) []byte {
	t.Helper()
	reader := bytes.NewReader(raw)
	dec := msgpack.NewDecoder(reader)
	code, err := dec.PeekCode()
	require.NoError(t, err)
	isMap := msgpcode.IsFixedMap(code) || code == msgpcode.Map16 || code == msgpcode.Map32
	isArray := msgpcode.IsFixedArray(code) || code == msgpcode.Array16 || code == msgpcode.Array32
	if !isMap && !isArray {
		return raw
	}
	var count int
	if isMap {
		count, err = dec.DecodeMapLen()
	} else {
		count, err = dec.DecodeArrayLen()
	}
	require.NoError(t, err)
	header := append([]byte(nil), raw[:len(raw)-reader.Len()]...)
	if isMap {
		type entry struct{ key, value []byte }
		entries := make([]entry, count)
		for i := range entries {
			key, err := dec.DecodeRaw()
			require.NoError(t, err)
			value, err := dec.DecodeRaw()
			require.NoError(t, err)
			entries[i] = entry{canonicalNodeWireMapOrder(t, key), canonicalNodeWireMapOrder(t, value)}
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		for i, e := range entries {
			if i > 0 {
				require.NotEqual(t, entries[i-1].key, e.key, "duplicate map key")
			}
			header = append(header, e.key...)
			header = append(header, e.value...)
		}
	} else {
		for i := 0; i < count; i++ {
			value, err := dec.DecodeRaw()
			require.NoError(t, err)
			header = append(header, canonicalNodeWireMapOrder(t, value)...)
		}
	}
	require.Zero(t, reader.Len(), "trailing wire data")
	return header
}

func TestNodeWireComparisonPreservesEncodedTypes(t *testing.T) {
	a := []byte{0x82, 0xa1, 'b', 0xc3, 0xa1, 'a', 0xcc, 1}
	b := []byte{0x82, 0xa1, 'a', 0xcc, 1, 0xa1, 'b', 0xc3}
	require.Equal(t, canonicalNodeWireMapOrder(t, a), canonicalNodeWireMapOrder(t, b))
	changed := append([]byte(nil), b...)
	changed[3] = 0xd0
	require.NotEqual(t, canonicalNodeWireMapOrder(t, a), canonicalNodeWireMapOrder(t, changed), "unsigned and signed numeric encodings differ")
	require.NotEqual(t, canonicalNodeWireMapOrder(t, []byte{0xc0}), canonicalNodeWireMapOrder(t, []byte{0x90}), "nil and empty arrays differ")
}

func TestNodeRuntimeReadsHistoricalGobMetadata(t *testing.T) {
	historical := storedNodeGob{Database: "legacy-db", ID: "legacy:n", Labels: []string{"Doc"}, Properties: map[string]any{"text": "retained"}, EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}
	var body bytes.Buffer
	require.NoError(t, gob.NewEncoder(&body).Encode(historical))
	node, err := decodeNodeV1(body.Bytes())
	require.NoError(t, err)
	require.Equal(t, NodeRuntimeMetadata{Database: "legacy-db", EmbeddingsStoredSeparately: true, VisibilitySuppressed: true}, node.RuntimeMetadata())
	require.Equal(t, historical.ID, node.ID)
	require.Equal(t, historical.Properties, node.Properties)
	require.Zero(t, node.EmbeddingRevision())
	require.Nil(t, node.EmbeddingAttempt())
}
