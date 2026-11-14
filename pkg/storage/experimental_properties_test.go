package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func experimentalStorageFlag(t *testing.T, enabled bool) {
	t.Helper()
	previous := config.IsExperimentalPropertyStorageEnabled()
	config.SetExperimentalPropertyStorageEnabled(enabled)
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
}
func TestExperimentalPropertyValueRoundtrip(t *testing.T) {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i % 16)
	}
	for _, value := range []any{strings.Repeat("Nornic property documentation ", 4096), values, map[string]any{"nested": strings.Repeat("value", 512), "at": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}} {
		var raw bytes.Buffer
		enc := msgpack.NewEncoder(&raw)
		require.NoError(t, enc.Encode(encodeStoredTemporalValue(value)))
		encoded, err := encodeExperimentalProperty(value, raw.Bytes())
		require.NoError(t, err)
		require.True(t, isExperimentalProperty(encoded))
		require.Less(t, len(encoded), len(raw.Bytes()))
		decoded, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(encoded)))
		require.NoError(t, err)
		baseline, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(raw.Bytes())))
		require.NoError(t, err)
		require.Equal(t, restoreStoredTemporalValue(baseline), restoreStoredTemporalValue(decoded))
	}
	small := []byte{1}
	got, err := encodeExperimentalProperty(int64(1), small)
	require.NoError(t, err)
	require.Equal(t, small, got)
}

func TestExperimentalPropertyStoragePublicLifecycle(t *testing.T) {
	experimentalStorageFlag(t, true)
	ctx := context.Background()
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	text := strings.Repeat("Nornic documentation and property storage. ", 2048)
	ints := make([]int64, 1024)
	for i := range ints {
		ints[i] = int64(i % 64)
	}
	floats := []float64{0, math.Copysign(0, -1), math.Inf(1), math.Float64frombits(0x7ff8000000000001), math.SmallestNonzeroFloat64}
	props := map[string]any{"text": text, "small": int64(7), "numbers": ints, "floats": floats, "at": time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	_, err = engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: props})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:edge", StartNode: "test:a", EndNode: "test:b", Type: "RELATED", Properties: map[string]any{"text": text}}))
	require.NoError(t, engine.withView(func(tx *badger.Txn) error {
		item, e := tx.Get(nodeKey("test:a"))
		if e != nil {
			return e
		}
		return item.Value(func(body []byte) error {
			envelope, e := SplitNodeEnvelope(body)
			if e != nil {
				return e
			}
			reader := bytes.NewReader(envelope.properties)
			count, e := binary.ReadUvarint(reader)
			if e != nil {
				return e
			}
			dec := msgpack.NewDecoder(reader)
			packed := 0
			for i := uint64(0); i < count; i++ {
				if _, e = binary.ReadUvarint(reader); e != nil {
					return e
				}
				raw, e := dec.DecodeRaw()
				if e != nil {
					return e
				}
				if isExperimentalProperty(raw) {
					packed++
				}
			}
			require.Positive(t, packed, "public writer reaches fork codec")
			return nil
		})
	}))
	expectedMatches := 1
	check := func() {
		node, e := engine.GetNode(ctx, "test:a")
		require.NoError(t, e)
		require.Equal(t, text, node.Properties["text"])
		require.Equal(t, ints, node.Properties["numbers"])
		actual := node.Properties["floats"].([]float64)
		for i, v := range floats {
			require.Equal(t, math.Float64bits(v), math.Float64bits(actual[i]))
		}
		projected, e := engine.GetNodeProjected(ctx, "test:a", []string{"small"})
		require.NoError(t, e)
		require.Equal(t, map[string]any{"small": int64(7)}, projected.Properties)
		edge, e := engine.GetEdge(ctx, "test:edge")
		require.NoError(t, e)
		require.Equal(t, text, edge.Properties["text"])
		seen := 0
		require.NoError(t, engine.StreamNodesWithOptions(ctx, StreamNodesOptions{Projection: []string{"text"}, PropertyStringEquals: map[string]string{"text": text}, PropertyFilter: func(p map[string]any) bool { return p["text"] == text }}, func(node *Node) error { seen++; require.Equal(t, text, node.Properties["text"]); return nil }))
		require.Equal(t, expectedMatches, seen)
	}
	check()
	config.SetExperimentalPropertyStorageEnabled(false)
	check() // reads do not depend on the write flag.
	_, err = engine.CreateNode(ctx, &Node{ID: "test:plain", Properties: map[string]any{"text": text}})
	require.NoError(t, err)
	expectedMatches = 2
	var snapshot bytes.Buffer
	require.NoError(t, WriteSnapshot(ctx, engine, &snapshot, SnapshotOptions{}))
	visitor := &collectingSnapshotVisitor{}
	_, err = ReadSnapshot(ctx, bytes.NewReader(snapshot.Bytes()), visitor)
	require.NoError(t, err)
	require.Len(t, visitor.edges, 1)
	require.Equal(t, text, visitor.edges[0].Properties["text"])
	for _, n := range visitor.nodes {
		if n.ID == "test:a" {
			require.Equal(t, text, n.Properties["text"])
		}
	}
	backup := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, engine.Backup(backup))
	require.NoError(t, engine.Close())
	engine, err = NewBadgerEngine(dir)
	require.NoError(t, err)
	check()
	restored, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer restored.Close()
	require.NoError(t, restored.Restore(backup))
	node, err := restored.GetNode(ctx, "test:a")
	require.NoError(t, err)
	require.Equal(t, text, node.Properties["text"])
	require.Equal(t, ints, node.Properties["numbers"])
}

func TestExperimentalPropertyFlagOffPreservesBytes(t *testing.T) {
	experimentalStorageFlag(t, false)
	engine := createTestBadgerEngine(t)
	props := map[string]any{"text": strings.Repeat("text", 2048)}
	encoded := encodeTokenizedPropertiesForTest(t, engine, props)
	_, n, err := readUvarint(encoded)
	require.NoError(t, err)
	_, tokenSize, err := readUvarint(encoded[n:])
	require.NoError(t, err)
	require.False(t, isExperimentalProperty(encoded[n+tokenSize:]))
	decoded, err := engine.decodeTokenizedPropertiesProjected(nil, "ns", encoded, nil)
	require.NoError(t, err)
	require.Equal(t, props, decoded)
}

func TestExperimentalPropertyProjectionSkipsUnreadPayload(t *testing.T) {
	experimentalStorageFlag(t, true)
	engine := createTestBadgerEngine(t)
	var body []byte
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(tx *badger.Txn) error {
		var err error
		body, _, err = engine.encodeNodeInTxn(tx, "test", &Node{ID: "test:skip", Properties: map[string]any{"small": int64(7), "large": strings.Repeat("payload", 4096)}})
		return err
	}))
	envelope, err := SplitNodeEnvelope(body)
	require.NoError(t, err)
	reader := bytes.NewReader(envelope.properties)
	count, err := binary.ReadUvarint(reader)
	require.NoError(t, err)
	dec := msgpack.NewDecoder(reader)
	corrupted := false
	for i := uint64(0); i < count; i++ {
		_, err = binary.ReadUvarint(reader)
		require.NoError(t, err)
		raw, err := dec.DecodeRaw()
		require.NoError(t, err)
		if isExperimentalProperty(raw) {
			pos := bytes.Index(body, raw)
			require.NotEqual(t, -1, pos)
			body[pos+len(raw)-1] ^= 0xff
			corrupted = true
			break
		}
	}
	require.True(t, corrupted)
	projected, err := engine.decodeNodeProjected(nil, "test", body, map[string]struct{}{"small": {}})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"small": int64(7)}, projected.Properties)
	_, err = engine.decodeNodeProjected(nil, "test", body, nil)
	require.Error(t, err, "selected corrupted payload remains an error")
}
