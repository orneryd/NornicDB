package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// flagChangingProperty models a custom value that leaves encoder flags set.
// Each following property must still use the same defaults as Marshal.
type flagChangingProperty int64

func (value flagChangingProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.Encode(int64(value)); err != nil {
		return err
	}
	enc.UseCompactInts(true)
	return nil
}

// Typed properties may supply their own serialization and use the writer
// exposed by MessagePack. Their buffer must contain only that property's value.
type resettingTypedProperty int64

func (resettingTypedProperty) PropertyValueKind() string { return "point" }

func (value resettingTypedProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	writer, ok := enc.Writer().(*bytes.Buffer)
	if !ok {
		return fmt.Errorf("expected the value's buffer, got %T", enc.Writer())
	}
	writer.Reset()
	return enc.Encode(int64(value))
}

func TestTokenizedPropertyCustomEncoderOwnsValueBuffer(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:custom-value", Properties: map[string]any{
		"custom": resettingTypedProperty(42),
		"other":  int64(17),
	}})
	require.NoError(t, err)
	node, err := engine.GetStoredNode("test:custom-value")
	require.NoError(t, err)
	require.Equal(t, int64(42), node.Properties["custom"])
	require.Equal(t, int64(17), node.Properties["other"])
}

func TestTokenizedPropertyNestedCustomEncoderPreservesContainers(t *testing.T) {
	engine := newTestEngine(t)
	properties := map[string]any{
		"list":   []any{int64(17), resettingTypedProperty(42)},
		"nested": map[string]any{"custom": resettingTypedProperty(7), "other": "kept"},
	}
	_, err := engine.CreateNode(&Node{ID: "test:nested", Properties: properties})
	require.NoError(t, err)
	node, err := engine.GetStoredNode("test:nested")
	require.NoError(t, err)
	require.Equal(t, []int64{17, 42}, node.Properties["list"])
	require.Equal(t, map[string]any{"custom": int64(7), "other": "kept"}, node.Properties["nested"])

	_, err = engine.CreateNode(&Node{ID: "test:other"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:edge", StartNode: "test:nested", EndNode: "test:other", Type: "LINK", Properties: properties}))
	edge, err := engine.GetStoredEdge("test:edge")
	require.NoError(t, err)
	require.Equal(t, []int64{17, 42}, edge.Properties["list"])
	require.Equal(t, map[string]any{"custom": int64(7), "other": "kept"}, edge.Properties["nested"])
}

type malformedTokenizedProperty struct{}

func (malformedTokenizedProperty) PropertyValueKind() string { return "point" }
func (malformedTokenizedProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.Encode(int64(1)); err != nil {
		return err
	}
	return enc.Encode(int64(2))
}

func TestTokenizedPropertyRejectsMalformedCustomValue(t *testing.T) {
	engine := newTestEngine(t)
	for _, value := range []any{malformedTokenizedProperty{}, []any{malformedTokenizedProperty{}}, map[string]any{"x": malformedTokenizedProperty{}}} {
		_, err := engine.CreateNode(&Node{ID: "test:bad", Properties: map[string]any{"bad": value}})
		require.ErrorIs(t, err, ErrInvalidData)
	}
}

func TestTokenizedPropertyEncodingMatchesMarshalBytes(t *testing.T) {
	engine := newTestEngine(t)
	values := []any{
		nil, true, "", "Northwind", int64(-1), int64(math.MaxInt64), uint64(math.MaxUint64),
		float32(1.25), math.Float64frombits(0x7ff8000000000042), []byte{0, 1, 255},
		[]string{"one", "two"}, []int64{-1, 42}, []any{int64(1), "two"},
		map[string]any{"one": map[string]any{"nested": int64(7)}},
		time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("offset", -7*60*60)),
	}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		for _, properties := range []map[string]any{nil, {}} {
			encoded, err := engine.encodeTokenizedProperties(txn, "test", properties)
			require.NoError(t, err)
			require.Equal(t, []byte{0}, encoded)
		}
		for _, value := range values {
			encoded, err := engine.encodeTokenizedProperties(txn, "test", map[string]any{"value": value})
			if err != nil {
				return err
			}
			count, n := binary.Uvarint(encoded)
			require.Equal(t, uint64(1), count)
			require.Positive(t, n)
			_, idBytes := binary.Uvarint(encoded[n:])
			require.Positive(t, idBytes)
			want, err := msgpack.Marshal(encodeStoredTemporalValue(value))
			if err != nil {
				return err
			}
			require.Equal(t, want, encoded[n+idBytes:], "value type %T", value)
		}
		encoded, err := engine.encodeTokenizedProperties(txn, "test", map[string]any{"bad": make(chan int)})
		require.ErrorContains(t, err, "marshaling property")
		require.Nil(t, encoded)
		return nil
	}))
}

func TestTokenizedPropertyEncoderResetsBetweenValues(t *testing.T) {
	engine := newTestEngine(t)
	properties := map[string]any{"first": flagChangingProperty(17), "second": flagChangingProperty(258)}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		encoded, err := engine.encodeTokenizedProperties(txn, "test", properties)
		if err != nil {
			return err
		}
		count, n := binary.Uvarint(encoded)
		require.Equal(t, uint64(len(properties)), count)
		require.Positive(t, n)
		remaining := encoded[n:]
		for i := uint64(0); i < count; i++ {
			id, used := binary.Uvarint(remaining)
			require.Positive(t, used)
			remaining = remaining[used:]
			name, ok := engine.propKeyDict.lookup("test", id)
			require.True(t, ok)
			reader := bytes.NewReader(remaining)
			_, err := msgpack.NewDecoder(reader).DecodeInterface()
			require.NoError(t, err)
			valueBytes := len(remaining) - reader.Len()
			want, err := msgpack.Marshal(properties[name])
			require.NoError(t, err)
			require.Equal(t, want, remaining[:valueBytes], "encoder flags leaked into property %q", name)
			remaining = remaining[valueBytes:]
		}
		require.Empty(t, remaining)
		return nil
	}))
}
