package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type semanticResettingProperty int64

func (semanticResettingProperty) PropertyValueKind() string { return "point" }
func (p semanticResettingProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	buffer, ok := enc.Writer().(*bytes.Buffer)
	if !ok {
		return errors.New("expected property buffer")
	}
	buffer.Reset()
	if err := enc.Encode(int64(p)); err != nil {
		return err
	}
	enc.UseCompactInts(true)
	return nil
}

type semanticInvalidProperty struct{}

func (semanticInvalidProperty) PropertyValueKind() string { return "point" }
func (semanticInvalidProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.Encode(int64(1)); err != nil {
		return err
	}
	return enc.Encode(int64(2))
}

type semanticArrayProperty struct{}

func (semanticArrayProperty) PropertyValueKind() string { return "point" }
func (semanticArrayProperty) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(1); err != nil {
		return err
	}
	return enc.Encode(int64(3))
}

type semanticRegisteredProperty int64

func (semanticRegisteredProperty) PropertyValueKind() string { return "point" }

func init() {
	msgpack.RegisterExtEncoder(99, semanticRegisteredProperty(0), func(enc *msgpack.Encoder, value reflect.Value) ([]byte, error) {
		buffer, ok := enc.Writer().(*bytes.Buffer)
		if !ok {
			return nil, errors.New("expected property buffer")
		}
		buffer.Reset()
		return []byte{byte(value.Int())}, nil
	})
	msgpack.RegisterExtDecoder(99, semanticRegisteredProperty(0), func(dec *msgpack.Decoder, value reflect.Value, length int) error {
		if length != 1 {
			return fmt.Errorf("invalid extension length %d", length)
		}
		var data [1]byte
		if err := dec.ReadFull(data[:]); err != nil {
			return err
		}
		value.SetInt(int64(data[0]))
		return nil
	})
}

func TestSemanticTransactionIsolatesCustomPropertyEncoders(t *testing.T) {
	stamp := time.Date(2024, 3, 5, 10, 20, 30, 123456789, time.UTC)
	node := &Node{ID: "test:n", Labels: []string{"Probe"}, Properties: map[string]any{
		"custom":    semanticResettingProperty(42),
		"extension": semanticRegisteredProperty(19),
		"nested":    map[string]any{"custom": semanticResettingProperty(7), "int": int32(9)},
		"list":      []any{semanticResettingProperty(5), "end"},
		"emptyList": []any(nil), "emptyMap": map[string]any(nil), "when": stamp,
	}, EmbedMeta: map[string]any{"custom": semanticResettingProperty(12), "other": "preserved"}}
	edge := &Edge{ID: "test:e", StartNode: "test:n", EndNode: "test:m", Type: "LINK", Properties: map[string]any{"custom": semanticResettingProperty(8), "other": int64(258)}}
	payload, err := EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}, {Type: OpCreateEdge, Edge: edge}})
	require.NoError(t, err)
	_, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	got := ops[0].Node
	require.Equal(t, []string{"Probe"}, got.Labels)
	require.Equal(t, int64(42), got.Properties["custom"])
	require.Equal(t, semanticRegisteredProperty(19), got.Properties["extension"])
	require.Equal(t, map[string]any{"custom": int64(7), "int": int64(9)}, got.Properties["nested"])
	require.Equal(t, []any{int64(5), "end"}, got.Properties["list"])
	require.Equal(t, []any{}, got.Properties["emptyList"])
	require.Equal(t, map[string]any{}, got.Properties["emptyMap"])
	require.True(t, stamp.Equal(got.Properties["when"].(time.Time)))
	require.Equal(t, int64(12), got.EmbedMeta["custom"])
	require.Equal(t, "preserved", got.EmbedMeta["other"])
	require.Equal(t, int64(8), ops[1].Edge.Properties["custom"])
	require.Equal(t, int64(258), ops[1].Edge.Properties["other"])

	// Custom encoders cannot leave flags on the enclosing encoder: a sibling
	// keeps the same MessagePack bytes it has when encoded independently.
	var raw struct {
		Changes []struct {
			Edge struct{ Properties map[string]msgpack.RawMessage }
		}
	}
	require.NoError(t, msgpack.Unmarshal(payload, &raw))
	want, err := msgpack.Marshal(int64(258))
	require.NoError(t, err)
	require.Equal(t, want, []byte(raw.Changes[1].Edge.Properties["other"]))
}

func TestSemanticTransactionRejectsMalformedCustomValue(t *testing.T) {
	for _, value := range []any{semanticInvalidProperty{}, []any{semanticInvalidProperty{}}, map[string]any{"x": semanticInvalidProperty{}}} {
		node := &Node{ID: "test:n", Properties: map[string]any{"bad": value}}
		_, err := EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}})
		require.ErrorIs(t, err, ErrInvalidData)
	}
}

func TestSemanticTransactionCustomPropertyDepthBoundary(t *testing.T) {
	value := any(semanticResettingProperty(3))
	for range 1024 {
		value = []any{value}
	}
	node := &Node{ID: "test:n", Properties: map[string]any{"x": value}}
	payload, err := EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}})
	require.NoError(t, err)
	_, _, err = DecodeEntityTransaction(payload)
	require.NoError(t, err)
	node.Properties["x"] = []any{value}
	_, err = EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}})
	require.ErrorContains(t, err, "nesting depth")

	// A typed encoder can itself emit a container; count that container
	// against the same depth limit instead of returning an unreadable payload.
	value = semanticArrayProperty{}
	for range 1023 {
		value = []any{value}
	}
	node.Properties["x"] = value
	payload, err = EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}})
	require.NoError(t, err)
	_, _, err = DecodeEntityTransaction(payload)
	require.NoError(t, err)
	node.Properties["x"] = []any{value}
	_, err = EncodeEntityTransaction("test", "probe", []Operation{{Type: OpCreateNode, Node: node}})
	require.ErrorIs(t, err, ErrInvalidData)
	require.ErrorContains(t, err, "nesting depth")
}

// Commit preparation currently normalizes this test value before WAL
// serialization. Keep the public write and crash recovery as a control in
// addition to the direct transaction codec regression above.
func TestSemanticTransactionCustomPropertyWALRecovery(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, manualDrain: true})
	wal := engine.bufferedCommitWAL
	tx, err := BeginEntityTransaction(engine, CommitModeImplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"custom": semanticResettingProperty(42), "other": int64(17)}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), "custom-recovery")
	require.NoError(t, err)
	receipt, err := NewWALEngine(engine, wal).SubmitEntityCommit(context.Background(), prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(context.Background()))
	crashClose(t, engine, wal)
	engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, manualDrain: true})
	_, err = RecoverBufferedCommitsAtOpen(engine, engine.bufferedCommitWAL)
	require.NoError(t, err)
	got, err := engine.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, int64(42), got.Properties["custom"])
	require.Equal(t, int64(17), got.Properties["other"])
}
