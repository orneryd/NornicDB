package storage

// projectedNodeDecoder: the scan-scoped decoder of projected node bodies
// (#857) resolves key tokens per database, decodes only the projected
// properties, allocates nothing for the node map of a rejected node and
// rejects a node whose stored value is not a required string by its bytes.

import (
	"context"
	"encoding/binary"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestProjectedNodeDecoderAcrossDatabases(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	day := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	// Database a allocates "name" before "id", database b the reverse, so the
	// same name has different tokens in each; database c never uses "id".
	for _, node := range []*Node{
		{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "x", "id": "k1", "at": day}},
		{ID: "a:2", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "y", "id": "k2"}},
		{ID: "b:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "k1", "name": "z"}},
		{ID: "c:1", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "k1"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	stream := func(opts StreamNodesOptions) map[NodeID]map[string]interface{} {
		got := map[NodeID]map[string]interface{}{}
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			got[node.ID] = node.Properties
			return nil
		}))
		return got
	}
	isK1 := func(props map[string]interface{}) bool { return props["id"] == "k1" }
	require.Equal(t, map[NodeID]map[string]interface{}{
		"a:1": {"id": "k1"},
		"b:1": {"id": "k1"},
	}, stream(StreamNodesOptions{Projection: []string{"id"}, PropertyFilter: isK1}))
	got := stream(StreamNodesOptions{Projection: []string{"id", "at"}})
	require.Len(t, got, 4)
	at, ok := got["a:1"]["at"].(time.Time)
	require.True(t, ok)
	require.True(t, at.Equal(day), "temporal values are restored")
	require.Empty(t, got["c:1"], "a database without the key has no projected properties")
	ids := make([]string, 0, len(got))
	for id := range got {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	require.Equal(t, []string{"a:1", "a:2", "b:1", "c:1"}, ids)
}

func TestProjectedNodeDecoderRejectedNodeAllocations(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(&Node{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 7, "name": "x", "body": "some body text"}})
	require.NoError(t, err)
	var key, body []byte
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("a:1"))
		if err != nil {
			return err
		}
		key = item.KeyCopy(nil)
		body, err = item.ValueCopy(nil)
		return err
	}))
	decoder := newProjectedNodeDecoder(engine, StreamNodesOptions{Projection: []string{"id"}, PropertyFilter: func(props map[string]interface{}) bool { return props["id"] == int64(8) }})
	node, err := decoder.decode(key, body)
	require.NoError(t, err)
	require.Nil(t, node, "rejected")
	// An integer value is boxed without allocating; the rejected node costs no
	// map, reader or node ID.
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = decoder.decode(key, body)
	})
	require.LessOrEqual(t, allocs, 1.0)

	// Corrupt property lists are errors.
	for name, props := range map[string][]byte{
		"count varint":  {0xff},
		"too few":       {0x02, 0x01, 0x07},
		"token varint":  {0x01, 0xff},
		"skip value":    {0x01, 0x7f, 0xc1},
		"bad value":     {0x01, byte(decoderToken(t, engine, "a", "id")), 0xc1},
		"trailing data": {0x00, 0x01},
	} {
		propsLen, n := binary.Uvarint(body[1:])
		data := append([]byte{nodeFormatTokenizedV1, byte(len(props))}, props...)
		data = append(data, body[1+n+int(propsLen):]...)
		_, err := decoder.decode(key, data)
		require.Error(t, err, name)
	}
}

func decoderToken(t *testing.T, engine *BadgerEngine, namespace, name string) uint64 {
	t.Helper()
	token, ok := engine.propKeyDict.lookupID(namespace, name)
	require.True(t, ok)
	return token
}

// msgpackStringEquals compares a stored value's bytes with a string: equal
// only for a msgpack string (any of its four header sizes) with those bytes.
func TestMsgpackStringEquals(t *testing.T) {
	for _, length := range []int{0, 5, 31, 32, 255, 256, 65535, 65536} {
		text := strings.Repeat("a", length)
		raw, err := msgpack.Marshal(text)
		require.NoError(t, err)
		require.True(t, msgpackStringEquals(raw, text), length)
		require.True(t, msgpackStringEquals(append(raw, 0x01), text), "trailing properties are not read: %d", length)
		require.False(t, msgpackStringEquals(raw, text+"b"), length)
		if length > 0 {
			require.False(t, msgpackStringEquals(raw, strings.Repeat("b", length)), length)
			require.False(t, msgpackStringEquals(raw[:len(raw)-1], text), "truncated: %d", length)
		}
	}
	for name, value := range map[string]interface{}{"int": 7, "bytes": []byte("7"), "nil": nil, "list": []string{"7"}} {
		raw, err := msgpack.Marshal(value)
		require.NoError(t, err)
		require.False(t, msgpackStringEquals(raw, "7"), name)
	}
	require.False(t, msgpackStringEquals(nil, ""))
	for _, header := range [][]byte{{0xd9}, {0xda, 0x00}, {0xdb, 0x00, 0x00, 0x00}} {
		require.False(t, msgpackStringEquals(header, ""), "truncated header % x", header)
	}
}

// PropertyStringEquals skips a node whose stored value is not the string,
// whatever its type, without decoding it; a node without the property is left
// to the PropertyFilter.
func TestProjectedNodeDecoderPropertyStringEquals(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	long := strings.Repeat("k", 300)
	for id, props := range map[NodeID]map[string]interface{}{
		"a:hit":   {"name": "x", "id": "k1"},
		"a:case":  {"id": "K1"},
		"a:long":  {"id": long},
		"a:int":   {"id": 7},
		"a:time":  {"id": time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)},
		"a:list":  {"id": []string{"k1"}},
		"a:none":  {"name": "k1"},
		"a:empty": {},
		"b:hit":   {"id": "k1"},
		"b:other": {"id": "k2"},
	} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: props})
		require.NoError(t, err)
	}
	stream := func(opts StreamNodesOptions) []string {
		var ids []string
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			ids = append(ids, string(node.ID))
			return nil
		}))
		sort.Strings(ids)
		return ids
	}
	equals := func(text string) StreamNodesOptions {
		return StreamNodesOptions{Projection: []string{"id", "name"}, PropertyStringEquals: map[string]string{"id": text}}
	}
	require.Equal(t, []string{"a:empty", "a:hit", "a:none", "b:hit"}, stream(equals("k1")))
	require.Equal(t, []string{"a:empty", "a:long", "a:none"}, stream(equals(long)))
	require.Equal(t, []string{"a:empty", "a:none"}, stream(equals("7")))
	withFilter := equals("k1")
	withFilter.PropertyFilter = func(props map[string]interface{}) bool { return props["id"] != nil }
	require.Equal(t, []string{"a:hit", "b:hit"}, stream(withFilter))

	var key, body []byte
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("a:case"))
		if err != nil {
			return err
		}
		key = item.KeyCopy(nil)
		body, err = item.ValueCopy(nil)
		return err
	}))
	decoder := newProjectedNodeDecoder(engine, equals("k1"))
	node, err := decoder.decode(key, body)
	require.NoError(t, err)
	require.Nil(t, node, "rejected")
	require.Zero(t, testing.AllocsPerRun(100, func() {
		_, _ = decoder.decode(key, body)
	}), "a node rejected by its bytes allocates nothing")

	// A body without a property list has nothing to reject.
	propsLen, n := binary.Uvarint(body[1:])
	bare := append([]byte{nodeFormatTokenizedV1, 0x00}, body[1+n+int(propsLen):]...)
	node, err = decoder.decode(key, bare)
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Empty(t, node.Properties)
}
