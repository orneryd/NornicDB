package storage

// projectedNodeDecoder: the scan-scoped decoder of projected node bodies
// (#857) resolves key tokens per database, decodes only the projected
// properties and allocates nothing for the node map of a rejected node.

import (
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
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
	decoder := newProjectedNodeDecoder(engine, []string{"id"}, func(props map[string]interface{}) bool { return props["id"] == int64(8) })
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
