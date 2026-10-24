package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Regression coverage (, storage write entry). Integer policy at the codec owner:
// every on-disk encoding of a signed-range integer list must decode to the
// shape the cache-shape owner publishes for the same value.
func TestPropertyWriteEntryCodecListPolicy(t *testing.T) {
	for name, input := range map[string]any{
		"control-3-128-m129":   []int{3, 128, -129},
		"uint8-first-128-3":    []int{128, 3},
		"int16-first-m129-128": []int{-129, 128},
		"uint-only-200-300":    []int{200, 300},
		"iface-128-3":          []any{int(128), int(3)},
		"nested-map-list":      map[string]any{"l": []any{int(128), int(3)}},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := msgpack.Marshal(input) // legacy (pre-PR) encoder: raw value
			require.NoError(t, err)
			cold, err := decodeStrictTypedValue(msgpack.NewDecoder(bytes.NewReader(encoded)))
			require.NoError(t, err)
			warm := normalizePropertyValueShape(cold) // cache fill normalizes the decoded body
			written := normalizePropertyValueShape(input)
			t.Logf("cold=%#v warm=%#v written=%#v", cold, warm, written)
			if !reflect.DeepEqual(cold, warm) {
				t.Errorf("COLD!=WARM for legacy bytes: cold=%#v warm=%#v", cold, warm)
			}
			if !reflect.DeepEqual(cold, written) {
				t.Errorf("COLD!=CANONICAL: cold=%#v canonical=%#v", cold, written)
			}
		})
	}
}

func TestPropertyWriteEntryNormalizerPolicy(t *testing.T) {
	for name, tc := range map[string]struct{ in, want any }{
		"uint64-scalar":      {uint64(5), int64(5)},
		"uint64-list":        {[]any{uint64(5), uint64(6)}, []int64{5, 6}},
		"mixed-width-list":   {[]any{uint8(128), int8(3)}, []int64{128, 3}},
		"uint-then-int-list": {[]any{uint64(5), int64(3)}, []int64{5, 3}},
		"overflow-kept-list": {[]any{uint64(math.MaxUint64)}, []uint64{math.MaxUint64}},
		"float-int-distinct": {[]any{float64(3), int64(3)}, []any{float64(3), int64(3)}},
	} {
		t.Run(name, func(t *testing.T) {
			once := normalizePropertyValueShape(tc.in)
			twice := normalizePropertyValueShape(once)
			if !reflect.DeepEqual(once, twice) {
				t.Errorf("NOT IDEMPOTENT: once=%#v twice=%#v", once, twice)
			}
			if !reflect.DeepEqual(once, tc.want) {
				t.Errorf("POLICY: got %#v want %#v", once, tc.want)
			}
		})
	}
}

// End to end: a node body written by the pre-PR encoder (raw msgpack values)
// is read cold, then warm, then inside a transaction.
func TestPropertyWriteEntryLegacyBodyColdWarm(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID(prefixTestID("r73-legacy"))
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{
		"xs": []int64{1}, "ys": []int64{1}, "zs": []int64{1}, "s": int64(1),
	}})
	require.NoError(t, err)
	legacy := map[string]any{"xs": []int{128, 3}, "ys": []int{-129, 128}, "zs": []int{3, 128, -129}, "s": int(128)}
	namespace := namespaceForNodeID(id)
	require.NoError(t, b.withUpdate(singleNodeCacheWrites(id), func(txn *badger.Txn) error {
		var buf bytes.Buffer
		scratch := make([]byte, binary.MaxVarintLen64)
		buf.Write(scratch[:binary.PutUvarint(scratch, uint64(len(legacy)))])
		for name, value := range legacy {
			key, err := b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, name)
			if err != nil {
				return err
			}
			buf.Write(scratch[:binary.PutUvarint(scratch, key)])
			raw, err := msgpack.Marshal(value)
			if err != nil {
				return err
			}
			buf.Write(raw)
		}
		props := buf.Bytes()
		body := Node{ID: id, Labels: []string{"L"}}
		bodyBytes, err := encodeValue(&body)
		if err != nil {
			return err
		}
		out := []byte{nodeFormatTokenizedV1}
		out = binary.AppendUvarint(out, uint64(len(props)))
		out = append(out, props...)
		out = append(out, bodyBytes...)
		return txn.Set(nodeKey(id), out)
	}))
	cold, err := b.GetNode(id)
	require.NoError(t, err)
	warm, err := b.GetNode(id)
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	inTx, err := tx.GetNode(id)
	require.NoError(t, err)
	for _, key := range []string{"xs", "ys", "zs", "s"} {
		t.Logf("%s: cold=%#v warm=%#v tx=%#v", key, cold.Properties[key], warm.Properties[key], inTx.Properties[key])
		if !reflect.DeepEqual(cold.Properties[key], warm.Properties[key]) || !reflect.DeepEqual(cold.Properties[key], inTx.Properties[key]) {
			t.Errorf("%s DIVERGES: cold=%#v warm=%#v tx=%#v", key, cold.Properties[key], warm.Properties[key], inTx.Properties[key])
		}
	}
}

// The writer's own property map belongs to the caller.
func TestPropertyWriteEntryCallerMapNotMutated(t *testing.T) {
	b := createTestBadgerEngine(t)
	props := map[string]any{"n": int(3), "l": []int{1, 2}}
	_, err := b.CreateNode(&Node{ID: NodeID(prefixTestID("caller-map")), Labels: []string{"C"}, Properties: props})
	require.NoError(t, err)
	t.Logf("caller map after CreateNode: n=%T l=%T", props["n"], props["l"])
	if _, ok := props["n"].(int); !ok {
		t.Errorf("CALLER MAP MUTATED by CreateNode: n=%#v l=%#v", props["n"], props["l"])
	}
}

// Concurrent writers that share one read-only property map.
func TestPropertyWriteEntrySharedMapConcurrentCreate(t *testing.T) {
	b := createTestBadgerEngine(t)
	shared := map[string]any{"a": int64(1), "s": "x", "n": int(3)}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				id := NodeID(prefixTestID(fmt.Sprintf("shared-%d-%d", g, i)))
				if _, err := b.CreateNode(&Node{ID: id, Labels: []string{"S"}, Properties: shared}); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestPropertyWriteEntrySharedMapConcurrentTransactions(t *testing.T) {
	b := createTestBadgerEngine(t)
	shared := map[string]any{"a": int64(1), "s": "x", "n": int(3)}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				tx, err := b.BeginTransaction()
				if err != nil {
					t.Error(err)
					return
				}
				id := NodeID(prefixTestID(fmt.Sprintf("shared-tx-%d-%d", g, i)))
				if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"S"}, Properties: shared}); err != nil {
					t.Error(err)
				}
				if err := tx.Commit(); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
}

// History: a superseded Go-int version read by an older snapshot.
func TestPropertyWriteEntrySnapshotHistoryTypes(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID(prefixTestID("r73-history"))
	other := NodeID(prefixTestID("r73-history-other"))
	_, err := b.CreateNode(&Node{ID: other, Labels: []string{"H"}, Properties: map[string]any{"k": "o"}})
	require.NoError(t, err)
	tx0, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx0.CreateNode(&Node{ID: id, Labels: []string{"H"}, Properties: map[string]any{
		"v": int(128), "l": []int{128, 3}, "li": []any{int(-129), int(128)}, "m": map[string]any{"x": int(-129)}, "f": float64(3),
	}})
	require.NoError(t, err)
	require.NoError(t, tx0.Commit())
	want := map[string]any{"v": int64(128), "l": []int64{128, 3}, "li": []int64{-129, 128}, "m": map[string]any{"x": int64(-129)}, "f": float64(3)}
	head, err := b.GetNode(id)
	require.NoError(t, err)
	reader, err := b.BeginTransaction()
	require.NoError(t, err)
	defer reader.Rollback()
	_, err = reader.GetNode(other)
	require.NoError(t, err)
	updated := copyNode(head)
	updated.Properties["v"] = int64(1)
	require.NoError(t, b.UpdateNode(updated))
	old, err := reader.GetNode(id)
	require.NoError(t, err)
	for key, value := range want {
		t.Logf("%s: head=%#v snapshot=%#v", key, head.Properties[key], old.Properties[key])
		if !reflect.DeepEqual(value, old.Properties[key]) || !reflect.DeepEqual(value, head.Properties[key]) {
			t.Errorf("%s: want %#v head %#v snapshot %#v", key, value, head.Properties[key], old.Properties[key])
		}
	}
}

// Runtime claim-after-write: production has no pre-commit scope check, so an
// undeclared scope is only discovered by the issuer after CommitAt.
func TestPropertyWriteEntryRuntimeZeroScopeClaimAfterWrite(t *testing.T) {
	saved := nodeCacheScopeCheck
	nodeCacheScopeCheck = nil
	t.Cleanup(func() { nodeCacheScopeCheck = saved })
	b := createTestBadgerEngine(t)
	id := NodeID(prefixTestID("r73-zero-scope"))
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Z"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	warm, err := b.GetNode(id)
	require.NoError(t, err)
	next := copyNode(warm)
	next.Properties["v"] = int64(2)
	callbackRan := false
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		err = b.withUpdate(nodeCacheWriteScope{}, func(txn *badger.Txn) error {
			callbackRan = true
			data, _, err := b.encodeNodeInTxn(txn, namespaceForNodeID(id), next)
			if err != nil {
				return err
			}
			return txn.Set(nodeKey(id), data)
		})
	}()
	require.Nil(t, panicked)
	require.ErrorIs(t, err, ErrInvalidData)
	require.False(t, callbackRan, "undeclared writer callback must never run")
	served, _ := b.GetNode(id)
	b.nodeCacheMu.Lock()
	delete(b.nodeCache, id)
	delete(b.queryNodeCache, id)
	b.nodeCacheMu.Unlock()
	durable, _ := b.GetNode(id)
	require.Equal(t, int64(1), durable.Properties["v"])
	require.Equal(t, durable.Properties, served.Properties)
	t.Logf("panic=%v err=%v served=%v durable=%v", panicked, err, served.Properties["v"], durable.Properties["v"])
	if panicked != nil && durable.Properties["v"] == int64(2) {
		t.Errorf("missing pre-write admission: undeclared scope rejected only after the write became durable (served=%v durable=%v)", served.Properties["v"], durable.Properties["v"])
	}
}

// Runtime: the metadata route rejects a node key under the test hook.
func TestPropertyWriteEntryMetadataRouteRejectsNodeKey(t *testing.T) {
	b := createTestBadgerEngine(t)
	id := NodeID(prefixTestID("r73-meta"))
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"M"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	err = b.withMetadataUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey(id), []byte{0}) })
	t.Logf("metadata route node-key write: hook=%v err=%v", metadataCommitCheck != nil, err)
	require.Error(t, err, "metadata route accepted a node key under the test hook")
	err = updateBadgerMetadata(b.db, func(txn *badger.Txn) error { return txn.Set(nodeKey(id), []byte{0}) })
	require.Error(t, err, "updateBadgerMetadata accepted a node key under the test hook")
}
