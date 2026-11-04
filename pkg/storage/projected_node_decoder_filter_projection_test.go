package storage

// projectedNodeDecoder filter-first body decode (#1682): a scan without a
// projection that names the properties its PropertyFilter reads
// (StreamNodesOptions.PropertyFilterProjection) settles the filter on those
// before it decodes the rest of a body. A rejected node costs its required
// properties, not the unrelated payload the body carries; a node the filter
// keeps decodes the values that pass skipped, in stored order and through the
// same per-value owner, so its row and its errors are the ones of a scan that
// carries no hint.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// filterProjectionStoredBody returns the stored key and body of one node.
func filterProjectionStoredBody(t *testing.T, engine *BadgerEngine, id NodeID) (key, body []byte) {
	t.Helper()
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err != nil {
			return err
		}
		key = item.KeyCopy(nil)
		body, err = item.ValueCopy(nil)
		return err
	}))
	return key, body
}

// filterProjectionBodyWith returns the stored body with a hand-built property
// payload (count varint, then token/value pairs) and the stored metadata.
func filterProjectionBodyWith(t *testing.T, body, payload []byte) []byte {
	t.Helper()
	propsLen, n := binary.Uvarint(body[1:])
	require.Greater(t, n, 0)
	out := binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, uint64(len(payload)))
	out = append(out, payload...)
	return append(out, body[1+n+int(propsLen):]...)
}

// A rejected body is settled on its required property: the 1 KiB content and
// the temporal value it also carries are never decoded.
func TestProjectedNodeDecoderFilterProjectionRejectsBeforeFullBody(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	content := strings.Repeat("c", 1024)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "a:1", Labels: []string{"Record"}, Properties: map[string]interface{}{
		"id":      7,
		"content": content,
		"at":      time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC),
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "a:1")
	rejects := func(props map[string]interface{}) bool { return props["id"] == int64(8) }

	whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: rejects})
	node, err := whole.decode(key, body)
	require.NoError(t, err)
	require.Nil(t, node, "the filter rejects the node")
	require.Greater(t, testing.AllocsPerRun(100, func() { _, _ = whole.decode(key, body) }), 0.0,
		"a scan without the hint decodes the unrelated 1 KiB content of a node it rejects")

	hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           rejects,
		PropertyFilterProjection: []string{"id"},
	})
	node, err = hinted.decode(key, body)
	require.NoError(t, err)
	require.Nil(t, node, "the filter rejects the node on its required property")
	require.Zero(t, testing.AllocsPerRun(100, func() { _, _ = hinted.decode(key, body) }),
		"a rejected body costs its required property, none of the values it never reads")
}

// A body the filter keeps is the whole node, whatever the hint decoded first.
func TestProjectedNodeDecoderFilterProjectionKeepsWholeRow(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	day := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "a:1", Labels: []string{"Record", "Code"}, Properties: map[string]interface{}{
		"id":    7,
		"name":  "x",
		"at":    day,
		"score": 1.5,
		"flag":  true,
		"tags":  []string{"a", "b"},
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "a:1")

	whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{})
	want, err := whole.decode(key, body)
	require.NoError(t, err)
	require.NotNil(t, want)

	hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	})
	got, err := hinted.decode(key, body)
	require.NoError(t, err)
	require.Equal(t, want, got, "the kept row is the whole node a scan without the hint decodes")
	require.Len(t, got.Properties, 6)
	at, ok := got.Properties["at"].(time.Time)
	require.True(t, ok, "a temporal value is restored")
	require.True(t, at.Equal(day))
	require.Equal(t, int64(7), got.Properties["id"])
	require.Equal(t, "x", got.Properties["name"])
}

// The deferred values are the values of the row a scan without the hint
// returns, for every stored type the deferred decode can meet.
func TestProjectedNodeDecoderFilterProjectionMatchesWholeRowForValueTypes(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:typed", Labels: []string{"T"}, Properties: map[string]interface{}{
		"id":      7,
		"long":    strings.Repeat("s", 300),
		"ints":    []int64{1, -2, 3},
		"floats":  []float64{1.5, -2.5},
		"bools":   []bool{true, false},
		"mixed":   []interface{}{int64(1), "two", 3.5},
		"bytes":   []byte("raw"),
		"nested":  map[string]interface{}{"k": "v", "n": int64(2)},
		"at":      time.Date(2026, 10, 4, 12, 30, 0, 0, time.UTC),
		"nothing": []string{},
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "ns:typed")

	whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{})
	want, err := whole.decode(key, body)
	require.NoError(t, err)
	require.NotNil(t, want)

	hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	})
	got, err := hinted.decode(key, body)
	require.NoError(t, err)
	require.Equal(t, want.Properties, got.Properties,
		"every deferred value is the value, of the type, the whole-body decode gives")
	require.Empty(t, hinted.deferred, "the spans of a decoded body are released before decode returns")
}

// A stored body that names one property twice keeps the value the whole-body
// decode keeps, the last, whether the hint decoded that property first or
// deferred it.
func TestProjectedNodeDecoderFilterProjectionKeepsLastDuplicateValue(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:dup", Labels: []string{"P"}, Properties: map[string]interface{}{
		"id":   7,
		"name": "x",
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "ns:dup")
	idToken := decoderToken(t, engine, "ns", "id")
	nameToken := decoderToken(t, engine, "ns", "name")
	require.Less(t, idToken, uint64(0x80))
	require.Less(t, nameToken, uint64(0x80))

	for name, tc := range map[string]struct {
		payload []byte
		keeps   func(map[string]interface{}) bool
	}{
		"deferred property repeated": {
			payload: []byte{
				0x03,
				byte(idToken), 0x07,
				byte(nameToken), 0xa1, 'a',
				byte(nameToken), 0xa1, 'b',
			},
			keeps: func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		},
		"required property repeated": {
			payload: []byte{
				0x03,
				byte(idToken), 0x07,
				byte(nameToken), 0xa1, 'a',
				byte(idToken), 0x08,
			},
			keeps: func(props map[string]interface{}) bool { return props["id"] != nil },
		},
	} {
		t.Run(name, func(t *testing.T) {
			duplicate := filterProjectionBodyWith(t, body, tc.payload)
			whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: tc.keeps})
			want, err := whole.decode(key, duplicate)
			require.NoError(t, err)
			require.NotNil(t, want)
			hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
				PropertyFilter:           tc.keeps,
				PropertyFilterProjection: []string{"id"},
			})
			got, err := hinted.decode(key, duplicate)
			require.NoError(t, err)
			require.Equal(t, want.Properties, got.Properties,
				"the hint keeps the value the whole-body decode keeps")
			require.Empty(t, hinted.deferred, "the spans of a decoded body are released before decode returns")
		})
	}
}

// The properties after the required one are skipped, not decoded, for a body
// the filter rejects; a body it keeps is decoded in full, so a value the skip
// accepted still reports the error the whole-body decode produces.
func TestProjectedNodeDecoderFilterProjectionToleratesUnreadValue(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:probe", Labels: []string{"P"}, Properties: map[string]interface{}{
		"id":      7,
		"content": "x",
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "ns:probe")
	idToken := decoderToken(t, engine, "ns", "id")
	contentToken := decoderToken(t, engine, "ns", "content")
	require.Less(t, idToken, uint64(0x80))
	require.Less(t, contentToken, uint64(0x80))
	// The extension frame is skippable, but its two-byte temporal payload is
	// malformed. Decoding content must fail when the filter keeps the node.
	malformed := filterProjectionBodyWith(t, body, []byte{
		0x02,
		byte(idToken), 0x07,
		byte(contentToken), 0xd5, 0x2a, 0x00, 0x00,
	})

	rejects := func(props map[string]interface{}) bool { return props["id"] == int64(8) }
	hintedRejects := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           rejects,
		PropertyFilterProjection: []string{"id"},
	})
	node, err := hintedRejects.decode(key, malformed)
	require.NoError(t, err)
	require.Nil(t, node, "the value after the required property is never decoded")
	require.Empty(t, hintedRejects.deferred, "a rejected body leaves no borrowed span behind")

	hintedKeeps := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	})
	node, err = hintedKeeps.decode(key, malformed)
	require.ErrorContains(t, err, `key "content" value`)
	keptErr := err
	require.Nil(t, node, "a kept body is decoded in full and reports its errors")
	require.Empty(t, hintedKeeps.deferred, "a failed decode leaves no borrowed span behind")

	whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: rejects})
	node, err = whole.decode(key, malformed)
	require.EqualError(t, err, keptErr.Error(), "the hint preserves the canonical full-body decode error")
	require.Nil(t, node, "without the hint the whole body is decoded before the filter runs")
}

// A value the hinted pass cannot skip is a scan error in that pass: the filter
// never settles, so the body is neither kept nor silently dropped.
func TestProjectedNodeDecoderFilterProjectionRejectsUnskippableValue(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:probe", Labels: []string{"P"}, Properties: map[string]interface{}{
		"id":      7,
		"content": "x",
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "ns:probe")
	idToken := decoderToken(t, engine, "ns", "id")
	contentToken := decoderToken(t, engine, "ns", "content")
	require.Less(t, idToken, uint64(0x80))
	require.Less(t, contentToken, uint64(0x80))
	// A Str8 that declares 64 payload bytes while one follows: no span of the
	// value exists in the body, whatever the filter decides.
	truncated := filterProjectionBodyWith(t, body, []byte{
		0x02,
		byte(idToken), 0x07,
		byte(contentToken), 0xd9, 0x40, 'x',
	})
	for name, filter := range map[string]func(map[string]interface{}) bool{
		"rejects": func(props map[string]interface{}) bool { return props["id"] == int64(8) },
		"keeps":   func(props map[string]interface{}) bool { return props["id"] == int64(7) },
	} {
		t.Run(name, func(t *testing.T) {
			hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
				PropertyFilter:           filter,
				PropertyFilterProjection: []string{"id"},
			})
			node, err := hinted.decode(key, truncated)
			require.ErrorContains(t, err, "decoding tokenized properties")
			require.Nil(t, node)
			require.Empty(t, hinted.deferred, "a failed decode leaves no borrowed span behind")

			whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: filter})
			node, err = whole.decode(key, truncated)
			require.ErrorContains(t, err, "decoding tokenized properties")
			require.Nil(t, node)
		})
	}
}

// The hint resolves through the namespace of the body it is reading: two
// namespaces number their own property names, so the same token means a
// different property in each.
func TestProjectedNodeDecoderFilterProjectionResolvesNamesPerNamespace(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	content := strings.Repeat("b", 1024)
	// The first name each namespace allocates is its first token: "id" in a,
	// "other" in b.
	for _, node := range []*Node{
		{ID: "b:seed", Labels: []string{"P"}, Properties: map[string]interface{}{"other": 7}},
		{ID: "a:seed", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 7}},
		{ID: "a:keep", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 7, "content": content}},
		{ID: "a:drop", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 8, "content": content}},
		{ID: "b:keep", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 7, "other": 1, "content": content}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), decoderToken(t, engine, "a", "id"))
	require.Equal(t, uint64(1), decoderToken(t, engine, "b", "other"))

	got := map[NodeID]map[string]interface{}{}
	require.NoError(t, engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	}, func(node *Node) error {
		got[node.ID] = node.Properties
		return nil
	}))
	ids := make([]string, 0, len(got))
	for id := range got {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	require.Equal(t, []string{"a:keep", "a:seed", "b:keep"}, ids,
		"b:seed carries b's first token, which is not b's id")
	require.Equal(t, map[string]interface{}{"id": int64(7), "content": content}, got["a:keep"])
	require.Equal(t, map[string]interface{}{"id": int64(7)}, got["a:seed"])
	require.Equal(t, map[string]interface{}{"id": int64(7), "other": int64(1), "content": content}, got["b:keep"],
		"a kept body carries every property, named as its own namespace stored them")
}

// The hint never narrows a projected scan: the projection already names what
// the filter reads, so the row is the projection.
func TestProjectedNodeDecoderFilterProjectionLeavesProjectedScanToItsProjection(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "a:1", Labels: []string{"P"}, Properties: map[string]interface{}{
		"id":      7,
		"name":    "x",
		"content": strings.Repeat("c", 1024),
	}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "a:1")

	decoder := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		Projection:               []string{"id", "name"},
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	})
	node, err := decoder.decode(key, body)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"id": int64(7), "name": "x"}, node.Properties)
}

// The filter's selected values are also the row's values: accepting a body
// must not allocate another string for the same property during hydration.
func TestProjectedNodeDecoderFilterProjectionKeptAllocations(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:kept", Properties: map[string]interface{}{"id": "kept-string-value", "content": strings.Repeat("x", 1024)}})
	require.NoError(t, err)
	key, body := filterProjectionStoredBody(t, engine, "ns:kept")
	filter := func(properties map[string]interface{}) bool { return properties["id"] == "kept-string-value" }
	whole := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: filter})
	hinted := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: filter, PropertyFilterProjection: []string{"id"}})
	baseline := testing.AllocsPerRun(100, func() { _, err := whole.decode(key, body); require.NoError(t, err) })
	candidate := testing.AllocsPerRun(100, func() { _, err := hinted.decode(key, body); require.NoError(t, err) })
	require.LessOrEqual(t, candidate, baseline, "accepting the body must reuse the filter's decoded values")

	// The deferred values are decoded once each, so a wide body whose hint
	// defers most of it must add no allocation per record either.
	wide := map[string]interface{}{"id": int64(7)}
	for i := range 15 {
		wide[fmt.Sprintf("v%02d", i)] = i
	}
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:wide", Properties: wide})
	require.NoError(t, err)
	wideKey, wideBody := filterProjectionStoredBody(t, engine, "ns:wide")
	keepsIntID := func(properties map[string]interface{}) bool { return properties["id"] == int64(7) }
	wholeWide := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: keepsIntID})
	hintedWide := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: keepsIntID, PropertyFilterProjection: []string{"id"}})
	wholeRow, err := wholeWide.decode(wideKey, wideBody)
	require.NoError(t, err)
	hintedRow, err := hintedWide.decode(wideKey, wideBody)
	require.NoError(t, err)
	require.Equal(t, wholeRow.Properties, hintedRow.Properties, "the wide row carries every deferred value")
	wideBaseline := testing.AllocsPerRun(100, func() { _, err := wholeWide.decode(wideKey, wideBody); require.NoError(t, err) })
	wideCandidate := testing.AllocsPerRun(100, func() { _, err := hintedWide.decode(wideKey, wideBody); require.NoError(t, err) })
	require.LessOrEqual(t, wideCandidate, wideBaseline, "the deferred values add no per-record allocation")
	t.Logf("kept wide row allocations: whole %.1f, hinted %.1f", wideBaseline, wideCandidate)
}

// One decoder serves every body of a scan, so a row carries its own properties
// and no borrowed body outlives the call that decoded it.
func TestProjectedNodeDecoderFilterProjectionReusesScanScratchAcrossRows(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	wide := map[string]interface{}{"id": 7}
	for i := range 16 {
		wide[fmt.Sprintf("p%02d", i)] = i
	}
	for _, node := range []*Node{
		{ID: "a:wide", Labels: []string{"P"}, Properties: wide},
		{ID: "a:narrow", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 7, "name": "x"}},
		{ID: "a:drop", Labels: []string{"P"}, Properties: map[string]interface{}{"id": 8, "name": "y"}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	decoder := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{
		PropertyFilter:           func(props map[string]interface{}) bool { return props["id"] == int64(7) },
		PropertyFilterProjection: []string{"id"},
	})
	wideKey, wideBody := filterProjectionStoredBody(t, engine, "a:wide")
	narrowKey, narrowBody := filterProjectionStoredBody(t, engine, "a:narrow")
	dropKey, dropBody := filterProjectionStoredBody(t, engine, "a:drop")

	first, err := decoder.decode(wideKey, wideBody)
	require.NoError(t, err)
	require.Len(t, first.Properties, 17, "the first row carries every property of its own body")
	require.Empty(t, decoder.deferred, "the spans of a decoded body are released before decode returns")
	require.Zero(t, decoder.reader.Size(), "the scan reader must also release its borrowed body")
	// Releasing drops the borrowed spans themselves, not merely their count:
	// a capacity-only leftover must not keep a body alive.
	spans := decoder.deferred[:cap(decoder.deferred)]
	for i := range spans {
		require.Nil(t, spans[i].encoded, "span %d outlived its body", i)
	}

	second, err := decoder.decode(narrowKey, narrowBody)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"id": int64(7), "name": "x"}, second.Properties,
		"the second row carries its own properties, not the previous row's")
	require.Empty(t, decoder.deferred)

	node, err := decoder.decode(dropKey, dropBody)
	require.NoError(t, err)
	require.Nil(t, node, "the filter rejects the node")
	require.Empty(t, decoder.deferred, "a rejected body leaves no borrowed span behind")

	// The row owns its values: rewriting the stored body after the decode must
	// not change a value the decoder returned.
	for i := range narrowBody {
		narrowBody[i] = 0xff
	}
	require.Equal(t, map[string]interface{}{"id": int64(7), "name": "x"}, second.Properties)
}

// Compare selective and non-selective filters, including all-kept bodies:
// avoiding rejected payloads must not hide extra allocations for kept rows.
func BenchmarkProjectedNodeDecoderFilterProjection(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	// Reuse the codec fixture encoder: only dictionaries are committed, so
	// large bodies remain in memory and do not hit the small in-memory
	// engine's stored-value cap. Both modes decode the exact same frames.
	for _, payloadBytes := range []int{64, 1024, 32768, 65536} {
		for _, valueType := range []string{"integer", "string"} {
			var wanted, rejected interface{} = int64(7), int64(8)
			if valueType == "string" {
				wanted, rejected = "kept-string-value", "rejected-string-value"
			}
			content := strings.Repeat("x", payloadBytes)
			prefix := fmt.Sprintf("bench:%s-%d-", valueType, payloadBytes)
			bodies := [2][]byte{}
			keys := [2][]byte{}
			for i, id := range []interface{}{wanted, rejected} {
				nodeID := NodeID(fmt.Sprintf("%s%d", prefix, i))
				keys[i] = nodeKey(nodeID)
				bodies[i] = benchmarkEncodedFixture(b, engine, false, func(txn *badger.Txn) ([]byte, error) {
					encoded, _, err := engine.encodeNodeInTxn(txn, "bench", &Node{ID: nodeID, Properties: map[string]interface{}{"id": id, "content": content}})
					return encoded, err
				})
			}
			for _, percentKept := range []int{0, 50, 100} {
				for _, hint := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/bytes_%d/kept_%d/hint_%t", valueType, payloadBytes, percentKept, hint), func(b *testing.B) {
						opts := StreamNodesOptions{PropertyFilter: func(properties map[string]interface{}) bool { return properties["id"] == wanted }}
						if hint {
							opts.PropertyFilterProjection = []string{"id"}
						}
						decoder := newProjectedNodeDecoder(engine, nil, opts)
						kept := 0
						var last *Node
						b.ReportAllocs()
						b.ResetTimer()
						for i := range b.N {
							body := 1
							if i%100 < percentKept {
								body = 0
							}
							node, err := decoder.decode(keys[body], bodies[body])
							if err != nil {
								b.Fatal(err)
							}
							if node != nil {
								kept++
								last = node
							}
						}
						b.StopTimer()
						expected := b.N/100*percentKept + min(b.N%100, percentKept)
						require.Equal(b, expected, kept)
						if kept > 0 {
							require.Equal(b, wanted, last.Properties["id"])
							require.Equal(b, content, last.Properties["content"])
						}
					})
				}
			}
		}
	}
}
