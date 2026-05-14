package storage

import (
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// TestEncodeTokenizedProperties_Empty covers the early-return path for
// empty/nil property maps. Decoders must still see a well-formed
// msgpack payload (an empty map), not zero bytes.
func TestEncodeTokenizedProperties_Empty(t *testing.T) {
	eng := newTestEngine(t)
	cases := []struct {
		name  string
		props map[string]any
	}{
		{"nil", nil},
		{"empty", map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var data []byte
			require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
				var err error
				data, err = eng.encodeTokenizedProperties(txn, "ns", tc.props)
				return err
			}))
			require.NotEmpty(t, data, "empty property map must still produce a msgpack body")

			// Round-trip back to a string map and verify length 0.
			out, err := eng.decodeTokenizedProperties("ns", data)
			require.NoError(t, err)
			require.NotNil(t, out, "decode of empty map must return a non-nil map")
			require.Empty(t, out)
		})
	}
}

// TestEncodeTokenizedProperties_AllocatesIDs verifies that encoding
// reuses an existing ID when a key has been seen and allocates fresh
// when it has not. Round-trips through decode to assert id-reuse went
// to the right place.
func TestEncodeTokenizedProperties_AllocatesIDs(t *testing.T) {
	eng := newTestEngine(t)
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		// Pre-allocate "name" so we can verify the encoder reuses its ID.
		preID, err := eng.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "name")
		require.NoError(t, err)

		data, err := eng.encodeTokenizedProperties(txn, "ns", map[string]any{
			"name":   "Alice",
			"newKey": int64(7),
		})
		require.NoError(t, err)

		// Verify the ID for "name" was reused (still exists in the
		// dict) and the new key got allocated.
		gotName, ok := eng.propKeyDict.lookup("ns", preID)
		require.True(t, ok)
		require.Equal(t, "name", gotName)

		// Round-trip and assert both keys decode correctly.
		out, err := eng.decodeTokenizedProperties("ns", data)
		require.NoError(t, err)
		require.Equal(t, "Alice", out["name"])
		require.Equal(t, int64(7), out["newKey"])

		return eng.propKeyDict.flushTxnCounters(txn)
	}))
}

// TestDecodeTokenizedProperties_EmptyData covers the empty-input early
// return — should succeed and return a non-nil empty map so callers can
// range over it without nil-checking.
func TestDecodeTokenizedProperties_EmptyData(t *testing.T) {
	eng := newTestEngine(t)
	for _, data := range [][]byte{nil, {}} {
		out, err := eng.decodeTokenizedProperties("ns", data)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.Empty(t, out)
	}
}

// TestDecodeTokenizedProperties_MalformedPayload asserts a deterministic
// error when the payload is not valid msgpack-encoded uint64-keyed map
// data. The error message names the failure context.
func TestDecodeTokenizedProperties_MalformedPayload(t *testing.T) {
	eng := newTestEngine(t)
	_, err := eng.decodeTokenizedProperties("ns", []byte{0xff, 0xff, 0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding tokenized properties")
}

// TestDecodeTokenizedProperties_UnknownIDErrors is the corruption-path
// test: a body that references a dictionary ID with no reverse-map
// entry must return an error of the form
// "property key id %d not in dictionary for namespace %q".
//
// Cypher property-name semantics test #3 (per plan §6.8): the engine
// MUST surface this as an error rather than silently dropping the
// property. Tests assert on the exact message because callers depend
// on parsing it for diagnostic output.
func TestDecodeTokenizedProperties_UnknownIDErrors(t *testing.T) {
	eng := newTestEngine(t)

	// Construct a payload that references id 999 — never allocated.
	val, err := msgpack.Marshal("ghost")
	require.NoError(t, err)
	body := []byte{}
	body = appendUvarint(body, 1)   // count = 1
	body = appendUvarint(body, 999) // id = 999
	body = append(body, val...)

	_, err = eng.decodeTokenizedProperties("ns", body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "property key id 999 not in dictionary for namespace \"ns\"")
}

// TestDecodeTokenizedProperties_EmptyTokenizedMap covers the path
// where the body parses to a zero-count payload.
func TestDecodeTokenizedProperties_EmptyTokenizedMap(t *testing.T) {
	eng := newTestEngine(t)
	body := appendUvarint(nil, 0) // count = 0
	out, err := eng.decodeTokenizedProperties("ns", body)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.Empty(t, out)
}

// appendUvarint grows its input slice with the uvarint encoding of v.
// Used by tests that synthesize tokenized payloads to exercise
// specific decoder branches.
func appendUvarint(buf []byte, v uint64) []byte {
	return binary.AppendUvarint(buf, v)
}

// TestVarintHelpers covers boundary cases on readUvarint that the
// higher-level codec tests don't otherwise reach. Encoding is
// delegated to encoding/binary; the wrapper only exists to translate
// binary.Uvarint's int return codes into named errors.
func TestVarintHelpers(t *testing.T) {
	t.Run("round-trip across a wide range of values", func(t *testing.T) {
		cases := []uint64{0, 1, 127, 128, 16383, 16384, 1 << 32, ^uint64(0)}
		for _, v := range cases {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, v)
			require.Greater(t, n, 0)
			got, consumed, err := readUvarint(buf[:n])
			require.NoError(t, err)
			require.Equal(t, n, consumed)
			require.Equal(t, v, got)
		}
	})

	t.Run("readUvarint rejects truncated input", func(t *testing.T) {
		// 0x80 alone signals "more bytes follow" but there are none.
		_, _, err := readUvarint([]byte{0x80})
		require.Error(t, err)
		require.Contains(t, err.Error(), "varint truncated")
	})

	t.Run("readUvarint rejects 9 continuation bytes with no terminator", func(t *testing.T) {
		// Boundary: 9 bytes all 0x80 — every byte says "more follows"
		// but the buffer ran out before the 10th byte. This is
		// truncation (would have fit) not overflow.
		input := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
		_, _, err := readUvarint(input)
		require.Error(t, err)
		require.Contains(t, err.Error(), "varint truncated")
	})

	t.Run("readUvarint accepts max-size 10-byte uint64", func(t *testing.T) {
		// math.MaxUint64 round-trips through 10 bytes.
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, ^uint64(0))
		require.Equal(t, 10, n)
		got, consumed, err := readUvarint(buf)
		require.NoError(t, err)
		require.Equal(t, 10, consumed)
		require.Equal(t, ^uint64(0), got)
	})

	t.Run("readUvarint rejects 10th-byte continuation (would need 11th)", func(t *testing.T) {
		// Nine continuation bytes followed by a 10th byte that ALSO
		// has its continuation bit set — would need an 11th byte to
		// terminate. encoding/binary.Uvarint classifies this as
		// truncation (it stopped reading at the 10-byte ceiling and
		// could not finish), not overflow. Either label is honest:
		// the value can't fit in uint64. Pin the wrapper's behavior.
		input := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
		_, _, err := readUvarint(input)
		require.Error(t, err)
		require.Contains(t, err.Error(), "varint truncated")
	})

	t.Run("readUvarint rejects 10th byte > 1", func(t *testing.T) {
		// Nine continuation bytes followed by a byte > 1 in the 10th
		// position — overflows uint64.
		input := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
		_, _, err := readUvarint(input)
		require.Error(t, err)
		require.Contains(t, err.Error(), "overflow")
	})
}

// TestDecodeTokenizedProperties_TruncatedID covers the branch where
// the count says there's another (id, value) pair but the id varint
// runs off the end of the buffer.
func TestDecodeTokenizedProperties_TruncatedID(t *testing.T) {
	eng := newTestEngine(t)
	body := appendUvarint(nil, 1) // count=1
	body = append(body, 0x80)     // start of varint, but truncated
	_, err := eng.decodeTokenizedProperties("ns", body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "id varint")
}

// TestDecodeTokenizedProperties_CountExceedsPayload covers the branch
// where the leading count claims more entries than the payload could
// possibly contain.
func TestDecodeTokenizedProperties_CountExceedsPayload(t *testing.T) {
	eng := newTestEngine(t)
	body := appendUvarint(nil, 100) // count=100, but no entries follow
	_, err := eng.decodeTokenizedProperties("ns", body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ran out of bytes")
}

// TestDecodeTokenizedProperties_TruncatedValue covers the branch
// where a value's msgpack stream is incomplete.
func TestDecodeTokenizedProperties_TruncatedValue(t *testing.T) {
	eng := newTestEngine(t)
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		_, err := eng.propKeyDict.resolveOrAllocateInTxn(txn, "ns", "k")
		require.NoError(t, err)
		return eng.propKeyDict.flushTxnCounters(txn)
	}))

	body := appendUvarint(nil, 1) // count=1
	body = appendUvarint(body, 1) // id=1 (mapped to "k")
	body = append(body, 0xc4)     // bin8 type tag, expects 1B length + bytes
	// Truncated — no length byte follows.

	_, err := eng.decodeTokenizedProperties("ns", body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding tokenized properties")
}

// TestDecodeTokenizedProperties_TrailingBytes covers the branch where
// the count reads as 0 but there are trailing bytes after.
func TestDecodeTokenizedProperties_TrailingBytes(t *testing.T) {
	eng := newTestEngine(t)
	body := appendUvarint(nil, 0) // count=0
	body = append(body, 0xff, 0xff)
	_, err := eng.decodeTokenizedProperties("ns", body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing bytes")
}

// TestEncodeTokenizedProperties_RejectsUnencodableValue covers the
// branch where msgpack.Marshal fails on a property value.
func TestEncodeTokenizedProperties_RejectsUnencodableValue(t *testing.T) {
	eng := newTestEngine(t)
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		_, err := eng.encodeTokenizedProperties(txn, "ns", map[string]any{
			"badField": make(chan int),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "marshaling property")
		require.Contains(t, err.Error(), "badField")
		return nil
	}))
}

// TestEncodeDecodeNode_TokenizedFormat is the full node round-trip
// with mixed property types.
func TestEncodeDecodeNode_TokenizedFormat(t *testing.T) {
	original := &Node{
		ID:     NodeID("test:mixed-types"),
		Labels: []string{"Sample"},
		Properties: map[string]any{
			"stringField": "hello",
			"intField":    int64(42),
			"floatField":  3.14,
			"boolField":   true,
			"sliceField":  []any{"a", "b", "c"},
			"nestedField": map[string]any{"inner": int64(7)},
		},
	}

	decoded, err := codecRoundTripNode(t, original)
	require.NoError(t, err)
	require.Equal(t, original.ID, decoded.ID)
	require.Equal(t, original.Properties["stringField"], decoded.Properties["stringField"])
	require.Equal(t, original.Properties["intField"], decoded.Properties["intField"])
	require.InDelta(t, original.Properties["floatField"], decoded.Properties["floatField"], 1e-9)
	require.Equal(t, original.Properties["boolField"], decoded.Properties["boolField"])
}

// TestEncodeDecodeEdge_TokenizedFormat is the edge counterpart with
// mixed property types and timestamps.
func TestEncodeDecodeEdge_TokenizedFormat(t *testing.T) {
	original := &Edge{
		ID:        EdgeID("test:edge-mixed"),
		StartNode: NodeID("test:node-a"),
		EndNode:   NodeID("test:node-b"),
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"weight":     0.75,
			"label":      "primary",
			"createdAt":  int64(1700000000),
			"properties": []any{int64(1), int64(2), int64(3)},
		},
		Confidence:    0.9,
		AutoGenerated: true,
	}

	decoded, err := codecRoundTripEdge(t, original)
	require.NoError(t, err)
	require.Equal(t, original.ID, decoded.ID)
	require.Equal(t, original.StartNode, decoded.StartNode)
	require.Equal(t, original.EndNode, decoded.EndNode)
	require.Equal(t, original.Type, decoded.Type)
	require.Equal(t, original.Properties["label"], decoded.Properties["label"])
	require.InDelta(t, original.Properties["weight"], decoded.Properties["weight"], 1e-9)
	require.Equal(t, original.AutoGenerated, decoded.AutoGenerated)
}

// TestDecodeNode_RejectsNonV2Body covers the V2 hot-path decoder's
// refusal to accept legacy V1 bodies. After a V2 store has been fully
// migrated, encountering a non-V2 body indicates corruption and must
// be surfaced as an error, not silently mis-decoded.
func TestDecodeNode_RejectsNonV2Body(t *testing.T) {
	eng := newTestEngine(t)

	t.Run("empty body", func(t *testing.T) {
		_, err := eng.decodeNode("ns", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "node body empty")
	})

	t.Run("wrong leading byte", func(t *testing.T) {
		_, err := eng.decodeNode("ns", []byte{0xff, 0x00})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected format byte 0xff")
		require.Contains(t, err.Error(), "expected V2 (0x10)")
	})

	t.Run("truncated props length varint", func(t *testing.T) {
		_, err := eng.decodeNode("ns", []byte{nodeFormatTokenizedV1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed properties length varint")
	})

	t.Run("props payload truncated", func(t *testing.T) {
		// Format byte + varint claiming 100-byte props + only 1 byte.
		body := []byte{nodeFormatTokenizedV1}
		body = append(body, 100) // varint(100)
		body = append(body, 0xff)
		_, err := eng.decodeNode("ns", body)
		require.Error(t, err)
		require.Contains(t, err.Error(), "properties payload truncated")
	})
}

// TestDecodeEdge_RejectsNonV2Body covers the V2 edge decoder's refusal
// to accept legacy V1 compact bodies in production code paths.
func TestDecodeEdge_RejectsNonV2Body(t *testing.T) {
	eng := newTestEngine(t)

	t.Run("empty body", func(t *testing.T) {
		_, err := eng.decodeEdgeBodyWithID("ns", nil, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "edge body empty")
	})

	t.Run("V1 compact format byte rejected", func(t *testing.T) {
		body := []byte{edgeFormatCompactV1, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
		_, err := eng.decodeEdgeBodyWithID("ns", body, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected format byte 0x02")
	})

	t.Run("missing endpoint numID in dict", func(t *testing.T) {
		// Construct a synthetic V2 body referencing numIDs the dict does
		// not know about. Decoder must surface the dict miss.
		fakeEdge := &Edge{
			ID:   EdgeID("test:edge-x"),
			Type: "T",
		}
		body, err := encodeEdgeCompactV2Direct(t, eng, fakeEdge, 9999, 9998)
		require.NoError(t, err)
		_, err = eng.decodeEdgeBodyWithID("ns", body, "test:edge-x")
		require.Error(t, err)
		require.Contains(t, err.Error(), "numID")
	})
}

// encodeEdgeCompactV2Direct is a tests-only helper that builds a V2
// edge body with explicit start/end numIDs. Used to construct
// corruption scenarios.
func encodeEdgeCompactV2Direct(t *testing.T, eng *BadgerEngine, edge *Edge, startNum, endNum uint64) ([]byte, error) {
	t.Helper()
	var data []byte
	err := eng.withUpdate(func(txn *badger.Txn) error {
		var err error
		data, err = eng.encodeEdgeCompactV2(txn, "ns", edge, startNum, endNum)
		if err != nil {
			return err
		}
		return eng.propKeyDict.flushTxnCounters(txn)
	})
	return data, err
}

// TestEncodedBodySize_ReductionForRepeatedKeys is the size-savings
// sanity check called out in the plan. A 100-node corpus that all
// share the same 8 property keys must encode to materially less than
// the equivalent legacy form.
func TestEncodedBodySize_ReductionForRepeatedKeys(t *testing.T) {
	eng := newTestEngine(t)

	properties := map[string]any{
		"productID":    int64(0),
		"productName":  "Sample Product Name",
		"sku":          "SKU-PLACEHOLDER",
		"unitPrice":    9.99,
		"unitsInStock": int64(100),
		"discontinued": false,
		"description":  "A long-ish product description that pads the body",
		"tags":         []any{"a", "b", "c", "d"},
	}

	var v2Total, v1Total int
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		for i := 0; i < 100; i++ {
			node := &Node{
				ID:         NodeID("test:product-" + string(rune('a'+i%26))),
				Labels:     []string{"Product"},
				Properties: properties,
			}
			v2Body, _, err := eng.encodeNodeInTxn(txn, "test", node)
			if err != nil {
				return err
			}
			v1Body, _, err := encodeNodeV1(node)
			if err != nil {
				return err
			}
			v2Total += len(v2Body)
			v1Total += len(v1Body)
		}
		return eng.propKeyDict.flushTxnCounters(txn)
	}))

	t.Logf("V1 total: %d bytes, V2 total: %d bytes (%.1f%% of V1)",
		v1Total, v2Total, 100.0*float64(v2Total)/float64(v1Total))
	require.Less(t, v2Total, v1Total, "V2 must be smaller than V1 for repeated keys")
	// Plan §10 quotes 20–25% reduction band on a corpus of repeated
	// keys. Assert at least 15% off so the test is robust to msgpack
	// implementation tweaks and corpus tweaks but still surfaces a
	// regression that loses the win.
	require.Less(t, float64(v2Total)/float64(v1Total), 0.85)
}
