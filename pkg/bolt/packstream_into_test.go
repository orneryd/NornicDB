package bolt

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestEncodePackStreamIntInto_MatchesExisting(t *testing.T) {
	values := []int64{
		-17, -16, -15,
		0, 1, 42, 127, 128,
		-128, -129,
		32767, 32768,
		-32768, -32769,
		2147483647, 2147483648,
		-2147483648, -2147483649,
	}

	for _, v := range values {
		v := v
		t.Run(fmt.Sprintf("%d", v), func(t *testing.T) {
			got := encodePackStreamIntInto(nil, v)
			want := encodePackStreamInt(v)
			if string(got) != string(want) {
				t.Fatalf("bytes mismatch for %d: got=%x want=%x", v, got, want)
			}
		})
	}
}

func TestEncodePackStreamValueInto_StorageNodeStructure(t *testing.T) {
	node := &storage.Node{
		ID:     "node-1",
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
		},
	}

	data := encodePackStreamValueInto(nil, node)
	got, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// decodePackStreamValue decodes STRUCT values as map[string]any with signature/type metadata.
	// Existing decode implementation is used elsewhere in tests; assert the raw bytes at least
	// begin with the expected Bolt structure header for Node.
	if len(data) < 2 || data[0] != 0xB3 || data[1] != 0x4E {
		t.Fatalf("expected Node structure header B3 4E, got=%x", data[:2])
	}

	// Also sanity check decode returned non-nil
	if got == nil {
		t.Fatalf("expected decoded value, got nil")
	}
}

func TestEncodePackStreamValueInto_StorageEdgeStructure(t *testing.T) {
	edge := &storage.Edge{
		ID:        "edge-1",
		StartNode: "node-1",
		EndNode:   "node-2",
		Type:      "KNOWS",
		Properties: map[string]any{
			"since": int64(2020),
		},
	}

	data := encodePackStreamValueInto(nil, edge)
	got, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(data) < 2 || data[0] != 0xB5 || data[1] != 0x52 {
		t.Fatalf("expected Relationship structure header B5 52, got=%x", data[:2])
	}

	if got == nil {
		t.Fatalf("expected decoded value, got nil")
	}
}

func TestEncodePackStreamValueInto_TimeUsesDateTimeUTCStructureByDefault(t *testing.T) {
	dt := time.Date(2026, 6, 9, 18, 47, 32, 123456789, time.FixedZone("MST", -7*3600))
	data := encodePackStreamValueInto(nil, dt)
	if len(data) < 2 {
		t.Fatalf("unexpected encoded length: %d", len(data))
	}
	if data[0] != 0xB3 || data[1] != 0x49 {
		t.Fatalf("expected DateTime struct header B3 49, got=%x", data[:2])
	}
}

func TestEncodePackStreamDateTimeIntoWithUTC_UsesLegacyOrUTCSignature(t *testing.T) {
	dt := time.Date(2026, 6, 9, 18, 47, 32, 123456789, time.FixedZone("MST", -7*3600))

	legacy := encodePackStreamDateTimeIntoWithUTC(nil, dt, false)
	if len(legacy) < 2 || legacy[0] != 0xB3 || legacy[1] != 0x46 {
		t.Fatalf("expected legacy DateTime struct header B3 46, got=%x", legacy[:2])
	}

	utc := encodePackStreamDateTimeIntoWithUTC(nil, dt, true)
	if len(utc) < 2 || utc[0] != 0xB3 || utc[1] != 0x49 {
		t.Fatalf("expected UTC DateTime struct header B3 49, got=%x", utc[:2])
	}
}

func TestDecodePackStreamValue_DateTimeStructures(t *testing.T) {
	sec := int64(1791537396)
	nanos := int64(123456789)
	offset := int64(-7 * 3600)

	// UTC-patched DateTime: struct(3) sig=0x49 [seconds, nanos, offsetSeconds]
	utcPatched := []byte{0xB3, 0x49}
	utcPatched = encodePackStreamIntInto(utcPatched, sec)
	utcPatched = encodePackStreamIntInto(utcPatched, nanos)
	utcPatched = encodePackStreamIntInto(utcPatched, offset)
	v, _, err := decodePackStreamValue(utcPatched, 0)
	if err != nil {
		t.Fatalf("decode utc-patched datetime failed: %v", err)
	}
	if _, ok := v.(time.Time); !ok {
		t.Fatalf("expected time.Time for utc-patched datetime, got %T", v)
	}

	// Legacy DateTime: struct(3) sig=0x46 [seconds, nanos, offsetSeconds]
	legacy := append([]byte{0xB3, 0x46}, utcPatched[2:]...)
	v, _, err = decodePackStreamValue(legacy, 0)
	if err != nil {
		t.Fatalf("decode legacy datetime failed: %v", err)
	}
	if _, ok := v.(time.Time); !ok {
		t.Fatalf("expected time.Time for legacy datetime, got %T", v)
	}
}
