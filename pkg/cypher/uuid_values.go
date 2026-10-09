package cypher

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// CypherUUID is a Cypher 25 UUID value (#907): 128 bits, written as
// 8-4-4-4-12 lower-case hex digits. As in Neo4j:
//   - uuid() is a random version 7 UUID (time-ordered), uuid(msb, lsb) the
//     UUID of two 64-bit halves and uuid(text) the UUID text names (in any
//     case, with its four dashes);
//   - two UUIDs are equal when their bits are; a UUID is never equal to its
//     text, and UUIDs have no < or > but sort by their bits as unsigned;
//   - toString writes the text, uuid.mostSignificantBits and
//     uuid.leastSignificantBits read the halves as signed integers.
type CypherUUID struct {
	// bits are the UUID's 128 bits, big-endian. A struct, not an array, so
	// no list check reads a UUID as a list of bytes.
	bits [16]byte
}

// String is the UUID's text: 550e8400-e29b-41d4-a716-446655440000.
func (u CypherUUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u.bits[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u.bits[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u.bits[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u.bits[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u.bits[10:16])
	return string(b[:])
}

// MostSignificantBits is the first 64 bits as a signed integer.
func (u CypherUUID) MostSignificantBits() int64 {
	return int64(binary.BigEndian.Uint64(u.bits[0:8]))
}

// LeastSignificantBits is the last 64 bits as a signed integer.
func (u CypherUUID) LeastSignificantBits() int64 {
	return int64(binary.BigEndian.Uint64(u.bits[8:16]))
}

// compareUUIDs orders UUIDs by their bits, unsigned.
func compareUUIDs(left, right CypherUUID) int {
	for i := range left.bits {
		if left.bits[i] != right.bits[i] {
			if left.bits[i] < right.bits[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// uuidFromHalves is the UUID whose halves are msb and lsb.
func uuidFromHalves(msb, lsb int64) CypherUUID {
	var u CypherUUID
	binary.BigEndian.PutUint64(u.bits[0:8], uint64(msb))
	binary.BigEndian.PutUint64(u.bits[8:16], uint64(lsb))
	return u
}

// parseCypherUUID reads 8-4-4-4-12 hex text, in any case.
func parseCypherUUID(text string) (CypherUUID, bool) {
	var u CypherUUID
	if len(text) != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return u, false
	}
	digits := strings.ReplaceAll(text, "-", "")
	if len(digits) != 32 {
		return u, false
	}
	if _, err := hex.Decode(u.bits[:], []byte(digits)); err != nil {
		return u, false
	}
	return u, true
}

// newVersion7UUID is a random version 7 UUID: 48 bits of Unix milliseconds,
// then random bits with the version and variant set.
func newVersion7UUID(now time.Time) CypherUUID {
	var u CypherUUID
	_, _ = rand.Read(u.bits[:])
	milliseconds := uint64(now.UnixMilli())
	for i := 0; i < 6; i++ {
		u.bits[i] = byte(milliseconds >> (40 - 8*i))
	}
	u.bits[6] = u.bits[6]&0x0F | 0x70
	u.bits[8] = u.bits[8]&0x3F | 0x80
	return u
}

// UnsupportedTypePlaceholder is what Neo4j sends a Bolt 5 client for a value
// the protocol has no structure for (a VECTOR or a UUID): the map
// {reason: 'UNKNOWN_TYPE', originalType: <type>}, originalType as
// VECTOR(3, INTEGER64) or UUID. ok is false for any other value.
func UnsupportedTypePlaceholder(value interface{}) (map[string]interface{}, bool) {
	var originalType string
	switch typed := value.(type) {
	case CypherVector:
		originalType = "VECTOR(" + strconv.Itoa(typed.Dimension()) + ", " + typed.Type.String() + ")"
	case CypherUUID:
		originalType = "UUID"
	default:
		return nil, false
	}
	return map[string]interface{}{"reason": "UNKNOWN_TYPE", "originalType": originalType}, true
}

func init() {
	// Vectors and UUIDs are stored as properties: msgpack extensions 48 and
	// 49 (points are 47).
	msgpack.RegisterExtEncoder(48, CypherVector{}, func(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
		vector := value.Interface().(CypherVector)
		data := make([]byte, 1, 1+8*vector.Dimension())
		data[0] = byte(vector.Type)
		for i := 0; i < vector.Dimension(); i++ {
			bits := uint64(0)
			if vector.Type.IsFloat() {
				bits = math.Float64bits(vector.Floats[i])
			} else {
				bits = uint64(vector.Ints[i])
			}
			data = binary.BigEndian.AppendUint64(data, bits)
		}
		return data, nil
	})
	msgpack.RegisterExtDecoder(48, CypherVector{}, func(decoder *msgpack.Decoder, value reflect.Value, length int) error {
		data := make([]byte, length)
		if err := decoder.ReadFull(data); err != nil {
			return err
		}
		if len(data) < 1 || (len(data)-1)%8 != 0 || data[0] > byte(VectorFloat32) {
			return fmt.Errorf("vector: stored value of %d bytes is malformed", len(data))
		}
		vector := CypherVector{Type: VectorCoordinateType(data[0])}
		for at := 1; at < len(data); at += 8 {
			bits := binary.BigEndian.Uint64(data[at:])
			if vector.Type.IsFloat() {
				vector.Floats = append(vector.Floats, math.Float64frombits(bits))
			} else {
				vector.Ints = append(vector.Ints, int64(bits))
			}
		}
		value.Set(reflect.ValueOf(vector))
		return nil
	})
	msgpack.RegisterExtEncoder(49, CypherUUID{}, func(_ *msgpack.Encoder, value reflect.Value) ([]byte, error) {
		u := value.Interface().(CypherUUID)
		return u.bits[:], nil
	})
	msgpack.RegisterExtDecoder(49, CypherUUID{}, func(decoder *msgpack.Decoder, value reflect.Value, length int) error {
		var u CypherUUID
		if length != len(u.bits) {
			return fmt.Errorf("uuid: stored value has %d bytes, want 16", length)
		}
		if err := decoder.ReadFull(u.bits[:]); err != nil {
			return err
		}
		value.Set(reflect.ValueOf(u))
		return nil
	})
}
