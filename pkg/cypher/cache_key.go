package cypher

import (
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"time"
)

// Cache key value tags: each parameter value is written as a tag and an
// unambiguous payload, so two values share a key only when they are the
// same Cypher value (#729). All Go integer widths are one Cypher INTEGER and
// share a tag; a typed Go list or map is written like the Cypher list or
// map it stands for.
const (
	cacheKeyNull    byte = 'n'
	cacheKeyBoolean byte = 'b'
	cacheKeyInteger byte = 'i'
	cacheKeyUint    byte = 'u'
	cacheKeyFloat   byte = 'd'
	cacheKeyString  byte = 's'
	cacheKeyBytes   byte = 'y'
	cacheKeyList    byte = 'l'
	cacheKeyMap     byte = 'm'
	cacheKeyTime    byte = 't'
	cacheKeyOther   byte = 'o'
)

// hashSortedParams writes the parameters into h in sorted name order, each
// name length-prefixed and each value type-tagged (writeCacheKeyValue): 1,
// 1.0, "1" and true / "true" are different keys, as are a list and its
// text.
func hashSortedParams(h hash.Hash64, params map[string]interface{}) {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var scratch [binary.MaxVarintLen64 + 1]byte
	for _, k := range keys {
		writeCacheKeyString(h, &scratch, k)
		writeCacheKeyValue(h, &scratch, params[k])
	}
}

// writeCacheKeyValue writes value's tag and payload.
func writeCacheKeyValue(h hash.Hash64, scratch *[binary.MaxVarintLen64 + 1]byte, value interface{}) {
	switch typed := value.(type) {
	case nil:
		h.Write([]byte{cacheKeyNull})
		return
	case bool:
		payload := byte(0)
		if typed {
			payload = 1
		}
		h.Write([]byte{cacheKeyBoolean, payload})
		return
	case int:
		writeCacheKeyInteger(h, int64(typed))
		return
	case int8:
		writeCacheKeyInteger(h, int64(typed))
		return
	case int16:
		writeCacheKeyInteger(h, int64(typed))
		return
	case int32:
		writeCacheKeyInteger(h, int64(typed))
		return
	case int64:
		writeCacheKeyInteger(h, typed)
		return
	case uint8:
		writeCacheKeyInteger(h, int64(typed))
		return
	case uint16:
		writeCacheKeyInteger(h, int64(typed))
		return
	case uint32:
		writeCacheKeyInteger(h, int64(typed))
		return
	case uint:
		writeCacheKeyUnsigned(h, uint64(typed))
		return
	case uint64:
		writeCacheKeyUnsigned(h, typed)
		return
	case float32:
		writeCacheKeyFloat(h, float64(typed))
		return
	case float64:
		writeCacheKeyFloat(h, typed)
		return
	case string:
		h.Write([]byte{cacheKeyString})
		writeCacheKeyString(h, scratch, typed)
		return
	case []byte:
		h.Write([]byte{cacheKeyBytes})
		writeCacheKeyLength(h, scratch, len(typed))
		h.Write(typed)
		return
	case []interface{}:
		h.Write([]byte{cacheKeyList})
		writeCacheKeyLength(h, scratch, len(typed))
		for _, item := range typed {
			writeCacheKeyValue(h, scratch, item)
		}
		return
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		h.Write([]byte{cacheKeyMap})
		writeCacheKeyLength(h, scratch, len(keys))
		for _, key := range keys {
			writeCacheKeyString(h, scratch, key)
			writeCacheKeyValue(h, scratch, typed[key])
		}
		return
	case time.Time:
		h.Write([]byte{cacheKeyTime})
		binary.BigEndian.PutUint64(scratch[:8], uint64(typed.UnixNano()))
		h.Write(scratch[:8])
		writeCacheKeyString(h, scratch, typed.Location().String())
		return
	}
	// Typed lists and maps ([]string, []int64, map[string]string, …) are the
	// Cypher list or map they hold.
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		h.Write([]byte{cacheKeyList})
		writeCacheKeyLength(h, scratch, reflected.Len())
		for index := 0; index < reflected.Len(); index++ {
			writeCacheKeyValue(h, scratch, reflected.Index(index).Interface())
		}
		return
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			keys := make([]string, 0, reflected.Len())
			for _, key := range reflected.MapKeys() {
				keys = append(keys, key.String())
			}
			sort.Strings(keys)
			h.Write([]byte{cacheKeyMap})
			writeCacheKeyLength(h, scratch, len(keys))
			for _, key := range keys {
				writeCacheKeyString(h, scratch, key)
				writeCacheKeyValue(h, scratch, reflected.MapIndex(reflect.ValueOf(key).Convert(reflected.Type().Key())).Interface())
			}
			return
		}
	case reflect.Pointer:
		if reflected.IsNil() {
			h.Write([]byte{cacheKeyNull})
			return
		}
	}
	// Any other value (a temporal or spatial value, an entity) is its Go
	// type and its text: values of different types never share a key.
	h.Write([]byte{cacheKeyOther})
	writeCacheKeyString(h, scratch, fmt.Sprintf("%T", value))
	writeCacheKeyString(h, scratch, fmt.Sprintf("%v", value))
}

func writeCacheKeyInteger(h hash.Hash64, value int64) {
	var payload [9]byte
	payload[0] = cacheKeyInteger
	binary.BigEndian.PutUint64(payload[1:], uint64(value))
	h.Write(payload[:])
}

// writeCacheKeyUnsigned writes a uint / uint64: an INTEGER when it fits
// int64, else a tag of its own.
func writeCacheKeyUnsigned(h hash.Hash64, value uint64) {
	if value <= math.MaxInt64 {
		writeCacheKeyInteger(h, int64(value))
		return
	}
	var payload [9]byte
	payload[0] = cacheKeyUint
	binary.BigEndian.PutUint64(payload[1:], value)
	h.Write(payload[:])
}

func writeCacheKeyFloat(h hash.Hash64, value float64) {
	var payload [9]byte
	payload[0] = cacheKeyFloat
	binary.BigEndian.PutUint64(payload[1:], math.Float64bits(value))
	h.Write(payload[:])
}

func writeCacheKeyString(h hash.Hash64, scratch *[binary.MaxVarintLen64 + 1]byte, value string) {
	writeCacheKeyLength(h, scratch, len(value))
	h.Write([]byte(value))
}

func writeCacheKeyLength(h hash.Hash64, scratch *[binary.MaxVarintLen64 + 1]byte, length int) {
	n := binary.PutUvarint(scratch[:], uint64(length))
	h.Write(scratch[:n])
}
