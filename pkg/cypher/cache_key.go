package cypher

import (
	"encoding/binary"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"time"
)

// Cache key value tags: each parameter value is written as a tag and an
// unambiguous payload, so two values share a key only when they are the
// same Cypher value (#729). All Go integer widths are one Cypher INTEGER and
// share a tag; a typed Go list or map is written like the Cypher list or
// map it stands for.
const (
	cacheKeyNull         byte = 'n'
	cacheKeyBoolean      byte = 'b'
	cacheKeyInteger      byte = 'i'
	cacheKeyUint         byte = 'u'
	cacheKeyFloat        byte = 'd'
	cacheKeyString       byte = 's'
	cacheKeyBytes        byte = 'y'
	cacheKeyList         byte = 'l'
	cacheKeyMap          byte = 'm'
	cacheKeyRemoteEntity byte = 'e'
	cacheKeyTime         byte = 't'
	cacheKeyOther        byte = 'o'
)

// cacheKeyBuilder builds a cache key: the statement text and its
// parameters in an unambiguous encoding. The whole key is the cache's map
// key, so two statements share an entry only when their keys are equal,
// never on a hash collision.
type cacheKeyBuilder struct {
	buf []byte
}

func (k *cacheKeyBuilder) Write(p []byte) (int, error) {
	k.buf = append(k.buf, p...)
	return len(p), nil
}

// statementCacheKey is the key of text run with params: the text
// length-prefixed, then the parameters (writeCacheKeyParams). The walk
// observes the statement over the parameters' values: ok is false when it
// was canceled, and there is no key.
func statementCacheKey(work *materializedRowCheckpoint, text string, params map[string]interface{}) (key string, ok bool) {
	builder := cacheKeyBuilder{buf: make([]byte, 0, len(text)+16+16*len(params))}
	var scratch [binary.MaxVarintLen64 + 1]byte
	writeCacheKeyString(&builder, &scratch, text)
	if len(params) > 0 && !writeCacheKeyParams(work, &builder, &scratch, params) {
		return "", false
	}
	return string(builder.buf), true
}

// writeCacheKeyParams writes the parameters in sorted name order, each name
// length-prefixed and each value type-tagged (writeCacheKeyValue): 1, 1.0,
// "1" and true / "true" are different keys, as are a list and its text.
func writeCacheKeyParams(work *materializedRowCheckpoint, h *cacheKeyBuilder, scratch *[binary.MaxVarintLen64 + 1]byte, params map[string]interface{}) bool {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	if work.sortKeys(keys) {
		return false
	}
	for _, k := range keys {
		writeCacheKeyString(h, scratch, k)
		if !writeCacheKeyValue(work, h, scratch, params[k]) {
			return false
		}
	}
	return true
}

// writeCacheKeyValue writes value's tag and payload; false means the
// statement was canceled (its walk stopped).
func writeCacheKeyValue(work *materializedRowCheckpoint, h *cacheKeyBuilder, scratch *[binary.MaxVarintLen64 + 1]byte, value interface{}) bool {
	if work.visit() {
		return false
	}
	switch typed := value.(type) {
	case nil:
		h.Write([]byte{cacheKeyNull})
		return true
	case bool:
		payload := byte(0)
		if typed {
			payload = 1
		}
		h.Write([]byte{cacheKeyBoolean, payload})
		return true
	case int:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case int8:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case int16:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case int32:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case int64:
		writeCacheKeyInteger(h, typed)
		return true
	case uint8:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case uint16:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case uint32:
		writeCacheKeyInteger(h, int64(typed))
		return true
	case uint:
		writeCacheKeyUnsigned(h, uint64(typed))
		return true
	case uint64:
		writeCacheKeyUnsigned(h, typed)
		return true
	case float32:
		writeCacheKeyFloat(h, float64(typed))
		return true
	case float64:
		writeCacheKeyFloat(h, typed)
		return true
	case string:
		h.Write([]byte{cacheKeyString})
		writeCacheKeyString(h, scratch, typed)
		return true
	case []byte:
		h.Write([]byte{cacheKeyBytes})
		writeCacheKeyLength(h, scratch, len(typed))
		h.Write(typed)
		return true
	case []interface{}:
		h.Write([]byte{cacheKeyList})
		writeCacheKeyLength(h, scratch, len(typed))
		for _, item := range typed {
			if !writeCacheKeyValue(work, h, scratch, item) {
				return false
			}
		}
		return true
	case storage.RemoteEntityValue:
		// Graph functions distinguish a transport graph entity from an ordinary
		// map with identical contents; the result cache must distinguish them too.
		h.Write([]byte{cacheKeyRemoteEntity})
		return writeCacheKeyValue(work, h, scratch, map[string]interface{}(typed))
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		if work.sortKeys(keys) {
			return false
		}
		h.Write([]byte{cacheKeyMap})
		writeCacheKeyLength(h, scratch, len(keys))
		for _, key := range keys {
			writeCacheKeyString(h, scratch, key)
			if !writeCacheKeyValue(work, h, scratch, typed[key]) {
				return false
			}
		}
		return true
	case time.Time:
		h.Write([]byte{cacheKeyTime})
		binary.BigEndian.PutUint64(scratch[:8], uint64(typed.UnixNano()))
		h.Write(scratch[:8])
		writeCacheKeyString(h, scratch, typed.Location().String())
		return true
	}
	// Typed lists and maps ([]string, []int64, map[string]string, …) are the
	// Cypher list or map they hold.
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		h.Write([]byte{cacheKeyList})
		writeCacheKeyLength(h, scratch, reflected.Len())
		for index := 0; index < reflected.Len(); index++ {
			if !writeCacheKeyValue(work, h, scratch, reflected.Index(index).Interface()) {
				return false
			}
		}
		return true
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			keys := make([]string, 0, reflected.Len())
			for _, key := range reflected.MapKeys() {
				keys = append(keys, key.String())
			}
			if work.sortKeys(keys) {
				return false
			}
			h.Write([]byte{cacheKeyMap})
			writeCacheKeyLength(h, scratch, len(keys))
			for _, key := range keys {
				writeCacheKeyString(h, scratch, key)
				if !writeCacheKeyValue(work, h, scratch, reflected.MapIndex(reflect.ValueOf(key).Convert(reflected.Type().Key())).Interface()) {
					return false
				}
			}
			return true
		}
	case reflect.Pointer:
		if reflected.IsNil() {
			h.Write([]byte{cacheKeyNull})
			return true
		}
	}
	// Any other value (a temporal or spatial value, an entity) is its Go
	// type and its text: values of different types never share a key.
	h.Write([]byte{cacheKeyOther})
	writeCacheKeyString(h, scratch, fmt.Sprintf("%T", value))
	writeCacheKeyString(h, scratch, fmt.Sprintf("%v", value))
	return true
}

func writeCacheKeyInteger(h *cacheKeyBuilder, value int64) {
	var payload [9]byte
	payload[0] = cacheKeyInteger
	binary.BigEndian.PutUint64(payload[1:], uint64(value))
	h.Write(payload[:])
}

// writeCacheKeyUnsigned writes a uint / uint64: an INTEGER when it fits
// int64, else a tag of its own.
func writeCacheKeyUnsigned(h *cacheKeyBuilder, value uint64) {
	if value <= math.MaxInt64 {
		writeCacheKeyInteger(h, int64(value))
		return
	}
	var payload [9]byte
	payload[0] = cacheKeyUint
	binary.BigEndian.PutUint64(payload[1:], value)
	h.Write(payload[:])
}

func writeCacheKeyFloat(h *cacheKeyBuilder, value float64) {
	var payload [9]byte
	payload[0] = cacheKeyFloat
	binary.BigEndian.PutUint64(payload[1:], math.Float64bits(value))
	h.Write(payload[:])
}

func writeCacheKeyString(h *cacheKeyBuilder, scratch *[binary.MaxVarintLen64 + 1]byte, value string) {
	writeCacheKeyLength(h, scratch, len(value))
	h.Write([]byte(value))
}

func writeCacheKeyLength(h *cacheKeyBuilder, scratch *[binary.MaxVarintLen64 + 1]byte, length int) {
	n := binary.PutUvarint(scratch[:], uint64(length))
	h.Write(scratch[:n])
}
