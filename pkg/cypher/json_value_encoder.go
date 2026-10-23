package cypher

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// The one JSON encoder of Cypher values. It writes what encoding/json writes
// (Marshal, or MarshalIndent with an indent: map keys in order, the same
// string escaping and number forms), but it walks the value's lists and maps
// itself, a member at a time, and escapes a long string a chunk at a time,
// each step through the cancellation owner's check (walk). encoding/json
// sees only one scalar, or one chunk of a string, per call.

// encodeJSONValue is json.MarshalIndent(value, prefix, indent) (json.Marshal
// when indent is empty and prefix too).
func encodeJSONValue(walk *materializedRowCheckpoint, value interface{}, prefix, indent string) ([]byte, error) {
	var out bytes.Buffer
	if err := writeJSONValue(walk, &out, value, prefix, indent, 0); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeJSONValue(walk *materializedRowCheckpoint, out *bytes.Buffer, value interface{}, prefix, indent string, depth int) error {
	if err := walk.Observe(); err != nil {
		return err
	}
	newline := func(level int) {
		if indent == "" && prefix == "" {
			return
		}
		out.WriteByte('\n')
		out.WriteString(prefix)
		for i := 0; i < level; i++ {
			out.WriteString(indent)
		}
	}
	switch typed := value.(type) {
	case string:
		return writeJSONString(walk, out, typed)
	case map[string]interface{}:
		if typed == nil {
			out.WriteString("null")
			return nil
		}
		if len(typed) == 0 {
			out.WriteString("{}")
			return nil
		}
		keys := make([]string, 0, len(typed))
		for key := range observedEntries(walk, typed) {
			keys = append(keys, key)
		}
		sortWalkKeys(walk, keys)
		out.WriteByte('{')
		for index, key := range observedItems(walk, keys) {
			if index > 0 {
				out.WriteByte(',')
			}
			newline(depth + 1)
			if err := writeJSONString(walk, out, key); err != nil {
				return err
			}
			out.WriteByte(':')
			if indent != "" || prefix != "" {
				out.WriteByte(' ')
			}
			if err := writeJSONValue(walk, out, typed[key], prefix, indent, depth+1); err != nil {
				return err
			}
		}
		newline(depth)
		out.WriteByte('}')
		return nil
	case []interface{}:
		if typed == nil {
			out.WriteString("null")
			return nil
		}
		return writeJSONList(walk, out, len(typed), func(index int) interface{} { return typed[index] }, prefix, indent, depth, newline)
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		if reflected.Kind() == reflect.Slice && reflected.IsNil() {
			out.WriteString("null")
			return nil
		}
		if reflected.Type().Elem().Kind() == reflect.Uint8 {
			// []byte is base64 text, as encoding/json writes it.
			break
		}
		return writeJSONList(walk, out, reflected.Len(), func(index int) interface{} { return reflected.Index(index).Interface() }, prefix, indent, depth, newline)
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			if reflected.IsNil() {
				out.WriteString("null")
				return nil
			}
			members := make(map[string]interface{}, reflected.Len())
			for entries := reflected.MapRange(); entries.Next(); {
				if err := walk.Observe(); err != nil {
					return err
				}
				members[entries.Key().String()] = entries.Value().Interface()
			}
			return writeJSONValue(walk, out, members, prefix, indent, depth)
		}
	}
	// A scalar, or a value whose encoding is its own (time, a struct).
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	out.Write(encoded)
	return nil
}

func writeJSONList(walk *materializedRowCheckpoint, out *bytes.Buffer, length int, member func(int) interface{}, prefix, indent string, depth int, newline func(int)) error {
	if length == 0 {
		out.WriteString("[]")
		return nil
	}
	out.WriteByte('[')
	for index := range observedCount(walk, length) {
		if index > 0 {
			out.WriteByte(',')
		}
		newline(depth + 1)
		if err := writeJSONValue(walk, out, member(index), prefix, indent, depth+1); err != nil {
			return err
		}
	}
	newline(depth)
	out.WriteByte(']')
	return nil
}

// writeJSONString writes text as a JSON string, escaping at most
// bulk.CopyStride bytes (cut at a rune) between observations.
func writeJSONString(walk *materializedRowCheckpoint, out *bytes.Buffer, text string) error {
	out.WriteByte('"')
	for len(text) > 0 {
		if err := walk.Observe(); err != nil {
			return err
		}
		end := min(len(text), bulk.CopyStride)
		for end < len(text) && end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		if end == 0 {
			end = min(len(text), bulk.CopyStride)
		}
		encoded, err := json.Marshal(text[:end])
		if err != nil {
			return err
		}
		out.Write(encoded[1 : len(encoded)-1])
		text = text[end:]
	}
	out.WriteByte('"')
	return nil
}

// writeObservedText writes text to out at most bulk.CopyStride bytes at a
// time, each through the cancellation owner's check (walk): the one writer of
// data-sized text into a builder.
func writeObservedText(walk *materializedRowCheckpoint, out interface{ WriteString(string) (int, error) }, text string) {
	for len(text) > 0 {
		walk.Check()
		end := min(len(text), bulk.CopyStride)
		out.WriteString(text[:end])
		text = text[end:]
	}
}

// writeObservedBytes is writeObservedText for encoded bytes, written without
// a copy of them into a string first.
func writeObservedBytes(walk *materializedRowCheckpoint, out interface{ Write([]byte) (int, error) }, text []byte) {
	for len(text) > 0 {
		walk.Check()
		end := min(len(text), bulk.CopyStride)
		out.Write(text[:end])
		text = text[end:]
	}
}
