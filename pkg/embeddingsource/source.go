// Package embeddingsource defines the input contract carried by managed vectors.
package embeddingsource

import (
	"strconv"
	"strings"
)

// MetaKey is reserved for the persisted consumed-source descriptor.
const MetaKey = "__nornic_embedding_source"

// Source describes inputs consumed by one result, including its chunks. Text
// uses the recorded worker policy; Properties names whole node-property values,
// including absent keys whose later addition changes input. AllProperties is
// the conservative default for legacy structured APIs. Providers declare
// Text/Properties/AllProperties; the text builder supplies the captured policy.
type Source struct {
	Text          bool
	IncludeLabels bool
	Include       []string
	Exclude       []string
	Properties    []string
	AllProperties bool
}

// Metadata returns an immutable, codec-neutral snapshot. Its private v1 format
// is version:flags: followed by three counted, byte-length-prefixed key lists
// (include, exclude, properties). Flags are text=1, all=2, labels=4. Key bytes
// need no delimiter escaping. A string avoids repeated deep copies of a nested
// descriptor on every node/transaction copy; both JSON and msgpack preserve it.
func (s Source) Metadata() string {
	flags := 0
	if s.Text {
		flags |= 1
	}
	if s.AllProperties {
		flags |= 2
	}
	if s.IncludeLabels {
		flags |= 4
	}
	var b strings.Builder
	size := 32
	for _, keys := range [][]string{s.Include, s.Exclude, s.Properties} {
		for _, key := range keys {
			size += len(key) + 8
		}
	}
	b.Grow(size)
	b.WriteString("1:")
	b.WriteString(strconv.Itoa(flags))
	b.WriteByte(':')
	for _, keys := range [][]string{s.Include, s.Exclude, s.Properties} {
		b.WriteString(strconv.Itoa(len(keys)))
		b.WriteByte(':')
		for _, key := range keys {
			b.WriteString(strconv.Itoa(len(key)))
			b.WriteByte(':')
			b.WriteString(key)
		}
	}
	return b.String()
}

// Read decodes an immutable snapshot. Unsupported or malformed present records
// never fall back to a narrower legacy source definition. Counts and lengths
// are bounded by the record itself before allocating or slicing.
func Read(meta map[string]any) (s Source, present, valid bool) {
	raw, present := meta[MetaKey]
	if !present {
		return s, false, true
	}
	data, ok := raw.(string)
	if !ok {
		return s, true, false
	}
	at := 0
	number := func() (int, bool) {
		start, n := at, 0
		for at < len(data) && data[at] != ':' {
			d := int(data[at] - '0')
			if data[at] < '0' || data[at] > '9' || n > len(data)/10 || n*10+d > len(data) {
				return 0, false
			}
			n = n*10 + d
			at++
		}
		if at == start || at >= len(data) {
			return 0, false
		}
		at++
		return n, true
	}
	version, ok := number()
	if !ok || version != 1 {
		return s, true, false
	}
	flags, ok := number()
	if !ok || flags&^7 != 0 {
		return s, true, false
	}
	s.Text, s.AllProperties, s.IncludeLabels = flags&1 != 0, flags&2 != 0, flags&4 != 0
	read := func() ([]string, bool) {
		n, ok := number()
		if !ok || n > len(data)-at {
			return nil, false
		}
		keys := make([]string, n)
		for i := range keys {
			length, ok := number()
			if !ok || length > len(data)-at {
				return nil, false
			}
			keys[i] = data[at : at+length]
			at += length
		}
		return keys, true
	}
	if s.Include, ok = read(); !ok {
		return s, true, false
	}
	if s.Exclude, ok = read(); !ok {
		return s, true, false
	}
	if s.Properties, ok = read(); !ok {
		return s, true, false
	}
	return s, true, at == len(data)
}
