package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// projectedNodeDecoder decodes the projected properties of one node scan's
// bodies (StreamNodesWithOptions with a Projection). A label-less property
// match scans every node and keeps almost none (#857), so what a rejected node
// costs decides the scan. decodeNodeFiltered paid, for every node, a property
// map, a bytes.Reader, the node ID string and a locked property-key dictionary
// lookup for each of the node's properties. The scan decoder instead:
//
//   - resolves the projected names to their key tokens once per database, so
//     each property is a token comparison and the others are skipped unread;
//   - decodes the projected values into one map it clears per node, and
//     copies them into the node's own map only for a node the filter keeps;
//   - reuses one reader and one msgpack decoder reading from it, and reads
//     the database from the key bytes without building the node ID (the body
//     holds the ID);
//   - rejects a node whose property must equal a string
//     (PropertyStringEquals) at the first stored value whose bytes differ,
//     without decoding it or the properties after it.
//
// Tokens resolved after the scan's read snapshot is open cover every body in
// it: a body is written with tokens that already exist. Not safe for
// concurrent use; one per scan.
type projectedNodeDecoder struct {
	b       *BadgerEngine
	include map[string]struct{}
	filter  func(map[string]interface{}) bool
	equals  map[string]string

	namespace      string
	namespaceBytes []byte
	tokens         map[uint64]projectedProperty // projected key tokens of namespace

	scratch map[string]interface{}
	reader  bytes.Reader
	// dec reads reader directly (a bytes.Reader is an io.ByteScanner, so the
	// decoder buffers nothing), so resetting reader starts the next body.
	dec *msgpack.Decoder
}

// projectedProperty is a projected property's name and, when the scan
// requires the property to equal a string (PropertyStringEquals), that string.
type projectedProperty struct {
	name      string
	equals    string
	hasEquals bool
}

// newProjectedNodeDecoder decodes opts.Projection, rejecting nodes by
// opts.PropertyStringEquals and opts.PropertyFilter.
func newProjectedNodeDecoder(b *BadgerEngine, opts StreamNodesOptions) *projectedNodeDecoder {
	d := &projectedNodeDecoder{
		b:       b,
		include: propertyProjectionSet(opts.Projection),
		filter:  opts.PropertyFilter,
		equals:  opts.PropertyStringEquals,
		scratch: make(map[string]interface{}, len(opts.Projection)),
	}
	d.dec = msgpack.NewDecoder(&d.reader)
	return d
}

// useNamespace selects the database of a node key's ID ("db:id"), resolving
// the projected tokens when it changes.
func (d *projectedNodeDecoder) useNamespace(id []byte) {
	namespace := id[:0]
	if idx := bytes.IndexByte(id, ':'); idx > 0 && idx < len(id)-1 {
		namespace = id[:idx]
	}
	if d.tokens != nil && bytes.Equal(namespace, d.namespaceBytes) {
		return
	}
	d.namespaceBytes = append(d.namespaceBytes[:0], namespace...)
	d.namespace = string(namespace)
	d.tokens = make(map[uint64]projectedProperty, len(d.include))
	for name := range d.include {
		if token, ok := d.b.propKeyDict.lookupID(d.namespace, name); ok {
			equals, hasEquals := d.equals[name]
			d.tokens[token] = projectedProperty{name: name, equals: equals, hasEquals: hasEquals}
		}
	}
}

// decode returns the node of body (the value of node key key) with only the
// projected properties, or nil when the filter rejects it. Errors follow
// decodeNodeFiltered: a body that is not V2 or is truncated.
func (d *projectedNodeDecoder) decode(key, data []byte) (*Node, error) {
	if len(key) <= 1 {
		return nil, nil
	}
	d.useNamespace(key[1:])
	if len(data) < 1 {
		return nil, fmt.Errorf("node body empty")
	}
	if data[0] != nodeFormatTokenizedV1 {
		return nil, fmt.Errorf("node body has unexpected format byte 0x%02x; expected V2 (0x%02x)", data[0], nodeFormatTokenizedV1)
	}
	rest := data[1:]
	propsLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("node body: malformed properties length varint")
	}
	rest = rest[n:]
	if uint64(len(rest)) < propsLen {
		return nil, fmt.Errorf("node body: properties payload truncated")
	}
	clear(d.scratch)
	kept, err := d.decodeProperties(rest[:propsLen])
	if err != nil {
		return nil, err
	}
	if !kept || (d.filter != nil && !d.filter(d.scratch)) {
		return nil, nil
	}
	node := &Node{}
	if err := decodeValue(rest[propsLen:], node); err != nil {
		return nil, err
	}
	node.Properties = make(map[string]interface{}, len(d.scratch))
	for name, value := range d.scratch {
		node.Properties[name] = value
	}
	return node, nil
}

// decodeProperties reads the tokenized property list (count, then per
// property a key token and a msgpack value) into scratch, decoding only the
// projected tokens. It returns false, leaving the rest unread, at a property
// whose stored value is not the string the scan requires.
func (d *projectedNodeDecoder) decodeProperties(data []byte) (bool, error) {
	if len(data) == 0 {
		return true, nil
	}
	count, consumed, err := readUvarint(data)
	if err != nil {
		return false, fmt.Errorf("decoding tokenized properties: count varint: %w", err)
	}
	rest := data[consumed:]
	d.reader.Reset(rest)
	dec := d.dec
	for i := uint64(0); i < count; i++ {
		offset := len(rest) - d.reader.Len()
		if offset >= len(rest) {
			return false, fmt.Errorf("decoding tokenized properties: ran out of bytes after %d/%d entries", i, count)
		}
		token, n, err := readUvarint(rest[offset:])
		if err != nil {
			return false, fmt.Errorf("decoding tokenized properties: key %d id varint: %w", i, err)
		}
		// The value follows the token; offsets stay relative to rest.
		d.reader.Reset(rest[offset+n:])
		property, wanted := d.tokens[token]
		if !wanted {
			if err := dec.Skip(); err != nil {
				return false, fmt.Errorf("decoding tokenized properties: skip key %d value: %w", token, err)
			}
			continue
		}
		if property.hasEquals && !msgpackStringEquals(rest[offset+n:], property.equals) {
			return false, nil
		}
		value, err := decodeStrictTypedValue(dec)
		if err != nil {
			return false, fmt.Errorf("decoding tokenized properties: key %q value: %w", property.name, err)
		}
		d.scratch[property.name] = restoreStoredTemporalValue(value)
	}
	if d.reader.Len() != 0 {
		return false, fmt.Errorf("decoding tokenized properties: %d trailing bytes", d.reader.Len())
	}
	return true, nil
}

// msgpackStringEquals reports whether raw starts with the msgpack encoding of
// a string equal to s. A value of another type, or a truncated string, is not
// equal: msgpack's str family decodes only to Go strings, and temporal and
// point values are extension types.
func msgpackStringEquals(raw []byte, s string) bool {
	if len(raw) == 0 {
		return false
	}
	var size, header int
	switch code := raw[0]; {
	case msgpcode.IsFixedString(code):
		size, header = int(code&msgpcode.FixedStrMask), 1
	case code == msgpcode.Str8 && len(raw) >= 2:
		size, header = int(raw[1]), 2
	case code == msgpcode.Str16 && len(raw) >= 3:
		size, header = int(binary.BigEndian.Uint16(raw[1:])), 3
	case code == msgpcode.Str32 && len(raw) >= 5:
		size, header = int(binary.BigEndian.Uint32(raw[1:])), 5
	default:
		return false
	}
	return size == len(s) && len(raw)-header >= size && string(raw[header:header+size]) == s
}
