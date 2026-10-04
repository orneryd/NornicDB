package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
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
//   - reuses one reader, and reads the database from the key bytes without
//     building the node ID (the body holds the ID).
//
// Tokens resolved after the scan's read snapshot is open cover every body in
// it: a body is written with tokens that already exist. Not safe for
// concurrent use; one per scan.
type projectedNodeDecoder struct {
	b       *BadgerEngine
	include map[string]struct{}
	filter  func(map[string]interface{}) bool

	namespace      string
	namespaceBytes []byte
	tokens         map[uint64]string // projected key tokens of namespace

	scratch map[string]interface{}
	reader  bytes.Reader
}

func newProjectedNodeDecoder(b *BadgerEngine, projection []string, filter func(map[string]interface{}) bool) *projectedNodeDecoder {
	return &projectedNodeDecoder{
		b:       b,
		include: propertyProjectionSet(projection),
		filter:  filter,
		scratch: make(map[string]interface{}, len(projection)),
	}
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
	d.tokens = make(map[uint64]string, len(d.include))
	for name := range d.include {
		if token, ok := d.b.propKeyDict.lookupID(d.namespace, name); ok {
			d.tokens[token] = name
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
	if err := d.decodeProperties(rest[:propsLen]); err != nil {
		return nil, err
	}
	if d.filter != nil && !d.filter(d.scratch) {
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
// projected tokens.
func (d *projectedNodeDecoder) decodeProperties(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	count, consumed, err := readUvarint(data)
	if err != nil {
		return fmt.Errorf("decoding tokenized properties: count varint: %w", err)
	}
	rest := data[consumed:]
	d.reader.Reset(rest)
	dec := msgpack.GetDecoder()
	dec.Reset(&d.reader)
	defer msgpack.PutDecoder(dec)
	for i := uint64(0); i < count; i++ {
		offset := len(rest) - d.reader.Len()
		if offset >= len(rest) {
			return fmt.Errorf("decoding tokenized properties: ran out of bytes after %d/%d entries", i, count)
		}
		token, n, err := readUvarint(rest[offset:])
		if err != nil {
			return fmt.Errorf("decoding tokenized properties: key %d id varint: %w", i, err)
		}
		if _, err := d.reader.Seek(int64(offset+n), 0); err != nil {
			return fmt.Errorf("decoding tokenized properties: advance past key %d: %w", i, err)
		}
		name, wanted := d.tokens[token]
		if !wanted {
			if err := dec.Skip(); err != nil {
				return fmt.Errorf("decoding tokenized properties: skip key %d value: %w", token, err)
			}
			continue
		}
		value, err := decodeStrictTypedValue(dec)
		if err != nil {
			return fmt.Errorf("decoding tokenized properties: key %q value: %w", name, err)
		}
		d.scratch[name] = restoreStoredTemporalValue(value)
	}
	if d.reader.Len() != 0 {
		return fmt.Errorf("decoding tokenized properties: %d trailing bytes", d.reader.Len())
	}
	return nil
}
