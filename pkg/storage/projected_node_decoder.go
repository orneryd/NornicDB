package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
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
//   - resolves a stored property token to its name through the scan's own
//     Badger snapshot (readStoredBinding), once per token per database and
//     whether or not the projection wants it, so each property is a memo
//     lookup and the names are the ones the stored bodies were written with;
//   - decodes the projected values into one map it clears per node, and
//     copies them into the node's own map only for a node the filter keeps;
//   - reuses one reader and one msgpack decoder reading from it, and reads
//     the database from the key bytes without building the node ID (the body
//     holds the ID);
//   - rejects a node whose property must equal a string
//     (PropertyStringEquals) at the first stored value whose bytes differ,
//     without decoding it or the properties after it;
//   - when the caller names the properties its filter reads
//     (PropertyFilterProjection) for a scan without a projection, decodes those
//     and, in the same token walk, finds the encoded span of every other value
//     without decoding it, so a body the filter rejects costs them alone
//     (#1682).
//
// A scan without a projection decodes every property and filters before it
// materializes the node, so a filter alone still spares a rejected node its
// map and node; the required-property hint also spares a rejected body its
// other values, and the body the filter keeps decodes the spans that walk
// recorded, in stored order, through the same per-value owner. The kept node's
// sidecar is settled through the same snapshot: metadata for QueryNodes, and
// vectors too for WithEmbeddings. Resolved names belong to that read snapshot;
// dropping and recreating a database cannot rename a previously stored body's
// properties. The memo covers one scan in one database. Not concurrency-safe.
type projectedNodeDecoder struct {
	b       *BadgerEngine
	txn     *badger.Txn
	include map[string]struct{}
	filter  func(map[string]interface{}) bool
	equals  map[string]string
	// filterSelection is the required-property hint of a scan without a
	// projection: the names PropertyFilter reads, so a rejected body costs
	// those properties only. nil decodes the whole body before the filter.
	filterSelection map[string]struct{}
	// hydration preserves the caller's body, query-metadata or full-vector
	// read contract. Sidecars are read only after the filter keeps the body.
	hydration nodeItemHydration

	namespace      string
	namespaceBytes []byte
	// bindings memoizes this scan's token resolutions for the current
	// database, resolved through txn.
	bindings map[uint64]projectedProperty

	scratch map[string]interface{}
	// deferred is this scan's one reusable slice of the stored values a hinted
	// walk skipped: their encoded spans, borrowed from the body being decoded,
	// which the filter's surviving body decodes in stored order. It is emptied
	// before decode returns, on success and on error, so no body outlives its
	// row. It grows with the properties a body actually carries, never with a
	// count the body merely declares.
	deferred []deferredProperty
	reader   bytes.Reader
	// dec reads reader directly (a bytes.Reader is an io.ByteScanner, so the
	// decoder buffers nothing), so resetting reader starts the next body.
	dec *msgpack.Decoder
}

// projectedProperty is a resolved property token: the name it was stored with,
// whether the scan's projection wants it, and, when the scan requires the
// property to equal a string (PropertyStringEquals), that string.
type projectedProperty struct {
	name      string
	wanted    bool
	equals    string
	hasEquals bool
}

// deferredProperty is one stored property value a hinted walk skipped to its
// span without decoding it: the resolved property it belongs to and the
// msgpack bytes it occupies in the body being decoded.
type deferredProperty struct {
	property projectedProperty
	encoded  []byte
}

// newProjectedNodeDecoder decodes opts.Projection, every property when it is
// nil, rejecting nodes by opts.PropertyStringEquals and opts.PropertyFilter. txn is the read snapshot
// of the scan whose bodies it decodes; nil is the context-free standalone
// boundary (a body that belongs to no stored snapshot), which resolves through
// the in-memory dictionary.
func newProjectedNodeDecoder(b *BadgerEngine, txn *badger.Txn, opts StreamNodesOptions) *projectedNodeDecoder {
	d := &projectedNodeDecoder{
		b:       b,
		txn:     txn,
		include: propertyProjectionSet(opts.Projection),
		filter:  opts.PropertyFilter,
		equals:  opts.PropertyStringEquals,
		scratch: make(map[string]interface{}, len(opts.Projection)),
	}
	if opts.Projection == nil && opts.PropertyFilter != nil && len(opts.PropertyFilterProjection) > 0 {
		d.filterSelection = propertyProjectionSet(opts.PropertyFilterProjection)
	}
	if opts.WithEmbeddings {
		d.hydration = nodeItemFullEmbeddings
	} else if opts.QueryNodes {
		d.hydration = nodeItemQueryMetadata
	}
	d.dec = msgpack.NewDecoder(&d.reader)
	return d
}

// useNamespace selects the database of a node key's ID ("db:id"). A database
// change starts a new token memo: a token number names a property per database.
func (d *projectedNodeDecoder) useNamespace(id []byte) {
	namespace := id[:0]
	if idx := bytes.IndexByte(id, ':'); idx > 0 && idx < len(id)-1 {
		namespace = id[:idx]
	}
	if d.bindings != nil && bytes.Equal(namespace, d.namespaceBytes) {
		return
	}
	d.namespaceBytes = append(d.namespaceBytes[:0], namespace...)
	d.namespace = string(namespace)
	if d.bindings == nil {
		d.bindings = make(map[uint64]projectedProperty)
	} else {
		clear(d.bindings)
	}
}

// resolveToken returns what this scan knows about one stored property token: the
// name the body was written with, whether the scan wants it, and the required
// string when it has one. The binding is resolved through the scan's snapshot
// once per token; the memo is safe because every body of the scan is read from
// the same snapshot. A nil include wants every property, so a token the
// snapshot does not name is an error there, as it is for the full decoder
// (decodeNodeProjected): the node would otherwise lose a property silently. A
// projection that does not name the token selects nothing, as it did not
// before, and its value is skipped unread.
func (d *projectedNodeDecoder) resolveToken(token uint64) (projectedProperty, error) {
	if property, resolved := d.bindings[token]; resolved {
		return property, nil
	}
	property := projectedProperty{}
	name, found, err := d.b.readStoredBinding(d.txn, storedBinding{kind: propertyNameBinding, namespace: d.namespace, number: token})
	if err != nil {
		return property, fmt.Errorf("reading property key id %d for namespace %q: %w", token, d.namespace, err)
	}
	if !found {
		if d.include == nil {
			return property, fmt.Errorf("property key id %d not in dictionary for namespace %q", token, d.namespace)
		}
		d.bindings[token] = property
		return property, nil
	}
	property.name = name
	property.wanted = d.include == nil
	if !property.wanted {
		_, property.wanted = d.include[name]
	}
	property.equals, property.hasEquals = d.equals[name]
	d.bindings[token] = property
	return property, nil
}

// decode returns the node of body (the value of node key key) with only the
// projected properties (every property when the scan has no projection), or nil
// when the filter rejects it. Invalid envelopes and values that must be read fail decoding; a rejected
// body need not decode values outside the declared filter properties.
func (d *projectedNodeDecoder) decode(key, data []byte) (*Node, error) {
	if len(key) <= 1 {
		return nil, nil
	}
	d.useNamespace(key[1:])
	envelope, err := SplitNodeEnvelope(data)
	if err != nil {
		return nil, err
	}
	// One token walk per body. A scan without a projection whose caller named
	// the filter's properties (#1682) decodes those in that walk and records
	// the encoded span of every other value without decoding it, so a rejected
	// node costs its required properties, never its node, its remaining values
	// or its sidecar; the body the filter keeps decodes those spans, in stored
	// order, through the same per-value owner, so its values and value decode
	// errors match the whole-body decode. Framing/skip errors remain errors even
	// when the filter would reject the body.
	// The spans borrow this body, which lives no longer than this call.
	if d.filterSelection != nil {
		defer d.releaseDeferred()
	}
	clear(d.scratch)
	kept, err := d.decodeProperties(envelope.properties)
	if err != nil {
		return nil, err
	}
	filterSettled := false
	if d.filterSelection != nil {
		if !kept || !d.filter(d.scratch) {
			return nil, nil
		}
		// Keep the selected values: the deferred decode reads only the values
		// the filter did not already decode, into the same map.
		if kept, err = d.decodeDeferredProperties(); err != nil {
			return nil, err
		}
		filterSettled = true
	}
	if !kept || (!filterSettled && d.filter != nil && !d.filter(d.scratch)) {
		return nil, nil
	}
	node := &Node{}
	if err := decodeValue(envelope.metadata, node); err != nil {
		return nil, err
	}
	node.Properties = make(map[string]interface{}, len(d.scratch))
	for name, value := range d.scratch {
		node.Properties[name] = value
	}
	if d.hydration != nodeItemBodyOnly {
		// Settle the requested sidecar state from this scan's snapshot over
		// the body already decoded, just as readNodeItemInTxn does. Full-read
		// callers keep their vectors; query reads load metadata only. Rejected
		// bodies pay for neither, and kept bodies need no second body read.
		return d.b.loadNodeEmbeddingState(embeddingRecordReader{txn: d.txn}, node, NodeID(key[1:]), d.hydration == nodeItemFullEmbeddings)
	}
	return node, nil
}

// decodeProperties reads the tokenized property list (count, then per property
// a key token and a msgpack value) into scratch in one walk of the stored
// tokens. A property the scan selects, whether its projection wants it or the
// hint says its filter reads it, is decoded here; an unselected value is
// skipped, and a hinted walk records the span it occupies for
// decodeDeferredProperties instead of decoding it. It returns false, leaving
// the rest unread, at a property whose stored value is not the string the scan
// requires.
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
		property, err := d.resolveToken(token)
		if err != nil {
			return false, err
		}
		// The memoized selection is the projection's; a hinted scan reads the
		// filter's names instead, and defers the rest.
		selected := property.wanted
		if d.filterSelection != nil {
			_, selected = d.filterSelection[property.name]
		}
		if !selected {
			// Skip the value to find the span it occupies. A hinted scan
			// records that span for the body its filter keeps; a property its
			// projection does not want is never decoded.
			before := d.reader.Len()
			if err := dec.Skip(); err != nil {
				return false, fmt.Errorf("decoding tokenized properties: skip key %d value: %w", token, err)
			}
			if d.filterSelection != nil {
				start := offset + n
				d.deferred = append(d.deferred, deferredProperty{property: property, encoded: rest[start : start+before-d.reader.Len()]})
			}
			continue
		}
		kept, err := d.decodePropertyValue(dec, property, rest[offset+n:])
		if err != nil || !kept {
			return false, err
		}
	}
	if d.reader.Len() != 0 {
		return false, fmt.Errorf("decoding tokenized properties: %d trailing bytes", d.reader.Len())
	}
	return true, nil
}

// decodePropertyValue decodes the stored value the decoder is positioned at,
// which starts at raw, into scratch under the property's name. It is the one
// owner of a kept value's semantics: the token walk and a hinted walk's
// deferred decode both come through it, so a value's Go type, the
// PropertyStringEquals rejection and the error it reports are the same
// whichever walk read it.
func (d *projectedNodeDecoder) decodePropertyValue(dec *msgpack.Decoder, property projectedProperty, raw []byte) (bool, error) {
	if property.hasEquals && !msgpackStringEquals(raw, property.equals) {
		return false, nil
	}
	value, err := decodeStrictTypedValue(dec)
	if err != nil {
		return false, fmt.Errorf("decoding tokenized properties: key %q value: %w", property.name, err)
	}
	d.scratch[property.name] = restoreStoredTemporalValue(value)
	return true, nil
}

// decodeDeferredProperties decodes the stored values a hinted walk recorded, in
// the order it recorded them, through the same per-value owner, so the body its
// filter keeps is the row, and reports the errors, of a body decoded in one
// pass.
func (d *projectedNodeDecoder) decodeDeferredProperties() (bool, error) {
	for _, deferred := range d.deferred {
		d.reader.Reset(deferred.encoded)
		kept, err := d.decodePropertyValue(d.dec, deferred.property, deferred.encoded)
		if err != nil || !kept {
			return false, err
		}
	}
	return true, nil
}

// releaseDeferred drops the spans borrowed from the body just decoded, so no
// hinted body outlives the call that read it and the next row starts from none. The
// collected capacity stays with the scan, bounded by the properties its widest
// kept-or-rejected body carried.
func (d *projectedNodeDecoder) releaseDeferred() {
	clear(d.deferred)
	d.deferred = d.deferred[:0]
	d.reader.Reset(nil)
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

// NodeEnvelope contains borrowed V2 property and metadata payloads. Keep the
// input alive and unchanged while using either view. Payload interpretation and
// field selection belong to the consumer; framing belongs to SplitNodeEnvelope.
type NodeEnvelope struct {
	properties []byte
	metadata   []byte
}

// PropertyPayload returns the borrowed serialized property payload.
func (e NodeEnvelope) PropertyPayload() []byte { return e.properties }

// MetadataPayload returns the borrowed metadata payload, including its serializer header.
func (e NodeEnvelope) MetadataPayload() []byte { return e.metadata }

// SplitNodeEnvelope validates the V2 format byte and property-length framing.
// It never decodes properties or metadata and never copies payloads. Full,
// projected, metadata-only (label) and WAL consumers share this owner; metadata
// is interpreted only by the stored decoder (decodeValue). Other stored
// operation clients must reuse it rather than implement another splitter.
func SplitNodeEnvelope(data []byte) (NodeEnvelope, error) {
	envelope, failure := splitNodeEnvelope(data)
	switch failure {
	case nodeEnvelopeEmpty:
		return NodeEnvelope{}, fmt.Errorf("node body empty")
	case nodeEnvelopeWrongFormat:
		return NodeEnvelope{}, fmt.Errorf("node body has unexpected format byte 0x%02x; expected V2 (0x%02x)", data[0], nodeFormatTokenizedV1)
	case nodeEnvelopeShortVarint, nodeEnvelopeOverflowVarint:
		return NodeEnvelope{}, fmt.Errorf("node body: malformed properties length varint")
	case nodeEnvelopeShortProperties:
		return NodeEnvelope{}, fmt.Errorf("node body: properties payload truncated")
	}
	return envelope, nil
}

// nodeEnvelopeFailure preserves protocol-specific diagnostics without a second
// framing reader. The exported reader and WAL bind these same parser facts.
type nodeEnvelopeFailure uint8

const (
	nodeEnvelopeValid nodeEnvelopeFailure = iota
	nodeEnvelopeEmpty
	nodeEnvelopeWrongFormat
	nodeEnvelopeShortVarint
	nodeEnvelopeOverflowVarint
	nodeEnvelopeShortProperties
)

func splitNodeEnvelope(data []byte) (NodeEnvelope, nodeEnvelopeFailure) {
	if len(data) == 0 {
		return NodeEnvelope{}, nodeEnvelopeEmpty
	}
	if data[0] != nodeFormatTokenizedV1 {
		return NodeEnvelope{}, nodeEnvelopeWrongFormat
	}
	rest := data[1:]
	propsLen, n := binary.Uvarint(rest)
	if n == 0 {
		return NodeEnvelope{}, nodeEnvelopeShortVarint
	}
	if n < 0 {
		return NodeEnvelope{}, nodeEnvelopeOverflowVarint
	}
	rest = rest[n:]
	if uint64(len(rest)) < propsLen {
		return NodeEnvelope{}, nodeEnvelopeShortProperties
	}
	return NodeEnvelope{properties: rest[:propsLen], metadata: rest[propsLen:]}, nodeEnvelopeValid
}
