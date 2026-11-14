package storage

import (
	"bytes"

	"github.com/dgraph-io/badger/v4/experimental/columnar"
	"github.com/vmihailenco/msgpack/v5"
)

// experimentalColumnLayout annotates existing bytes only. No property names,
// dictionary cache, current view or mutable graph state enter physical encoding.
// Unrecognized/corrupt application frames remain opaque and retain their bytes;
// the ordinary application decoder remains responsible for their diagnostics.
func experimentalColumnLayout(key, value []byte) []columnar.Span {
	if len(key) == 0 || len(value) == 0 {
		return nil
	}
	kind := key[0]
	var spans []columnar.Span
	switch kind {
	case prefixNode, prefixMVCCNode:
		envelope, err := SplitNodeEnvelope(value)
		if err != nil {
			return nil
		}
		base := len(value) - len(envelope.metadata) - len(envelope.properties)
		spans, ok := propertyColumnSpans(spans, kind|0x80, envelope.properties, base)
		if !ok {
			return nil
		}
		return metadataColumnSpans(spans, kind, envelope.metadata, base+len(envelope.properties))
	case prefixEdge, prefixMVCCEdge, prefixOutgoingIndex, prefixIncomingIndex:
		var edge Edge
		_, _, end, err := decodeEdgeCompactHeaderInto(value, edgeFormatCompactV2, &edge)
		if err != nil {
			return nil
		}
		spans = append(spans, columnar.Span{Start: 0, Length: 2, Kind: kind, Column: 0})
		for i := 0; i < 5; i++ {
			spans = append(spans, columnar.Span{Start: 2 + i*8, Length: 8, Kind: kind, Column: uint64(i + 1)})
		}
		spans = append(spans, columnar.Span{Start: end - len(edge.Type), Length: len(edge.Type), Kind: kind, Column: 6})
		if (kind == prefixEdge || kind == prefixMVCCEdge) && end < len(value) {
			var ok bool
			spans, ok = propertyColumnSpans(spans, kind|0x80, value[end:], end)
			if !ok {
				return nil
			}
		}
		return spans
	default:
		return nil
	}
}
func propertyColumnSpans(spans []columnar.Span, kind byte, data []byte, base int) ([]columnar.Span, bool) {
	original := len(spans)
	_, err := walkPhysicalPropertyValues(data, nil, func(token uint64, start, end int) {
		spans = append(spans, columnar.Span{Start: base + start, Length: end - start, Kind: kind, Column: token})
	})
	if err != nil {
		return spans[:original], false
	}
	return spans, true
}

// Metadata field order belongs to the existing Node MessagePack encoder. Its
// keys stay in the structural stream; values align by their encoded ordinal.
func metadataColumnSpans(spans []columnar.Span, kind byte, data []byte, base int) []columnar.Span {
	serializer, payload, ok, err := splitSerializationHeader(data)
	if err != nil || !ok || serializer != serializerIDMsgpack {
		return spans
	}
	base += len(data) - len(payload)
	original := len(spans)
	reader := bytes.NewReader(payload)
	decoder := msgpack.GetDecoder()
	decoder.Reset(reader)
	defer msgpack.PutDecoder(decoder)
	count, err := decoder.DecodeMapLen()
	if err != nil || count < 0 || count > 256 {
		return spans
	}
	for i := 0; i < count; i++ {
		if decoder.Skip() != nil {
			return spans[:original]
		}
		start := len(payload) - reader.Len()
		if decoder.Skip() != nil {
			return spans[:original]
		}
		end := len(payload) - reader.Len()
		spans = append(spans, columnar.Span{Start: base + start, Length: end - start, Kind: kind, Column: uint64(i)})
	}
	if reader.Len() != 0 {
		return spans[:original]
	}
	return spans
}
