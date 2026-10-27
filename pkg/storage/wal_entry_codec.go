package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
)

// WAL entry payload codec. The outer record framing (magic/version/length/
// CRC/trailer/alignment) is unchanged across v1..v3; only the payload's
// representation of the Data field differs:
//
//   - v1/v2: the whole WALEntry is one JSON object; encoding/json renders the
//     Data []byte field as base64, inflating it by ~33%.
//   - v3: [jsonLen:4 LE][json: WALEntry with Data omitted][dataLen:4 LE][data: raw]
//
// v3 exists to kill that base64 tax on the dominant buffered-commit payload
// (now also zstd-compressed). Readers dispatch on the per-record version byte
// and accept both layouts; new writes always emit v3.

// encodeWALEntryV3 marshals a WALEntry into the v3 payload layout. The Data
// field is appended as raw bytes rather than base64.
func encodeWALEntryV3(buf *bytes.Buffer, e *WALEntry) ([]byte, error) {
	header := WALEntry{
		Sequence:  e.Sequence,
		Timestamp: e.Timestamp,
		Operation: e.Operation,
		Checksum:  e.Checksum,
		Database:  e.Database,
	}
	jsonBytes, err := marshalJSONCompact(buf, &header)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to serialize entry header: %w", err)
	}

	payload := make([]byte, 4+len(jsonBytes)+4+len(e.Data))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(jsonBytes)))
	copy(payload[4:], jsonBytes)
	dataOff := 4 + len(jsonBytes)
	binary.LittleEndian.PutUint32(payload[dataOff:dataOff+4], uint32(len(e.Data)))
	copy(payload[dataOff+4:], e.Data)
	return payload, nil
}

// decodeWALEntry decodes a WAL entry payload into a WALEntry. Payloads from
// v1/v2 records are plain JSON (base64 Data); v3 payloads carry the Data
// field as raw bytes after the JSON header.
func decodeWALEntry(payload []byte, version uint8) (WALEntry, error) {
	if version > walFormatVersionPropertyEpoch {
		return WALEntry{}, fmt.Errorf("wal: unsupported WAL version %d (max supported: %d)", version, walFormatVersionPropertyEpoch)
	}
	if version < walFormatVersionV3 {
		var e WALEntry
		if err := jsontext.Unmarshal(payload, &e); err != nil {
			return WALEntry{}, err
		}
		return e, nil
	}

	if len(payload) < 8 {
		return WALEntry{}, fmt.Errorf("wal: v3 payload too short: %d bytes", len(payload))
	}
	jsonLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	if jsonLen < 0 || 4+jsonLen+4 > len(payload) {
		return WALEntry{}, fmt.Errorf("wal: v3 payload json length %d exceeds payload of %d bytes", jsonLen, len(payload))
	}
	dataOff := 4 + jsonLen
	dataLen := int(binary.LittleEndian.Uint32(payload[dataOff : dataOff+4]))
	if dataOff+4+dataLen != len(payload) {
		return WALEntry{}, fmt.Errorf("wal: v3 payload data length %d does not match payload of %d bytes", dataLen, len(payload))
	}

	var e WALEntry
	if err := jsontext.Unmarshal(payload[4:dataOff], &e); err != nil {
		return WALEntry{}, err
	}
	e.Data = payload[dataOff+4:]
	e.framingVersion = version
	if version == walFormatVersionPropertyEpoch && !isWALPropertyEpoch(e) {
		return WALEntry{}, fmt.Errorf("wal: framing version 4 is reserved for property dictionary controls")
	}
	return e, nil
}
