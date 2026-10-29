package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// WAL entry payloads use a JSON metadata header followed by raw Data bytes:
// [jsonLen:4 LE][json header][dataLen:4 LE][data]. The outer framing identifies
// the current format. The V4 startup migration converts supported historical
// framing before ordinary readers run.
type walPayloadMode uint8

const (
	walPayloadEncode walPayloadMode = iota
	walPayloadDecode
)

// processWALEntryPayload owns both directions of the WAL entry payload format.
// Encoding returns a new payload. Decoding returns nil bytes, leaves Data
// pointing into payload, and clears entry on every error.
func processWALEntryPayload(mode walPayloadMode, buf *bytes.Buffer, entry *WALEntry, payload []byte, version uint8) ([]byte, error) {
	switch mode {
	case walPayloadEncode:
		header := WALEntry{
			Sequence:      entry.Sequence,
			Timestamp:     entry.Timestamp,
			Operation:     entry.Operation,
			Checksum:      entry.Checksum,
			Database:      entry.Database,
			PropertyEpoch: entry.PropertyEpoch,
		}
		jsonBytes, err := marshalJSONCompact(buf, &header)
		if err != nil {
			return nil, fmt.Errorf("wal: failed to serialize entry header: %w", err)
		}
		// Never acknowledge a record recovery would be unable to read.
		if len(jsonBytes) > int(walMaxEntrySize)-8 || len(entry.Data) > int(walMaxEntrySize)-8-len(jsonBytes) {
			return nil, fmt.Errorf("wal: entry exceeds maximum WAL frame size of %d bytes", walMaxEntrySize)
		}
		encoded := make([]byte, 4+len(jsonBytes)+4+len(entry.Data))
		binary.LittleEndian.PutUint32(encoded[0:4], uint32(len(jsonBytes)))
		copy(encoded[4:], jsonBytes)
		dataOff := 4 + len(jsonBytes)
		binary.LittleEndian.PutUint32(encoded[dataOff:dataOff+4], uint32(len(entry.Data)))
		copy(encoded[dataOff+4:], entry.Data)
		return encoded, nil

	case walPayloadDecode:
		*entry = WALEntry{}
		if version != walFormatVersionCurrent {
			return nil, unsupportedWALVersion(version)
		}
		if len(payload) < 8 {
			return nil, fmt.Errorf("wal: v3 payload too short: %d bytes", len(payload))
		}
		jsonLen := int(binary.LittleEndian.Uint32(payload[0:4]))
		if jsonLen < 0 || 4+jsonLen+4 > len(payload) {
			return nil, fmt.Errorf("wal: v3 payload json length %d exceeds payload of %d bytes", jsonLen, len(payload))
		}
		dataOff := 4 + jsonLen
		dataLen := int(binary.LittleEndian.Uint32(payload[dataOff : dataOff+4]))
		if dataOff+4+dataLen != len(payload) {
			return nil, fmt.Errorf("wal: v3 payload data length %d does not match payload of %d bytes", dataLen, len(payload))
		}
		var decoded WALEntry
		if err := jsontext.Unmarshal(payload[4:dataOff], &decoded); err != nil {
			return nil, err
		}
		decoded.Data = payload[dataOff+4:]
		decoded.framingVersion = version
		*entry = decoded
		return nil, nil

	default:
		return nil, fmt.Errorf("wal: unknown entry payload mode %d", mode)
	}
}
