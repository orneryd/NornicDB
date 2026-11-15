package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// walRecordRejection proves this logical record failed pure encoding before
// its physical bytes were attempted. Other control records may already exist.
type walRecordRejection struct{ cause error }

func (e *walRecordRejection) Error() string { return e.cause.Error() }
func (e *walRecordRejection) Unwrap() error { return e.cause }

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
		encoded, _, err := encodeWALEntryPayload(buf, entry, false)
		return encoded, err

	case walPayloadDecode:
		*entry = WALEntry{}
		if !walFrameVersionReadable(version) {
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

// encodeWALEntryPayload encodes the entry payload layout. A payload that fits
// one frame is encoded to the same bytes whatever spanning says. Without
// spanning, a record that does not fit is refused before its payload is
// allocated, with the frame-size error. With spanning (WALRecordsLarge, or a
// stored spanned record being rewritten) the payload is returned whole and
// reported large for the frame writer to split, unless its header alone
// exceeds a frame or its data exceeds the format's 32-bit length (NOR-412).
func encodeWALEntryPayload(buf *bytes.Buffer, entry *WALEntry, spanning bool) (encoded []byte, large bool, err error) {
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
		return nil, false, &walRecordRejection{cause: fmt.Errorf("wal: failed to serialize entry header: %w", err)}
	}
	large, err = checkWALEntrySize(len(jsonBytes), len(entry.Data), spanning)
	if err != nil {
		return nil, false, err
	}
	encoded = make([]byte, 4+len(jsonBytes)+4+len(entry.Data))
	binary.LittleEndian.PutUint32(encoded[0:4], uint32(len(jsonBytes)))
	copy(encoded[4:], jsonBytes)
	dataOff := 4 + len(jsonBytes)
	binary.LittleEndian.PutUint32(encoded[dataOff:dataOff+4], uint32(len(entry.Data)))
	copy(encoded[dataOff+4:], entry.Data)
	return encoded, large, nil
}

// checkWALEntrySize is shared by capacity admission and physical encoding.
func checkWALEntrySize(headerSize, dataSize int, spanning bool) (bool, error) {
	if dataSize < 0 {
		return false, ErrInvalidData
	}
	if headerSize > int(walMaxEntrySize)-8 || dataSize > int(walMaxEntrySize)-8-headerSize {
		if !spanning || headerSize > int(walMaxEntrySize)-8 {
			return false, &walRecordRejection{cause: fmt.Errorf("wal: entry exceeds maximum WAL frame size of %d bytes", walMaxEntrySize)}
		}
		if uint64(dataSize) > math.MaxUint32 {
			return false, &walRecordRejection{cause: fmt.Errorf("wal: journal record data of %d bytes exceeds the record format's limit of %d bytes", dataSize, uint64(math.MaxUint32))}
		}
		return true, nil
	}
	return false, nil
}
