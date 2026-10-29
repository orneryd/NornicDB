package search

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

var relationshipVectorDecodeBuffers = sync.Pool{New: func() any { return new([4096]byte) }}

// DecodeMsgpack validates the envelope and grows its entries only as records
// arrive, rather than trusting a declared map size in an untrusted file.
func (snapshot *relationshipVectorsFile) DecodeMsgpack(dec *msgpack.Decoder) error {
	*snapshot = relationshipVectorsFile{}
	count, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("relationship snapshot has %d fields, expected 2", count)
	}
	var decoded relationshipVectorsFile
	var seen uint8
	for i := 0; i < count; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		var bit uint8
		switch key {
		case "Format":
			bit = 1
		case "Entries":
			bit = 2
		default:
			return fmt.Errorf("unknown relationship snapshot field %q", key)
		}
		if seen&bit != 0 {
			return fmt.Errorf("duplicate relationship snapshot field %q", key)
		}
		seen |= bit
		if bit == 1 {
			decoded.Format, err = dec.DecodeInt()
		} else {
			decoded.Entries, err = decodeRelationshipSnapshotEntries(dec)
		}
		if err != nil {
			return fmt.Errorf("relationship snapshot field %s: %w", key, err)
		}
	}
	if seen != 3 {
		return fmt.Errorf("relationship snapshot is missing fields")
	}
	*snapshot = decoded
	return nil
}

func decodeRelationshipSnapshotEntries(dec *msgpack.Decoder) (map[string]*relationshipEntry, error) {
	count, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return nil, nil
	}
	if count < -1 {
		return nil, fmt.Errorf("invalid relationship snapshot entry count %d", count)
	}
	entries := make(map[string]*relationshipEntry, min(count, 64))
	for i := 0; i < count; i++ {
		id, err := dec.DecodeString()
		if err != nil {
			return nil, err
		}
		if _, exists := entries[id]; exists {
			return nil, fmt.Errorf("duplicate relationship snapshot entry %q", id)
		}
		var entry *relationshipEntry
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("relationship snapshot entry %q: %w", id, err)
		}
		entries[id] = entry
	}
	return entries, nil
}

// DecodeMsgpack reads the packed snapshot representation of one entry.
// Declared array and binary lengths do not allocate their full size until the
// corresponding input has actually been read.
func (entry *relationshipEntry) DecodeMsgpack(dec *msgpack.Decoder) error {
	*entry = relationshipEntry{}
	count, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}
	if count != 3 {
		return fmt.Errorf("relationship entry has %d fields, expected 3", count)
	}
	var decoded relationshipEntry
	var seen uint8
	for i := 0; i < count; i++ {
		key, err := dec.DecodeString()
		if err != nil {
			return err
		}
		var bit uint8
		switch key {
		case "Source":
			bit = 1
		case "Vectors":
			bit = 2
		case "Spans":
			bit = 4
		default:
			return fmt.Errorf("unknown relationship entry field %q", key)
		}
		if seen&bit != 0 {
			return fmt.Errorf("duplicate relationship entry field %q", key)
		}
		seen |= bit
		switch bit {
		case 1:
			decoded.Source, err = dec.DecodeUint64()
		case 2:
			decoded.Vectors, err = decodePackedRelationshipVectors(dec)
		case 4:
			decoded.Spans, err = decodeRelationshipChunkSpans(dec)
		}
		if err != nil {
			return fmt.Errorf("relationship entry field %s: %w", key, err)
		}
	}
	if seen != 7 {
		return fmt.Errorf("relationship entry is missing fields")
	}
	*entry = decoded
	return nil
}

func decodePackedRelationshipVectors(dec *msgpack.Decoder) ([][]float32, error) {
	count, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return nil, nil
	}
	if count < -1 {
		return nil, fmt.Errorf("invalid relationship vector count %d", count)
	}
	vectors := make([][]float32, 0, min(count, 16))
	for i := 0; i < count; i++ {
		code, err := dec.PeekCode()
		if err != nil {
			return nil, err
		}
		if code == msgpcode.Nil {
			if err := dec.DecodeNil(); err != nil {
				return nil, err
			}
			vectors = append(vectors, nil)
			continue
		}
		if code != msgpcode.Bin8 && code != msgpcode.Bin16 && code != msgpcode.Bin32 {
			return nil, fmt.Errorf("relationship vector %d must be binary", i)
		}
		length, err := dec.DecodeBytesLen()
		if err != nil {
			return nil, err
		}
		if length < 0 || length%4 != 0 {
			return nil, fmt.Errorf("invalid relationship vector byte length %d", length)
		}
		vector := make([]float32, 0, min(length/4, 1024))
		buffer := relationshipVectorDecodeBuffers.Get().(*[4096]byte)
		for offset := 0; offset < length; {
			chunk := min(length-offset, len(buffer))
			if err = dec.ReadFull(buffer[:chunk]); err != nil {
				break
			}
			for pos := 0; pos < chunk; pos += 4 {
				vector = append(vector, math.Float32frombits(binary.LittleEndian.Uint32(buffer[pos:])))
			}
			offset += chunk
		}
		relationshipVectorDecodeBuffers.Put(buffer)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, vector)
	}
	return vectors, nil
}

func decodeRelationshipChunkSpans(dec *msgpack.Decoder) ([]embeddingutil.ChunkSpan, error) {
	count, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if count == -1 {
		return nil, nil
	}
	if count < -1 {
		return nil, fmt.Errorf("invalid relationship span count %d", count)
	}
	spans := make([]embeddingutil.ChunkSpan, 0, min(count, 16))
	for i := 0; i < count; i++ {
		var span embeddingutil.ChunkSpan
		if err := dec.Decode(&span); err != nil {
			return nil, err
		}
		spans = append(spans, span)
	}
	return spans, nil
}
