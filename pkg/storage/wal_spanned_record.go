package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// A spanned record is one logical WAL record whose encoded payload does not
// fit one frame (walMaxEntrySize). It is written as count consecutive part
// frames under one hold of the WAL mutex (NOR-412). Every part:
//
//   - is an ordinary atomic frame whose version byte is walFormatVersionSpanned,
//     so a build that predates spanned records refuses the log at open
//     ("unsupported WAL version") instead of skipping an operation it does not
//     know, which its recovery would otherwise do silently;
//   - carries an ordinary entry payload with operation OpRecordPart and the
//     logical record's own sequence, timestamp and database, so sequence-based
//     cuts (snapshot truncation, segment retention) keep or drop the record
//     whole;
//   - holds [index:4 LE][count:4 LE] and the next chunk of the logical
//     payload, which is exactly the payload a single frame would have held.
//
// A record that fits one frame is written exactly as before, byte for byte,
// with walFormatVersionCurrent. Only the last part completes a record: a
// group that ends early was never acknowledged and is dropped by readers and
// cut by tail repair.
const (
	walFormatVersionSpanned uint8         = 7
	OpRecordPart            OperationType = "record_part"
	walSpannedPartHeaderLen               = 8
)

// walSpannedChunkBytes is the largest logical-payload chunk one part carries.
// Its frame payload adds the part header, the JSON entry header and the eight
// length bytes, far below the headroom left here. Tests shrink it to exercise
// spanning with small records.
var walSpannedChunkBytes = int(walMaxEntrySize) - 64*1024

// walFrameVersionReadable reports whether this build reads a frame version.
func walFrameVersionReadable(version uint8) bool {
	return version == walFormatVersionCurrent || version == walFormatVersionSpanned
}

// walRecordFrames is one encoded record ready to write: the single frame of a
// record that fits one, or the part frames of a spanned record.
type walRecordFrames struct {
	single []byte
	parts  [][]byte
}

// encodeWALRecord is the single owner of turning one logical entry into
// physical frames for every writer (append, forced-flush append, batch and the
// snapshot truncation rewrite). A record that fits one frame is the payload
// the one-frame encoder returns, with nothing added. Without spanning a larger
// record is refused by that encoder. The returned payloads are owned by the
// caller.
func encodeWALRecord(buf *bytes.Buffer, entry *WALEntry, spanning bool) (walRecordFrames, error) {
	payload, large, err := encodeWALEntryPayload(buf, entry, spanning)
	if err != nil || !large {
		return walRecordFrames{single: payload}, err
	}
	chunk := walSpannedChunkBytes
	count := (len(payload) + chunk - 1) / chunk
	if count < 2 || count > math.MaxUint32 {
		return walRecordFrames{}, &walRecordRejection{cause: fmt.Errorf("wal: record of %d bytes cannot be spanned", len(payload))}
	}
	parts := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		piece := payload[i*chunk : min((i+1)*chunk, len(payload))]
		data := make([]byte, walSpannedPartHeaderLen+len(piece))
		binary.LittleEndian.PutUint32(data[0:4], uint32(i))
		binary.LittleEndian.PutUint32(data[4:8], uint32(count))
		copy(data[walSpannedPartHeaderLen:], piece)
		part := WALEntry{
			Sequence:  entry.Sequence,
			Timestamp: entry.Timestamp,
			Operation: OpRecordPart,
			Data:      data,
			Checksum:  crc32Checksum(data),
			Database:  entry.Database,
		}
		partPayload, err := processWALEntryPayload(walPayloadEncode, buf, &part, nil, 0)
		if err != nil {
			return walRecordFrames{}, err
		}
		parts = append(parts, partPayload)
	}
	return walRecordFrames{parts: parts}, nil
}

// write writes the record's frames in order with writeFrame, the atomic frame
// writer of the calling path, and returns their aligned length.
func (r walRecordFrames) write(w io.Writer, writeFrame func(io.Writer, []byte, uint8) (int64, error)) (int64, error) {
	if r.parts == nil {
		return writeFrame(w, r.single, walFormatVersionCurrent)
	}
	var total int64
	for _, part := range r.parts {
		n, err := writeFrame(w, part, walFormatVersionSpanned)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// walRecordAssembler is the single owner of reading spanned records. Readers
// feed it every decoded frame entry in file order with its frame version; it
// returns each logical entry once, when it is complete.
type walRecordAssembler struct {
	seq     uint64
	count   uint32
	next    uint32
	payload []byte
	dropped int
}

// add accepts one decoded frame entry. complete is false while a spanned
// record is still open. An open group that is followed by an ordinary record
// or by the first part of another record was never acknowledged: it is
// dropped and counted.
func (a *walRecordAssembler) add(entry WALEntry, version uint8) (logical WALEntry, complete bool, err error) {
	if version != walFormatVersionSpanned {
		// Only the frame version makes a part. An entry of any other version
		// is an ordinary record whatever its operation is called, as it was
		// before spanned records existed.
		a.drop()
		return entry, true, nil
	}
	if entry.Operation != OpRecordPart || len(entry.Data) < walSpannedPartHeaderLen {
		return WALEntry{}, false, fmt.Errorf("%w: malformed spanned frame at seq %d", ErrWALCorrupted, entry.Sequence)
	}
	index := binary.LittleEndian.Uint32(entry.Data[0:4])
	count := binary.LittleEndian.Uint32(entry.Data[4:8])
	piece := entry.Data[walSpannedPartHeaderLen:]
	if count < 2 || index >= count {
		return WALEntry{}, false, fmt.Errorf("%w: spanned part %d of %d at seq %d", ErrWALCorrupted, index, count, entry.Sequence)
	}
	if index == 0 {
		a.drop()
		a.seq, a.count, a.next = entry.Sequence, count, 1
		// Grow with the parts actually read: a damaged count must not size
		// an allocation.
		a.payload = append([]byte(nil), piece...)
	} else {
		if a.payload == nil || entry.Sequence != a.seq || count != a.count || index != a.next {
			return WALEntry{}, false, fmt.Errorf("%w: spanned part %d of %d at seq %d does not continue its record", ErrWALCorrupted, index, count, entry.Sequence)
		}
		a.payload = append(a.payload, piece...)
		a.next++
	}
	if a.next < a.count {
		return WALEntry{}, false, nil
	}
	payload, seq := a.payload, a.seq
	a.reset()
	// The logical entry remembers that it was stored spanned, so maintenance
	// that rewrites it keeps that form (encodeStoredRecord).
	if _, err := processWALEntryPayload(walPayloadDecode, nil, &logical, payload, walFormatVersionSpanned); err != nil {
		return WALEntry{}, false, fmt.Errorf("%w: spanned record at seq %d: %v", ErrWALCorrupted, seq, err)
	}
	if logical.Sequence != seq {
		return WALEntry{}, false, fmt.Errorf("%w: spanned record at seq %d holds seq %d", ErrWALCorrupted, seq, logical.Sequence)
	}
	if logical.Checksum != crc32Checksum(logical.Data) {
		return WALEntry{}, false, fmt.Errorf("%w: data checksum mismatch at seq %d", ErrWALChecksumFailed, seq)
	}
	return logical, true, nil
}

// open reports whether a spanned record has started but not completed.
func (a *walRecordAssembler) open() bool { return a.payload != nil }

// drop discards an open group, which was never acknowledged.
func (a *walRecordAssembler) drop() {
	if a.payload != nil {
		a.dropped++
		a.reset()
	}
}

func (a *walRecordAssembler) reset() {
	a.seq, a.count, a.next, a.payload = 0, 0, 0, nil
}
