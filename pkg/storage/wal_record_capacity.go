package storage

import (
	"math"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
)

// CheckRecordCapacity checks an encoded body's size before ownership transfers
// to an asynchronous producer. It reserves the largest append-generated sequence,
// checksum and timestamp header, so later sequence growth cannot invalidate the
// admission. Near a frame boundary this requires a few bytes more headroom than
// an immediate append at the current sequence. It performs no I/O or reservation
// and does not promise that a later append will succeed against I/O failures.
//
// For example, replication checks its JSON body's encoded size here before
// enrolling a command; the actual append still owns sequence assignment and I/O.
func (w *WAL) CheckRecordCapacity(op OperationType, dataSize int, database string) error {
	if dataSize < 0 {
		return ErrInvalidData
	}
	// JSON needs at most six output bytes for each input string byte. Prove
	// ordinary records fit without serializing their small metadata at all.
	bound := uint64(maxAppendHeaderSize) + 6*uint64(len(op)) + 6*uint64(len(database))
	if bound <= uint64(walMaxEntrySize)-8 &&
		(uint64(dataSize) <= uint64(walMaxEntrySize)-8-bound ||
			w.recordPolicy == WALRecordsLarge && uint64(dataSize) <= math.MaxUint32) {
		return nil
	}
	header := WALEntry{
		Sequence:  math.MaxUint64,
		Timestamp: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", 23*60*60+59*60)),
		Operation: op,
		Checksum:  math.MaxUint32,
		Database:  database,
	}
	encoded, err := jsontext.Marshal(&header)
	if err != nil {
		return &walRecordRejection{cause: err}
	}
	_, err = checkWALEntrySize(len(encoded), dataSize, w.recordPolicy == WALRecordsLarge)
	return err
}

// This immutable bound uses the encoder's actual field layout, not a second
// hand-maintained JSON schema. The one placeholder database byte is removed;
// string contents are bounded separately at each admission.
var maxAppendHeaderSize = func() int {
	header := WALEntry{Sequence: math.MaxUint64, Checksum: math.MaxUint32, Database: "x",
		Timestamp: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", 23*60*60+59*60))}
	encoded, err := jsontext.Marshal(&header)
	if err != nil {
		panic(err)
	}
	return len(encoded) - 1
}()
