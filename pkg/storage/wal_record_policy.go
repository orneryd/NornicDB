package storage

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

// encodeRecordFrames encodes entry under this WAL's record policy. A WAL with
// SingleFrameRecords (the replication journal) encodes every record as one
// frame with the one-frame encoder and refuses a larger one exactly as every
// WAL did before records could span frames; any other WAL writes a record of
// any size, in parts beyond one frame (encodeWALEntryFrames).
func (w *WAL) encodeRecordFrames(buf *bytes.Buffer, entry *WALEntry) ([]walFrame, error) {
	if !w.config.SingleFrameRecords {
		return encodeWALEntryFrames(buf, entry)
	}
	payload, err := processWALEntryPayload(walPayloadEncode, buf, entry, nil, 0)
	if err != nil {
		return nil, err
	}
	return []walFrame{{payload: payload, version: walFormatVersionCurrent}}, nil
}

// walWidestHeaderTime is a timestamp whose JSON form is as long as any the
// header can carry: a four-digit year, nine fractional digits and a numeric
// zone offset.
var walWidestHeaderTime = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", -(11*3600+59*60)))

// CheckOneFrameRecord decides, before anything is written, whether a record
// of data appended to this single-frame WAL would be accepted (NOR-412). It
// marshals data and encodes the entry with the same encoders an append uses,
// with the widest sequence number and timestamp the entry header can carry,
// so a record it accepts fits one frame whenever it is appended. It refuses
// with the error that append would return. A WAL whose records may span
// frames accepts the record.
func (w *WAL) CheckOneFrameRecord(op OperationType, data any, database string) error {
	if !w.config.SingleFrameRecords {
		return nil
	}
	dataBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(dataBuf)
	entryBuf := walJSONBufPool.Get().(*bytes.Buffer)
	defer walJSONBufPool.Put(entryBuf)
	dataBytes, err := marshalWALRecoveryPayload(dataBuf, data)
	if err != nil {
		return &walRecordRejection{cause: fmt.Errorf("wal: failed to marshal data: %w", err)}
	}
	entry := WALEntry{
		Sequence:  math.MaxUint64,
		Timestamp: walWidestHeaderTime,
		Operation: op,
		Data:      dataBytes,
		Checksum:  crc32Checksum(dataBytes),
		Database:  database,
	}
	if _, err := processWALEntryPayload(walPayloadEncode, entryBuf, &entry, nil, 0); err != nil {
		return fmt.Errorf("wal: failed to serialize entry: %w", err)
	}
	return nil
}
