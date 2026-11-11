package storage

import "bytes"

// WALRecordPolicy is how a WAL frames the journal records it creates
// (NOR-412). NewWAL copies it from WALConfig.RecordPolicy, and no code
// assigns the WAL's copy afterwards.
//
// The copy is read by encodeNewRecord, where it decides whether a record that
// does not fit one frame is refused or written in parts, and by
// journalsLargeRecords. The readers do not consult it, and maintenance that
// rewrites a stored record keeps the form the record was stored in.
type WALRecordPolicy uint8

const (
	// WALRecordsStandard, the zero value: a record is one frame, and a
	// record that does not fit one is refused with the frame-size error
	// (walMaxEntrySize). A WAL has this policy unless its configuration
	// asks for WALRecordsLarge, which nornicdb.Open does for a standalone
	// server only.
	WALRecordsStandard WALRecordPolicy = iota

	// WALRecordsLarge writes a record that does not fit one frame as a
	// spanned record (frame version 7): consecutive part frames that the
	// readers assemble into one record before they deliver it
	// (walRecordAssembler). A record that fits one frame is encoded to the
	// same bytes as under WALRecordsStandard (encodeWALEntryPayload).
	// nornicdb.Open selects it for a standalone server and the standard
	// policy for every other mode.
	WALRecordsLarge
)

// encodeNewRecord encodes a record this WAL creates now, under the policy it
// was constructed with.
func (w *WAL) encodeNewRecord(buf *bytes.Buffer, entry *WALEntry) (walRecordFrames, error) {
	return encodeWALRecord(buf, entry, w.recordPolicy == WALRecordsLarge)
}

// encodeStoredRecord re-encodes a record a WAL already holds, for maintenance
// that rewrites its file. The record keeps the form it was stored in whatever
// policy the WAL writes new records with: a spanned record stays spanned, and
// a one-frame record is encoded by the one-frame encoder.
func encodeStoredRecord(buf *bytes.Buffer, entry *WALEntry) (walRecordFrames, error) {
	return encodeWALRecord(buf, entry, entry.framingVersion == walFormatVersionSpanned)
}
