package storage

import "bytes"

// WALRecordPolicy is how a WAL frames the journal records it creates
// (NOR-412). NewWAL copies it from WALConfig.RecordPolicy, and no code
// assigns the WAL's copy afterwards.
//
// The copy is read by encodeNewRecord, where it decides whether a record that
// does not fit one frame is refused or written in parts, and by
// journalsLargeRecords. The readers do not consult it, and maintenance that
// rewrites a stored record keeps the form the record was stored in. One
// operation, the managed vector anchor, spans frames whatever the policy
// (walRecordMaySpan).
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
// was constructed with, for every operation whose framing that policy alone
// decides (walRecordMaySpan).
func (w *WAL) encodeNewRecord(buf *bytes.Buffer, entry *WALEntry) (walRecordFrames, error) {
	payload, err := w.layoutNewRecord(buf, entry)
	if err != nil {
		return walRecordFrames{}, err
	}
	return payload.frames(buf, entry)
}

// layoutNewRecord lays out the record this WAL would write for entry under its
// policy: the entry payload and whether that payload needs more than one frame.
// The append that must decide before it writes a spanned record asks this
// (appendDecidingLargeRecord) and cuts the frames only after the decision, so
// the record it writes is the one it laid out.
func (w *WAL) layoutNewRecord(buf *bytes.Buffer, entry *WALEntry) (walEntryPayload, error) {
	return layoutWALEntryPayload(buf, entry, w.recordPolicy == WALRecordsLarge || walRecordMaySpan(entry.Operation))
}

// walRecordMaySpan reports whether a record of op is written in parts whatever
// record policy this WAL was constructed with. It is the one exception to the
// policy, and it belongs to the record's own approved capability rather than to
// the journal: the managed vector anchor carries an acknowledged node's managed
// embedding state (B30), which no record policy bounds, so the standard policy's
// 16 MiB one-frame limit cannot hold it. Every other operation keeps that
// limit: an ordinary OpCreateNode or OpUpdateNode record that does not fit one
// frame is still refused by encodeWALEntryPayload under WALRecordsStandard.
func walRecordMaySpan(op OperationType) bool {
	return op == opManagedVectorAnchor
}

// encodeStoredRecord re-encodes a record a WAL already holds, for maintenance
// that rewrites its file. The record keeps the form it was stored in whatever
// policy the WAL writes new records with: a spanned record stays spanned, and a
// one-frame record is encoded by the one-frame encoder.
func encodeStoredRecord(buf *bytes.Buffer, entry *WALEntry) (walRecordFrames, error) {
	return encodeWALRecord(buf, entry, entry.framingVersion == walFormatVersionSpanned)
}
