package storage

import "bytes"

// WALRecordPolicy is how a WAL frames the journal records it creates
// (NOR-412). It is chosen once, when the WAL is constructed, and never changes
// afterwards: the WAL keeps the value WALConfig.RecordPolicy carried.
//
// The policy governs framing only, never a record's content: under both
// policies a record carries exactly the same bytes. It governs writing only:
// every reader accepts both forms in every runtime, and maintenance that
// rewrites a stored record keeps the form the record was stored in.
type WALRecordPolicy uint8

const (
	// WALRecordsStandard, the zero value, writes what every release before
	// NOR-412 wrote: a record is one frame, refused when it does not fit
	// (walMaxEntrySize). Every WAL gets this unless its configuration asks
	// for WALRecordsLarge, so what a cluster journals and replicates is
	// unchanged.
	WALRecordsStandard WALRecordPolicy = iota

	// WALRecordsLarge writes a record that does not fit one frame as a
	// spanned record (frame version 7): consecutive frames that every reader
	// assembles into the one record before it sees it. A record that fits
	// one frame is written exactly as under WALRecordsStandard. A single-node
	// server opens its WAL with it (nornicdb.Open); the replication journal
	// and transport carry one-frame records only, so a cluster member must
	// not.
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
