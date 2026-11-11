package storage

import "bytes"

// WALRecordPolicy is how one runtime writes the journal records it creates
// (NOR-412). It is chosen once, when the engine and its WAL are constructed,
// and never changes afterwards: BadgerEngine and WAL each keep the value
// their options carried at construction.
//
// The policy governs writing only. Every reader accepts both forms in every
// runtime, and maintenance that rewrites a stored record keeps the form the
// record was stored in.
type WALRecordPolicy uint8

const (
	// WALRecordsStandard, the zero value, writes what every release before
	// NOR-412 wrote: a node update's record carries the node's whole image
	// (semantic record version 3), and a record is one frame, refused when it
	// does not fit (walMaxEntrySize). Every engine, WAL and replicator gets
	// this unless its options ask for WALRecordsLarge, so what a cluster
	// journals and replicates is unchanged.
	WALRecordsStandard WALRecordPolicy = iota

	// WALRecordsLarge lets one transaction's record grow past one frame and
	// keeps it small where it can: a node update's record leaves out the
	// vectors the update kept and identifies them (semantic record version 5), and
	// a record that does not fit one frame is written as a spanned record
	// (frame version 7). Set it on both EngineOptions and WALConfig of one
	// runtime. A single-node server opens with it (nornicdb.Open); the
	// replication journal and transport carry one-frame records only, so a
	// cluster member must not.
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
