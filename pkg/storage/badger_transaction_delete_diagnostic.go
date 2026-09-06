package storage

import "fmt"

// logMissingDeleteSnapshotLocked is a temporary, error-only diagnostic for
// Eshu #6579. These reads are observations after the failed lookup, not an
// atomic view of the competing writer. They never change the returned error.
func (tx *BadgerTransaction) logMissingDeleteSnapshotLocked(edgeID EdgeID) {
	head, headErr := tx.engine.loadEdgeMVCCHead(edgeID)
	_, latestErr := tx.engine.GetEdgeLatestVisible(edgeID)
	_, repeatedErr := tx.engine.GetEdgeVisibleAt(edgeID, tx.readTS)
	_, alreadyDeleted := tx.deletedEdges[edgeID]
	tx.engine.log.Warn("edge deletion snapshot lookup failed",
		"transaction_id", tx.ID,
		"edge_id", string(edgeID),
		"namespace", tx.namespace,
		"read_version", fmt.Sprint(tx.readTS),
		"head_version", fmt.Sprint(head.Version),
		"floor_version", fmt.Sprint(head.FloorVersion),
		"head_tombstoned", head.Tombstoned,
		"head_error", fmt.Sprint(headErr),
		"latest_error", fmt.Sprint(latestErr),
		"latest_visible", latestErr == nil,
		"repeated_snapshot_error", fmt.Sprint(repeatedErr),
		"repeated_snapshot_visible", repeatedErr == nil,
		"snapshot_readers", tx.engine.activeMVCCSnapshotReaders.Load(),
		"snapshot_reader_registered", tx.snapshotDeregister != nil,
		"retention_retains_history", tx.engine.retentionRetainsHistory(),
		"must_archive_now", tx.engine.mustArchiveForHistory(),
		"already_deleted_in_transaction", alreadyDeleted,
		"pending_operations", len(tx.operations))
}
