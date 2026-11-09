package storage

// capturePublicationOperations snapshots listener payloads before the physical
// commit reserves their position. Derived-state publication uses its own images.
func capturePublicationOperations(n *commitNotification, ops []Operation) {
	if n == nil {
		return
	}
	n.ops = append([]Operation(nil), ops...)
	for i := range n.ops {
		n.ops[i].snapshotForNotification()
	}
}
