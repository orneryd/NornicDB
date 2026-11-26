package storage

import (
	"context"
	"fmt"
	"os"
)

// LoadSnapshotWithKey reads a snapshot using the database's at-rest key. A
// keyless call retains the historical JSON/NDS2 compatibility contract; a
// keyed call requires an authenticated encrypted NDS2 stream and never falls
// through to plaintext JSON.
func LoadSnapshotWithKey(ctx context.Context, path string, key []byte) (*Snapshot, error) {
	if len(key) == 0 {
		return LoadSnapshot(ctx, path)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open snapshot: %w", err)
	}
	defer file.Close()
	snapshot := &Snapshot{Version: "1.0"}
	metadata, err := ReadSnapshotWithKey(ctx, file, snapshotCollectionVisitor{snapshot: snapshot}, key)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to decode encrypted snapshot: %w", err)
	}
	snapshot.Sequence = metadata.Sequence
	snapshot.Timestamp = metadata.Timestamp
	return snapshot, nil
}
