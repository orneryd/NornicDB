package storage

import (
	"context"
	"io"
)

// compactionSnapshot is the population fixed while entity mutations are held.
// Custom stored-reader overrides retain the existing exclusive acquisition.
// encryptionKey is the owner-supplied snapshot-at-rest key: nil keeps the
// unchanged plaintext snapshot, a 32-byte key encrypts the file as it is
// written.
type compactionSnapshot struct {
	engine        Engine
	source        *storedSnapshotSource
	sequence      uint64
	encryptionKey []byte
}
type compactionSnapshotWriter func(context.Context, compactionSnapshot, string) error

func saveCompactionSnapshot(ctx context.Context, snapshot compactionSnapshot, path string) error {
	options := SnapshotOptions{Sequence: snapshot.sequence, EncryptionKey: snapshot.encryptionKey}
	if snapshot.source == nil {
		return SaveStreamingSnapshot(ctx, snapshot.engine, path, options)
	}
	return saveSnapshotFile(path, func(writer io.Writer) error {
		return writeSnapshotPopulation(ctx, writer, options, snapshot.source.StreamNodes, snapshot.source.StreamEdges)
	})
}
