package storage

import (
	"context"
	"io"
)

// compactionSnapshot is the population fixed while entity mutations are held.
// Custom stored-reader overrides retain the existing exclusive acquisition.
type compactionSnapshot struct {
	engine   Engine
	source   *storedSnapshotSource
	sequence uint64
}
type compactionSnapshotWriter func(context.Context, compactionSnapshot, string) error

func saveCompactionSnapshot(ctx context.Context, snapshot compactionSnapshot, path string) error {
	options := SnapshotOptions{Sequence: snapshot.sequence}
	if snapshot.source == nil {
		return SaveStreamingSnapshot(ctx, snapshot.engine, path, options)
	}
	return saveSnapshotFile(path, func(writer io.Writer) error {
		return writeSnapshotPopulation(ctx, writer, options, snapshot.source.StreamNodes, snapshot.source.StreamEdges)
	})
}
