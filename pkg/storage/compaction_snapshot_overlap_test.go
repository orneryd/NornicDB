package storage

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type compactionWriteGate struct {
	io.Writer
	context context.Context
	started chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (w *compactionWriteGate) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.resume:
	case <-w.context.Done():
		return 0, w.context.Err()
	}
	return w.Writer.Write(data)
}

func TestCompactionSnapshotAllowsPostCutWritesAndRecoversBothSides(t *testing.T) {
	_, journal, owner := newWALClientFixture(t, nil)
	owner.snapshotDir = t.TempDir()
	_, err := owner.CreateNode(t.Context(), &Node{ID: "test:before", Properties: map[string]any{"value": "before"}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var release sync.Once
	joined := false
	defer func() {
		release.Do(func() { close(resume) })
		cancel()
		if !joined {
			<-done
		}
	}()
	var cut uint64
	var snapshotPath string
	go func() {
		done <- owner.createSnapshotAndCompact(ctx, func(ctx context.Context, image compactionSnapshot, path string) error {
			if !owner.mutationMu.TryRLock() {
				return errors.New("snapshot file serialization still holds the entity mutation barrier")
			}
			owner.mutationMu.RUnlock()
			if image.source == nil {
				return errors.New("native snapshot did not pin its population")
			}
			cut, snapshotPath = image.sequence, path
			return saveSnapshotFile(path, func(writer io.Writer) error {
				gate := &compactionWriteGate{Writer: writer, context: ctx, started: started, resume: resume}
				return writeSnapshotPopulation(ctx, gate, SnapshotOptions{Sequence: image.sequence}, image.source.StreamNodes, image.source.StreamEdges)
			})
		})
	}()
	select {
	case <-started:
	case err := <-done:
		joined = true
		t.Fatalf("snapshot did not reach file output: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot did not reach file output")
	}
	// The actual file write is parked. These public writes must finish before
	// releasing it; the timeout is a deadlock guard, not a latency benchmark.
	writeCtx, writeCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer writeCancel()
	node, err := owner.GetNode(writeCtx, "test:before")
	require.NoError(t, err)
	node.Properties["value"] = "after"
	require.NoError(t, owner.UpdateNode(writeCtx, NodeUpdateRequest{After: node}))
	_, err = owner.CreateNode(writeCtx, &Node{ID: "test:after"})
	require.NoError(t, err)
	release.Do(func() { close(resume) })
	err = <-done
	joined = true
	require.NoError(t, err)
	snapshot, err := LoadSnapshot(t.Context(), snapshotPath)
	require.NoError(t, err)
	require.Equal(t, cut, snapshot.Sequence)
	require.Len(t, snapshot.Nodes, 1, "the pinned image must exclude post-cut creates")
	require.Equal(t, "before", snapshot.Nodes[0].Properties["value"])
	recovered := newTestMemoryEngine(t)
	_, _, err = RecoverIntoEngine(t.Context(), recovered, journal.Config().Dir, snapshotPath)
	require.NoError(t, err)
	before, err := recovered.GetNode(t.Context(), "test:before")
	require.NoError(t, err)
	require.Equal(t, "after", before.Properties["value"])
	_, err = recovered.GetNode(t.Context(), "test:after")
	require.NoError(t, err)
	initial, _ := owner.GetSnapshotStats()
	require.NoError(t, owner.createSnapshotAndCompact(t.Context(), saveCompactionSnapshot))
	next, _ := owner.GetSnapshotStats()
	require.Equal(t, initial+1, next, "post-cut records must trigger the next compaction")
	require.NoError(t, owner.createSnapshotAndCompact(t.Context(), saveCompactionSnapshot))
	idle, _ := owner.GetSnapshotStats()
	require.Equal(t, next, idle, "an idle cut must not write again")
}
