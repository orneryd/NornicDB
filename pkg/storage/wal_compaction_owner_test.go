package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// snapshotHoldEngine holds a snapshot's node stream until the snapshot's
// context ends, so a test can close the owner while a snapshot is running.
type snapshotHoldEngine struct {
	Engine
	entered chan struct{}
	once    sync.Once
}

func (e *snapshotHoldEngine) StreamNodesWithOptions(ctx context.Context, _ StreamNodesOptions, _ func(*Node) error) error {
	e.once.Do(func() { close(e.entered) })
	<-ctx.Done()
	return ctx.Err()
}

// An automatic snapshot is the WAL engine owner's enrolled work: closing the
// owner stops a snapshot in progress, Close returns, and no snapshot is
// published.
func TestAutoCompactionSnapshotStopsWithOwner(t *testing.T) {
	defer config.WithWALEnabled()()
	root := t.TempDir()
	dir := filepath.Join(root, "snapshots")
	wal, err := NewWAL("", &WALConfig{Dir: filepath.Join(root, "wal"), SyncMode: "immediate", SnapshotInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	inner := &snapshotHoldEngine{Engine: newTestMemoryEngine(t), entered: make(chan struct{})}
	engine := NewWALEngine(inner, wal)
	_, err = engine.CreateNode(&Node{ID: "nornic:snapshot-owner", Properties: map[string]any{"name": "held"}})
	require.NoError(t, err)
	require.NoError(t, engine.EnableAutoCompaction(dir))
	select {
	case <-inner.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the automatic snapshot did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return: the running snapshot does not stop with its owner")
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), "snapshot-"), "a stopped snapshot was published: %s", entry.Name())
	}
}
