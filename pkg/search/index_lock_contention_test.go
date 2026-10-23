package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIndexNode_DoesNotBlockSearchStateReads(t *testing.T) {
	engine := newTestMemoryEngine(t)
	svc := NewServiceWithDimensions(engine, 4)

	tmpDir := t.TempDir()
	vfsPath := filepath.Join(tmpDir, "vectors")
	vfs, err := NewVectorFileStore(vfsPath, 4)
	require.NoError(t, err)
	defer vfs.Close()

	writeStarted := make(chan struct{}, 1)
	vfs.writeRecord = func(f *os.File, id string, vec []float32) error {
		select {
		case writeStarted <- struct{}{}:
		default:
		}
		time.Sleep(250 * time.Millisecond)
		return writeVectorRecord(f, id, vec)
	}

	svc.indexMu.Lock()
	svc.vectorFileStore = vfs
	svc.indexMu.Unlock()

	node := &storage.Node{
		ID:              storage.NodeID("n1"),
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"title": "test"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
	}

	indexDone := make(chan error, 1)
	go func() {
		indexDone <- svc.IndexNode(node)
	}()

	select {
	case <-writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow vector write to start")
	}

	start := time.Now()
	_, err = svc.Search(context.Background(), "hello", nil, DefaultSearchOptions())
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.Less(t, elapsed, 100*time.Millisecond, "search state read should not wait for slow IndexNode write")

	require.NoError(t, <-indexDone)
}
