package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The first real storage snapshot has already been enumerated when entered
// closes. No build completion or search population is fabricated by this gate.
type staleInitialSearchSnapshot struct {
	storage.Engine
	once             sync.Once
	entered, release chan struct{}
}

func (s *staleInitialSearchSnapshot) IterateNodes(visit func(*storage.Node) bool) error {
	nodes, err := s.Engine.AllNodes(context.Background())
	if err != nil {
		return err
	}
	s.once.Do(func() { close(s.entered); <-s.release })
	for _, node := range nodes {
		if !visit(node) {
			break
		}
	}
	return nil
}

type buildJoinContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *buildJoinContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestBuildSearchIndexesRebuildsAfterJoiningStaleInitialSnapshot(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	gate := &staleInitialSearchSnapshot{Engine: storage.NewNamespacedEngine(base, "nornic"), entered: make(chan struct{}), release: make(chan struct{})}
	db := &DB{storage: gate.Engine, baseStorage: base, config: DefaultConfig(), searchServices: make(map[string]*dbSearchService), embeddingDims: 3, searchMinSimilarity: 0.1, buildCtx: context.Background()}
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan struct{})
	done := make(chan error, 1)
	var release sync.Once
	launched := false
	t.Cleanup(func() {
		cancel()
		release.Do(func() { close(gate.release) })
		if launched {
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Error("BuildSearchIndexes worker did not join")
			}
		}
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	_, err := db.EnsureSearchIndexes(context.Background(), "nornic", gate, SearchIndexBuildStart)
	require.NoError(t, err)
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial snapshot not captured")
	}
	_, err = gate.CreateNode(t.Context(), &storage.Node{ID: "late", Labels: []string{"Doc"}, Properties: map[string]any{"content": "late searchable document"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
	require.NoError(t, err)
	observer := &buildJoinContext{Context: ctx, waiting: make(chan struct{})}
	launched = true
	go func() { defer close(joined); done <- db.BuildSearchIndexes(observer) }()
	select {
	case <-observer.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("public call did not join in-flight build")
	}
	release.Do(func() { close(gate.release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("public build did not finish")
	}
	require.Equal(t, 1, db.EmbeddingCount(), "successful public build must include nodes loaded after initial snapshot")
	results, err := db.Search(ctx, "searchable document", nil, 5)
	require.NoError(t, err)
	require.Len(t, results, 1, "fulltext consumers must see the late node immediately after the public build")
}
