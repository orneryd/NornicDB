package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeferredBuildGenerationDoesNotRestoreDroppedPropertyVectors(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(map[bool]string{false: "storage_snapshot", true: "persisted_baseline"}[persisted], func(t *testing.T) {
			engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "generation")
			svc := NewService(engine, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetPersistenceEnabled(persisted)
			if persisted {
				dir := t.TempDir()
				svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
				svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
				svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
			}
			schema := engine.GetSchema()
			require.NoError(t, schema.AddVectorIndex("retired", "Doc", "embedding", 3, "cosine"))
			_, err := engine.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "preservedword", "embedding": []float32{1, 0, 0}, "other": []float32{0, 1, 0}}})
			require.NoError(t, err)
			require.NoError(t, svc.BuildIndexes(context.Background()))
			require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
			build := svc.PrepareIndexBuild(storage.CaptureVectorWorkOrigin())
			require.NoError(t, schema.DropIndex("retired"))
			svc.RemovePropertyVectorIndex("embedding")
			require.NoError(t, build(context.Background()))
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "a build queued before DROP restored retired property vectors")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
			require.Equal(t, []string{"source"}, buildWriteSearchIDs(t, svc, "preservedword"))
		})
	}
}
