package search

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Persisting live writes made before any build runs the node population
// first, and persists only when it completed: a population that fails, or a
// base operation another owner holds, leaves the disk untouched.
func TestPersistOfUnpopulatedWritesNeedsACompletedPopulation(t *testing.T) {
	newService := func(engine storage.Engine) (*Service, string) {
		dir := t.TempDir()
		svc := NewServiceWithDimensions(engine, 3)
		t.Cleanup(func() { require.NoError(t, svc.Close()) })
		svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		svc.SetPersistenceEnabled(true)
		return svc, dir
	}
	node := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")

	t.Run("population fails", func(t *testing.T) {
		engine := &iteratorEngine{Engine: newNamespacedEngine(t), iterateErr: errors.New("storage traversal unavailable")}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		svc, dir := newService(engine)
		require.NoError(t, svc.IndexNode(node))
		svc.runPersistWithContext(nil)
		require.False(t, svc.nodePopulationCanPersist())
		_, statErr := os.Stat(filepath.Join(dir, "bm25"))
		require.True(t, os.IsNotExist(statErr), "a failed population persists nothing")
	})

	t.Run("base operation held", func(t *testing.T) {
		engine := newNamespacedEngine(t)
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		svc, dir := newService(engine)
		require.NoError(t, svc.IndexNode(node))
		svc.buildMu.Lock()
		require.NoError(t, svc.persistBaseIndexes())
		svc.buildMu.Unlock()
		_, statErr := os.Stat(filepath.Join(dir, "bm25"))
		require.True(t, os.IsNotExist(statErr), "a held base operation skips the persist")
		// Once free, the persist populates first and then writes.
		require.NoError(t, svc.persistBaseIndexes())
		require.True(t, svc.nodePopulationCanPersist())
		require.FileExists(t, filepath.Join(dir, "bm25"))
	})
}
