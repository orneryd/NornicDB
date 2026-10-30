package search

import (
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// newSearchPersistenceTestService creates a 3-dimension search service whose
// persisted artifacts live under dir. hnsw adds the HNSW index path the same
// way; without it the HNSW artifact and build settings stay unattached to that
// directory. The caller owns dir's lifetime (usually t.TempDir()).
func newSearchPersistenceTestService(t *testing.T, engine storage.Engine, dir string, hnsw bool) *Service {
	t.Helper()
	svc := NewService(engine, 3, nil)
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	if hnsw {
		svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	}
	svc.SetPersistenceEnabled(true)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}
