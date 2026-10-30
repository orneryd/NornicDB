package search

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The canonical clustering vector-service fixture: the engine, Service, scope,
// clustering enablement and node-indexing loop every clustering/vector case
// builds its own population on. A case supplies only its nodes (and its own
// env overrides, if any); ownership of the service, its scope and its indexing
// stays here.

// newVectorService creates the fixture engine and the Service over it at dims,
// closing both when the test ends.
func newVectorService(t testing.TB, dims int, namespace string) (*Service, storage.Engine) {
	t.Helper()
	var engine storage.Engine = fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	if namespace != "" {
		engine = storage.NewNamespacedEngine(engine, namespace)
	}
	svc := ownVectorService(t, engine, dims)
	return svc, engine
}

// indexVectorNodes indexes each node, persisting it first when an engine is
// supplied. A nil engine preserves fixtures that exercise index-only admission.
func indexVectorNodes(t testing.TB, svc *Service, engine storage.Engine, nodes ...*storage.Node) {
	t.Helper()
	for _, node := range nodes {
		if engine != nil {
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}
		require.NoError(t, svc.IndexNode(node))
	}
}

// enableVectorClustering enables clustering for the Service with numClusters,
// covering every embedding the test indexes (threshold 1).
func enableVectorClustering(t testing.TB, svc *Service, numClusters int) {
	t.Helper()
	svc.EnableClustering(nil, numClusters)
	svc.SetMinEmbeddingsForClustering(1)
}

// ownVectorService owns only Service construction/closure. Callers retain their
// engine, dimensions and persistence/metrics configuration.
func ownVectorService(t testing.TB, engine storage.Engine, dims int) *Service {
	t.Helper()
	svc := NewService(engine, dims, nil)
	t.Cleanup(func() { closeAfterBuilds(t, svc) })
	return svc
}
