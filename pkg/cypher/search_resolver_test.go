package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An executor given a resolver uses the database's current service on each
// statement, and the executors it is cloned into for another database (USE,
// a composite constituent) use that database's service, not its own.
func TestExecutorResolvesItsDatabasesSearchService(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = inner.Close() })
	services := map[string]*search.Service{
		"nornic": search.NewService(storage.NewNamespacedEngine(inner, "nornic")),
		"tenant": search.NewService(storage.NewNamespacedEngine(inner, "tenant")),
	}
	resolve := func(database string) *search.Service { return services[database] }
	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	exec.SetSearchServiceResolver("nornic", resolve)
	require.Same(t, services["nornic"], exec.search())

	// A reset replaces the database's service: the executor follows.
	services["nornic"] = search.NewService(storage.NewNamespacedEngine(inner, "nornic"))
	require.Same(t, services["nornic"], exec.search(), "the executor kept the replaced service")

	used := exec.cloneForStorage(storage.NewNamespacedEngine(inner, "tenant"))
	require.Same(t, services["tenant"], used.search(), "USE of another database searched the executor's own database")
	tx := exec.cloneWithStorage(storage.NewNamespacedEngine(inner, "nornic"))
	require.Same(t, services["nornic"], tx.search())
}

// Two services at the same revision, a reset database's search and the one
// it replaced, key different cached results.
func TestSearchCacheKeyTellsReplacedServicesApart(t *testing.T) {
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = inner.Close() })
	replaced := search.NewService(storage.NewNamespacedEngine(inner, "nornic"))
	current := search.NewService(storage.NewNamespacedEngine(inner, "nornic"))
	require.Equal(t, replaced.IndexRevision(), current.IndexRevision())
	require.NotEqual(t, searchCacheKey(replaced), searchCacheKey(current))
}
