package multidb

// Author: #1665 implementation lane (Claude Opus 5.5).

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The database catalog is a system record. Creating a database, adding and
// dropping its alias, and dropping the database write it as system work, so
// no storage owner has to admit the write as unrequested vector work.
func TestCatalogTransitionsAreSystemWork(t *testing.T) {
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, inner.Close()) })
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	counts := func() map[string]int64 {
		out := map[string]int64{}
		for _, fallback := range storage.VectorWorkFallbacks() {
			out[fallback.Site] = fallback.Count
		}
		return out
	}
	before := counts()
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "catalog_admission"}))
	require.NoError(t, manager.CreateAlias(t.Context(), "catalog_alias", "catalog_admission"))
	require.NoError(t, manager.DropAlias(t.Context(), "catalog_alias"))
	require.NoError(t, manager.DropDatabase(t.Context(), "catalog_admission"))
	fired := map[string]int64{}
	for site, count := range counts() {
		if delta := count - before[site]; delta != 0 {
			fired[site] = delta
		}
	}
	require.Empty(t, fired, "catalog transitions reached a storage owner without an admission")
}
