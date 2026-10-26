package storage_test

import (
	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"go/types"
	"testing"
)

func TestStoredViewPopulationCoverage(t *testing.T) {
	result, err := capabilitycensus.Discover("../..")
	require.NoError(t, err)
	fixtures := storage.StoredViewModeFixtures(t)
	for name, obj := range result.StoredViews {
		if _, engine := result.Types[name]; engine {
			continue
		} // Engine gate requires its real Factory/Oracle/Stored adapter and all public logical consumers.
		require.NotNil(t, fixtures[name], "new real stored view needs an executable mode fixture: %s", name)
		typ := types.Unalias(obj.Type())
		named, ok := populationNamedType(typ)
		require.True(t, ok)
		ownPolicy, ownOperation := false, false
		for i := 0; i < named.NumMethods(); i++ {
			switch named.Method(i).Name() {
			case "StoredReadPolicy":
				ownPolicy = true
			case "ReadStored":
				ownOperation = true
			}
		}
		require.True(t, ownPolicy && ownOperation, "%s requires its own implementation or explicit refusal", name)
		delete(fixtures, name)
	}
	require.Empty(t, fixtures, "stale retained-view fixture")
}
