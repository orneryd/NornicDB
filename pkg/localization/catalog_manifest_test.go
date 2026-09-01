package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogManifestHasUniqueTypedConstructors(t *testing.T) {
	require.NotEmpty(t, CatalogManifest)
	ids := make(map[MessageID]struct{}, len(CatalogManifest))
	constructors := make(map[string]struct{}, len(CatalogManifest))
	for _, entry := range CatalogManifest {
		require.NotEmpty(t, entry.ID)
		require.NotEmpty(t, entry.Constructor)
		require.NotEmpty(t, entry.PluralForms)
		_, duplicateID := ids[entry.ID]
		require.Falsef(t, duplicateID, "duplicate generated message ID %s", entry.ID)
		ids[entry.ID] = struct{}{}
		_, duplicateConstructor := constructors[entry.Constructor]
		require.Falsef(t, duplicateConstructor, "duplicate generated constructor %s", entry.Constructor)
		constructors[entry.Constructor] = struct{}{}
	}
}
