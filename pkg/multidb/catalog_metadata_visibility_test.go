package multidb

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCatalogReloadIncludesSuppressedMetadata(t *testing.T) {
	for _, route := range []string{"pre_recovery", "manager"} {
		t.Run(route, func(t *testing.T) {
			inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
			require.NoError(t, err)
			manager, err := NewDatabaseManager(t.Context(), inner, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "captured", Options: map[string]string{OptionTxLogEnrichment: "FULL"}}))
			metadataID := storage.NodeID(manager.config.SystemDatabase + ":" + metadataNodeID)
			require.NoError(t, inner.SetManualSuppression(t.Context(), storage.StoredEntityNodeKind, string(metadataID), true))
			_, err = inner.GetNode(t.Context(), metadataID)
			require.ErrorIs(t, err, storage.ErrNotFound, "fixture must hide the catalog from ordinary entity reads")
			before, err := storage.GetStoredNode(t.Context(), inner, metadataID)
			require.NoError(t, err)
			inner.SetChangeCapture("captured", storage.ChangeCaptureOff)
			if route == "pre_recovery" {
				require.NoError(t, LoadChangeCaptureModes(t.Context(), inner, ""))
			} else {
				reloaded, err := NewDatabaseManager(t.Context(), inner, nil)
				require.NoError(t, err)
				require.True(t, reloaded.Exists("captured"), "reload must not replace a hidden persisted catalog with a new catalog")
				options, err := reloaded.DatabaseOptions("captured")
				require.NoError(t, err)
				require.Equal(t, "FULL", options[OptionTxLogEnrichment])
			}
			require.Equal(t, storage.ChangeCaptureFull, inner.ChangeCapture("captured"))
			after, err := storage.GetStoredNode(t.Context(), inner, metadataID)
			require.NoError(t, err)
			require.Equal(t, before.Properties, after.Properties, "loading must preserve the persisted catalog")
		})
	}
}
