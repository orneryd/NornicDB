package nornicdb

import (
	"errors"
	"testing"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveDatabaseStorageUsesConfiguredResolver(t *testing.T) {
	db := &DB{}
	wantErr := errors.New("quota storage unavailable")
	db.SetDatabaseStorageResolver(func(databaseName string) (storage.Engine, error) {
		require.Equal(t, "tenant", databaseName)
		return nil, wantErr
	})

	_, err := db.resolveDatabaseStorage("tenant")
	require.ErrorIs(t, err, wantErr)
}

func TestInferenceServices_PerDatabaseIsolation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.AutoLinksEnabled = true
	cfg.Memory.EmbeddingDimensions = 3

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	defaultInfer, err := db.GetOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NotNil(t, defaultInfer)

	db2Storage := storage.NewNamespacedEngine(db.baseStorage, "db2")
	db2Infer, err := db.GetOrCreateInferenceService("db2", db2Storage)
	require.NoError(t, err)
	require.NotNil(t, db2Infer)
	require.NotSame(t, defaultInfer, db2Infer)

	// Reset and ensure a new instance is created on next request.
	db.ResetInferenceService("db2")
	db2Infer2, err := db.GetOrCreateInferenceService("db2", db2Storage)
	require.NoError(t, err)
	require.NotNil(t, db2Infer2)
	require.NotSame(t, db2Infer, db2Infer2)
}

func TestInferenceServices_AdditionalBranches(t *testing.T) {
	t.Run("disabled autolinks returns nil inference service", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = false
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		svc, err := db.GetOrCreateInferenceService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		require.Nil(t, svc)
	})

	t.Run("nil base storage returns explicit error when storage is not provided", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = true
		db := &DB{
			config:            cfg,
			inferenceServices: make(map[string]*inference.Engine),
		}

		svc, err := db.getOrCreateInferenceService("tenant_a", nil)
		require.Error(t, err)
		require.Nil(t, svc)
		require.Contains(t, err.Error(), "base storage is nil")
	})

	t.Run("closed db returns ErrClosed via public wrapper", func(t *testing.T) {
		db, err := Open("", DefaultConfig())
		require.NoError(t, err)
		require.NoError(t, db.Close())

		svc, err := db.GetOrCreateInferenceService("tenant_a", nil)
		require.ErrorIs(t, err, ErrClosed)
		require.Nil(t, svc)
	})

	t.Run("feature-flag integrations are wired when enabled", func(t *testing.T) {
		cleanupTLP := featureflags.WithAutoTLPEnabled()
		defer cleanupTLP()
		cleanupKalman := featureflags.WithKalmanEnabled()
		defer cleanupKalman()

		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = true
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		svc, err := db.GetOrCreateInferenceService("", nil) // empty dbName uses default
		require.NoError(t, err)
		require.NotNil(t, svc)
		require.NotNil(t, svc.GetTopologyIntegration())
		require.NotNil(t, svc.GetKalmanAdapter())
	})

	t.Run("ResetInferenceService with empty dbName resets default service", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.AutoLinksEnabled = true
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		before, err := db.GetOrCreateInferenceService("", nil)
		require.NoError(t, err)
		require.NotNil(t, before)

		db.ResetInferenceService("")

		after, err := db.GetOrCreateInferenceService("", nil)
		require.NoError(t, err)
		require.NotNil(t, after)
		require.NotSame(t, before, after)
	})
}
