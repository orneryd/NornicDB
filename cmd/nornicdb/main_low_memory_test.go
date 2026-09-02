package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplyLowMemoryModeConfiguresStorageAndCaches(t *testing.T) {
	cfg := config.LoadDefaults()

	applyLowMemoryMode(cfg)

	require.Equal(t, "low", cfg.Storage.Mode)
	require.Equal(t, 1000, cfg.Database.BadgerNodeCacheMaxEntries)
	require.Equal(t, 10, cfg.Database.BadgerEdgeTypeCacheMaxTypes)
}
