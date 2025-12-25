package qdrantgrpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, ":6334", cfg.ListenAddr)
	assert.Equal(t, 4096, cfg.MaxVectorDim)
	assert.Equal(t, 1000, cfg.MaxBatchPoints)
	assert.Equal(t, 1024*1024, cfg.MaxPayloadBytes)
	assert.Equal(t, 1000, cfg.MaxTopK)
	assert.Equal(t, 100, cfg.MaxFilterClauses)
	assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
	assert.Equal(t, uint32(100), cfg.MaxConcurrentStreams)
	assert.Equal(t, 64*1024*1024, cfg.MaxRecvMsgSize)
	assert.Equal(t, 64*1024*1024, cfg.MaxSendMsgSize)
	assert.True(t, cfg.EnableReflection)
}

func TestNewServer(t *testing.T) {
	t.Run("success with all parameters", func(t *testing.T) {
		cfg := DefaultConfig()
		store := storage.NewMemoryEngine()
		registry := NewMemoryCollectionRegistry()

		srv, err := NewServer(cfg, store, registry, nil) // nil searchService for testing
		require.NoError(t, err)
		require.NotNil(t, srv)
		assert.False(t, srv.IsRunning())
	})

	t.Run("success with nil config uses defaults", func(t *testing.T) {
		store := storage.NewMemoryEngine()
		registry := NewMemoryCollectionRegistry()

		srv, err := NewServer(nil, store, registry, nil)
		require.NoError(t, err)
		require.NotNil(t, srv)
	})

	t.Run("error on nil storage", func(t *testing.T) {
		cfg := DefaultConfig()
		registry := NewMemoryCollectionRegistry()

		_, err := NewServer(cfg, nil, registry, nil)
		require.Error(t, err)
	})

	t.Run("error on nil registry", func(t *testing.T) {
		cfg := DefaultConfig()
		store := storage.NewMemoryEngine()

		_, err := NewServer(cfg, store, nil, nil)
		require.Error(t, err)
	})
}

func TestServerStartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ":0" // Random port

	store := storage.NewMemoryEngine()
	registry := NewMemoryCollectionRegistry()

	srv, err := NewServer(cfg, store, registry, nil) // nil searchService for testing
	require.NoError(t, err)

	// Start server
	err = srv.Start()
	require.NoError(t, err)
	assert.True(t, srv.IsRunning())

	// Get actual address
	addr := srv.Addr()
	assert.NotEmpty(t, addr)
	t.Logf("Server listening on %s", addr)

	// Can't start twice
	err = srv.Start()
	require.Error(t, err)

	// Stop server
	srv.Stop()
	assert.False(t, srv.IsRunning())

	// Stopping again is safe
	srv.Stop()
}

func TestHealthService(t *testing.T) {
	svc := NewHealthService()
	require.NotNil(t, svc)

	// The health check should return SERVING status
	// This is tested via the proto interface in integration tests
}

