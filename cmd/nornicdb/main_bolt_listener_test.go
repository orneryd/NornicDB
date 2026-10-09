package main

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/bolt"
	appconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplyBoltListenerConfig(t *testing.T) {
	cfg := appconfig.LoadDefaults()
	cfg.Server.BoltMaxConnections = 512
	cfg.Server.BoltServerAnnouncement = "Neo4j/5.26.0"
	cfg.Server.BoltTLSRequire = true
	cfg.Server.BoltSniffTimeout = 2 * time.Second
	cfg.Server.BoltAuthTimeout = 15 * time.Second
	cfg.Server.BoltStatementTimeout = time.Minute
	cfg.Server.BoltWebSocketEnabled = false
	cfg.Server.BoltWebSocketAllowedOrigins = "https://app.example.com"
	cfg.Server.BoltWebSocketMaxMessageSize = 1024
	cfg.Server.BoltWebSocketWriteBufferSize = 4096
	cfg.Server.BoltWebSocketPingInterval = 10 * time.Second
	cfg.Server.BoltWebSocketPongTimeout = 20 * time.Second

	boltConfig := bolt.DefaultConfig()
	applyBoltListenerConfig(boltConfig, cfg)

	require.Equal(t, 512, boltConfig.MaxConnections)
	require.Equal(t, "Neo4j/5.26.0", boltConfig.ServerAnnouncement)
	require.True(t, boltConfig.RequireTLS)
	require.Equal(t, 2*time.Second, boltConfig.BoltSniffTimeout)
	require.Equal(t, 15*time.Second, boltConfig.BoltAuthTimeout)
	require.Equal(t, time.Minute, boltConfig.BoltStatementTimeout)
	require.False(t, boltConfig.WebSocketEnabled)
	require.Equal(t, "https://app.example.com", boltConfig.WebSocketAllowedOrigins)
	require.Equal(t, int64(1024), boltConfig.WebSocketMaxMessageSize)
	require.Equal(t, 4096, boltConfig.WebSocketWriteBufferSize)
	require.Equal(t, 10*time.Second, boltConfig.WebSocketPingInterval)
	require.Equal(t, 20*time.Second, boltConfig.WebSocketPongTimeout)
}

// The defaults keep the Bolt server's: 100 connections, and no statement
// timeout unless one is set.
func TestApplyBoltListenerConfigDefaults(t *testing.T) {
	boltConfig := bolt.DefaultConfig()
	applyBoltListenerConfig(boltConfig, appconfig.LoadDefaults())
	require.Equal(t, bolt.DefaultConfig().MaxConnections, boltConfig.MaxConnections)
	require.Zero(t, boltConfig.BoltStatementTimeout)

	unset := &appconfig.Config{}
	boltConfig = bolt.DefaultConfig()
	applyBoltListenerConfig(boltConfig, unset)
	require.Equal(t, bolt.DefaultConfig().BoltSniffTimeout, boltConfig.BoltSniffTimeout)
	require.Equal(t, bolt.DefaultConfig().WebSocketMaxMessageSize, boltConfig.WebSocketMaxMessageSize)
}
