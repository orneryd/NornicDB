package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A real wire standby receives, applies and acknowledges the leader's own
// published record. This qualifies the publication+ACK boundary over the actual
// TCP/MessagePack cluster transport with the existing startup fixtures, instead
// of a protocol-shaped mock peer.
func TestHAWireStandbyAcknowledgesOwnPublishedRecord(t *testing.T) {
	standbyFixture := setupTestAdapter(t)
	primaryFixture := setupTestAdapter(t)

	standbyCfg := DefaultConfig()
	standbyCfg.Mode = ModeHAStandby
	standbyCfg.NodeID = "wire-standby"
	standbyCfg.BindAddr = "127.0.0.1:0"
	standbyCfg.HAStandby.Role = "standby"
	standbyCfg.HAStandby.PeerAddr = "127.0.0.1:0"
	standbyCfg.HAStandby.AutoFailover = false
	standbyCfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	standby, err := NewHAStandbyReplicator(standbyCfg, standbyFixture.adapter)
	require.NoError(t, err)
	standbyTransport := NewClusterTransport(&ClusterTransportConfig{NodeID: standbyCfg.NodeID, BindAddr: standbyCfg.BindAddr})
	RegisterClusterHandlers(standbyTransport, standby)
	standby.SetTransport(standbyTransport)
	require.NoError(t, standby.Start(context.Background()))
	t.Cleanup(func() {
		_ = standby.Shutdown()
		_ = standbyTransport.Close()
	})

	require.Eventually(t, func() bool {
		standbyTransport.resources.mu.RLock()
		defer standbyTransport.resources.mu.RUnlock()
		return standbyTransport.resources.listener != nil
	}, 5*time.Second, time.Millisecond, "the standby transport must listen")
	standbyTransport.resources.mu.RLock()
	standbyAddr := standbyTransport.bindAddr
	standbyTransport.resources.mu.RUnlock()
	require.NotEmpty(t, standbyAddr)

	primaryCfg := DefaultConfig()
	primaryCfg.Mode = ModeHAStandby
	primaryCfg.NodeID = "wire-primary"
	primaryCfg.BindAddr = "127.0.0.1:0"
	primaryCfg.HAStandby.Role = "primary"
	primaryCfg.HAStandby.PeerAddr = standbyAddr
	primaryCfg.HAStandby.SyncMode = SyncQuorum
	primaryCfg.HAStandby.WALBatchTimeout = 5 * time.Millisecond
	primaryCfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	primary, err := NewHAStandbyReplicator(primaryCfg, primaryFixture.adapter)
	require.NoError(t, err)
	primaryTransport := NewClusterTransport(&ClusterTransportConfig{NodeID: primaryCfg.NodeID})
	primary.SetTransport(primaryTransport)
	require.NoError(t, primary.Start(context.Background()))
	t.Cleanup(func() {
		_ = primary.Shutdown()
		_ = primaryTransport.Close()
	})
	require.Eventually(t, func() bool {
		primary.mu.RLock()
		defer primary.mu.RUnlock()
		return primary.standbyConn != nil && primary.standbyConn.IsConnected()
	}, 5*time.Second, time.Millisecond, "the primary must connect to the wire standby")

	engine := NewReplicatedEngine(primaryFixture.adapter.engine, primary, time.Second)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "test:wire-own-record", Labels: []string{"Wire"}})
	require.NoError(t, err, "quorum succeeds only after the wire standby acknowledges the leader's own record")

	position, err := primaryFixture.adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(1), position)
	require.GreaterOrEqual(t, primary.walStreamer.LastAcked(), position,
		"the acknowledgement covers the record's own published position")
	records, err := primaryFixture.adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, position, records[0].Position)
	require.Equal(t, CmdCommitTransaction, records[0].Command.Type)
	require.NotEmpty(t, records[0].Command.RequestID)

	require.Eventually(t, func() bool {
		_, nodeErr := standbyFixture.adapter.engine.GetNode(t.Context(), "test:wire-own-record")
		return nodeErr == nil
	}, 5*time.Second, time.Millisecond, "the wire standby applied the leader's own record")
	local, err := storage.GetStoredNode(t.Context(), primaryFixture.adapter.engine, "test:wire-own-record")
	require.NoError(t, err)
	remote, err := storage.GetStoredNode(t.Context(), standbyFixture.adapter.engine, "test:wire-own-record")
	require.NoError(t, err)
	require.Equal(t, local.ID, remote.ID)
	require.Equal(t, local.Labels, remote.Labels)
	require.Equal(t, local.Properties, remote.Properties)
	require.True(t, local.CreatedAt.Equal(remote.CreatedAt))
	require.True(t, local.UpdatedAt.Equal(remote.UpdatedAt))
	require.Empty(t, local.EmbedMeta)
	require.Empty(t, remote.EmbedMeta)
}
