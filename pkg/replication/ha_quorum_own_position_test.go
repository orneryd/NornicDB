package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// startOwnPositionPrimary starts a real HA primary over the fixture adapter with
// a protocol-shaped standby connection.
//
// The periodic streamer interval is widened so it cannot mask the boundary under
// test; quorum's own proactive send still runs through the HA logic.
func startOwnPositionPrimary(t *testing.T, adapter Storage, mode SyncMode) *HAStandbyReplicator {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Mode = ModeHAStandby
	cfg.NodeID = "own-position-primary"
	cfg.HAStandby.Role = "primary"
	cfg.HAStandby.PeerAddr = "standby:7000"
	cfg.HAStandby.SyncMode = mode
	cfg.HAStandby.WALBatchTimeout = time.Hour
	cfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	primary, err := NewHAStandbyReplicator(cfg, adapter)
	require.NoError(t, err)
	primary.SetTransport(NewMockTransport())
	require.NoError(t, primary.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, primary.Shutdown()) })
	require.Eventually(t, func() bool {
		primary.mu.RLock()
		defer primary.mu.RUnlock()
		return primary.standbyConn != nil && primary.standbyConn.IsConnected()
	}, 5*time.Second, time.Millisecond)
	return primary
}

// deletePrefixCommand is a real adapter command that the leader normalization
// owner does not intercept, so the test drives the quorum path directly.
func deletePrefixCommand(t *testing.T, prefix string) *Command {
	t.Helper()
	payload, err := storage.EncodeCommandPayload(struct{ Prefix string }{Prefix: prefix})
	require.NoError(t, err)
	return &Command{Type: CmdDeleteByPrefix, Data: payload, Timestamp: time.Now()}
}

// A quorum write publishes and acknowledges its own normalized record. A blocked
// writer therefore cannot report success, and the released record's exact
// position - not an earlier acknowledged prefix - is what the standby must
// acknowledge.
func TestHAQuorumAcknowledgesItsOwnNormalizedRecord(t *testing.T) {
	for _, seeded := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior-ack=%t", seeded), func(t *testing.T) {
			fixture := setupTestAdapter(t)
			adapter := fixture.adapter
			primary := startOwnPositionPrimary(t, adapter, SyncQuorum)
			engine := NewReplicatedEngine(adapter.engine, primary, time.Second)
			if seeded {
				_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "test:quorum-seed"})
				require.NoError(t, err)
				require.NoError(t, adapter.FlushWAL())
				primary.streamPendingWAL(context.Background())
				require.Equal(t, uint64(1), primary.walStreamer.LastAcked())
			}
			priorACK := primary.walStreamer.LastAcked()
			priorPosition, err := adapter.GetWALPosition()
			require.NoError(t, err)

			// Hold the physical WAL owner so the normalized record cannot publish.
			adapter.walMu.Lock()
			var once sync.Once
			unlock := func() { once.Do(adapter.walMu.Unlock) }
			t.Cleanup(unlock)
			applied := make(chan struct{})
			var appliedOnce sync.Once
			// The deadline never suppresses the native listener delivery; this test
			// creates a later node too, so the signal is delivered once.
			require.NoError(t, storage.SubscribeEvents(adapter.engine, storage.EventHandlers{
				NodeCreated: func(*storage.Node) { appliedOnce.Do(func() { close(applied) }) },
			}))
			result := make(chan error, 1)
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "test:quorum-current"})
				result <- err
			}()
			t.Cleanup(func() {
				unlock()
				<-joined
			})

			select {
			case <-applied:
			case <-time.After(5 * time.Second):
				t.Fatal("the leader-normalized native write did not apply")
			}
			select {
			case err := <-result:
				requireAcceptedDiagnosticCause(t, err, context.DeadlineExceeded)
			case <-time.After(5 * time.Second):
				t.Fatal("the quorum caller did not return")
			}
			require.Equal(t, priorACK, primary.walStreamer.LastAcked(),
				"no standby acknowledged the unpublished record, so the earlier ACK must not have counted")

			unlock()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("the quorum caller did not join after the WAL was released")
			}
			require.NoError(t, adapter.FlushWAL())
			records, err := adapter.GetWALEntries(0, 10)
			require.NoError(t, err)
			want := 1
			if seeded {
				want = 2
			}
			require.Len(t, records, want)

			node, err := adapter.engine.GetNode(t.Context(), "test:quorum-current")
			require.NoError(t, err, "the accepted native write survives the publication deadline")
			require.NotNil(t, node)

			// The released record carries its own exact published sequence.
			own := records[len(records)-1]
			require.Equal(t, CmdCommitTransaction, own.Command.Type, "leader normalization precedes the adapter")
			require.Equal(t, uint64(want), own.Position)
			require.Greater(t, own.Position, priorACK, "the earlier acknowledged prefix is insufficient for this command")
			require.Greater(t, own.Position, priorPosition)

			// Only this record's own (or a newer) standby ACK can release quorum.
			_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "test:quorum-after"})
			require.NoError(t, err, "a quorum write whose own record is acknowledged must succeed")
			records, err = adapter.GetWALEntries(0, 10)
			require.NoError(t, err)
			require.Len(t, records, want+1)
			require.Equal(t, uint64(want+1), records[len(records)-1].Position)
			require.GreaterOrEqual(t, primary.walStreamer.LastAcked(), own.Position,
				"the standby acknowledged the record at its own published position")
		})
	}
}

// One deadline covers publication and acknowledgement: a stalled writer spends
// the same budget instead of granting each wait its own full timeout, and the
// accepted record plus native application stay in place.
func TestHAQuorumSpendsOneDeadlineOnPublicationAndAcknowledgement(t *testing.T) {
	fixture := setupTestAdapter(t)
	adapter := fixture.adapter
	primary := startOwnPositionPrimary(t, adapter, SyncQuorum)
	node := &storage.Node{ID: "test:one-deadline"}
	_, err := adapter.engine.CreateNode(t.Context(), node)
	require.NoError(t, err)

	const budget = 200 * time.Millisecond
	command := deletePrefixCommand(t, "one-deadline")
	adapter.walMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(adapter.walMu.Unlock) }
	t.Cleanup(unlock)

	started := time.Now()
	err = primary.Apply(t.Context(), command, budget)
	elapsed := time.Since(started)

	require.ErrorIs(t, err, context.DeadlineExceeded, "the stalled publication must consume the quorum budget")
	require.GreaterOrEqual(t, elapsed, budget/2, "the wait spends the caller's publication budget")
	require.Less(t, elapsed, 2*budget, "publication and acknowledgement share one deadline instead of doubling it")

	_, err = adapter.engine.GetNode(t.Context(), "test:one-deadline")
	require.Error(t, err, "the accepted native application still ran under the deadline")

	unlock()
	require.NoError(t, adapter.FlushWAL())
	records, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1, "the accepted record is retained after the deadline")
	require.Equal(t, uint64(1), records[0].Position)
}

// Async keeps the nonwaiting native-result contract: no publication wait, no new
// per-write deadline or timer, and the accepted record publishes under the
// writer's ownership.
func TestHAAsyncApplyAcquiresNoPublicationWait(t *testing.T) {
	fixture := setupTestAdapter(t)
	adapter := fixture.adapter
	primary := startOwnPositionPrimary(t, adapter, SyncAsync)
	node := &storage.Node{ID: "test:async-no-wait"}
	_, err := adapter.engine.CreateNode(t.Context(), node)
	require.NoError(t, err)

	command := deletePrefixCommand(t, "async-no-wait")
	adapter.walMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(adapter.walMu.Unlock) }
	t.Cleanup(unlock)

	started := time.Now()
	err = primary.Apply(t.Context(), command, 5*time.Second)
	elapsed := time.Since(started)
	require.NoError(t, err, "async returns the native result without waiting for publication")
	require.Less(t, elapsed, 2*time.Second, "async must not acquire a publication wait or timer")

	_, err = adapter.engine.GetNode(t.Context(), "test:async-no-wait")
	require.Error(t, err, "native application ran while publication was blocked")

	unlock()
	require.NoError(t, adapter.FlushWAL())
	records, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, uint64(1), records[0].Position)
}
