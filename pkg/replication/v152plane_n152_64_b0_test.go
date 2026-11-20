package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-209 report (Linear, 2026-11-04, heads 31208b17f and b098f7e70):
// "real TCP forward: follower got context canceled and deadline exceeded, leader
// preparation was still running 3 s later in both cases". These are the narrow
// unit controls for that report at the current head. They reuse the existing
// real ClusterTransport fixtures and the current standalone/HA standby/Raft
// owners; no production code and no new API is added here.
// Author: NOR-209 unit-test lane (DeepSeek), 2026-11-18.

// forwardPreparationGate parks the leader's entity preparation until the test
// releases it or the preparation context ends. It wraps the real StorageAdapter,
// so normalization, admission and the accepted boundary stay owned by the
// current implementation.
type forwardPreparationGate struct {
	*StorageAdapter
	entered chan context.Context
	release chan struct{}
	once    sync.Once
}

func newForwardPreparationGate(adapter *StorageAdapter) *forwardPreparationGate {
	return &forwardPreparationGate{
		StorageAdapter: adapter,
		entered:        make(chan context.Context, 1),
		release:        make(chan struct{}),
	}
}

func (g *forwardPreparationGate) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	select {
	case g.entered <- ctx:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, nil, true, ctx.Err()
	case <-g.release:
	}
	return g.StorageAdapter.PrepareEntityCommand(ctx, cmd)
}

func (g *forwardPreparationGate) releasePreparation() { g.once.Do(func() { close(g.release) }) }

// acceptedApplyGate parks the command after the accepted boundary, in the
// replicated application step, so caller cancellation can be observed there.
type acceptedApplyGate struct {
	*StorageAdapter
	entered chan context.Context
	release chan struct{}
	once    sync.Once
}

func newAcceptedApplyGate(adapter *StorageAdapter) *acceptedApplyGate {
	return &acceptedApplyGate{
		StorageAdapter: adapter,
		entered:        make(chan context.Context, 1),
		release:        make(chan struct{}),
	}
}

func (g *acceptedApplyGate) ApplyCommand(ctx context.Context, cmd *Command, mode WALPublicationMode) (CommandPublication, error) {
	select {
	case g.entered <- ctx:
	default:
	}
	select {
	case <-ctx.Done():
	case <-g.release:
	}
	return g.StorageAdapter.ApplyCommand(ctx, cmd, mode)
}

func (g *acceptedApplyGate) releaseApply() { g.once.Do(func() { close(g.release) }) }

// forwardLeaderFixture is a real Raft leader owner fronted by a real TCP
// cluster transport; RegisterClusterHandlers is the production forwarding
// registration.
type forwardLeaderFixture struct {
	raft      *RaftReplicator
	transport *ClusterTransport
	addr      string
}

func startForwardLeader(t *testing.T, owner Storage) *forwardLeaderFixture {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Mode = ModeRaft
	cfg.NodeID = "nor209-leader"
	cfg.AdvertiseAddr = "127.0.0.1:0"
	cfg.Raft.Bootstrap = true
	raft, err := NewRaftReplicator(cfg, owner)
	require.NoError(t, err)
	transport := NewClusterTransport(&ClusterTransportConfig{
		NodeID:     cfg.NodeID,
		BindAddr:   "127.0.0.1:0",
		AuthSecret: []byte(replayTestSecret),
	})
	raft.SetTransport(transport)
	RegisterClusterHandlers(transport, raft)
	require.NoError(t, raft.Start(t.Context()))
	t.Cleanup(func() {
		_ = raft.Shutdown()
		_ = transport.Close()
	})
	return &forwardLeaderFixture{raft: raft, transport: transport, addr: awaitBoundClusterAddress(t, transport)}
}

func awaitBoundClusterAddress(t *testing.T, transport *ClusterTransport) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		transport.resources.mu.RLock()
		listener, addr := transport.resources.listener, transport.bindAddr
		transport.resources.mu.RUnlock()
		if listener != nil && addr != "" {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("forwarding leader did not bind its cluster listener")
	return ""
}

// startForwardFollower is a follower Raft owner holding the discovered leader
// address, exactly as a follower holds it after peer discovery. Its Apply is the
// public forwarding path under test.
func startForwardFollower(t *testing.T, leaderAddr string) *RaftReplicator {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Mode = ModeRaft
	cfg.NodeID = "nor209-follower"
	cfg.AdvertiseAddr = "127.0.0.1:0"
	cfg.Raft.Peers = []PeerConfig{{ID: "nor209-leader", Addr: leaderAddr}}
	follower, err := NewRaftReplicator(cfg, NewMockStorage())
	require.NoError(t, err)
	transport := NewClusterTransport(&ClusterTransportConfig{
		NodeID:     cfg.NodeID,
		AuthSecret: []byte(replayTestSecret),
	})
	follower.SetTransport(transport)
	t.Cleanup(func() { _ = transport.Close() })
	follower.started.Store(true)
	follower.mu.Lock()
	follower.state = StateFollower
	follower.leaderAddr = leaderAddr
	follower.mu.Unlock()
	return follower
}

// requirePreparationStops asserts the leader-side preparation observed the
// ending, not only that the follower returned. A follower-only assertion is the
// reported gap.
func requirePreparationStops(t *testing.T, preparation context.Context, target error) {
	t.Helper()
	select {
	case <-preparation.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("leader preparation was still running 2s after the forwarded caller ended (leader preparation ctx err = %v)", preparation.Err())
	}
	require.ErrorIs(t, preparation.Err(), target)
}

// TestForwardApplyLeaderPreparationObservesFollowerCancel is test-matrix row 1
// over real TCP: cancel the originating caller while leader preparation is
// blocked. Both the follower outcome and the leader preparation are asserted.
func TestForwardApplyLeaderPreparationObservesFollowerCancel(t *testing.T) {
	fixture := setupTestAdapter(t)
	gate := newForwardPreparationGate(fixture.adapter)
	leader := startForwardLeader(t, gate)
	t.Cleanup(gate.releasePreparation)
	follower := startForwardFollower(t, leader.addr)

	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	outcome := make(chan error, 1)
	go func() {
		outcome <- follower.Apply(caller, publicationNodeCommand(t, "test:nor209-tcp-cancel"), 30*time.Second)
	}()

	preparation := awaitReplicationValue(t, gate.entered)
	cancelCaller()

	followerErr := awaitReplicationValue(t, outcome)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, followerErr, &accepted)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	requireAcceptedDiagnosticCause(t, followerErr, context.Canceled)
	requirePreparationStops(t, preparation, context.Canceled)
}

// TestForwardApplyLeaderPreparationUsesFollowerDeadline is test-matrix row 2
// over real TCP: a forwarded command with a short deadline must bound leader
// preparation with that deadline, not with the leader's own default budget.
func TestForwardApplyLeaderPreparationUsesFollowerDeadline(t *testing.T) {
	fixture := setupTestAdapter(t)
	gate := newForwardPreparationGate(fixture.adapter)
	leader := startForwardLeader(t, gate)
	t.Cleanup(gate.releasePreparation)
	follower := startForwardFollower(t, leader.addr)

	caller, cancelCaller := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancelCaller()
	callerDeadline, bounded := caller.Deadline()
	require.True(t, bounded)
	outcome := make(chan error, 1)
	go func() {
		outcome <- follower.Apply(caller, publicationNodeCommand(t, "test:nor209-tcp-deadline"), 30*time.Second)
	}()

	preparation := awaitReplicationValue(t, gate.entered)
	followerErr := awaitReplicationValue(t, outcome)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, followerErr, &accepted)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	requireAcceptedDiagnosticCause(t, followerErr, context.DeadlineExceeded)

	preparationDeadline, preparationBounded := preparation.Deadline()
	require.True(t, preparationBounded, "leader preparation was not bounded by the forwarded caller's deadline")
	require.WithinDuration(t, callerDeadline, preparationDeadline, 100*time.Millisecond,
		"leader preparation must keep the forwarded caller's deadline, not restart or default it")
}

// TestForwardPreparationStopsOnLeaderOwnerShutdownBeforeAcceptance is the
// pre-acceptance owner lifetime control: with no caller deadline, stopping the
// owning replicator stops blocked preparation and nothing is accepted.
func TestForwardPreparationStopsOnLeaderOwnerShutdownBeforeAcceptance(t *testing.T) {
	fixture := setupTestAdapter(t)
	gate := newForwardPreparationGate(fixture.adapter)
	leader := startForwardLeader(t, gate)
	t.Cleanup(gate.releasePreparation)
	follower := startForwardFollower(t, leader.addr)

	outcome := make(chan error, 1)
	go func() {
		outcome <- follower.Apply(t.Context(), publicationNodeCommand(t, "test:nor209-owner-shutdown"), 30*time.Second)
	}()
	preparation := awaitReplicationValue(t, gate.entered)

	shutdown := make(chan error, 1)
	go func() { shutdown <- leader.raft.Shutdown() }()
	requirePreparationStops(t, preparation, context.Canceled)
	gate.releasePreparation()
	awaitReplicationValue(t, shutdown)
	require.Error(t, awaitReplicationValue(t, outcome), "the forwarded caller cannot be told the command succeeded")

	require.NoError(t, fixture.adapter.FlushWAL())
	position, err := fixture.adapter.GetWALPosition()
	require.NoError(t, err)
	require.Zero(t, position, "stopped preparation must not cross the accepted boundary")
	count, err := fixture.adapter.engine.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestForwardedCallerCancellationReachesModeOwnerPreparation is test-matrix
// row 3 on the local leader path of every supported mode: the owner's own Apply
// prepares under the caller's context, exits on cancellation before the
// accepted boundary, and records nothing.
func TestForwardedCallerCancellationReachesModeOwnerPreparation(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		fixture := setupTestAdapter(t)
		gate := newForwardPreparationGate(fixture.adapter)
		owner := NewStandaloneReplicator(DefaultConfig(), gate)
		require.NoError(t, owner.Start(t.Context()))
		t.Cleanup(func() { require.NoError(t, owner.Shutdown()) })
		t.Cleanup(gate.releasePreparation)
		requireCanceledPreparationStopsBeforeAcceptance(t, gate, fixture.adapter, func(ctx context.Context) error {
			return owner.Apply(ctx, publicationNodeCommand(t, "test:nor209-standalone"), 0)
		})
	})

	t.Run("ha standby primary", func(t *testing.T) {
		fixture := setupTestAdapter(t)
		gate := newForwardPreparationGate(fixture.adapter)
		primary := startOwnPositionPrimary(t, gate, SyncAsync)
		t.Cleanup(gate.releasePreparation)
		requireCanceledPreparationStopsBeforeAcceptance(t, gate, fixture.adapter, func(ctx context.Context) error {
			return primary.Apply(ctx, publicationNodeCommand(t, "test:nor209-ha"), 0)
		})
	})

	t.Run("raft leader", func(t *testing.T) {
		fixture := setupTestAdapter(t)
		gate := newForwardPreparationGate(fixture.adapter)
		cfg := DefaultConfig()
		cfg.Mode = ModeRaft
		cfg.NodeID = "nor209-raft"
		cfg.AdvertiseAddr = "127.0.0.1:17000"
		cfg.Raft.Bootstrap = true
		raft, err := NewRaftReplicator(cfg, gate)
		require.NoError(t, err)
		raft.SetTransport(NewMockTransport())
		require.NoError(t, raft.Start(t.Context()))
		t.Cleanup(func() { _ = raft.Shutdown() })
		t.Cleanup(gate.releasePreparation)
		requireCanceledPreparationStopsBeforeAcceptance(t, gate, fixture.adapter, func(ctx context.Context) error {
			return raft.Apply(ctx, publicationNodeCommand(t, "test:nor209-raft"), 0)
		})
	})
}

func requireCanceledPreparationStopsBeforeAcceptance(t *testing.T, gate *forwardPreparationGate, adapter *StorageAdapter, apply func(context.Context) error) {
	t.Helper()
	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	outcome := make(chan error, 1)
	go func() { outcome <- apply(caller) }()
	preparation := awaitReplicationValue(t, gate.entered)
	cancelCaller()
	require.ErrorIs(t, awaitReplicationValue(t, outcome), context.Canceled)
	requirePreparationStops(t, preparation, context.Canceled)

	require.NoError(t, adapter.FlushWAL())
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Zero(t, position, "canceled preparation must not cross the accepted boundary")
	count, err := adapter.engine.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
}

// TestAcceptedForwardedCommandKeepsDurableOutcomeAfterCallerCancel is
// test-matrix row 5: once the command has crossed the accepted boundary, caller
// cancellation cannot cancel or discard it.
func TestAcceptedForwardedCommandKeepsDurableOutcomeAfterCallerCancel(t *testing.T) {
	fixture := setupTestAdapter(t)
	gate := newAcceptedApplyGate(fixture.adapter)
	owner := NewStandaloneReplicator(DefaultConfig(), gate)
	require.NoError(t, owner.Start(t.Context()))
	t.Cleanup(func() {
		gate.releaseApply()
		require.NoError(t, owner.Shutdown())
	})

	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	outcome := make(chan error, 1)
	go func() {
		outcome <- owner.Apply(caller, publicationNodeCommand(t, "test:nor209-accepted"), 0)
	}()

	applyContext := awaitReplicationValue(t, gate.entered)
	cancelCaller()
	err := awaitReplicationValue(t, outcome)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	require.NoError(t, applyContext.Err(), "caller cancellation cannot cancel accepted native work")

	gate.releaseApply()
	require.Eventually(t, func() bool {
		_, readErr := storage.GetStoredNode(t.Context(), fixture.adapter.engine, "test:nor209-accepted")
		return readErr == nil
	}, 5*time.Second, 5*time.Millisecond, "accepted work must still reach its durable outcome")
	require.NoError(t, fixture.adapter.FlushWAL())
	position, err := fixture.adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(1), position, "accepted work publishes exactly once")
}
