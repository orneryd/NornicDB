package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: a single node commits a transaction of any journal record size,
// but a cluster carries each replicated transaction as one entry of its
// replication journal, which keeps every entry in one frame as before records
// could span frames, and ships entries in batches the transport carries as
// one message. A cluster refuses, at commit and before any node holds it, a
// transaction whose journal entry the journal cannot hold; smaller
// transactions replicate as before. These run over the real cluster
// transport.

type limitNode struct {
	native  storage.Engine
	fixture *adapterFixture
}

func newLimitNode(t *testing.T) limitNode {
	t.Helper()
	native := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return limitNode{native: native, fixture: setupTestAdapter(t, adapterFixtureOptions{backing: storage.NewNamespacedEngine(native, "test")})}
}

func (n limitNode) count(t *testing.T, label string) int {
	t.Helper()
	nodes, err := n.fixture.adapter.Engine().GetNodesByLabel(context.Background(), label)
	require.NoError(t, err)
	return len(nodes)
}

func limitTestNodes(label string, n, size int) []*storage.Node {
	text := strings.Repeat("q", size)
	nodes := make([]*storage.Node, n)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:%s-%d", strings.ToLower(label), i)), Labels: []string{label}, Properties: map[string]any{"i": int64(i), "text": text}}
	}
	return nodes
}

func limitFreeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func limitTransport(t *testing.T, replicator Replicator, nodeID, bindAddr string) {
	t.Helper()
	transport := NewClusterTransport(&ClusterTransportConfig{NodeID: nodeID, BindAddr: bindAddr})
	RegisterClusterHandlers(transport, replicator)
	replicator.(interface{ SetTransport(Transport) }).SetTransport(transport)
	require.NoError(t, replicator.Start(context.Background()))
	t.Cleanup(func() {
		_ = replicator.Shutdown()
		_ = transport.Close()
	})
}

// startHALimitPair starts a primary and a wire standby.
func startHALimitPair(t *testing.T, sync SyncMode) (*ReplicatedEngine, limitNode, limitNode) {
	t.Helper()
	primaryNode, standbyNode := newLimitNode(t), newLimitNode(t)
	standbyAddr := limitFreeAddr(t)
	standbyCfg := DefaultConfig()
	standbyCfg.Mode = ModeHAStandby
	standbyCfg.NodeID = "limit-standby"
	standbyCfg.BindAddr = standbyAddr
	standbyCfg.HAStandby.Role = "standby"
	standbyCfg.HAStandby.PeerAddr = "127.0.0.1:0"
	standbyCfg.HAStandby.AutoFailover = false
	standbyCfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	standby, err := NewHAStandbyReplicator(standbyCfg, standbyNode.fixture.adapter)
	require.NoError(t, err)
	limitTransport(t, standby, standbyCfg.NodeID, standbyAddr)

	primaryCfg := DefaultConfig()
	primaryCfg.Mode = ModeHAStandby
	primaryCfg.NodeID = "limit-primary"
	primaryCfg.BindAddr = "127.0.0.1:0"
	primaryCfg.HAStandby.Role = "primary"
	primaryCfg.HAStandby.PeerAddr = standbyAddr
	primaryCfg.HAStandby.SyncMode = sync
	primaryCfg.HAStandby.WALBatchTimeout = 5 * time.Millisecond
	primaryCfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	primary, err := NewHAStandbyReplicator(primaryCfg, primaryNode.fixture.adapter)
	require.NoError(t, err)
	limitTransport(t, primary, primaryCfg.NodeID, "")
	require.Eventually(t, func() bool {
		primary.mu.RLock()
		defer primary.mu.RUnlock()
		return primary.standbyConn != nil && primary.standbyConn.IsConnected()
	}, 10*time.Second, 5*time.Millisecond, "the primary connects to the wire standby")
	return NewReplicatedEngine(primaryNode.fixture.adapter.engine, primary, 30*time.Second), primaryNode, standbyNode
}

// startRaftLimitPair starts two Raft nodes and returns the leader's engine.
func startRaftLimitPair(t *testing.T) (*ReplicatedEngine, limitNode, limitNode) {
	t.Helper()
	nodes := [2]limitNode{newLimitNode(t), newLimitNode(t)}
	ids := [2]string{"limit-raft-a", "limit-raft-b"}
	addrs := [2]string{limitFreeAddr(t), limitFreeAddr(t)}
	var replicators [2]*RaftReplicator
	for i := range nodes {
		cfg := DefaultConfig()
		cfg.Mode = ModeRaft
		cfg.NodeID = ids[i]
		cfg.BindAddr = addrs[i]
		cfg.AdvertiseAddr = addrs[i]
		cfg.Raft.Peers = []PeerConfig{{ID: ids[1-i], Addr: addrs[1-i]}}
		r, err := NewRaftReplicator(cfg, nodes[i].fixture.adapter)
		require.NoError(t, err)
		replicators[i] = r
		limitTransport(t, r, ids[i], addrs[i])
	}
	leader := -1
	require.Eventually(t, func() bool {
		for i, r := range replicators {
			if r.IsLeader() && replicators[1-i].LeaderID() == ids[i] {
				leader = i
				return true
			}
		}
		return false
	}, 30*time.Second, 10*time.Millisecond, "one Raft node leads and the other follows it")
	engine := NewReplicatedEngine(nodes[leader].fixture.adapter.engine, replicators[leader], 30*time.Second)
	return engine, nodes[leader], nodes[1-leader]
}

// limitCommit commits nodes in one transaction through route: "explicit" is
// an explicit transaction's commit, "bulk" a bulk create command.
func limitCommit(t *testing.T, engine *ReplicatedEngine, leader limitNode, route string, nodes []*storage.Node) error {
	t.Helper()
	if route == "bulk" {
		return engine.BulkCreateNodes(context.Background(), nodes)
	}
	tx, err := leader.native.(interface {
		BeginTransaction(context.Context) (*storage.BadgerTransaction, error)
	}).BeginTransaction(context.Background())
	require.NoError(t, err)
	for _, node := range nodes {
		_, err := tx.CreateNode(context.Background(), node)
		require.NoError(t, err)
	}
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outcome := receipt.WaitOutcome(ctx)
	if outcome.State == storage.CommitApplied {
		return nil
	}
	if outcome.Err == nil {
		return fmt.Errorf("commit outcome %v", outcome.State)
	}
	return outcome.Err
}

func TestReplicatedTransactionLimit(t *testing.T) {
	for _, mode := range []string{"ha-async", "ha-quorum", "raft"} {
		for _, route := range []string{"explicit", "bulk"} {
			t.Run(mode+"/"+route, func(t *testing.T) {
				var engine *ReplicatedEngine
				var leader, follower limitNode
				switch mode {
				case "ha-async":
					engine, leader, follower = startHALimitPair(t, SyncAsync)
				case "ha-quorum":
					engine, leader, follower = startHALimitPair(t, SyncQuorum)
				default:
					engine, leader, follower = startRaftLimitPair(t)
				}
				positionBefore, err := leader.fixture.adapter.GetWALPosition()
				require.NoError(t, err)

				// About 70 MB of journal record: over the transport's
				// 64 MiB message limit as well as the replicated bound.
				over := limitTestNodes("Over", 700, 100<<10)
				overErr := limitCommit(t, engine, leader, route, over)
				time.Sleep(2 * time.Second)
				positionAfter, err := leader.fixture.adapter.GetWALPosition()
				require.NoError(t, err)
				t.Logf("MEASURE mode=%s route=%s over: err=%v leaderNodes=%d followerNodes=%d logPosition %d->%d",
					mode, route, overErr, leader.count(t, "Over"), follower.count(t, "Over"), positionBefore, positionAfter)

				// An ordinary transaction afterwards: about 0.8 MB.
				under := limitTestNodes("Under", 200, 4<<10)
				underErr := limitCommit(t, engine, leader, route, under)
				replicated := false
				deadline := time.Now().Add(30 * time.Second)
				for time.Now().Before(deadline) && !replicated {
					replicated = leader.count(t, "Under") == len(under) && follower.count(t, "Under") == len(under)
					if !replicated {
						time.Sleep(50 * time.Millisecond)
					}
				}
				t.Logf("MEASURE mode=%s route=%s under: err=%v leaderNodes=%d followerNodes=%d",
					mode, route, underErr, leader.count(t, "Under"), follower.count(t, "Under"))

				require.Error(t, overErr, "the oversized transaction is refused at commit")
				require.True(t, errors.Is(overErr, ErrTransactionTooLargeToReplicate), "refused for its size: %v", overErr)
				require.Contains(t, overErr.Error(), "exceeds maximum WAL frame size of 16777216 bytes", "the error is the journal's own and names its limit")
				require.Zero(t, leader.count(t, "Over"), "no node holds the refused transaction")
				require.Zero(t, follower.count(t, "Over"))
				require.Equal(t, positionBefore, positionAfter, "nothing reached the replication log")
				// The ordinary transaction must land on both nodes. On Raft
				// its receipt can report committed-pending although it
				// applied; main does the same without any refusal (an
				// intermittent receipt outcome of the Raft commit path), so
				// only that outcome is tolerated, never a refusal.
				if underErr != nil {
					var pending *storage.AcceptedCommitError
					require.True(t, errors.As(underErr, &pending) && pending.State == storage.CommitRecoveryPending, "the ordinary transaction is not refused: %v", underErr)
				}
				require.True(t, replicated, "an ordinary transaction lands on both nodes")
			})
		}
	}
}
