package replication

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Control for the Raft -race failure (NOR-412 round 3): the same two-node Raft
// fixture over the real transport, with the timings of head 18cdd0331's
// cluster test (150 ms election, 30 ms heartbeat, 10 s engine timeout, 15 s
// receipt and replication waits) and its ordinary ~6 MB transaction, but no
// oversized transaction before it. Builds unchanged on main and on the head.

type ei4ctlNode struct {
	native  storage.Engine
	fixture *adapterFixture
}

func ei4ctlNewNode(t *testing.T) ei4ctlNode {
	t.Helper()
	native := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return ei4ctlNode{native: native, fixture: setupTestAdapter(t, adapterFixtureOptions{backing: storage.NewNamespacedEngine(native, "test")})}
}

func (n ei4ctlNode) count(t *testing.T, label string) int {
	t.Helper()
	nodes, err := n.fixture.adapter.Engine().GetNodesByLabel(context.Background(), label)
	require.NoError(t, err)
	return len(nodes)
}

func ei4ctlNodes(label string, n, size int) []*storage.Node {
	text := strings.Repeat("q", size)
	nodes := make([]*storage.Node, n)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:%s-%d", strings.ToLower(label), i)), Labels: []string{label}, Properties: map[string]any{"i": int64(i), "text": text}}
	}
	return nodes
}

func ei4ctlFreeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// ei4ctlTimings are the fixture timings of one head of the cluster test.
type ei4ctlTimings struct {
	name                string
	election, heartbeat time.Duration // zero: DefaultConfig's (1 s, 100 ms)
	engine, wait        time.Duration
	underNodes          int
	underSize           int
}

var (
	// ei4ctlH2 is head 18cdd0331's fixture.
	ei4ctlH2 = ei4ctlTimings{name: "h2-timings", election: 150 * time.Millisecond, heartbeat: 30 * time.Millisecond, engine: 10 * time.Second, wait: 15 * time.Second, underNodes: 300, underSize: 20 << 10}
	// ei4ctlH3 is head 6c2c4bae7's fixture.
	ei4ctlH3 = ei4ctlTimings{name: "h3-timings", engine: 30 * time.Second, wait: 30 * time.Second, underNodes: 200, underSize: 4 << 10}
)

// ei4ctlStartRaftPair is the cluster test's two-node Raft fixture.
func ei4ctlStartRaftPair(t *testing.T, timing ei4ctlTimings) (*ReplicatedEngine, ei4ctlNode, ei4ctlNode) {
	t.Helper()
	nodes := [2]ei4ctlNode{ei4ctlNewNode(t), ei4ctlNewNode(t)}
	ids := [2]string{"ctl-raft-a", "ctl-raft-b"}
	addrs := [2]string{ei4ctlFreeAddr(t), ei4ctlFreeAddr(t)}
	var replicators [2]*RaftReplicator
	for i := range nodes {
		cfg := DefaultConfig()
		cfg.Mode = ModeRaft
		cfg.NodeID = ids[i]
		cfg.BindAddr = addrs[i]
		cfg.AdvertiseAddr = addrs[i]
		cfg.Raft.Peers = []PeerConfig{{ID: ids[1-i], Addr: addrs[1-i]}}
		if timing.election > 0 {
			cfg.Raft.ElectionTimeout = timing.election
			cfg.Raft.HeartbeatTimeout = timing.heartbeat
		}
		r, err := NewRaftReplicator(cfg, nodes[i].fixture.adapter)
		require.NoError(t, err)
		replicators[i] = r
		transport := NewClusterTransport(&ClusterTransportConfig{NodeID: ids[i], BindAddr: addrs[i]})
		RegisterClusterHandlers(transport, r)
		r.SetTransport(transport)
		require.NoError(t, r.Start(context.Background()))
		t.Cleanup(func() {
			_ = r.Shutdown()
			_ = transport.Close()
		})
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
	}, 20*time.Second, 10*time.Millisecond, "one Raft node leads and the other follows it")
	return NewReplicatedEngine(nodes[leader].fixture.adapter.engine, replicators[leader], timing.engine), nodes[leader], nodes[1-leader]
}

// ei4ctlCommit is the cluster test's commit: an explicit transaction's commit or
// a bulk create command, with the fixture's receipt wait.
func ei4ctlCommit(t *testing.T, engine *ReplicatedEngine, leader ei4ctlNode, route string, nodes []*storage.Node, wait time.Duration) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), wait)
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

// ei4ctlOrdinary commits the fixture's ordinary transaction and waits for the
// follower, as the cluster test did.
func ei4ctlOrdinary(t *testing.T, engine *ReplicatedEngine, leader, follower ei4ctlNode, route, tag string, timing ei4ctlTimings) {
	t.Helper()
	under := ei4ctlNodes("Under", timing.underNodes, timing.underSize)
	stillLeader := engine.IsLeader()
	start := time.Now()
	err := ei4ctlCommit(t, engine, leader, route, under, timing.wait)
	committed := time.Since(start)
	replicated := false
	deadline := time.Now().Add(timing.wait)
	for time.Now().Before(deadline) && !replicated {
		replicated = follower.count(t, "Under") == len(under)
		if !replicated {
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Logf("CONTROL %s %s route=%s ordinary: err=%v commitTook=%s leaderNodes=%d followerNodes=%d landed=%v stillLeader=%v",
		timing.name, tag, route, err, committed.Round(time.Millisecond), leader.count(t, "Under"), follower.count(t, "Under"), replicated, stillLeader)
	require.NoError(t, err)
	require.True(t, replicated)
}

func TestEI4RaftRaceControlOrdinaryTransactionWithoutRefusal(t *testing.T) {
	for _, timing := range []ei4ctlTimings{ei4ctlH2, ei4ctlH3} {
		for _, route := range []string{"explicit", "bulk"} {
			t.Run(timing.name+"/"+route, func(t *testing.T) {
				engine, leader, follower := ei4ctlStartRaftPair(t, timing)
				ei4ctlOrdinary(t, engine, leader, follower, route, "without-refusal", timing)
			})
		}
	}
}

