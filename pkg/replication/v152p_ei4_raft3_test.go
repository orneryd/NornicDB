package replication

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// L556 on a three-node Raft cluster with the shipped default configuration
// (DefaultConfig: election, heartbeat and commit timeouts unchanged) over the
// real cluster transport; the same ordinary ~6 MB transaction (300 nodes x
// 20 KiB) as the two-node fixture, explicit and bulk routes.
func TestEI4Raft3NodeDefaultConfig(t *testing.T) {
	for _, route := range []string{"explicit", "bulk"} {
		t.Run(route, func(t *testing.T) {
			const n = 3
			var nodes [n]ei4ctlNode
			var ids, addrs [n]string
			var replicators [n]*RaftReplicator
			for i := 0; i < n; i++ {
				nodes[i] = ei4ctlNewNode(t)
				ids[i] = fmt.Sprintf("raft3-%d", i)
				addrs[i] = ei4ctlFreeAddr(t)
			}
			var commitTimeout time.Duration
			for i := 0; i < n; i++ {
				cfg := DefaultConfig()
				cfg.Mode = ModeRaft
				cfg.NodeID = ids[i]
				cfg.BindAddr = addrs[i]
				cfg.AdvertiseAddr = addrs[i]
				for j := 0; j < n; j++ {
					if j != i {
						cfg.Raft.Peers = append(cfg.Raft.Peers, PeerConfig{ID: ids[j], Addr: addrs[j]})
					}
				}
				commitTimeout = cfg.Raft.CommitTimeout
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
					if !r.IsLeader() {
						continue
					}
					ok := true
					for j := 0; j < n; j++ {
						if j != i && replicators[j].LeaderID() != ids[i] {
							ok = false
						}
					}
					if ok {
						leader = i
						return true
					}
				}
				return false
			}, 30*time.Second, 10*time.Millisecond, "one Raft node leads and the other two follow it")
			engine := NewReplicatedEngine(nodes[leader].fixture.adapter.engine, replicators[leader], 10*time.Second)
			under := ei4ctlNodes("Under", 300, 20<<10)
			start := time.Now()
			err := ei4ctlCommit(t, engine, nodes[leader], route, under, 15*time.Second)
			took := time.Since(start)
			var inner error
			var accepted *storage.AcceptedCommitError
			if errors.As(err, &accepted) {
				inner = accepted.Err
			}
			counts := make([]int, 0, n)
			deadline := time.Now().Add(15 * time.Second)
			for {
				counts = counts[:0]
				all := true
				for i := 0; i < n; i++ {
					c := nodes[i].count(t, "Under")
					counts = append(counts, c)
					if c != len(under) {
						all = false
					}
				}
				if all || time.Now().After(deadline) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Logf("RAFT3 route=%s commitTimeout=%s err=%v inner=%v took=%s stillLeader=%v counts(leader=%d)=%v", route, commitTimeout, err, inner, took.Round(time.Millisecond), engine.IsLeader(), leader, counts)
		})
	}
}
