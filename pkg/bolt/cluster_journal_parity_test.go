package bolt

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: what a two-server cluster does with one transaction of a chosen
// size. The test opens two servers per cluster with nornicdb.Open (the
// persistent engine, its WAL, the replication journal and the cluster
// transport), commits the transaction on the leader through a Bolt server over
// the database manager, and logs a PARITY line with what the client got, what
// each node holds and where the journals stand. The file uses only API that
// main has without this change. Comparing the PARITY lines of a run on main
// with those of a run on this change is done by hand, not by the test.
//
// By default only the HA standby cases named "below" run (400 plain nodes of 20
// KB, or 24 nodes with 32 chunk vectors each): for them the test asserts that
// the transaction commits and that the follower holds what the leader holds.
// With NORNICDB_CLUSTER_PARITY_FULL set, the larger transactions and a two-node
// Raft cluster run too; their outcomes are logged, not asserted.

// 32 vectors of 2,048 float32 dimensions per embedded node: about 0.26 MB.
const parityChunks = 32

type parityNode struct {
	lab *journalLab
	db  *nornicdb.DB
	dir string
}

func parityFreeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// parityOpen opens one server configured through the NORNICDB_CLUSTER_*
// environment.
func parityOpen(t *testing.T, env map[string]string) parityNode {
	t.Helper()
	dir := t.TempDir()
	settings := map[string]string{
		"NORNICDB_CLUSTER_DATA_DIR":           filepath.Join(dir, "replication"),
		"NORNICDB_CLUSTER_REPLICATION_SECRET": "",
		"NORNICDB_CLUSTER_TLS_ENABLED":        "false",
	}
	for key, value := range env {
		settings[key] = value
	}
	for key, value := range settings {
		t.Setenv(key, value)
	}
	db, err := nornicdb.Open(dir, journalConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return parityNode{db: db, dir: dir}
}

// parityServe puts a Bolt server over the database manager in front of the
// leader.
func parityServe(t *testing.T, node parityNode) parityNode {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(context.Background(), node.db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	require.NoError(t, err)
	node.lab = &journalLab{t: t, root: node.dir, db: node.db, d: driver, s: server, done: done}
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
		_ = server.Close()
		<-done
		// The server's close error is logged, not asserted.
		if err := node.db.Close(); err != nil {
			t.Logf("PARITY close: %s", parityError(err))
		}
	})
	return node
}

func parityHAPair(t *testing.T, sync string) (leader, follower parityNode) {
	t.Helper()
	standbyAddr := parityFreeAddr(t)
	common := map[string]string{
		"NORNICDB_CLUSTER_MODE": "ha_standby", "NORNICDB_CLUSTER_HA_SYNC_MODE": sync,
		"NORNICDB_CLUSTER_HA_AUTO_FAILOVER": "false", "NORNICDB_CLUSTER_HA_HEARTBEAT_MS": "10",
		"NORNICDB_CLUSTER_HA_WAL_BATCH_TIMEOUT": "5ms", "NORNICDB_CLUSTER_HA_RECONNECT_INTERVAL": "20ms",
		"NORNICDB_CLUSTER_HA_MAX_RECONNECT_BACKOFF": "20ms",
	}
	node := func(extra map[string]string) parityNode {
		env := map[string]string{}
		for key, value := range common {
			env[key] = value
		}
		for key, value := range extra {
			env[key] = value
		}
		return parityOpen(t, env)
	}
	follower = node(map[string]string{
		"NORNICDB_CLUSTER_NODE_ID": "parity-standby", "NORNICDB_CLUSTER_HA_ROLE": "standby",
		"NORNICDB_CLUSTER_BIND_ADDR": standbyAddr, "NORNICDB_CLUSTER_ADVERTISE_ADDR": standbyAddr,
		"NORNICDB_CLUSTER_HA_PEER_ADDR": "127.0.0.1:1",
	})
	leader = node(map[string]string{
		"NORNICDB_CLUSTER_NODE_ID": "parity-primary", "NORNICDB_CLUSTER_HA_ROLE": "primary",
		"NORNICDB_CLUSTER_BIND_ADDR": "127.0.0.1:0", "NORNICDB_CLUSTER_ADVERTISE_ADDR": "127.0.0.1:0",
		"NORNICDB_CLUSTER_HA_PEER_ADDR": standbyAddr,
	})
	return parityServe(t, leader), follower
}

func parityRaftPair(t *testing.T) (leader, follower parityNode) {
	t.Helper()
	ids := [2]string{"parity-raft-a", "parity-raft-b"}
	addrs := [2]string{parityFreeAddr(t), parityFreeAddr(t)}
	var nodes [2]parityNode
	for i := range nodes {
		nodes[i] = parityOpen(t, map[string]string{
			"NORNICDB_CLUSTER_MODE": "raft", "NORNICDB_CLUSTER_NODE_ID": ids[i],
			"NORNICDB_CLUSTER_BIND_ADDR": addrs[i], "NORNICDB_CLUSTER_ADVERTISE_ADDR": addrs[i],
			"NORNICDB_CLUSTER_RAFT_PEERS": ids[1-i] + ":" + addrs[1-i], "NORNICDB_CLUSTER_RAFT_BOOTSTRAP": "false",
			// A long election timeout keeps the leader through the heavy
			// storage work of the measured transaction.
			"NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT": "5s",
		})
	}
	type raftView interface {
		IsLeader() bool
		LeaderID() string
	}
	lead := -1
	require.Eventually(t, func() bool {
		for i := range nodes {
			self, selfOK := nodes[i].db.GetReplicator().(raftView)
			other, otherOK := nodes[1-i].db.GetReplicator().(raftView)
			if selfOK && otherOK && self.IsLeader() && other.LeaderID() == ids[i] {
				lead = i
				return true
			}
		}
		return false
	}, 60*time.Second, 20*time.Millisecond, "one Raft node leads and the other follows it")
	return parityServe(t, nodes[lead]), nodes[1-lead]
}

// parityState is what one node holds for a label: how many nodes read back, how many of
// them have flag = true, and how many hold parityChunks (32) chunk vectors.
type parityState struct{ nodes, flagged, embedded int }

func (n parityNode) state(t *testing.T, label string) parityState {
	t.Helper()
	ctx := context.Background()
	listed, err := n.db.GetStorage().GetNodesByLabel(ctx, label)
	require.NoError(t, err)
	var state parityState
	for _, item := range listed {
		node, err := n.db.GetStorage().GetNode(ctx, item.ID)
		if err != nil {
			continue
		}
		state.nodes++
		if node.Properties["flag"] == true {
			state.flagged++
		}
		if len(node.ChunkEmbeddings) == parityChunks {
			state.embedded++
		}
	}
	return state
}

// journals reports, as text, how many transaction records one node's engine journal
// holds, its last sequence, and how many entries the node's replication journal holds.
func (n parityNode) journals(t *testing.T) string {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(context.Background(), filepath.Join(n.dir, "wal"))
	require.NoError(t, err, "the engine journal reads back")
	var commits int
	var last uint64
	for _, entry := range entries {
		last = max(last, entry.Sequence)
		if entry.Operation == storage.OpEntityCommit {
			commits++
		}
	}
	replicated, err := storage.ReadWALEntriesFromDir(context.Background(), filepath.Join(n.dir, "replication", "wal"))
	require.NoError(t, err, "the replication journal reads back")
	return fmt.Sprintf("engine{commits=%d last=%d} replication{entries=%d}", commits, last, len(replicated))
}

// paritySettle waits, for at most 20 seconds, until the follower's state for label equals the
// leader's or has not changed for 3 seconds, and returns the leader's state and the follower's.
func paritySettle(t *testing.T, leader, follower parityNode, label string) (parityState, parityState) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	previous, stableSince := follower.state(t, label), time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		current := follower.state(t, label)
		if current != previous {
			previous, stableSince = current, time.Now()
			continue
		}
		if current == leader.state(t, label) || time.Since(stableSince) > 3*time.Second {
			break
		}
	}
	return leader.state(t, label), previous
}

// parityQuiet reads the journals of both nodes every two seconds until two readings in a row
// are equal, for at most 30 seconds, and returns the last reading.
func parityQuiet(t *testing.T, leader, follower parityNode) string {
	t.Helper()
	read := func() string { return "leader " + leader.journals(t) + " follower " + follower.journals(t) }
	previous := read()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		time.Sleep(2 * time.Second)
		current := read()
		if current == previous {
			break
		}
		previous = current
	}
	return previous
}

func parityError(err error) string {
	if err == nil {
		return "<nil>"
	}
	return parityIDs.ReplaceAllString(strings.Join(strings.Fields(err.Error()), " "), "tx-ID")
}

var parityIDs = regexp.MustCompile("tx-[0-9a-f-]{36}")

func TestClusterJournalParity(t *testing.T) {
	cases := []struct {
		name     string
		embedded bool
		count    int
	}{
		// Plain nodes of 20 KB of text: 400, 660 and 900 of them.
		{"plain/below", false, 400},
		{"plain/between", false, 660},
		{"plain/above", false, 900},
		// Nodes with 32 chunk vectors each (about 0.26 MB of vectors per node), updated
		// without touching the vectors.
		{"embedded/below", true, 24},
		{"embedded/between", true, 41},
		{"embedded/above", true, 56},
	}
	full := os.Getenv("NORNICDB_CLUSTER_PARITY_FULL") != ""
	for _, mode := range []string{"ha-async", "ha-quorum", "raft"} {
		for _, tc := range cases {
			asserted := mode != "raft" && strings.HasSuffix(tc.name, "/below")
			if !full && !asserted {
				continue
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				var leader, follower parityNode
				switch mode {
				case "ha-async":
					leader, follower = parityHAPair(t, "async")
				case "ha-quorum":
					leader, follower = parityHAPair(t, "quorum")
				default:
					leader, follower = parityRaftPair(t)
				}
				ctx := context.Background()

				label, query, params := "Plain", "UNWIND range(1, $n) AS i CREATE (:Plain {i: i, flag: true, text: $text})", map[string]any{"n": int64(tc.count), "text": strings.Repeat("p", 20<<10)}
				if tc.embedded {
					label, query, params = "Emb", "MATCH (n:Emb) SET n.flag = true", nil
					vectors := journalVectors(parityChunks, 0)
					// A Raft commit can report committed-pending and then
					// apply or not; a node that did not land is seeded again.
					for attempt := 0; attempt < 5 && leader.state(t, label).nodes < tc.count; attempt++ {
						for i := 0; i < tc.count; i++ {
							id := storage.NodeID(fmt.Sprintf("emb-%d", i))
							if _, err := leader.db.GetStorage().GetNode(ctx, id); err == nil {
								continue
							}
							_, err := leader.db.GetStorage().CreateNode(ctx, &storage.Node{
								ID: id, Labels: []string{"Emb"},
								Properties:      map[string]any{"text": "kept", "flag": false},
								ChunkEmbeddings: vectors,
								EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": parityChunks, "embedding_dimensions": journalDims, "embedding_model": "synthetic"},
							})
							if err != nil {
								t.Logf("seeding embedded node %d: %s", i, parityError(err))
							}
						}
						time.Sleep(time.Second)
					}
					seededLeader, seededFollower := paritySettle(t, leader, follower, label)
					if mode == "raft" && (seededLeader != parityState{nodes: tc.count, embedded: tc.count} || seededFollower != seededLeader) {
						t.Logf("PARITY mode=%s case=%s count=%d | seed did not settle: leader %+v follower %+v", mode, tc.name, tc.count, seededLeader, seededFollower)
						return
					}
					require.Equal(t, parityState{nodes: tc.count, embedded: tc.count}, seededLeader, "the leader holds each seeded node with its vectors")
					require.Equal(t, seededLeader, seededFollower, "the seed replicated")
				}
				before := parityQuiet(t, leader, follower)

				_, txErr := leader.lab.run(query, params, false)
				onLeader, onFollower := paritySettle(t, leader, follower, label)
				after := parityQuiet(t, leader, follower)

				// An ordinary transaction afterwards: does the cluster still work?
				_, laterErr := leader.lab.run("CREATE (:Later {flag: true})", nil, false)
				laterLeader, laterFollower := paritySettle(t, leader, follower, "Later")

				t.Logf("PARITY mode=%s case=%s count=%d | transaction: %s | leader %+v follower %+v | journals before: %s | after: %s | later transaction: %s leader %+v follower %+v",
					mode, tc.name, tc.count, parityError(txErr), onLeader, onFollower, before, after, parityError(laterErr), laterLeader, laterFollower)

				if asserted {
					want := parityState{nodes: tc.count, flagged: tc.count}
					if tc.embedded {
						want.embedded = tc.count
					}
					require.NoError(t, txErr, "the transaction of a \"below\" case commits")
					require.Equal(t, want, onLeader)
					require.Equal(t, want, onFollower, "and the follower holds what the leader holds")
				}
			})
		}
	}
}
