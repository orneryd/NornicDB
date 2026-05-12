package bolt

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestBoltCrossSessionMergeUniqueConflict_Sequential reproduces the cross-
// Bolt-session constraint-cache desync that surfaces in Eshu's v2.5 tfstate
// drift verifier (eshu-hq/eshu#209). Two distinct Bolt sessions operate
// against the same shared storage engine, in strict serial order:
//
//  1. Session 1 declares a UNIQUE constraint on a node property and MERGEs
//     a node with uid="X".
//  2. Session 1 closes its connection.
//  3. Session 2 opens a fresh connection and MERGEs the same uid="X".
//
// The expected (and contractually correct) behavior: Session 2's MERGE
// matches the now-committed node and SETs, with no UNIQUE violation. The
// observed production behavior at timothyswt/nornicdb-amd64-cpu:v1.0.45
// and at upstream NornicDB main (03f546f) is that Session 2's MERGE
// planner treats the uid as absent, picks CREATE semantics, and the
// commit-time storage UNIQUE check fires with
//
//	Constraint violation (UNIQUE on Label.[prop]):
//	Node with prop=value already exists (nodeID: <Session 1's nodeID>)
//
// Returning a nodeID that the planner could not see during MERGE planning
// is the split-brain signature: the storage layer sees the node, the
// constraint cache used by MERGE does not. Retries within the second
// session return the same nodeID every time, confirming the cache stays
// empty for the new session for at least the retry window (~360ms).
//
// This test passes today (the MemoryEngine isolation case happens to
// stay coherent) and serves as the contract pin. The concurrent variant
// below is the one that goes red.
func TestBoltCrossSessionMergeUniqueConflict_Sequential(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)

	// Session 1: declare constraint, create the node, then disconnect.
	session1 := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, session1,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	runBoltQueryAndCollectRecords(t, session1,
		"MERGE (r:TerraformResource {uid: 'X'}) SET r.name = 'session1'")
	session1.Close()

	// Session 2: fresh connection. The MERGE on the same uid must MATCH the
	// committed node, not CREATE+UNIQUE-fail.
	session2 := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, session2,
		"MERGE (r:TerraformResource {uid: 'X'}) SET r.name = 'session2'")

	// Verify the final state: one node, name updated by session 2.
	records := runBoltQueryAndCollectRecords(t, session2,
		"MATCH (r:TerraformResource {uid: 'X'}) RETURN r.name, count(r)")
	if len(records) != 1 {
		t.Fatalf("expected one row, got %d (records=%v)", len(records), records)
	}
	row := records[0]
	if len(row) < 2 {
		t.Fatalf("row has fewer than 2 columns: %v", row)
	}
	if name, _ := row[0].(string); name != "session2" {
		t.Fatalf("expected name=session2 (MATCH+SET branch), got %v (full row=%v)", row[0], row)
	}
}

// TestBoltCrossSessionMergeUniqueConflict_Concurrent reproduces the
// production failure shape exactly: two Bolt sessions racing to MERGE
// the same uid against a UNIQUE-constrained label. Production has
// bootstrap-index Pass 2 worker N and resolution-engine projector M (or
// another bootstrap-index worker) both probing the constraint cache as
// "uid absent" and both proceeding with CREATE semantics; the second
// commit fires UNIQUE.
//
// Per the Eshu rule "Serialization Is Not A Fix"
// (CLAUDE.md/AGENTS.md), the fix lives at the storage layer in
// NornicDB, not at Eshu by single-threading the projector. This test
// pins the storage contract: concurrent MERGE on the same uid must
// converge — one writer creates, the other writer matches, neither
// surfaces a UNIQUE violation to the client. If both attempt CREATE
// and the storage uniquely fails the loser at commit time, the
// constraint cache is the bug surface.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)

	// Schema setup on a throwaway connection.
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	setup.Close()

	// concurrency=2 exercises the specific UNIQUE-on-MERGE race that this
	// patch fixes: two transactions both pass the deferred constraint
	// validation against an empty cache, then both commit, then the
	// constraint cache holds two writes for the same key. Higher
	// concurrency layers in a separate MVCC optimistic-concurrency error
	// class (Neo.TransientError.Transaction.Outdated, "node ... changed
	// after transaction start") that drivers and Eshu's RetryingExecutor
	// already classify as retryable; that is out of scope for this fix.
	const concurrency = 2
	var wg sync.WaitGroup
	failures := make([]string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				failures[idx] = fmt.Sprintf("dial: %v", err)
				return
			}
			defer conn.Close()
			if err := PerformHandshakeWithTesting(t, conn); err != nil {
				failures[idx] = fmt.Sprintf("handshake: %v", err)
				return
			}
			if err := SendHello(t, conn, nil); err != nil {
				failures[idx] = fmt.Sprintf("hello: %v", err)
				return
			}
			if _, _, err := ReadMessage(conn); err != nil {
				failures[idx] = fmt.Sprintf("hello-ack: %v", err)
				return
			}
			if err := SendRun(t, conn,
				fmt.Sprintf("MERGE (r:TerraformResource {uid: 'X'}) SET r.name = 'session%d'", idx),
				nil, nil); err != nil {
				failures[idx] = fmt.Sprintf("run: %v", err)
				return
			}
			msgType, msgData, err := ReadMessage(conn)
			if err != nil {
				failures[idx] = fmt.Sprintf("run-resp: %v", err)
				return
			}
			if msgType == MsgFailure {
				failures[idx] = fmt.Sprintf("MERGE returned failure: %s", string(msgData))
				return
			}
			if err := SendPull(t, conn, nil); err != nil {
				failures[idx] = fmt.Sprintf("pull: %v", err)
				return
			}
			for {
				mt, md, err := ReadMessage(conn)
				if err != nil {
					failures[idx] = fmt.Sprintf("pull-read: %v", err)
					return
				}
				if mt == MsgFailure {
					failures[idx] = fmt.Sprintf("commit-time failure: %s", string(md))
					return
				}
				if mt == MsgSuccess {
					return
				}
			}
		}(i)
	}
	wg.Wait()

	for idx, msg := range failures {
		if msg != "" {
			t.Errorf("session %d failed: %s", idx, msg)
		}
	}

	// Verify exactly one node exists.
	check := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, check,
		"MATCH (r:TerraformResource {uid: 'X'}) RETURN count(r)")
	if len(records) != 1 || len(records[0]) < 1 {
		t.Fatalf("expected one count row, got %v", records)
	}
	count, ok := records[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (value=%v)", records[0][0], records[0][0])
	}
	if count != 1 {
		t.Errorf("expected exactly one TerraformResource node, got %d", count)
	}
}
