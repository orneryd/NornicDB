package bolt

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// startBoltIntegrationServerWithExplicitTx wires the test Bolt server with a
// SessionExecutorFactory + TransactionalExecutor (txCapableCypherQueryExecutor)
// so BEGIN/RUN/COMMIT messages route to a real explicit BadgerTransaction
// instead of being protocol-acknowledged-but-ignored. This matches the
// runtime shape neo4j-go-driver session.ExecuteWrite emits from Eshu.
func startBoltIntegrationServerWithExplicitTx(t *testing.T, store storage.Engine) (*Server, int) {
	t.Helper()
	executor := newTxCapableCypherQueryExecutor(store)
	server := New(&Config{
		Port:            0,
		MaxConnections:  32,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.Port == 0 {
		t.Fatalf("expected TCP listener with assigned port, got %T %v", listener.Addr(), listener.Addr())
	}
	server.listener = listener

	errCh := make(chan error, 1)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Fatalf("close server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("serve: %v", err)
		}
	})

	go func() {
		errCh <- server.serve()
	}()
	return server, addr.Port
}

// driverRetryBudget mirrors neo4j-go-driver's default session.ExecuteWrite
// retry budget. The driver retries TransientError-coded failures with
// exponential backoff and bounded jitter for ~30s cumulative before
// surfacing the final error. Tests cap the attempt count at 20 to keep
// test wall time bounded while still proving the contract.
const driverRetryBudget = 20

// driverRetryBackoff returns a deterministic-but-jittered backoff per
// attempt. The lock-step backoff of `attempt * fixedDelay` causes
// concurrent retriers to wake up simultaneously and immediately re-collide;
// real neo4j-go-driver jitter avoids that. We mirror the driver shape with
// a 50ms*2^attempt envelope plus per-call pseudo-random jitter.
func driverRetryBackoff(attempt int, jitterSeed int) time.Duration {
	base := time.Duration(50<<uint(attempt)) * time.Millisecond
	if base > 2*time.Second {
		base = 2 * time.Second
	}
	jitter := time.Duration(((jitterSeed*1103515245+12345)>>16)&0x7fff) * time.Microsecond
	return base + jitter
}

// isTransientCommitCode reports whether a Bolt failure message body wraps a
// Neo.TransientError.* status code, which neo4j-go-driver classifies as
// retryable in session.ExecuteWrite. Used by the test helpers to mimic the
// driver's retry loop without pulling in the actual driver dependency.
func isTransientCommitCode(failureBody []byte) bool {
	return bytes.Contains(failureBody, []byte("Neo.TransientError."))
}

// runBoltExplicitMergeTx runs MERGE (label {uid:"X"}) SET name='sessionN'
// inside an explicit BEGIN/RUN/PULL/COMMIT envelope over a single Bolt
// connection. Returns the first error encountered. This is the wire shape
// Eshu produces through neo4j-go-driver's session.ExecuteWrite.
func runBoltExplicitMergeTx(t *testing.T, port int, label, prop, value string, sessionTag string) error {
	t.Helper()
	for attempt := 0; attempt < driverRetryBudget; attempt++ {
		err, transient := runBoltExplicitMergeTxAttempt(t, port, label, prop, value, sessionTag)
		if err == nil {
			return nil
		}
		if !transient {
			return err
		}
		time.Sleep(driverRetryBackoff(attempt, attempt*7919+int(time.Now().UnixNano()&0xffff)))
	}
	return fmt.Errorf("explicit-tx MERGE exhausted %d retries", driverRetryBudget)
}

// runBoltExplicitMergeTxAttempt runs one BEGIN/RUN/PULL/COMMIT attempt and
// returns (err, transient). transient=true means a Neo.TransientError
// surfaced and the caller should retry (mimics neo4j-go-driver behavior).
func runBoltExplicitMergeTxAttempt(t *testing.T, port int, label, prop, value string, sessionTag string) (error, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("dial: %w", err), false
	}
	defer func() {
		_ = conn.Close()
	}()
	if err := PerformHandshakeWithTesting(t, conn); err != nil {
		return fmt.Errorf("handshake: %w", err), false
	}
	if err := SendHello(t, conn, nil); err != nil {
		return fmt.Errorf("hello: %w", err), false
	}
	if _, _, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("hello-ack: %w", err), false
	}
	if err := SendBegin(t, conn, nil); err != nil {
		return fmt.Errorf("begin: %w", err), false
	}
	if mt, md, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("begin-resp: %w", err), false
	} else if mt == MsgFailure {
		return fmt.Errorf("BEGIN failure: %s", string(md)), isTransientCommitCode(md)
	}
	query := fmt.Sprintf("MERGE (r:%s {%s: %q}) SET r.name = %q", label, prop, value, sessionTag)
	if err := SendRun(t, conn, query, nil, nil); err != nil {
		return fmt.Errorf("run: %w", err), false
	}
	if mt, md, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("run-resp: %w", err), false
	} else if mt == MsgFailure {
		return fmt.Errorf("RUN failure: %s", string(md)), isTransientCommitCode(md)
	}
	if err := SendPull(t, conn, nil); err != nil {
		return fmt.Errorf("pull: %w", err), false
	}
	for {
		mt, md, err := ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("pull-read: %w", err), false
		}
		if mt == MsgFailure {
			return fmt.Errorf("PULL failure: %s", string(md)), isTransientCommitCode(md)
		}
		if mt == MsgSuccess {
			break
		}
	}
	if err := SendCommit(t, conn); err != nil {
		return fmt.Errorf("commit: %w", err), false
	}
	mt, md, err := ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("commit-resp: %w", err), false
	}
	if mt == MsgFailure {
		return fmt.Errorf("COMMIT failure: %s", string(md)), isTransientCommitCode(md)
	}
	return nil, false
}

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
	if err := session1.Close(); err != nil {
		t.Fatalf("close session1: %v", err)
	}

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

// TestBoltCrossSessionMergeUniqueConflict_Concurrent exercises two implicit
// Bolt sessions racing to MERGE the same UNIQUE-constrained value. NornicDB
// must preserve the UNIQUE invariant and may return a retryable conflict to
// the loser; it must not silently create duplicates or rely on server-side
// transaction replay.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)

	// Schema setup on a throwaway connection.
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	const concurrency = 2
	var wg sync.WaitGroup
	failures := make([]string, concurrency)
	transientConflicts := make([]bool, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				failures[idx] = fmt.Sprintf("dial: %v", err)
				return
			}
			defer func() {
				_ = conn.Close()
			}()
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
				if isTransientCommitCode(msgData) {
					transientConflicts[idx] = true
					return
				}
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
					if isTransientCommitCode(md) {
						transientConflicts[idx] = true
						return
					}
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
	transientCount := 0
	for _, transient := range transientConflicts {
		if transient {
			transientCount++
		}
	}
	if transientCount > 1 {
		t.Fatalf("expected at most one retryable conflict, got %d", transientCount)
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

// runBoltUnwindMergeExplicitTx runs the exact query shape Eshu emits from
// its projector and resolution-engine workers:
//
//	UNWIND $rows AS row MERGE (r:Label {uid: row.uid}) SET r.name = row.name
//
// inside an explicit BEGIN/RUN/PULL/COMMIT envelope. rows is a list of {uid,
// name} maps. This is what neo4j-go-driver session.ExecuteWrite emits in
// the v2.5 tfstate drift verifier path.
func runBoltUnwindMergeExplicitTx(t *testing.T, port int, label string, rows []map[string]any) error {
	t.Helper()
	for attempt := 0; attempt < driverRetryBudget; attempt++ {
		err, transient := runBoltUnwindMergeExplicitTxAttempt(t, port, label, rows)
		if err == nil {
			return nil
		}
		if !transient {
			return err
		}
		time.Sleep(driverRetryBackoff(attempt, attempt*7919+int(time.Now().UnixNano()&0xffff)))
	}
	return fmt.Errorf("UNWIND explicit-tx MERGE exhausted %d retries", driverRetryBudget)
}

func runBoltUnwindMergeExplicitTxAttempt(t *testing.T, port int, label string, rows []map[string]any) (error, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return fmt.Errorf("dial: %w", err), false
	}
	defer func() {
		_ = conn.Close()
	}()
	if err := PerformHandshakeWithTesting(t, conn); err != nil {
		return fmt.Errorf("handshake: %w", err), false
	}
	if err := SendHello(t, conn, nil); err != nil {
		return fmt.Errorf("hello: %w", err), false
	}
	if _, _, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("hello-ack: %w", err), false
	}
	if err := SendBegin(t, conn, nil); err != nil {
		return fmt.Errorf("begin: %w", err), false
	}
	if mt, md, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("begin-resp: %w", err), false
	} else if mt == MsgFailure {
		return fmt.Errorf("BEGIN failure: %s", string(md)), isTransientCommitCode(md)
	}
	query := fmt.Sprintf("UNWIND $rows AS row MERGE (r:%s {uid: row.uid}) SET r.name = row.name", label)
	params := map[string]any{"rows": rows}
	if err := SendRun(t, conn, query, params, nil); err != nil {
		return fmt.Errorf("run: %w", err), false
	}
	if mt, md, err := ReadMessage(conn); err != nil {
		return fmt.Errorf("run-resp: %w", err), false
	} else if mt == MsgFailure {
		return fmt.Errorf("RUN failure: %s", string(md)), isTransientCommitCode(md)
	}
	if err := SendPull(t, conn, nil); err != nil {
		return fmt.Errorf("pull: %w", err), false
	}
	for {
		mt, md, err := ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("pull-read: %w", err), false
		}
		if mt == MsgFailure {
			return fmt.Errorf("PULL failure: %s", string(md)), isTransientCommitCode(md)
		}
		if mt == MsgSuccess {
			break
		}
	}
	if err := SendCommit(t, conn); err != nil {
		return fmt.Errorf("commit: %w", err), false
	}
	mt, md, err := ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("commit-resp: %w", err), false
	}
	if mt == MsgFailure {
		return fmt.Errorf("COMMIT failure: %s", string(md)), isTransientCommitCode(md)
	}
	return nil, false
}

// TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx reproduces
// the same race as the *_Concurrent test inside explicit BEGIN/RUN/PULL/
// COMMIT envelopes. The helper retries only when the server returns a
// Neo.TransientError.* response, mirroring neo4j-go-driver ExecuteWrite.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	// Schema setup on a throwaway connection.
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	const concurrency = 2
	var wg sync.WaitGroup
	failures := make([]string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := runBoltExplicitMergeTx(t, port,
				"TerraformResource", "uid", "X",
				fmt.Sprintf("session%d", idx))
			if err != nil {
				failures[idx] = err.Error()
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

// TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx
// scales the explicit-tx reproduction toward the exact shape Eshu emits in
// production: many retry-aware Bolt sessions doing UNWIND $rows AS row
// MERGE (...) SET ... on overlapping uid sets.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	const (
		concurrency = 8
		batchSize   = 50
	)

	uids := make([]string, batchSize)
	for i := range uids {
		uids[i] = fmt.Sprintf("res-%03d", i)
	}

	var wg sync.WaitGroup
	failures := make([]string, concurrency)

	for s := 0; s < concurrency; s++ {
		wg.Add(1)
		go func(sessionIdx int) {
			defer wg.Done()
			rows := make([]map[string]any, batchSize)
			for i, uid := range uids {
				rows[i] = map[string]any{
					"uid":  uid,
					"name": fmt.Sprintf("session%d-%s", sessionIdx, uid),
				}
			}
			if err := runBoltUnwindMergeExplicitTx(t, port, "TerraformResource", rows); err != nil {
				failures[sessionIdx] = err.Error()
			}
		}(s)
	}
	wg.Wait()

	for idx, msg := range failures {
		if msg != "" {
			t.Errorf("session %d failed: %s", idx, msg)
		}
	}

	check := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, check,
		"MATCH (r:TerraformResource) RETURN count(r)")
	if len(records) != 1 || len(records[0]) < 1 {
		t.Fatalf("expected one count row, got %v", records)
	}
	count, ok := records[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (value=%v)", records[0][0], records[0][0])
	}
	if count != int64(batchSize) {
		t.Errorf("expected exactly %d TerraformResource nodes, got %d", batchSize, count)
	}
}
