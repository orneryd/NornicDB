package bolt

import (
	"bytes"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

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

// runBoltExplicitTxWithRetry runs query in BEGIN/RUN/PULL/COMMIT, retrying
// while the server answers with a Neo.TransientError.* status, as
// neo4j-go-driver's ExecuteWrite does.
func runBoltExplicitTxWithRetry(t *testing.T, port int, query string, params map[string]any) error {
	t.Helper()
	for attempt := 0; attempt < driverRetryBudget; attempt++ {
		err, transient := runBoltExplicitTxAttempt(t, port, query, params)
		if err == nil {
			return nil
		}
		if !transient {
			return err
		}
		time.Sleep(driverRetryBackoff(attempt, attempt*7919+int(time.Now().UnixNano()&0xffff)))
	}
	return fmt.Errorf("explicit-tx statement exhausted %d retries", driverRetryBudget)
}

// runBoltExplicitTxSessions is the one concurrent scheduling and error
// collection owner for the explicit-transaction race reproductions: it runs
// one retry-aware transaction per session concurrently and reports
// every session that failed. statement builds that session's query and params,
// so each caller keeps its own statement shape and graph oracle. scope, when
// set, prefixes the failure message, for example "round 3".
func runBoltExplicitTxSessions(t *testing.T, port, concurrency int, scope string, statement func(idx int) (string, map[string]any)) {
	t.Helper()
	var wg sync.WaitGroup
	failures := make([]string, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			query, params := statement(idx)
			if err := runBoltExplicitTxWithRetry(t, port, query, params); err != nil {
				failures[idx] = err.Error()
			}
		}(i)
	}
	wg.Wait()
	for idx, msg := range failures {
		if msg == "" {
			continue
		}
		if scope == "" {
			t.Errorf("session %d failed: %s", idx, msg)
			continue
		}
		t.Errorf("%s session %d failed: %s", scope, idx, msg)
	}
}

// runBoltExplicitTxAttempt runs query in one BEGIN/RUN/PULL/COMMIT attempt
// and returns (err, transient). transient=true means a Neo.TransientError
// surfaced and the caller should retry (mimics neo4j-go-driver behavior).
func runBoltExplicitTxAttempt(t *testing.T, port int, query string, params map[string]any) (error, bool) {
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
	baseStore := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationAutocommit)

	// Session 1: declare constraint, create the node, then disconnect.
	session1 := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, session1,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE", nil)
	runBoltQueryWithParams(t, session1,
		"MERGE (r:TerraformResource {uid: 'X'}) SET r.name = 'session1'", nil)
	if err := session1.Close(); err != nil {
		t.Fatalf("close session1: %v", err)
	}

	// Session 2: fresh connection. The MERGE on the same uid must MATCH the
	// committed node, not CREATE+UNIQUE-fail.
	session2 := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, session2,
		"MERGE (r:TerraformResource {uid: 'X'}) SET r.name = 'session2'", nil)

	// Verify the final state: one node, name updated by session 2.
	records := runBoltQueryWithParams(t, session2,
		"MATCH (r:TerraformResource {uid: 'X'}) RETURN r.name, count(r)", nil)
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
	baseStore := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationAutocommit)

	// Schema setup on a throwaway connection.
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE", nil)
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
	records := runBoltQueryWithParams(t, check,
		"MATCH (r:TerraformResource {uid: 'X'}) RETURN count(r)", nil)
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

// TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx reproduces the
// same race as the *_Concurrent test inside explicit BEGIN/RUN/PULL/COMMIT
// envelopes. The helper retries only when the server returns a
// Neo.TransientError.* response, mirroring neo4j-go-driver ExecuteWrite.
//
// Two populations share that scenario. "test-namespace" is the original
// two-session population on uid X. "eshu-default-namespace" pins the explicit
// transaction shape Eshu uses with DEFAULT_DATABASE=nornic: eight sessions
// race one uid there, and the retried MERGE attempts must observe a peer
// transaction's committed UNIQUE value through the nornic schema cache, not
// repeatedly choose CREATE and surface commit-time UNIQUE violations.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent_ExplicitTx(t *testing.T) {
	for _, population := range []struct {
		name        string
		namespace   string
		concurrency int
		uid         string
	}{
		{"test-namespace", "test", 2, "X"},
		{"eshu-default-namespace", "nornic", 8, "eshu-same-uid"},
	} {
		t.Run(population.name, func(t *testing.T) {
			baseStore := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			store := storage.NewNamespacedEngine(baseStore, population.namespace)
			_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

			// Schema setup on a throwaway connection.
			setup := openBoltTestConn(t, port)
			runBoltQueryWithParams(t, setup,
				"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE", nil)
			if err := setup.Close(); err != nil {
				t.Fatalf("close setup: %v", err)
			}

			runBoltExplicitTxSessions(t, port, population.concurrency, "", func(idx int) (string, map[string]any) {
				return fmt.Sprintf("MERGE (r:TerraformResource {uid: %q}) SET r.name = %q",
					population.uid, fmt.Sprintf("session%d", idx)), nil
			})

			// Verify exactly one node exists.
			check := openBoltTestConn(t, port)
			records := runBoltQueryWithParams(t, check,
				fmt.Sprintf("MATCH (r:TerraformResource {uid: '%s'}) RETURN count(r)", population.uid), nil)
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
		})
	}
}

// TestBoltCrossSessionMultiMergeUniqueConflict_Concurrent_ExplicitTx races
// a statement of several MERGE clauses on two uniquely constrained keys, whose
// ON CREATE SET writes a computed value to a property named like the other
// label's key (o.id against V.id). The losing commit must be the retryable
// transient race, not a ConstraintValidationFailed the driver gives up on
// (#961).
func TestBoltCrossSessionMultiMergeUniqueConflict_Concurrent_ExplicitTx(t *testing.T) {
	baseStore := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT o_hash IF NOT EXISTS FOR (o:O) REQUIRE o.hash IS UNIQUE", nil)
	runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT v_id IF NOT EXISTS FOR (v:V) REQUIRE v.id IS UNIQUE", nil)
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup: %v", err)
	}

	const concurrency = 2
	runBoltExplicitTxSessions(t, port, concurrency, "", func(idx int) (string, map[string]any) {
		return fmt.Sprintf("MERGE (o:O {hash: 'h1'}) ON CREATE SET o.id = 'sha256:' + o.hash "+
			"MERGE (v:V {id: 'v1'}) ON CREATE SET v.original_id = o.id "+
			"MERGE (v)-[:HAS_ORIGINAL]->(o) "+
			"MERGE (u:U {id: 'u%d'}) MERGE (u)-[:OF_VERSION]->(v) RETURN v.id", idx), nil
	})

	check := openBoltTestConn(t, port)
	records := runBoltQueryWithParams(t, check,
		"MATCH (v:V {id: 'v1'})-[:HAS_ORIGINAL]->(o:O {hash: 'h1'}) MATCH (u:U)-[:OF_VERSION]->(v) RETURN count(DISTINCT v), count(DISTINCT o), count(u)", nil)
	if len(records) != 1 || len(records[0]) != 3 {
		t.Fatalf("expected one count row, got %v", records)
	}
	if records[0][0] != int64(1) || records[0][1] != int64(1) || records[0][2] != int64(2) {
		t.Errorf("expected one V, one O and two uploads, got %v", records[0])
	}
}

// TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx
// scales the explicit-tx reproduction toward the exact shape Eshu emits in
// production: many retry-aware Bolt sessions doing UNWIND $rows AS row
// MERGE (...) SET ... on overlapping uid sets.
func TestBoltCrossSessionMergeUniqueConflict_Concurrent_UnwindExplicitTx(t *testing.T) {
	baseStore := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE", nil)
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

	runBoltExplicitTxSessions(t, port, concurrency, "", func(sessionIdx int) (string, map[string]any) {
		rows := make([]map[string]any, batchSize)
		for i, uid := range uids {
			rows[i] = map[string]any{
				"uid":  uid,
				"name": fmt.Sprintf("session%d-%s", sessionIdx, uid),
			}
		}
		return "UNWIND $rows AS row MERGE (r:TerraformResource {uid: row.uid}) SET r.name = row.name",
			map[string]any{"rows": rows}
	})

	check := openBoltTestConn(t, port)
	records := runBoltQueryWithParams(t, check,
		"MATCH (r:TerraformResource) RETURN count(r)", nil)
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

// TestBoltExplicitTxStatementUniqueViolationCode: an explicit transaction
// checks UNIQUE when the statement runs (#700). A MERGE of a value a peer
// committed after BEGIN fails that statement with the retryable
// Neo.TransientError.Transaction.Outdated, as the same race does at COMMIT,
// and the retry matches the peer's node. A write that breaks the constraint on
// every attempt (a CREATE, or a MERGE whose SET writes another node's value)
// fails the statement with ConstraintValidationFailed and stores nothing.
func TestBoltExplicitTxStatementUniqueViolationCode(t *testing.T) {
	baseStore := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() {
		_ = baseStore.Close()
	})
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)

	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE", nil)
	runBoltQueryWithParams(t, setup, "CREATE (:TerraformResource {uid: 'stored'})", nil)

	// A transaction that read through its snapshot keeps it: a MERGE of a
	// key a peer committed afterwards loses the race.
	begin := func() net.Conn {
		conn := openBoltTestConn(t, port)
		requireNoError(t, SendBegin(t, conn, nil))
		requireNoError(t, ReadSuccess(t, conn))
		runBoltQueryWithParams(t, conn, "MATCH (r:TerraformResource) RETURN r.uid", nil)
		return conn
	}
	const merge = "MERGE (r:TerraformResource {uid: 'peer'}) SET r.name = 'second'"

	conn := begin()
	runBoltQueryWithParams(t, setup, "CREATE (:TerraformResource {uid: 'peer'})", nil)
	if code, _ := runBoltQueryExpectFailure(t, conn, merge); code != "Neo.TransientError.Transaction.Outdated" {
		t.Errorf("MERGE losing a race: got %s, want Neo.TransientError.Transaction.Outdated", code)
	}

	// A count reads the committed counters, not the snapshot (Neo4j's
	// read-committed view), so after one the MERGE still moves the snapshot
	// forward and matches the peer's node, as in Neo4j (#961).
	counted := openBoltTestConn(t, port)
	requireNoError(t, SendBegin(t, counted, nil))
	requireNoError(t, ReadSuccess(t, counted))
	runBoltQueryWithParams(t, counted, "MATCH (r:TerraformResource) RETURN count(r)", nil)
	runBoltQueryWithParams(t, setup, "CREATE (:TerraformResource {uid: 'counted'})", nil)
	runBoltQueryWithParams(t, counted, "MERGE (r:TerraformResource {uid: 'counted'}) RETURN r.uid", nil)
	requireNoError(t, SendCommit(t, counted))
	requireNoError(t, ReadSuccess(t, counted))
	retry := begin()
	runBoltQueryWithParams(t, retry, merge, nil)
	requireNoError(t, SendCommit(t, retry))
	requireNoError(t, ReadSuccess(t, retry))

	for _, query := range []string{
		"CREATE (:TerraformResource {uid: 'stored'})",
		"MERGE (r:TerraformResource {uid: 'fresh'}) SET r.uid = 'stored'",
		"MERGE (r:TerraformResource {uid: 'fresh'}) ON CREATE SET r.uid = 'stored'",
	} {
		code, _ := runBoltQueryExpectFailure(t, begin(), query)
		if code != "Neo.ClientError.Schema.ConstraintValidationFailed" {
			t.Errorf("%s: got %s, want Neo.ClientError.Schema.ConstraintValidationFailed", query, code)
		}
	}

	records := runBoltQueryWithParams(t, setup,
		"MATCH (r:TerraformResource) RETURN r.uid AS uid, r.name AS name ORDER BY uid", nil)
	if got := fmt.Sprint(records); got != "[[counted <nil>] [peer second] [stored <nil>]]" {
		t.Errorf("stored nodes: got %s, want [[counted <nil>] [peer second] [stored <nil>]]", got)
	}
}
