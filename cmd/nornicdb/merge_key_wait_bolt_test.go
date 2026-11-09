package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MERGE waiting for a node key another transaction holds, over Bolt
// through the production executor (NOR-284). The wait ends at the server's
// transaction timeout even when the client sets no deadline, with the client's
// own deadline or cancellation, and with DeadlockDetected when it would close
// a cycle; one past the slow-query threshold is logged. A holder that commits
// or rolls back lets the waiter proceed as before.

const keyWaitStuckAfter = 15 * time.Second

// keyWaitLog collects the JSON log records the server writes.
type keyWaitLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *keyWaitLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// reset discards the records written so far.
func (l *keyWaitLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Reset()
}

// records returns the records with event_id and resets the log.
func (l *keyWaitLog) records(t *testing.T, eventID string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record), scanner.Text())
		if record["event_id"] == eventID {
			out = append(out, record)
		}
	}
	l.buf.Reset()
	return out
}

// startKeyWaitBolt serves a database whose transaction timeout is timeout and
// whose slow-query threshold is logAfter, with a UNIQUE constraint on
// (:Doc {id}).
func startKeyWaitBolt(t *testing.T, timeout, logAfter time.Duration) (neo4j.DriverWithContext, *keyWaitLog) {
	t.Helper()
	log := &keyWaitLog{}
	driver := startTransactionMemoryBolt(t, func(config *nornicdb.Config) {
		config.Database.TransactionTimeout = timeout
		config.Logging.SlowQueryThreshold = logAfter
		config.Logger = slog.New(slog.NewJSONHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
	_, err := runBolt(context.Background(), driver.NewSession(context.Background(), neo4j.SessionConfig{}), "CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (n:Doc) REQUIRE n.id IS UNIQUE")
	require.NoError(t, err)
	return driver, log
}

// boltKeyHolder is an open explicit transaction that has MERGEd keys.
type boltKeyHolder struct {
	session neo4j.SessionWithContext
	tx      neo4j.ExplicitTransaction
}

func holdBoltKeys(t *testing.T, driver neo4j.DriverWithContext, keys ...string) *boltKeyHolder {
	t.Helper()
	return holdBoltKeysWith(t, driver, nil, keys...)
}

// holdIdleBoltKeys is holdBoltKeys for a holder that stays idle past the
// server's transaction timeout while others wait for its keys: its client
// sets a timeout of its own, which replaces the server's, so the server
// doesn't end it (NOR-284 follow-up).
func holdIdleBoltKeys(t *testing.T, driver neo4j.DriverWithContext, keys ...string) *boltKeyHolder {
	t.Helper()
	return holdBoltKeysWith(t, driver, []func(*neo4j.TransactionConfig){neo4j.WithTxTimeout(time.Minute)}, keys...)
}

func holdBoltKeysWith(t *testing.T, driver neo4j.DriverWithContext, configurers []func(*neo4j.TransactionConfig), keys ...string) *boltKeyHolder {
	t.Helper()
	ctx := context.Background()
	holder := &boltKeyHolder{session: driver.NewSession(ctx, neo4j.SessionConfig{})}
	t.Cleanup(func() { _ = holder.session.Close(context.Background()) })
	tx, err := holder.session.BeginTransaction(ctx, configurers...)
	require.NoError(t, err)
	holder.tx = tx
	for _, key := range keys {
		require.NoError(t, holder.run(ctx, "MERGE (n:Doc {id: $k})", map[string]any{"k": key}))
	}
	return holder
}

func (h *boltKeyHolder) run(ctx context.Context, query string, params map[string]any) error {
	result, err := h.tx.Run(ctx, query, params)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	return err
}

// keyWaitResult is a waiting statement's outcome.
type keyWaitResult struct {
	err     error
	elapsed time.Duration
}

// startBoltKeyWaiter runs query without the driver's retries in a session of
// its own, auto-commit or in an explicit transaction, and reports its
// outcome. configurers (a tx_timeout) apply to its transaction. Its session
// is closed before the outcome is reported, so a test that ends on the
// outcome never closes the driver under a session still in use.
func startBoltKeyWaiter(ctx context.Context, driver neo4j.DriverWithContext, explicit bool, query string, params map[string]any, configurers ...func(*neo4j.TransactionConfig)) <-chan keyWaitResult {
	done := make(chan keyWaitResult, 1)
	go func() {
		started := time.Now()
		session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
		var err error
		if explicit {
			var tx neo4j.ExplicitTransaction
			if tx, err = session.BeginTransaction(ctx, configurers...); err == nil {
				var result neo4j.ResultWithContext
				if result, err = tx.Run(ctx, query, params); err == nil {
					_, err = result.Consume(ctx)
				}
				if err == nil {
					err = tx.Commit(ctx)
				} else {
					_ = tx.Rollback(context.Background())
				}
			}
		} else {
			var result neo4j.ResultWithContext
			if result, err = session.Run(ctx, query, params, configurers...); err == nil {
				_, err = result.Consume(ctx)
			}
		}
		elapsed := time.Since(started)
		_ = session.Close(context.Background())
		done <- keyWaitResult{err: err, elapsed: elapsed}
	}()
	return done
}

// awaitKeyWaiter returns the waiter's outcome. A waiter still blocked after
// keyWaitStuckAfter fails the test once release has let it go.
func awaitKeyWaiter(t *testing.T, done <-chan keyWaitResult, release func()) keyWaitResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(keyWaitStuckAfter):
		release()
		select {
		case <-done:
		case <-time.After(keyWaitStuckAfter):
		}
		t.Fatalf("the MERGE was still waiting for the key after %s", keyWaitStuckAfter)
		return keyWaitResult{}
	}
}

func requireNeo4jCode(t *testing.T, err error, code string, msgAndArgs ...any) {
	t.Helper()
	var neoErr *neo4j.Neo4jError
	require.True(t, errors.As(err, &neoErr), "%v %v", err, msgAndArgs)
	require.Equal(t, code, neoErr.Code, msgAndArgs...)
}

// keyWaitSignal reports each wait for a key lock another transaction holds.
func keyWaitSignal(t *testing.T) <-chan struct{} {
	waits := make(chan struct{}, 64)
	t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	return waits
}

func drainKeyWaits(waits <-chan struct{}) {
	for len(waits) > 0 {
		<-waits
	}
}

func awaitKeyWait(t *testing.T, waits <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-waits:
	case <-time.After(keyWaitStuckAfter):
		t.Fatalf("%s never waited for the key", what)
	}
}

func boltDocCount(t *testing.T, driver neo4j.DriverWithContext, key string) int64 {
	t.Helper()
	return boltRows(t, driver, "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]any{"k": key})[0].(int64)
}

// requireBoltKeyFree MERGEs key in a transaction of its own: a lock its
// earlier waiter left held would make it wait until the timeout.
func requireBoltKeyFree(t *testing.T, driver neo4j.DriverWithContext, key string) {
	t.Helper()
	result := awaitKeyWaiter(t, startBoltKeyWaiter(context.Background(), driver, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": key}), func() {})
	require.NoError(t, result.err, key)
	require.Less(t, result.elapsed, time.Second, "%s: a MERGE of a key nobody holds waited", key)
}

// TestBoltMergeKeyWaitEndsAtTransactionTimeout: the holder never ends and the
// client sets no deadline. The waiter fails at the server's transaction
// timeout with TransactionTimedOut, releases the key it took before waiting,
// and the wait is logged once past the slow-query threshold.
func TestBoltMergeKeyWaitEndsAtTransactionTimeout(t *testing.T) {
	const timeout, logAfter = time.Second, 200 * time.Millisecond
	driver, log := startKeyWaitBolt(t, timeout, logAfter)
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		held, free := fmt.Sprintf("held-%v", explicit), fmt.Sprintf("free-%v", explicit)
		holder := holdIdleBoltKeys(t, driver, held)
		log.reset()
		result := awaitKeyWaiter(t, startBoltKeyWaiter(ctx, driver, explicit, "UNWIND [$free, $held] AS k MERGE (n:Doc {id: k})", map[string]any{"free": free, "held": held}), func() {
			_ = holder.tx.Rollback(ctx)
		})
		requireNeo4jCode(t, result.err, "Neo.ClientError.Transaction.TransactionTimedOut", "explicit=%v", explicit)
		require.GreaterOrEqual(t, result.elapsed, timeout, "explicit=%v", explicit)
		require.Less(t, result.elapsed, timeout+5*time.Second, "explicit=%v", explicit)

		records := log.records(t, "storage.transaction.lock_wait_prolonged")
		require.Len(t, records, 1, "explicit=%v", explicit)
		require.Equal(t, "WARN", records[0]["level"])
		require.Equal(t, "Doc", records[0]["label"])
		require.Equal(t, "id", records[0]["property"])
		require.Equal(t, float64(timeout.Milliseconds()), records[0]["timeout_ms"])
		require.GreaterOrEqual(t, records[0]["waited_ms"], float64(logAfter.Milliseconds()))
		require.NotEmpty(t, records[0]["holder_transaction_id"])
		require.NotEqual(t, records[0]["holder_transaction_id"], records[0]["transaction_id"])

		require.Equal(t, int64(0), boltDocCount(t, driver, free), "the failed statement's MERGE is rolled back")
		requireBoltKeyFree(t, driver, free)
		require.NoError(t, holder.tx.Commit(ctx))
		requireBoltKeyFree(t, driver, held)
		require.Equal(t, int64(1), boltDocCount(t, driver, held))
	}
}

// TestBoltMergeKeyWaitEndsWithItsStatement: the client's own deadline and
// TERMINATE TRANSACTIONS end the wait before the server's timeout, and the
// waiter's keys are released.
func TestBoltMergeKeyWaitEndsWithItsStatement(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, time.Minute, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		held, free := fmt.Sprintf("deadline-held-%v", explicit), fmt.Sprintf("deadline-free-%v", explicit)
		holder := holdBoltKeys(t, driver, held)
		result := awaitKeyWaiter(t, startBoltKeyWaiter(ctx, driver, explicit, "UNWIND [$free, $held] AS k MERGE (n:Doc {id: k})", map[string]any{"free": free, "held": held}, neo4j.WithTxTimeout(300*time.Millisecond)), func() {
			_ = holder.tx.Rollback(ctx)
		})
		requireNeo4jCode(t, result.err, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", "explicit=%v", explicit)
		requireBoltKeyFree(t, driver, free)

		held, free = fmt.Sprintf("terminate-held-%v", explicit), fmt.Sprintf("terminate-free-%v", explicit)
		require.NoError(t, holder.run(ctx, "MERGE (n:Doc {id: $k})", map[string]any{"k": held}))
		drainKeyWaits(waits)
		done := startBoltKeyWaiter(ctx, driver, explicit, "UNWIND [$free, $held] AS k MERGE (n:Doc {id: k}) RETURN 'terminate me'", map[string]any{"free": free, "held": held})
		awaitKeyWait(t, waits, "the statement to terminate")
		rows := boltRows(t, driver, "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery STARTS WITH 'UNWIND [$free, $held]' RETURN collect(transactionId)", nil)
		ids := rows[0].([]any)
		require.Len(t, ids, 1, "explicit=%v", explicit)
		require.Equal(t, []any{"Transaction terminated."}, boltRows(t, driver, fmt.Sprintf("TERMINATE TRANSACTIONS '%s' YIELD message RETURN message", ids[0]), nil))
		result = awaitKeyWaiter(t, done, func() { _ = holder.tx.Rollback(ctx) })
		requireNeo4jCode(t, result.err, "Neo.ClientError.Transaction.Terminated", "explicit=%v", explicit)
		requireBoltKeyFree(t, driver, free)
		require.NoError(t, holder.tx.Rollback(ctx))
	}
}

// TestBoltMergeKeyWaitProceedsWhenTheHolderEnds: with the transaction timeout
// configured, a waiter still proceeds as soon as the holder commits (it
// matches the holder's node) or rolls back (it creates the node).
func TestBoltMergeKeyWaitProceedsWhenTheHolderEnds(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, time.Minute, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	for _, commit := range []bool{true, false} {
		for _, explicit := range []bool{false, true} {
			key := fmt.Sprintf("ends-%v-%v", commit, explicit)
			holder := holdBoltKeys(t, driver, key)
			drainKeyWaits(waits)
			done := startBoltKeyWaiter(ctx, driver, explicit, "MERGE (n:Doc {id: $k}) ON MATCH SET n.matched = true ON CREATE SET n.created = true", map[string]any{"k": key})
			awaitKeyWait(t, waits, "the waiter")
			if commit {
				require.NoError(t, holder.tx.Commit(ctx))
			} else {
				require.NoError(t, holder.tx.Rollback(ctx))
			}
			result := awaitKeyWaiter(t, done, func() {})
			require.NoError(t, result.err, key)
			require.Equal(t, []any{int64(1), commit, !commit}, boltRows(t, driver, "MATCH (n:Doc {id: $k}) RETURN count(n), coalesce(n.matched, false), coalesce(n.created, false)", map[string]any{"k": key}), key)
		}
	}
}

// TestBoltMergeKeyDeadlockFailsOneSide: two transactions each hold the key
// the other then MERGEs. One fails with DeadlockDetected; its keys are
// released, so the other proceeds and commits both nodes.
func TestBoltMergeKeyDeadlockFailsOneSide(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, time.Minute, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		k1, k2 := fmt.Sprintf("dl1-%v", explicit), fmt.Sprintf("dl2-%v", explicit)
		first := holdBoltKeys(t, driver, k1)
		drainKeyWaits(waits)
		// The second side holds k2 and waits for k1: as one auto-commit
		// statement, or an explicit transaction that MERGEd k2 first.
		var second <-chan keyWaitResult
		var secondHolder *boltKeyHolder
		if explicit {
			secondHolder = holdBoltKeys(t, driver, k2)
			second = startBoltKeyWaiterIn(ctx, secondHolder, "MERGE (n:Doc {id: $k})", map[string]any{"k": k1})
		} else {
			second = startBoltKeyWaiter(ctx, driver, false, "UNWIND [$k2, $k1] AS k MERGE (n:Doc {id: k})", map[string]any{"k1": k1, "k2": k2})
		}
		awaitKeyWait(t, waits, "the second side")
		firstErr := first.run(ctx, "MERGE (n:Doc {id: $k})", map[string]any{"k": k2})
		var secondResult keyWaitResult
		if firstErr != nil {
			requireNeo4jCode(t, firstErr, "Neo.TransientError.Transaction.DeadlockDetected", "explicit=%v", explicit)
			_ = first.tx.Rollback(ctx)
			secondResult = awaitKeyWaiter(t, second, func() {})
			require.NoError(t, secondResult.err, "explicit=%v", explicit)
			if secondHolder != nil {
				require.NoError(t, secondHolder.tx.Commit(ctx))
			}
		} else {
			secondResult = awaitKeyWaiter(t, second, func() {})
			requireNeo4jCode(t, secondResult.err, "Neo.TransientError.Transaction.DeadlockDetected", "explicit=%v", explicit)
			if secondHolder != nil {
				_ = secondHolder.tx.Rollback(ctx)
			}
			require.NoError(t, first.tx.Commit(ctx))
		}
		require.Equal(t, int64(1), boltDocCount(t, driver, k1), "explicit=%v", explicit)
		require.Equal(t, int64(1), boltDocCount(t, driver, k2), "explicit=%v", explicit)
	}
}

// startBoltKeyWaiterIn runs query in holder's open transaction and reports
// its outcome.
func startBoltKeyWaiterIn(ctx context.Context, holder *boltKeyHolder, query string, params map[string]any) <-chan keyWaitResult {
	done := make(chan keyWaitResult, 1)
	go func() {
		started := time.Now()
		err := holder.run(ctx, query, params)
		done <- keyWaitResult{err: err, elapsed: time.Since(started)}
	}()
	return done
}

// requireBoltSchemaChangeProceeds runs an unrelated CREATE CONSTRAINT, which
// needs every open transaction's write admission: a transaction that ended
// but still held it would block the schema change until its client went away.
func requireBoltSchemaChangeProceeds(t *testing.T, driver neo4j.DriverWithContext, name, what string) {
	t.Helper()
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:%s) REQUIRE n.k IS UNIQUE", name, name), nil, neo4j.WithTxTimeout(2*time.Second))
	if err == nil {
		_, err = result.Consume(ctx)
	}
	require.NoError(t, err, what)
}

// rawBoltSend writes one chunked Bolt message.
func rawBoltSend(t *testing.T, conn net.Conn, message []byte) {
	t.Helper()
	frame := make([]byte, len(message)+4)
	binary.BigEndian.PutUint16(frame, uint16(len(message)))
	copy(frame[2:], message)
	_, err := conn.Write(frame)
	require.NoError(t, err)
}

// rawBoltRead reads one chunked Bolt message.
func rawBoltRead(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	var message []byte
	for {
		var size [2]byte
		_, err := io.ReadFull(conn, size[:])
		require.NoError(t, err)
		n := int(binary.BigEndian.Uint16(size[:]))
		if n == 0 {
			if len(message) == 0 {
				continue
			}
			return message
		}
		chunk := make([]byte, n)
		_, err = io.ReadFull(conn, chunk)
		require.NoError(t, err)
		message = append(message, chunk...)
	}
}

// rawBoltRun is a Bolt RUN of query without parameters or metadata.
func rawBoltRun(query string) []byte {
	return append(append([]byte{0xb3, 0x10, 0xd0, byte(len(query))}, query...), 0xa0, 0xa0)
}

// TestBoltMergeKeyTimeoutEndsTheTransactionWithoutClientCleanup (NOR-284,
// review round 1): a raw Bolt client whose explicit transaction's MERGE hits
// the server's transaction timeout, and that then sends nothing more (no
// RESET, no ROLLBACK, no disconnect), holds nothing: an unrelated schema
// change proceeds once the key's holder ends, and the client's next message
// on the failed transaction is ignored, as for any failed transaction.
func TestBoltMergeKeyTimeoutEndsTheTransactionWithoutClientCleanup(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, 200*time.Millisecond, 0)
	ctx := context.Background()
	holder := holdIdleBoltKeys(t, driver, "raw-held")
	conn, err := net.Dial("tcp", driver.Target().Host)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = conn.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 4, 0, 0, 4, 3, 0, 0, 4, 2, 0, 0, 4, 1})
	require.NoError(t, err)
	var version [4]byte
	_, err = io.ReadFull(conn, version[:])
	require.NoError(t, err)
	rawBoltSend(t, conn, []byte{0xb1, 0x01, 0xa0}) // HELLO {}
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1])
	rawBoltSend(t, conn, []byte{0xb1, 0x11, 0xa0}) // BEGIN {}
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1])
	rawBoltSend(t, conn, rawBoltRun("MERGE (:Doc {id: 'raw-held'})"))
	failure := rawBoltRead(t, conn)
	require.Equal(t, byte(0x7f), failure[1], "FAILURE")
	require.Contains(t, string(failure), "Neo.ClientError.Transaction.TransactionTimedOut")

	require.NoError(t, holder.tx.Rollback(ctx))
	requireBoltSchemaChangeProceeds(t, driver, "RawUnrelated", "the timed-out transaction kept its write admission until its idle client disconnected")
	rawBoltSend(t, conn, rawBoltRun("RETURN 1"))
	require.Equal(t, byte(0x7e), rawBoltRead(t, conn)[1], "a statement on the failed transaction is IGNORED")
}

// TestBoltMergeKeyWaitEndingReleasesTheTransaction (NOR-284, review round 1):
// however a MERGE's wait in an explicit transaction ends (the server's
// timeout, the client's timeout, TERMINATE TRANSACTIONS), the transaction
// holds nothing afterwards, before its client rolls back or closes: an
// unrelated schema change proceeds once the key's holder ends.
func TestBoltMergeKeyWaitEndingReleasesTheTransaction(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, 200*time.Millisecond, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	for i, ending := range []string{"server timeout", "client timeout", "terminate"} {
		key := fmt.Sprintf("ending-%d", i)
		holder := holdIdleBoltKeys(t, driver, key)
		var waiter *boltKeyHolder
		if ending == "client timeout" {
			waiter = &boltKeyHolder{session: driver.NewSession(ctx, neo4j.SessionConfig{})}
			t.Cleanup(func() { _ = waiter.session.Close(context.Background()) })
			tx, err := waiter.session.BeginTransaction(ctx, neo4j.WithTxTimeout(100*time.Millisecond))
			require.NoError(t, err)
			waiter.tx = tx
		} else {
			waiter = holdBoltKeys(t, driver)
		}
		drainKeyWaits(waits)
		done := startBoltKeyWaiterIn(ctx, waiter, "MERGE (n:Doc {id: $k}) RETURN 'ending'", map[string]any{"k": key})
		var code string
		switch ending {
		case "server timeout":
			code = "Neo.ClientError.Transaction.TransactionTimedOut"
		case "client timeout":
			code = "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"
		case "terminate":
			code = "Neo.ClientError.Transaction.Terminated"
			awaitKeyWait(t, waits, "the statement to terminate")
			rows := boltRows(t, driver, "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery STARTS WITH 'MERGE (n:Doc {id: $k}) RETURN' RETURN collect(transactionId)", nil)
			ids := rows[0].([]any)
			require.Len(t, ids, 1)
			boltRows(t, driver, fmt.Sprintf("TERMINATE TRANSACTIONS '%s' YIELD message RETURN message", ids[0]), nil)
		}
		result := awaitKeyWaiter(t, done, func() { _ = holder.tx.Rollback(ctx) })
		requireNeo4jCode(t, result.err, code, ending)
		require.NoError(t, holder.tx.Rollback(ctx))
		requireBoltSchemaChangeProceeds(t, driver, fmt.Sprintf("Unrelated%d", i), ending+": the ended transaction kept its write admission until its client rolled back")
		_ = waiter.tx.Rollback(ctx)
	}
}
