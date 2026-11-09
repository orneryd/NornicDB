package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// requireBoltSchemaChangeWithin runs an unrelated CREATE CONSTRAINT, which
// needs every open transaction's write admission, under a client deadline of
// within: it proceeds only when no ended transaction still holds admission.
func requireBoltSchemaChangeWithin(t *testing.T, driver neo4j.DriverWithContext, name string, within time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:%s) REQUIRE n.k IS UNIQUE", name, name), nil)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	require.NoError(t, err, what)
}

// idleBoltHolderID is the transaction id of the one explicit transaction no
// statement runs in.
func idleBoltHolderID(t *testing.T, driver neo4j.DriverWithContext) string {
	t.Helper()
	rows := boltRows(t, driver, "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN collect(transactionId)", nil)
	ids := rows[0].([]any)
	require.Len(t, ids, 1)
	return ids[0].(string)
}

// TestBoltIdleHolderEndsAtTransactionTimeout (NOR-284 follow-up of #1756): a
// holder whose explicit transaction stays idle past the server's transaction
// timeout is ended at that timeout, though its client sends nothing: an
// unrelated schema change proceeds, a MERGE waiting for its key proceeds
// rather than timing out, and its COMMIT is refused with TransactionTimedOut.
func TestBoltIdleHolderEndsAtTransactionTimeout(t *testing.T) {
	const timeout = time.Second
	driver, _ := startKeyWaitBolt(t, timeout, 0)
	ctx := context.Background()

	t.Run("unrelated schema change", func(t *testing.T) {
		holder := holdBoltKeys(t, driver, "idle-schema")
		time.Sleep(timeout + 300*time.Millisecond)
		requireBoltSchemaChangeWithin(t, driver, "IdleSchema", 750*time.Millisecond, "the idle holder kept its write admission past its timeout")
		requireNeo4jCode(t, holder.tx.Commit(ctx), "Neo.ClientError.Transaction.TransactionTimedOut", "COMMIT of the timed-out holder")
		require.Equal(t, int64(0), boltDocCount(t, driver, "idle-schema"))
	})

	t.Run("waiting MERGE", func(t *testing.T) {
		holder := holdBoltKeys(t, driver, "idle-waited")
		time.Sleep(timeout / 2)
		result := awaitKeyWaiter(t, startBoltKeyWaiter(ctx, driver, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": "idle-waited"}), func() { _ = holder.tx.Rollback(ctx) })
		require.NoError(t, result.err, "the MERGE timed out behind an idle holder past its timeout")
		require.Less(t, result.elapsed, timeout, "the waiter proceeded only when its own wait timed out")
		requireNeo4jCode(t, holder.tx.Commit(ctx), "Neo.ClientError.Transaction.TransactionTimedOut", "COMMIT of the timed-out holder")
		require.Equal(t, int64(1), boltDocCount(t, driver, "idle-waited"))
	})
}

// TestBoltTerminateEndsAnIdleHolderAtOnce (NOR-284 follow-up of #1756):
// TERMINATE TRANSACTIONS on an idle holder releases its key and its write
// admission before it returns, though the holder's client sends nothing: the
// waiting MERGE proceeds at once, an unrelated schema change proceeds, and
// the holder's COMMIT is refused with Terminated.
func TestBoltTerminateEndsAnIdleHolderAtOnce(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, 5*time.Second, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	holder := holdBoltKeys(t, driver, "terminated")
	drainKeyWaits(waits)
	done := startBoltKeyWaiter(ctx, driver, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": "terminated"})
	awaitKeyWait(t, waits, "the waiter")
	id := idleBoltHolderID(t, driver)
	require.Equal(t, []any{"Transaction terminated."}, boltRows(t, driver, fmt.Sprintf("TERMINATE TRANSACTIONS '%s' YIELD message RETURN message", id), nil))
	terminated := time.Now()
	result := awaitKeyWaiter(t, done, func() { _ = holder.tx.Rollback(ctx) })
	require.NoError(t, result.err, "the waiter didn't get the terminated holder's key")
	require.Less(t, time.Since(terminated), time.Second, "the key was released only later")
	requireBoltSchemaChangeWithin(t, driver, "AfterTerminate", 750*time.Millisecond, "the terminated holder kept its write admission")
	requireNeo4jCode(t, holder.tx.Commit(ctx), "Neo.ClientError.Transaction.Terminated", "COMMIT of the terminated holder")
	require.Equal(t, int64(1), boltDocCount(t, driver, "terminated"))
}

// TestBoltClientTimeoutEndsAnIdleHolder: an idle holder whose client set
// tx_timeout is ended at that timeout with the same releases; its COMMIT is
// refused with TransactionTimedOutClientConfiguration.
func TestBoltClientTimeoutEndsAnIdleHolder(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, time.Minute, 0)
	waits := keyWaitSignal(t)
	ctx := context.Background()
	holder := holdBoltKeysWith(t, driver, []func(*neo4j.TransactionConfig){neo4j.WithTxTimeout(500 * time.Millisecond)}, "client-timeout")
	drainKeyWaits(waits)
	done := startBoltKeyWaiter(ctx, driver, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": "client-timeout"})
	awaitKeyWait(t, waits, "the waiter")
	result := awaitKeyWaiter(t, done, func() { _ = holder.tx.Rollback(ctx) })
	require.NoError(t, result.err)
	require.Less(t, result.elapsed, 2*time.Second)
	requireBoltSchemaChangeWithin(t, driver, "AfterClientTimeout", 750*time.Millisecond, "the timed-out holder kept its write admission")
	requireNeo4jCode(t, holder.tx.Commit(ctx), "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", "COMMIT of the timed-out holder")
}

// TestBoltWorkingHolderIsNotEndedEarly: a holder that keeps running
// statements is not ended by the server's transaction timeout, however long
// it runs. A MERGE waiting for its key proceeds when it commits within the
// waiter's timeout, and otherwise times out while the holder still commits.
func TestBoltWorkingHolderIsNotEndedEarly(t *testing.T) {
	const timeout = time.Second
	driver, _ := startKeyWaitBolt(t, timeout, 0)
	ctx := context.Background()
	for _, past := range []bool{false, true} {
		key := fmt.Sprintf("working-%v", past)
		holder := holdBoltKeys(t, driver, key)
		done := startBoltKeyWaiter(ctx, driver, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": key})
		steps := 3
		if past {
			steps = 8
		}
		for i := 0; i < steps; i++ {
			time.Sleep(timeout / 5)
			require.NoError(t, holder.run(ctx, "MERGE (n:Doc {id: $k})", map[string]any{"k": fmt.Sprintf("%s-%d", key, i)}), "step %d", i)
		}
		require.NoError(t, holder.tx.Commit(ctx), "the working holder was ended")
		result := awaitKeyWaiter(t, done, func() {})
		if past {
			requireNeo4jCode(t, result.err, "Neo.ClientError.Transaction.TransactionTimedOut", "the waiter behind a holder working past the timeout")
		} else {
			require.NoError(t, result.err)
		}
		require.Equal(t, int64(1), boltDocCount(t, driver, key))
	}
}

// TestBoltClientTimeoutOutlivesTheSessionExpiry (review round 1 of #1763):
// the embedded adapter's transport session doesn't end a transaction whose
// client set a longer timeout: an idle transaction past the session's 30 s
// expiry, within its 60 s client timeout, still commits.
func TestBoltClientTimeoutOutlivesTheSessionExpiry(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, time.Minute, 0)
	ctx := context.Background()
	holder := holdBoltKeysWith(t, driver, []func(*neo4j.TransactionConfig){neo4j.WithTxTimeout(time.Minute)}, "client-minute")
	time.Sleep(32 * time.Second)
	require.NoError(t, holder.tx.Commit(ctx), "the client's 60 s timeout has not elapsed")
	require.Equal(t, int64(1), boltDocCount(t, driver, "client-minute"))
}

// The Go driver sends no tx_timeout for a zero or negative timeout (its
// WithTxTimeout(0) is the server's default on the wire), so these tests send
// BEGIN {tx_timeout: 0} as a raw Bolt client does.
func rawBoltZeroTimeoutTransaction(t *testing.T, host, key string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", host)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = conn.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 4, 0, 0, 4, 3, 0, 0, 4, 2, 0, 0, 4, 1})
	require.NoError(t, err)
	var version [4]byte
	_, err = io.ReadFull(conn, version[:])
	require.NoError(t, err)
	rawBoltSend(t, conn, []byte{0xb1, 0x01, 0xa0}) // HELLO {}
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1])
	rawBoltSend(t, conn, append([]byte{0xb1, 0x11, 0xa1, 0x8a}, append([]byte("tx_timeout"), 0x00)...)) // BEGIN {tx_timeout: 0}
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1], "BEGIN")
	rawBoltSend(t, conn, rawBoltRun("MERGE (:Doc {id: '"+key+"'})"))
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1], "RUN")
	rawBoltSend(t, conn, []byte{0xb1, 0x3f, 0xa1, 0x81, 'n', 0xff}) // PULL {n: -1}
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1], "PULL")
	return conn
}

// TestBoltExplicitZeroClientTimeoutKeepsTheTransaction (review round 1 of
// #1763): a client tx_timeout of zero, Neo4j's "no expiry", replaces the
// server's transaction timeout like any client timeout, so an idle
// transaction past the server's timeout still commits.
func TestBoltExplicitZeroClientTimeoutKeepsTheTransaction(t *testing.T) {
	driver, _ := startKeyWaitBolt(t, 200*time.Millisecond, 0)
	conn := rawBoltZeroTimeoutTransaction(t, driver.Target().Host, "zero-client-timeout")
	time.Sleep(600 * time.Millisecond)
	rawBoltSend(t, conn, []byte{0xb0, 0x12}) // COMMIT
	require.Equal(t, byte(0x70), rawBoltRead(t, conn)[1], "an explicit zero client timeout must override the server timeout")
	require.Equal(t, int64(1), boltDocCount(t, driver, "zero-client-timeout"))
}
