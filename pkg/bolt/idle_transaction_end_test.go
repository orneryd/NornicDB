package bolt

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// startIdleEndBolt serves a persistent database through the database
// manager (the server's own Bolt route) whose transaction timeout is timeout,
// with a UNIQUE constraint on (:Doc {id}).
func startIdleEndBolt(t *testing.T, timeout time.Duration) neo4jdriver.DriverWithContext {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:       t.TempDir(),
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	mgr, err := multidb.NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 32, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
	port := serveBoltIntegrationServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	_, err = idleEndRun(t, driver, "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil, time.Minute)
	require.NoError(t, err)
	return driver
}

// idleEndRun runs query auto-commit in a session of its own under a client
// deadline of within, and returns its records' values.
func idleEndRun(t *testing.T, driver neo4jdriver.DriverWithContext, query string, params map[string]any, within time.Duration) ([][]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	session := driver.NewSession(context.Background(), neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, len(records))
	for i, record := range records {
		rows[i] = record.Values
	}
	return rows, nil
}

func idleEndHolder(t *testing.T, driver neo4jdriver.DriverWithContext, key string) neo4jdriver.ExplicitTransaction {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	tx, err := session.BeginTransaction(context.Background())
	require.NoError(t, err)
	result, err := tx.Run(context.Background(), "MERGE (n:Doc {id: $k})", map[string]any{"k": key})
	require.NoError(t, err)
	_, err = result.Consume(context.Background())
	require.NoError(t, err)
	return tx
}

func requireIdleEndCode(t *testing.T, err error, code string, what string) {
	t.Helper()
	var neoErr *neo4jdriver.Neo4jError
	require.True(t, errors.As(err, &neoErr), "%s: %v", what, err)
	require.Equal(t, code, neoErr.Code, what)
}

// TestBoltDatabaseManagerIdleTransactionEnds (NOR-284 follow-up of #1756): on
// the server's database-manager route, an explicit transaction left idle is
// ended at the server's transaction timeout, or at once by TERMINATE
// TRANSACTIONS, without any message from its client: its key and its write
// admission are free, and its COMMIT is refused with the termination's
// status.
func TestBoltDatabaseManagerIdleTransactionEnds(t *testing.T) {
	t.Run("transaction timeout", func(t *testing.T) {
		const timeout = 500 * time.Millisecond
		driver := startIdleEndBolt(t, timeout)
		holder := idleEndHolder(t, driver, "timeout")
		time.Sleep(timeout + 300*time.Millisecond)
		_, err := idleEndRun(t, driver, "CREATE CONSTRAINT unrelated FOR (n:Unrelated) REQUIRE n.k IS UNIQUE", nil, 750*time.Millisecond)
		require.NoError(t, err, "the idle transaction kept its write admission past its timeout")
		_, err = idleEndRun(t, driver, "MERGE (n:Doc {id: 'timeout'})", nil, 750*time.Millisecond)
		require.NoError(t, err, "the idle transaction kept its key past its timeout")
		requireIdleEndCode(t, holder.Commit(context.Background()), "Neo.ClientError.Transaction.TransactionTimedOut", "COMMIT of the timed-out transaction")
	})
	t.Run("terminate", func(t *testing.T) {
		driver := startIdleEndBolt(t, time.Minute)
		holder := idleEndHolder(t, driver, "terminate")
		rows, err := idleEndRun(t, driver, "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId", nil, 5*time.Second)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		_, err = idleEndRun(t, driver, fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", rows[0][0]), nil, 5*time.Second)
		require.NoError(t, err)
		_, err = idleEndRun(t, driver, "CREATE CONSTRAINT unrelated FOR (n:Unrelated) REQUIRE n.k IS UNIQUE", nil, 750*time.Millisecond)
		require.NoError(t, err, "the terminated idle transaction kept its write admission")
		_, err = idleEndRun(t, driver, "MERGE (n:Doc {id: 'terminate'})", nil, 750*time.Millisecond)
		require.NoError(t, err, "the terminated idle transaction kept its key")
		requireIdleEndCode(t, holder.Commit(context.Background()), "Neo.ClientError.Transaction.Terminated", "COMMIT of the terminated transaction")
	})
}

// The Go driver sends no tx_timeout for a zero or negative timeout (its
// WithTxTimeout(0) is the server's default on the wire), so these tests send
// BEGIN {tx_timeout: 0} as a raw Bolt client does.
func idleEndRawSend(t *testing.T, conn net.Conn, message []byte) {
	t.Helper()
	frame := make([]byte, len(message)+4)
	binary.BigEndian.PutUint16(frame, uint16(len(message)))
	copy(frame[2:], message)
	_, err := conn.Write(frame)
	require.NoError(t, err)
}

func idleEndRawRead(t *testing.T, conn net.Conn) []byte {
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

// TestBoltDatabaseManagerExplicitZeroClientTimeout (review round 1 of #1763):
// on the server's route too, a client tx_timeout of zero replaces the
// server's transaction timeout, so an idle transaction past it commits.
func TestBoltDatabaseManagerExplicitZeroClientTimeout(t *testing.T) {
	driver := startIdleEndBolt(t, 200*time.Millisecond)
	conn, err := net.Dial("tcp", driver.Target().Host)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	_, err = conn.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 4, 4, 0, 0, 4, 3, 0, 0, 4, 2, 0, 0, 4, 1})
	require.NoError(t, err)
	var version [4]byte
	_, err = io.ReadFull(conn, version[:])
	require.NoError(t, err)
	idleEndRawSend(t, conn, []byte{0xb1, 0x01, 0xa0}) // HELLO {}
	require.Equal(t, byte(0x70), idleEndRawRead(t, conn)[1])
	idleEndRawSend(t, conn, append([]byte{0xb1, 0x11, 0xa1, 0x8a}, append([]byte("tx_timeout"), 0x00)...)) // BEGIN {tx_timeout: 0}
	require.Equal(t, byte(0x70), idleEndRawRead(t, conn)[1], "BEGIN")
	query := "MERGE (:Doc {id: 'zero-client-timeout'})"
	idleEndRawSend(t, conn, append(append([]byte{0xb3, 0x10, 0xd0, byte(len(query))}, query...), 0xa0, 0xa0)) // RUN
	require.Equal(t, byte(0x70), idleEndRawRead(t, conn)[1], "RUN")
	idleEndRawSend(t, conn, []byte{0xb1, 0x3f, 0xa1, 0x81, 'n', 0xff}) // PULL {n: -1}
	require.Equal(t, byte(0x70), idleEndRawRead(t, conn)[1], "PULL")
	time.Sleep(600 * time.Millisecond)
	idleEndRawSend(t, conn, []byte{0xb0, 0x12}) // COMMIT
	commit := idleEndRawRead(t, conn)
	require.Equal(t, byte(0x70), commit[1], "an explicit zero client timeout must override the server timeout: %q", commit)
	rows, err := idleEndRun(t, driver, "MATCH (n:Doc {id: 'zero-client-timeout'}) RETURN count(n)", nil, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, rows)
}
