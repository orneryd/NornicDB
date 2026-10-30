package bolt

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const neo4jTransactionOracleTimeout = 500 * time.Millisecond

func TestNeo4j526TransactionLifecycleOracle(t *testing.T) {
	address := os.Getenv("NORNICDB_NEO4J_ORACLE_ADDR")
	if address == "" {
		t.Skip("set NORNICDB_NEO4J_ORACLE_ADDR to a Neo4j 5.26 Bolt endpoint")
	}

	t.Run("strict timeout long", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": "500"}))
		code, message, err := AssertFailure(t, conn)
		require.NoError(t, err)
		require.Equal(t, "Neo.ClientError.Request.Invalid", code)
		require.Contains(t, message, "tx_timeout")
		require.Contains(t, message, "Expected long")
	})

	maxGoDurationMilliseconds := int64((1<<63 - 1) / int64(time.Millisecond))
	for _, edge := range []struct {
		name  string
		value any
	}{
		{name: "null", value: nil},
		{name: "negative", value: int64(-1)},
		{name: "maximum Go duration milliseconds", value: maxGoDurationMilliseconds},
		{name: "first saturated millisecond", value: maxGoDurationMilliseconds + 1},
		{name: "maximum long", value: int64(1<<63 - 1)},
	} {
		t.Run("timeout metadata "+edge.name, func(t *testing.T) {
			conn := openNeo4jTransactionOracleConn(t, address)
			require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": edge.value}))
			messageType, data, err := ReadMessage(conn)
			require.NoError(t, err)
			metadata, _, decodeErr := decodePackStreamMap(data, 0)
			require.NoError(t, decodeErr)
			t.Logf("Neo4j 5.26 tx_timeout=%v response=0x%02x metadata=%v",
				edge.value, messageType, metadata)
			require.Equal(t, byte(MsgSuccess), messageType)
			time.Sleep(200 * time.Millisecond)
			require.NoError(t, SendCommit(t, conn))
			commitType, commitData, err := ReadMessage(conn)
			require.NoError(t, err)
			commitMetadata, _, decodeErr := decodePackStreamMap(commitData, 0)
			require.NoError(t, decodeErr)
			t.Logf("Neo4j 5.26 tx_timeout=%v COMMIT response=0x%02x metadata=%v",
				edge.value, commitType, commitMetadata)
			require.Equal(t, byte(MsgSuccess), commitType)
		})
	}

	t.Run("idle timeout prevents commit", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		beginExplicitTransaction(t, conn, map[string]any{
			"tx_timeout": int64(neo4jTransactionOracleTimeout / time.Millisecond),
		})
		time.Sleep(5 * time.Second)
		require.NoError(t, SendCommit(t, conn))
		code, _, err := AssertFailure(t, conn)
		require.NoError(t, err)
		require.Equal(t, transactionTimedOutCode, code)
	})

	t.Run("run after timeout fails until reset", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		beginExplicitTransaction(t, conn, map[string]any{
			"tx_timeout": int64(neo4jTransactionOracleTimeout / time.Millisecond),
		})
		time.Sleep(5 * time.Second)
		require.NoError(t, sendNeo4jTransactionOracleRun(conn, "RETURN 1"))
		code, _, err := AssertFailure(t, conn)
		require.NoError(t, err)
		require.Equal(t, transactionTimedOutCode, code)
		require.NoError(t, sendNeo4jTransactionOracleRun(conn, "RETURN 2"))
		_, err = AssertMessageType(t, conn, MsgIgnored)
		require.NoError(t, err)
		require.NoError(t, SendReset(t, conn))
		require.NoError(t, ReadSuccess(t, conn))
		require.Equal(t, [][]any{{int64(1)}}, runNeo4jTransactionOracleQuery(t, conn, "RETURN 1"))
	})

	for _, control := range []struct {
		name     string
		metadata map[string]any
	}{
		{name: "absent"},
		{name: "zero", metadata: map[string]any{"tx_timeout": int64(0)}},
	} {
		t.Run(control.name+" timeout commits", func(t *testing.T) {
			conn := openNeo4jTransactionOracleConn(t, address)
			beginExplicitTransaction(t, conn, control.metadata)
			time.Sleep(time.Second)
			require.NoError(t, SendCommit(t, conn))
			require.NoError(t, ReadSuccess(t, conn))
		})
	}

	t.Run("reset rolls back and reuses session", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		deleteNeo4jTransactionOracleNodes(t, conn)
		beginExplicitTransaction(t, conn, nil)
		runNeo4jTransactionOracleStatement(t, conn, "CREATE (:TxLifecycleOracle {terminal: 'reset'})")
		require.NoError(t, SendReset(t, conn))
		require.NoError(t, ReadSuccess(t, conn))
		require.Equal(t, [][]any{{int64(0)}}, runNeo4jTransactionOracleQuery(t, conn,
			"MATCH (n:TxLifecycleOracle {terminal: 'reset'}) RETURN count(n)"))
	})

	t.Run("goodbye rolls back", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		deleteNeo4jTransactionOracleNodes(t, conn)
		beginExplicitTransaction(t, conn, nil)
		runNeo4jTransactionOracleStatement(t, conn, "CREATE (:TxLifecycleOracle {terminal: 'goodbye'})")
		require.NoError(t, SendGoodbye(t, conn))
		require.NoError(t, conn.Close())
		assertNeo4jTransactionOracleNodeAbsent(t, address, "goodbye")
	})

	t.Run("eof rolls back", func(t *testing.T) {
		conn := openNeo4jTransactionOracleConn(t, address)
		deleteNeo4jTransactionOracleNodes(t, conn)
		beginExplicitTransaction(t, conn, nil)
		runNeo4jTransactionOracleStatement(t, conn, "CREATE (:TxLifecycleOracle {terminal: 'eof'})")
		require.NoError(t, conn.Close())
		assertNeo4jTransactionOracleNodeAbsent(t, address, "eof")
	})
}

func openNeo4jTransactionOracleConn(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, PerformHandshake(conn))
	require.NoError(t, SendHello(t, conn, map[string]string{"user_agent": "nornicdb-transaction-oracle/1"}))
	metadata, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo4j/5.26.28", metadata["server"],
		"transaction oracle must be the pinned Neo4j 5.26.28 endpoint")
	return conn
}

func deleteNeo4jTransactionOracleNodes(t *testing.T, conn net.Conn) {
	t.Helper()
	runNeo4jTransactionOracleQuery(t, conn, "MATCH (n:TxLifecycleOracle) DETACH DELETE n")
}

func assertNeo4jTransactionOracleNodeAbsent(t *testing.T, address, terminal string) {
	t.Helper()
	conn := openNeo4jTransactionOracleConn(t, address)
	query := fmt.Sprintf("MATCH (n:TxLifecycleOracle {terminal: '%s'}) RETURN count(n)", terminal)
	require.Equal(t, [][]any{{int64(0)}}, runNeo4jTransactionOracleQuery(t, conn, query))
}

func runNeo4jTransactionOracleStatement(t *testing.T, conn net.Conn, query string) {
	t.Helper()
	runNeo4jTransactionOracleQuery(t, conn, query)
}

func sendNeo4jTransactionOracleRun(conn net.Conn, query string) error {
	message := []byte{0xB3, MsgRun}
	message = append(message, encodePackStreamString(query)...)
	message = append(message, 0xA0, 0xA0)
	return SendMessage(conn, message)
}

func runNeo4jTransactionOracleQuery(t *testing.T, conn net.Conn, query string) [][]any {
	t.Helper()
	// RUN has three fields in Bolt 4.4: query, params, and extra metadata.
	message := []byte{0xB3, MsgRun}
	message = append(message, encodePackStreamString(query)...)
	message = append(message, 0xA0, 0xA0)
	require.NoError(t, SendMessage(conn, message))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))

	var records [][]any
	for {
		messageType, data, err := ReadMessage(conn)
		require.NoError(t, err)
		switch messageType {
		case MsgRecord:
			fields, _, err := decodePackStreamList(data, 0)
			require.NoError(t, err)
			records = append(records, fields)
		case MsgSuccess:
			return records
		default:
			if messageType == MsgFailure {
				metadata, _, decodeErr := decodePackStreamMap(data, 0)
				require.NoError(t, decodeErr)
				t.Fatalf("Neo4j failure for %q: %v", query, metadata)
			}
			t.Fatalf("unexpected Neo4j response 0x%02x for %q", messageType, query)
		}
	}
}
