package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExplicitTransactionStreamsResultsByQID(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	conn := openBoltTestConn(t, port)

	beginExplicitTransaction(t, conn, nil)
	firstQID := runStatementAndQID(t, conn, "UNWIND range(10, 12) AS x RETURN x")
	secondQID := runStatementAndQID(t, conn, "UNWIND range(20, 22) AS x RETURN x")
	require.Equal(t, int64(0), firstQID)
	require.Equal(t, int64(1), secondQID)

	records, metadata := pullStatement(t, conn, map[string]any{"n": int64(2), "qid": firstQID})
	require.Equal(t, [][]any{{int64(10)}, {int64(11)}}, records)
	require.Equal(t, true, metadata["has_more"])

	records, metadata = pullStatement(t, conn, map[string]any{"n": int64(1)})
	require.Equal(t, [][]any{{int64(20)}}, records, "omitted qid must select the latest statement")
	require.Equal(t, true, metadata["has_more"])

	records, metadata = pullStatement(t, conn, map[string]any{"n": int64(-1), "qid": firstQID})
	require.Equal(t, [][]any{{int64(12)}}, records)
	require.NotContains(t, metadata, "has_more")

	require.NoError(t, SendMessage(conn, buildDiscardMessage(map[string]any{"n": int64(1), "qid": secondQID})))
	metadata, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	require.Equal(t, true, metadata["has_more"])
	records, metadata = pullStatement(t, conn, map[string]any{"n": int64(-1), "qid": secondQID})
	require.Equal(t, [][]any{{int64(22)}}, records)
	require.NotContains(t, metadata, "has_more")

	thirdQID := runStatementAndQID(t, conn, "RETURN 30 AS x")
	fourthQID := runStatementAndQID(t, conn, "RETURN 40 AS x")
	require.NoError(t, SendMessage(conn, buildDiscardMessage(map[string]any{"n": int64(-1), "qid": thirdQID})))
	_, err = AssertSuccess(t, conn)
	require.NoError(t, err)

	records, _ = pullStatement(t, conn, map[string]any{"n": int64(-1), "qid": fourthQID})
	require.Equal(t, [][]any{{int64(40)}}, records, "discarding one qid must not discard another stream")

	require.NoError(t, SendCommit(t, conn))
	require.NoError(t, ReadSuccess(t, conn))

	beginExplicitTransaction(t, conn, nil)
	require.Equal(t, int64(0), runStatementAndQID(t, conn, "RETURN 50 AS x"),
		"statement IDs must be scoped to an explicit transaction")
	records, _ = pullStatement(t, conn, map[string]any{"n": int64(-1)})
	require.Equal(t, [][]any{{int64(50)}}, records)
	require.NoError(t, SendRollback(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
}

func TestExplicitTransactionRejectsUnknownQIDUntilReset(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	conn := openBoltTestConn(t, port)

	beginExplicitTransaction(t, conn, nil)
	qid := runStatementAndQID(t, conn, "RETURN 1 AS x")
	require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1), "qid": int64(99)}))
	code, message, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Request.InvalidFormat", code)
	require.Contains(t, message, "No such statement: 99")

	require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1), "qid": qid}))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
}

func TestStreamingOptionsRejectInvalidLimits(t *testing.T) {
	for _, limit := range []int64{-2, 0, maxStreamingLimit + 1} {
		_, err := parseStreamingOptions(encodePackStreamMap(map[string]any{"n": limit}))
		require.Error(t, err)
	}
}

func TestAddDurableQIDMetadataPreservesNumericQID(t *testing.T) {
	metadata := map[string]any{"qid": int64(7)}
	addDurableQIDMetadata(metadata, &QueryResult{Metadata: map[string]any{"durable_qid": "signed-token"}})
	require.Equal(t, int64(7), metadata["qid"])
	require.Equal(t, "signed-token", metadata["durable_qid"])

	metadata = map[string]any{"qid": int64(8)}
	addDurableQIDMetadata(metadata, &QueryResult{Metadata: map[string]any{"durable_qid": 42}})
	require.NotContains(t, metadata, "durable_qid")
}

func TestBoltRunEmitsDurableQIDForContinuation(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	for _, id := range []string{"bolt-a", "bolt-b"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	require.NoError(t, SendRun(t, conn, "CALL db.retrieve({mode: 'id', n: 1})", nil, nil))
	metadata, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	durableQID, ok := metadata["durable_qid"].(string)
	require.True(t, ok, "RUN metadata must contain a durable qid: %v", metadata)
	require.NotEmpty(t, durableQID)
	require.NotContains(t, metadata, "qid", "autocommit RUN must retain standard Bolt numeric-qid behavior")
}

func TestParseStreamingOptions(t *testing.T) {
	t.Run("typed fields and unknown metadata", func(t *testing.T) {
		data := encodePackStreamMap(map[string]any{
			"n":                        int64(100),
			"qid":                      int64(7),
			"ignored-streaming-option": "value",
		})
		options, err := parseStreamingOptions(data)
		require.NoError(t, err)
		require.Equal(t, streamingOptions{limit: 100, statementID: 7}, options)
	})

	t.Run("nullable qid selects latest", func(t *testing.T) {
		options, err := parseStreamingOptions(encodePackStreamMap(map[string]any{
			"n":   int64(1),
			"qid": nil,
		}))
		require.NoError(t, err)
		require.Equal(t, streamingOptions{limit: 1, statementID: -1}, options)
	})

	t.Run("map8 header", func(t *testing.T) {
		tinyMap := encodePackStreamMap(map[string]any{"n": int64(5), "qid": int64(2)})
		data := append([]byte{0xD8, 0x02}, tinyMap[1:]...)
		options, err := parseStreamingOptions(data)
		require.NoError(t, err)
		require.Equal(t, streamingOptions{limit: 5, statementID: 2}, options)
	})
}

func runStatementAndQID(t *testing.T, conn net.Conn, query string) int64 {
	t.Helper()
	require.NoError(t, SendRun(t, conn, query, nil, nil))
	metadata, err := AssertSuccess(t, conn)
	require.NoError(t, err)
	qid, ok := metadata["qid"].(int64)
	require.True(t, ok, "RUN metadata must contain an integer qid: %v", metadata)
	return qid
}

func pullStatement(t *testing.T, conn net.Conn, options map[string]any) ([][]any, map[string]any) {
	t.Helper()
	require.NoError(t, SendPull(t, conn, options))
	var records [][]any
	for {
		messageType, data, err := ReadMessage(conn)
		require.NoError(t, err)
		switch messageType {
		case MsgRecord:
			record, _, err := decodePackStreamList(data, 0)
			require.NoError(t, err)
			records = append(records, record)
		case MsgSuccess:
			metadata, _, err := decodePackStreamMap(data, 0)
			require.NoError(t, err)
			return records, metadata
		default:
			t.Fatalf("unexpected Bolt message 0x%02x", messageType)
		}
	}
}

func buildDiscardMessage(options map[string]any) []byte {
	message := []byte{0xB1, MsgDiscard}
	return append(message, encodePackStreamMap(options)...)
}
