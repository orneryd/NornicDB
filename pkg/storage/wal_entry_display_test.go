package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestEntryPayloadJSONRendersTypedRecords pins the display owner: a typed WAL
// record renders as its documented JSON payload, a control record keeps its
// historical JSON bytes exactly, and a record that cannot be rendered is an
// error rather than invented output.
func TestEntryPayloadJSONRendersTypedRecords(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	wal, err := NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })

	zone, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)
	when := time.Date(2026, 3, 4, 5, 6, 7, 8, zone)
	node := &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{
		"name": "Ada", "age": int64(36), "when": when, "tags": []any{"a", "b"},
	}}

	t.Run("a direct typed node record renders its documented node JSON", func(t *testing.T) {
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: node, TxID: "tx-1"}, "test"))
		entry := lastEntry(t, wal)
		require.NotContains(t, string(entry.Data), `"node"`, "the stored record must be typed, not JSON")

		payload, err := EntryPayloadJSON(entry)
		require.NoError(t, err)
		require.NotContains(t, payload, "\\x00", "the payload must never expose raw record bytes")

		var decoded WALNodeData
		require.NoError(t, json.Unmarshal([]byte(payload), &decoded))
		require.Equal(t, NodeID("n1"), decoded.Node.ID)
		require.Equal(t, []string{"Person"}, decoded.Node.Labels)
		require.Equal(t, "Ada", decoded.Node.Properties["name"])
		require.EqualValues(t, 36, decoded.Node.Properties["age"])
		require.Equal(t, []any{"a", "b"}, decoded.Node.Properties["tags"])
		require.Equal(t, when.Format(time.RFC3339Nano), decoded.Node.Properties["when"],
			"a temporal property preserves its instant and offset in the JSON payload")
		require.Equal(t, "tx-1", decoded.TxID, "transaction identity survives display")
	})

	t.Run("a batched typed node record renders the same documented JSON", func(t *testing.T) {
		batch := wal.NewBatchWithTxID("tx-batch")
		require.NoError(t, batch.AppendNode(OpCreateNode, node))
		require.NoError(t, batch.Commit())
		entry := lastEntry(t, wal)
		require.Equal(t, OpCreateNode, entry.Operation)

		payload, err := EntryPayloadJSON(entry)
		require.NoError(t, err)
		var decoded WALNodeData
		require.NoError(t, json.Unmarshal([]byte(payload), &decoded), payload)
		require.Equal(t, "tx-batch", decoded.TxID, "the batch's stamped transaction identity survives display")
		require.Equal(t, "Ada", decoded.Node.Properties["name"])
	})

	t.Run("a transaction marker keeps its historical JSON payload", func(t *testing.T) {
		_, err := wal.AppendWithDatabaseReturningSeq(OpTxBegin, WALTxData{TxID: "tx-2", Metadata: map[string]string{"app": "unit"}}, "test")
		require.NoError(t, err)
		entry := lastEntry(t, wal)
		payload, err := EntryPayloadJSON(entry)
		require.NoError(t, err)
		require.Equal(t, string(entry.Data), payload, "control records are already JSON and must not change")
		require.JSONEq(t, `{"tx_id":"tx-2","metadata":{"app":"unit"}}`, payload)
	})

	t.Run("a semantic record renders its opaque payload as base64", func(t *testing.T) {
		require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: []byte{0, 0xff, 0x80}}, "test"))
		payload, err := EntryPayloadJSON(lastEntry(t, wal))
		require.NoError(t, err)
		require.JSONEq(t, `{"payload":"AP+A"}`, payload)
	})
}

// TestEntryPayloadJSONRendersCompressedBufferedCommit checks that a supported
// compressed buffered-commit record is decompressed before display, and that
// binary physical keys are neither aliased nor printed as text.
func TestEntryPayloadJSONRendersCompressedBufferedCommit(t *testing.T) {
	engine, wal, commit := bindingCostFixture(t, 1)

	// A large, highly compressible commit so the writer's compression applies.
	// Every key is a real adjacency index key with the family's 17-byte
	// [prefix][nodeNumID 8B][edgeNumID 8B] layout, so the record carries the
	// numeric bindings the writer always records for physical keys, and its
	// bytes are not text.
	// The dictionary must hold both sides of every numeric pair the record
	// carries, so the fixture creates that many nodes before staging the keys.
	const pairs = 150
	nodes := make([]*Node, pairs)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("display:node%d", i)), Properties: map[string]any{"i": int64(i)}}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	// Adjacency keys and compact relationship bodies both refer to numeric
	// dictionary IDs. Allocate the source edge IDs before building the fixture.
	edgeNumIDs := make(map[EdgeID]uint64, pairs)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		for i := 0; i < pairs; i++ {
			edge := EdgeID(fmt.Sprintf("display:edge%d", i))
			num, err := resolveOrAllocateNumericIDInTxn(engine.idDict, txn, EdgeID(edge))
			if err != nil {
				return err
			}
			edgeNumIDs[edge] = num
		}
		return nil
	}))
	writes := map[string][]byte{}
	for i := 0; i < pairs; i++ {
		nodeNum, ok := engine.idDict.lookupNodeNumID(nodes[i].ID)
		require.True(t, ok, "the fixture node %q has a numeric ID", nodes[i].ID)
		edge := EdgeID(fmt.Sprintf("display:edge%d", i))
		edgeNum := edgeNumIDs[edge]
		for _, prefix := range []byte{prefixOutgoingIndex, prefixIncomingIndex} {
			key := make([]byte, 17)
			key[0] = prefix
			binary.BigEndian.PutUint64(key[1:9], nodeNum)
			binary.BigEndian.PutUint64(key[9:17], edgeNum)
			writes[string(key)] = encodeEdgeCompactHeader(edgeFormatCompactV2, &Edge{Type: "DISPLAY"}, nodeNum, nodeNum, 0)
		}
	}
	commit.walIDs = engine.idDict
	// Use a distinct, well-formed binary key whose two node IDs are known.
	firstNode, ok := engine.idDict.lookupNodeNumID(nodes[0].ID)
	require.True(t, ok)
	secondNode, ok := engine.idDict.lookupNodeNumID(nodes[1].ID)
	require.True(t, ok)
	deleteKey := make([]byte, 17)
	deleteKey[0] = prefixEdgeBetweenHead
	binary.BigEndian.PutUint64(deleteKey[1:9], firstNode)
	binary.BigEndian.PutUint64(deleteKey[9:17], secondNode)
	commit.logicalTxID, commit.txID, commit.namespace = "logical-1", "tx-buffered", "test"
	commit.ops = []Operation{{Type: OpCreateNode, Node: &Node{ID: "n1", Properties: map[string]any{"name": "Ada"}}}}
	commit.writes, commit.deletes = writes, map[string]bool{string(deleteKey): true}
	commit.version, commit.counterNodeMax, commit.hasCounters = MVCCVersion{CommitSequence: 7}, 1, true
	require.NoError(t, wal.AppendBufferedCommit(commit, engine.propKeyDict))
	entry := lastEntry(t, wal)

	require.Equal(t, byte(0x28), entry.Data[0], "the fixture must store a compressed record")
	display, err := EntryPayloadJSON(entry)
	require.NoError(t, err)

	decoded := decodeBufferedDisplayForTest(t, display)
	require.Equal(t, "logical-1", decoded.LogicalTxID)
	require.Equal(t, "tx-buffered", decoded.TxID)
	require.Equal(t, "test", decoded.Namespace)
	require.EqualValues(t, 7, decoded.Version.CommitSequence)
	require.Len(t, decoded.Ops, 1)
	require.Equal(t, OpCreateNode, decoded.Ops[0].Type)
	require.Equal(t, "Ada", decoded.Ops[0].Node.Properties["name"])
	require.Len(t, decoded.Writes, len(writes), "every physical write must be rendered")
	rendered := make(map[string][]byte, len(decoded.Writes))
	for i, write := range decoded.Writes {
		expected, ok := writes[string(write.Key)]
		require.True(t, ok, "write %q must be a physical key the record carried", write.Key)
		require.Equal(t, expected, write.Value)
		rendered[string(write.Key)] = write.Value
		if i > 0 {
			require.Less(t, bytes.Compare(decoded.Writes[i-1].Key, write.Key), 0, "writes are rendered in a stable order")
		}
	}
	require.Equal(t, writes, rendered, "no physical key is dropped or aliased")
	require.Equal(t, [][]byte{deleteKey}, decoded.Deletes)
}

func TestGetEntryTxIDForBufferedAndSemanticCommits(t *testing.T) {
	engine, wal, commit := bindingCostFixture(t, 1)
	commit.txID = "buffered-tx"
	commit.logicalTxID = "logical-tx"
	require.NoError(t, wal.AppendBufferedCommit(commit, engine.propKeyDict))
	buffered := lastEntry(t, wal)
	require.Equal(t, "buffered-tx", GetEntryTxID(buffered))

	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "perf", LogicalID: "semantic-tx", Operations: []Operation{{
		Type: OpCreateNode, Node: &Node{ID: "perf:n"},
	}}, Mode: CommitModeImplicit})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "perf"))
	require.Equal(t, "semantic-tx", GetEntryTxID(lastEntry(t, wal)))
}

// Commit records are self-contained accepted writes, even if their IDs match
// an unfinished explicit transaction marker. Exposing IDs for inspection
// must not make transaction recovery group or roll them back as marker entries.
func TestAcceptedCommitIDsDoNotChangeTransactionRecoveryGrouping(t *testing.T) {
	engine, wal, commit := bindingCostFixture(t, 1)
	_, err := wal.AppendWithDatabaseReturningSeq(OpTxBegin, WALTxData{TxID: "buffered-tx", Metadata: nil}, "perf")
	require.NoError(t, err)
	commit.txID = "buffered-tx"
	require.NoError(t, wal.AppendBufferedCommit(commit, engine.propKeyDict))
	_, err = wal.AppendWithDatabaseReturningSeq(OpTxBegin, WALTxData{TxID: "semantic-tx", Metadata: nil}, "perf")
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "perf", LogicalID: "semantic-tx", Operations: []Operation{{
		Type: OpCreateNode, Node: &Node{ID: "perf:n"},
	}}, Mode: CommitModeImplicit})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "perf"))

	// RecoverWithTransactions returns no store: the recovered engine is closed
	// before it reports, so the result is the only fixture that needs an owner.
	_, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(wal.Config().Dir, ""))
	require.NoError(t, err)
	require.Equal(t, 2, result.RolledBackTransactions)
	require.Empty(t, result.Transactions["buffered-tx"].Entries)
	require.Empty(t, result.Transactions["semantic-tx"].Entries)
	require.Equal(t, 2, result.NonTxApplied)
}

// decodeBufferedDisplayForTest decodes a rendered buffered-commit payload, so
// the display owner's JSON shape is asserted rather than the record's.
func decodeBufferedDisplayForTest(t *testing.T, payload string) bufferedWALDisplayPayload {
	t.Helper()
	require.True(t, json.Valid([]byte(payload)))
	var decoded bufferedWALDisplayPayload
	require.NoError(t, json.Unmarshal([]byte(payload), &decoded))
	return decoded
}

func lastEntry(t *testing.T, wal *WAL) WALEntry {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	return entries[len(entries)-1]
}
