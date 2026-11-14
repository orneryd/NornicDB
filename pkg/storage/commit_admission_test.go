package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412: commit admission on an engine whose journal writes large records
// (WALRecordsLarge, which nornicdb.Open selects for a standalone server). Each
// case of admissionCases is committed through the WAL engine with synchronous
// and with asynchronous writes, as an explicit and as an implicit transaction.
// It ends one of two ways: the commit returns no error and its nodes are in the
// store, or it returns ErrStoreValueTooLarge with the store's limit in the
// message and the journal holds no record of it. The store is then closed with
// crashClose and opened again with the recovery a server runs at open, and
// holds the same nodes. The test drains the write buffer before that close, so
// it does not show what an undrained buffer would lose. The cases are sized
// below and above two limits: the write buffer's forecast limit (64 MiB) and a
// low-memory store's value limit (32 MiB). They include one node of 20 MiB and
// one of 30 MiB on a default store, which commit.

type admissionCase struct {
	name string
	// lowMemory opens the store with the low-memory profile, whose value
	// limit is 32 MiB.
	lowMemory bool
	// stage writes the transaction's changes.
	stage func(ctx context.Context, tx *BadgerTransaction) error
	// refused, when set, is the error the commit must be refused with.
	refused error
	// nodes is how many nodes labelled Admission an applied commit leaves.
	nodes int
}

func admissionNodes(n int, properties func(i int) map[string]any, labels ...string) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		for i := range n {
			if _, err := tx.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:admission-%d", i)), Labels: append([]string{"Admission"}, labels...), Properties: properties(i)}); err != nil {
				return err
			}
		}
		return nil
	}
}

func admissionText(bytes int) func(int) map[string]any {
	text := strings.Repeat("a", bytes)
	return func(int) map[string]any { return map[string]any{"text": text} }
}

// admissionShrunk creates a node with a large value and replaces the value with
// a small one in the same transaction.
func admissionShrunk(bytes int) func(context.Context, *BadgerTransaction) error {
	return func(ctx context.Context, tx *BadgerTransaction) error {
		if err := admissionNodes(1, admissionText(bytes))(ctx, tx); err != nil {
			return err
		}
		node, err := tx.GetNode(ctx, "test:admission-0")
		if err != nil {
			return err
		}
		node = CopyNode(node)
		node.Properties["text"] = "small"
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node})
	}
}

func admissionCases() []admissionCase {
	floats := make([]float64, 500_000)
	for i := range floats {
		floats[i] = float64(i) / 3
	}
	return []admissionCase{
		// The write buffer's forecast limit is 64 MiB: 300 x 80 KB of text is below it,
		// 1,000 x 80 KB above it. Both commit; the test does not check which route each
		// takes.
		{name: "buffer forecast below its limit", nodes: 300, stage: admissionNodes(300, admissionText(80<<10))},
		{name: "buffer forecast above its limit", nodes: 1000, stage: admissionNodes(1000, admissionText(80<<10))},
		// Eight nodes with a list of 500,000 float64 values each. The commit applies;
		// the test does not check what the write buffer or the journal did with it.
		{name: "eight nodes of 500,000 float64 values", nodes: 8, stage: admissionNodes(8, func(int) map[string]any { return map[string]any{"values": floats} })},
		// One node larger than a journal frame (16 MiB), on a default store.
		{name: "one node of 20 MiB", nodes: 1, stage: admissionNodes(1, admissionText(20<<20))},
		{name: "one node of 30 MiB", nodes: 1, stage: admissionNodes(1, admissionText(30<<20))},
		// A value that is large only inside the transaction.
		{name: "large value shrunk before the commit", nodes: 1, stage: admissionShrunk(20 << 20)},
		// The store's value limit: 32 MiB on a low-memory store.
		{name: "low-memory store, one node of 20 MiB", lowMemory: true, nodes: 1, stage: admissionNodes(1, admissionText(20<<20))},
		{name: "low-memory store, one node of 40 MiB", lowMemory: true, refused: ErrStoreValueTooLarge, stage: admissionNodes(1, admissionText(40<<20))},
		{name: "low-memory store, large value shrunk before the commit", lowMemory: true, nodes: 1, stage: admissionShrunk(40 << 20)},
	}
}

func admissionEngine(t *testing.T, dir string, async, lowMemory bool) *BadgerEngine {
	t.Helper()
	return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !async, manualDrain: true, lowMemory: lowMemory, records: WALRecordsLarge})
}

// admissionRecords counts the journal's semantic records of logicalID.
func admissionRecords(t *testing.T, dir, logicalID string) int {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
	require.NoError(t, err)
	records := 0
	for _, entry := range entries {
		if entry.Operation != OpEntityCommit {
			continue
		}
		var data walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		envelope, _, err := DecodeEntityTransaction(data.Payload)
		require.NoError(t, err)
		if envelope.LogicalTxID == logicalID {
			records++
		}
	}
	return records
}

// admissionCommit stages stage in a transaction of mode, submits it through the WAL engine's SubmitEntityCommit
// and waits for its outcome. It returns nil only when the outcome is applied without an error.
func admissionCommit(t *testing.T, b *BadgerEngine, mode EntityCommitMode, logicalID string, stage func(context.Context, *BadgerTransaction) error) error {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, mode)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	if err := stage(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	if err != nil {
		return err
	}
	receipt, err := NewWALEngine(b, b.bufferedCommitWAL).SubmitEntityCommit(ctx, prepared, nil)
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	outcome := receipt.WaitOutcome(waitCtx)
	if outcome.State != CommitApplied {
		return fmt.Errorf("commit ended %v: %w", outcome.State, outcome.Err)
	}
	return outcome.Err
}

// admissionStored counts the Admission nodes the store holds once the write
// buffer is drained.
func admissionStored(t *testing.T, b *BadgerEngine) int {
	t.Helper()
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	nodes, err := b.GetNodesByLabel(context.Background(), "Admission")
	require.NoError(t, err)
	return len(nodes)
}

// admissionReopen closes b with crashClose (the write buffer is stopped and
// dropped without a drain; the store and the journal are closed) and opens the
// store again with RecoverBufferedCommitsAtOpen, the recovery nornicdb.Open
// runs. A caller that has called admissionStored or admissionLastByte before it
// has already drained the write buffer.
func admissionReopen(t *testing.T, b *BadgerEngine, dir string, async, lowMemory bool) *BadgerEngine {
	t.Helper()
	crashClose(t, b, b.bufferedCommitWAL)
	b = admissionEngine(t, dir, async, lowMemory)
	_, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
	require.NoError(t, err)
	return b
}

// requireCommitCapacityRefusal checks the capacity cause, diagnostic limit and
// absence of the refused logical commit from the journal for both key and value
// admission. An optional name verifies the offending key remains identifiable.
func requireCommitCapacityRefusal(t *testing.T, err, cause error, limit int64, dir, logicalID, name string) {
	t.Helper()
	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), fmt.Sprint(limit), "the error names the store's limit")
	if name != "" {
		require.Contains(t, err.Error(), name, "the error names the key")
	}
	t.Logf("refused: %v", err)
	require.Zero(t, admissionRecords(t, dir, logicalID), "the journal holds no record of the refused commit")
}

var admissionModes = map[EntityCommitMode]string{CommitModeExplicit: "explicit", CommitModeImplicit: "implicit"}

func TestCommitAdmissionAppliesOrRefusesBeforeTheJournal(t *testing.T) {
	for _, c := range admissionCases() {
		for _, async := range []bool{false, true} {
			for mode, modeName := range admissionModes {
				t.Run(fmt.Sprintf("%s/async=%v/%s", c.name, async, modeName), func(t *testing.T) {
					dir := t.TempDir()
					b := admissionEngine(t, dir, async, c.lowMemory)
					require.Equal(t, async, b.writeBehindEnabled())
					err := admissionCommit(t, b, mode, "admission", c.stage)
					if c.refused != nil {
						require.ErrorIs(t, err, c.refused)
						requireCommitCapacityRefusal(t, err, ErrStoreValueTooLarge, storeValueLimit(b.db), dir, "admission", "")
						require.Zero(t, admissionStored(t, b))
					} else {
						require.NoError(t, err, "the commit is applied")
						require.Equal(t, c.nodes, admissionStored(t, b), "and visible")
					}
					// After crashClose and the recovery at open the store holds the same nodes.
					b = admissionReopen(t, b, dir, async, c.lowMemory)
					require.Equal(t, c.nodes, admissionStored(t, b), "after crashClose and the recovery at open")
				})
			}
		}
	}
}

// admissionBodyBytes stages stage in a transaction, which it rolls back, and returns the length
// of the body staged for the node test:admission-0.
func admissionBodyBytes(t *testing.T, b *BadgerEngine, stage func(context.Context, *BadgerTransaction) error) int {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, stage(ctx, tx))
	return len(tx.pendingWrites[string(nodeKey("test:admission-0"))])
}

// admissionTextForBody returns body minus the overhead of the node that node
// stages: the length of its staged body for a text of 4 MiB, minus 4 MiB.
// TestStoreValueLimitBoundary asserts the body size that results.
func admissionTextForBody(t *testing.T, b *BadgerEngine, body int, node func(text int) func(context.Context, *BadgerTransaction) error) int {
	t.Helper()
	const sample = 4 << 20
	return body - (admissionBodyBytes(t, b, node(sample)) - sample)
}

// admissionBoundaryNode stages the node test:admission-0 with fixed timestamps
// and one property, a text of text bytes.
func admissionBoundaryNode(text int) func(context.Context, *BadgerTransaction) error {
	return admissionSizedNode(text, 0)
}

// admissionLongNameNode is admissionBoundaryNode with 64 more properties whose
// names are 1,000 characters long and whose values are the integers 0 to 63.
// The history test asserts, for the text it gives it, that its staged body is 64 bytes
// under the store's limit and that the history record of the node as stored is over the limit.
func admissionLongNameNode(text int) func(context.Context, *BadgerTransaction) error {
	return admissionSizedNode(text, 64)
}

func admissionSizedNode(text, longNames int) func(context.Context, *BadgerTransaction) error {
	at := time.Date(2026, 11, 12, 10, 0, 0, 123456789, time.UTC)
	return func(ctx context.Context, tx *BadgerTransaction) error {
		properties := map[string]any{"text": strings.Repeat("a", text)}
		for i := range longNames {
			properties[fmt.Sprintf("%03d", i)+strings.Repeat("n", 997)] = int64(i)
		}
		_, err := tx.CreateNode(ctx, &Node{ID: "test:admission-0", Labels: []string{"Admission"}, CreatedAt: at, UpdatedAt: at, Properties: properties})
		return err
	}
}

// On a low-memory store, whose value limit is 32 MiB (asserted): a node whose
// stored body is one byte below the limit and one exactly at it commit, and one
// a byte above it is refused with no record in the journal. With synchronous
// and asynchronous writes, as an explicit and as an implicit transaction.
func TestStoreValueLimitBoundary(t *testing.T) {
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			for _, over := range []int{-1, 0, 1} {
				t.Run(fmt.Sprintf("async=%v/%s/limit%+d", async, modeName, over), func(t *testing.T) {
					dir := t.TempDir()
					b := admissionEngine(t, dir, async, true)
					limit := int(storeValueLimit(b.db))
					require.Equal(t, 32<<20, limit, "a low-memory store's value limit")
					text := admissionTextForBody(t, b, limit+over, admissionBoundaryNode)
					require.Equal(t, limit+over, admissionBodyBytes(t, b, admissionBoundaryNode(text)), "the stored body's size")
					err := admissionCommit(t, b, mode, "boundary", admissionBoundaryNode(text))
					want := 1
					if over > 0 {
						want = 0
						requireCommitCapacityRefusal(t, err, ErrStoreValueTooLarge, storeValueLimit(b.db), dir, "boundary", "")
					} else {
						require.NoError(t, err, "a body within the store's limit commits")
					}
					require.Equal(t, want, admissionStored(t, b))
					b = admissionReopen(t, b, dir, async, true)
					require.Equal(t, want, admissionStored(t, b), "after crashClose and the recovery at open")
				})
			}
		}
	}
}

// narrowList returns a function that gives a node one property, data: a list
// of elements int32 values. The node recovered from the journal holds the list
// as []int64 (checked in the last subtest below).
func narrowList(elements int) func(int) map[string]any {
	list := make([]int32, elements)
	for i := range list {
		list[i] = int32(1_000_000 + i)
	}
	return func(int) map[string]any { return map[string]any{"data": list} }
}

// A node with a list of 5,000,000 int32 values on a low-memory store is refused
// with ErrStoreValueTooLarge, with "as the journal's record of it is applied"
// in the message, and the journal holds no record of it: through the WAL engine
// in both write modes and both commit modes, and through the WAL engine's own
// CreateNode. A node with a list of 2,000,000 on a default store, whose record
// is appended to the journal by hand without being applied, is applied by the
// recovery at open after crashClose and read back as []int64, and the store's
// streaming snapshot is then written.
func TestStoreValueLimitRefusesABodyReplayWouldWriteOverTheLimit(t *testing.T) {
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			t.Run(fmt.Sprintf("refused/async=%v/%s", async, modeName), func(t *testing.T) {
				dir := t.TempDir()
				b := admissionEngine(t, dir, async, true)
				// 5,000,000 elements: the commit is refused, and the error says the size is
				// that of the node as the journal's record of it is applied.
				err := admissionCommit(t, b, mode, "narrow", admissionNodes(1, narrowList(5_000_000)))
				requireCommitCapacityRefusal(t, err, ErrStoreValueTooLarge, storeValueLimit(b.db), dir, "narrow", "")
				require.Contains(t, err.Error(), "as the journal's record of it is applied")
				require.Zero(t, admissionStored(t, b))
				b = admissionReopen(t, b, dir, async, true)
				require.Zero(t, admissionStored(t, b), "after crashClose and the recovery at open")
			})
		}
	}
	t.Run("refused/native route", func(t *testing.T) {
		dir := t.TempDir()
		b := admissionEngine(t, dir, false, true)
		scoped := NewNamespacedEngine(NewWALEngine(b, b.bufferedCommitWAL), "test")
		_, err := scoped.CreateNode(context.Background(), &Node{ID: "narrow", Labels: []string{"Admission"}, Properties: narrowList(5_000_000)(0)})
		require.ErrorIs(t, err, ErrStoreValueTooLarge)
		t.Logf("refused: %v", err)
		entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
		require.NoError(t, err)
		for _, entry := range entries {
			require.NotEqual(t, OpEntityCommit, entry.Operation, "nothing of the refused write is journaled")
		}
		require.Zero(t, admissionStored(t, b))
	})
	// The record is appended to the journal without being applied, the engine is
	// closed with crashClose, and the recovery at open applies it.
	t.Run("admitted and recovered", func(t *testing.T) {
		dir := t.TempDir()
		b := admissionEngine(t, dir, false, false)
		ctx := context.Background()
		tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, admissionNodes(1, narrowList(2_000_000))(ctx, tx))
		prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "narrow-crash"})
		require.NoError(t, err)
		payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
		require.NoError(t, err)
		prepared.Abort()
		require.Greater(t, len(payload), int(walMaxEntrySize), "the record is larger than one frame")
		largeRecordJournal(t, b.bufferedCommitWAL, payload)
		b = admissionReopen(t, b, dir, false, false)
		require.Equal(t, 1, admissionStored(t, b), "the recovery at open applies the journaled record")
		node, err := b.GetNode(ctx, "test:admission-0")
		require.NoError(t, err)
		list, ok := node.Properties["data"].([]int64)
		require.True(t, ok, "stored as %T", node.Properties["data"])
		require.Len(t, list, 2_000_000)
		require.Equal(t, int64(1_000_000+1_999_999), list[1_999_999])
		require.NoError(t, SaveStreamingSnapshot(ctx, b, filepath.Join(dir, "after.nds"), SnapshotOptions{}))
	})
}

// admissionRetext commits an update of test:admission-0 that changes the last
// character of its text.
func admissionRetext(t *testing.T, b *BadgerEngine, mode EntityCommitMode, logicalID string, last byte) error {
	t.Helper()
	return admissionCommit(t, b, mode, logicalID, func(ctx context.Context, tx *BadgerTransaction) error {
		node, err := tx.GetNode(ctx, "test:admission-0")
		if err != nil {
			return err
		}
		node = CopyNode(node)
		text := []byte(node.Properties["text"].(string))
		text[len(text)-1] = last
		node.Properties["text"] = string(text)
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node})
	})
}

func admissionLastByte(t *testing.T, b *BadgerEngine) byte {
	t.Helper()
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	node, err := b.GetNode(context.Background(), "test:admission-0")
	require.NoError(t, err)
	text := node.Properties["text"].(string)
	return text[len(text)-1]
}

// A node's history record (encodeMVCCNodeRecord) is a different encoding from
// its stored body. Two cases on a low-memory store, in both write modes and
// both commit modes. A text node whose body is 64 bytes below the limit has a
// history record within the limit (asserted); with another transaction open, so
// that the engine archives superseded versions (asserted), an update of it
// commits. A node with 64 property names of 1,000 characters whose body is 64
// bytes below the limit has a history record over the limit (asserted); with no
// other transaction open, an update of it is refused with
// ErrStoreValueTooLarge, with "history record" in the message and no record in
// the journal, and a delete of it commits.
func TestStoreValueLimitRefusesAHistoryRecordOverTheLimit(t *testing.T) {
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			// A text node 64 bytes below the limit. The margin is there because the body an
			// update stores was seen one byte larger than the body the creation stored.
			t.Run(fmt.Sprintf("record within the limit/async=%v/%s", async, modeName), func(t *testing.T) {
				dir := t.TempDir()
				b := admissionEngine(t, dir, async, true)
				ctx := context.Background()
				limit := int(storeValueLimit(b.db))
				text := admissionTextForBody(t, b, limit-64, admissionBoundaryNode)
				require.NoError(t, admissionCommit(t, b, mode, "history-seed", admissionBoundaryNode(text)), "a body 64 bytes below the limit commits")
				require.NoError(t, b.FlushWriteBehind(ctx))
				stored, err := b.GetNode(ctx, "test:admission-0")
				require.NoError(t, err)
				record, err := encodeMVCCNodeRecord(stored, false)
				require.NoError(t, err)
				require.LessOrEqual(t, len(record), limit, "the node's history record fits the store")
				reader, err := b.BeginTransaction(ctx)
				require.NoError(t, err)
				require.NoError(t, reader.SetNamespace("test"))
				require.True(t, b.mustArchiveForHistory(), "the engine archives superseded versions while the other transaction is open")
				err = admissionRetext(t, b, mode, "history-kept", 'b')
				require.NoError(t, reader.Rollback())
				require.NoError(t, err, "the update commits")
				require.Equal(t, byte('b'), admissionLastByte(t, b))
				b = admissionReopen(t, b, dir, async, true)
				require.Equal(t, byte('b'), admissionLastByte(t, b), "after crashClose and the recovery at open")
			})
			// A node with long property names 64 bytes below the limit: an update of it is
			// refused.
			t.Run(fmt.Sprintf("record over the limit/async=%v/%s", async, modeName), func(t *testing.T) {
				dir := t.TempDir()
				b := admissionEngine(t, dir, async, true)
				ctx := context.Background()
				limit := int(storeValueLimit(b.db))
				text := admissionTextForBody(t, b, limit-64, admissionLongNameNode)
				require.Equal(t, limit-64, admissionBodyBytes(t, b, admissionLongNameNode(text)), "the stored body's size")
				require.NoError(t, admissionCommit(t, b, mode, "history-seed", admissionLongNameNode(text)), "a body 64 bytes below the limit commits")
				require.NoError(t, b.FlushWriteBehind(ctx))
				stored, err := b.GetNode(ctx, "test:admission-0")
				require.NoError(t, err)
				record, err := encodeMVCCNodeRecord(stored, false)
				require.NoError(t, err)
				require.Greater(t, len(record), limit, "the node's history record is larger than the store's limit")

				err = admissionRetext(t, b, mode, "history-over", 'b')
				requireCommitCapacityRefusal(t, err, ErrStoreValueTooLarge, storeValueLimit(b.db), dir, "history-over", "")
				require.Contains(t, err.Error(), "history record")
				require.Equal(t, byte('a'), admissionLastByte(t, b), "the refused update is not visible")

				// A delete of the node commits.
				require.NoError(t, admissionCommit(t, b, mode, "history-delete", func(ctx context.Context, tx *BadgerTransaction) error {
					return tx.DeleteGraphNode(ctx, "test:admission-0", CascadeEdges)
				}))
				require.Zero(t, admissionStored(t, b))
				b = admissionReopen(t, b, dir, async, true)
				require.Zero(t, admissionStored(t, b), "after crashClose and the recovery at open")
			})
		}
	}
}

// With full change capture on a low-memory store, in both write modes and both
// commit modes: creating a node of 20 MiB commits; an update of it is refused
// with ErrStoreValueTooLarge, with "change event" in the message and no record
// in the journal; with capture switched off the same update commits. The
// capture mode does not change while a commit is under way in this test; the PR
// description states what happens when it does.
func TestStoreValueLimitRefusesAChangeEventOverTheLimit(t *testing.T) {
	for _, async := range []bool{false, true} {
		for mode, modeName := range admissionModes {
			t.Run(fmt.Sprintf("async=%v/%s", async, modeName), func(t *testing.T) {
				dir := t.TempDir()
				b := admissionEngine(t, dir, async, true)
				b.SetChangeCapture("test", ChangeCaptureFull)
				// The creation commits.
				require.NoError(t, admissionCommit(t, b, mode, "event-create", admissionNodes(1, admissionText(20<<20))))
				require.Equal(t, 1, admissionStored(t, b))
				// The update is refused, and the error names the change event.
				err := admissionRetext(t, b, mode, "event-update", 'b')
				requireCommitCapacityRefusal(t, err, ErrStoreValueTooLarge, storeValueLimit(b.db), dir, "event-update", "")
				require.Contains(t, err.Error(), "change event")
				require.Equal(t, byte('a'), admissionLastByte(t, b), "the refused update is not visible")
				// Without capture the same update commits.
				b.SetChangeCapture("test", ChangeCaptureOff)
				require.NoError(t, admissionRetext(t, b, mode, "event-off", 'c'))
				require.Equal(t, byte('c'), admissionLastByte(t, b))
				b = admissionReopen(t, b, dir, async, true)
				require.Equal(t, byte('c'), admissionLastByte(t, b), "after crashClose and the recovery at open")
			})
		}
	}
}

// The WAL engine's own CreateNode on a low-memory store, with synchronous and
// with asynchronous writes: a node of 40 MiB is refused with the store's limit
// in the message and the journal holds no transaction record; a node of 20 MiB
// is written and is there after crashClose and the recovery at open.
func TestCommitAdmissionNativeRoute(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			dir := t.TempDir()
			b := admissionEngine(t, dir, async, true)
			scoped := NewNamespacedEngine(NewWALEngine(b, b.bufferedCommitWAL), "test")
			_, err := scoped.CreateNode(context.Background(), &Node{ID: "over", Labels: []string{"Admission"}, Properties: admissionText(40 << 20)(0)})
			require.Error(t, err, "a value over the store's limit is refused")
			require.Contains(t, err.Error(), fmt.Sprint(storeValueLimit(b.db)))
			t.Logf("refused: %v", err)
			entries, err := ReadWALEntriesFromDir(context.Background(), filepath.Join(dir, "wal"))
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotEqual(t, OpEntityCommit, entry.Operation, "nothing of the refused write is journaled")
			}
			_, err = scoped.CreateNode(context.Background(), &Node{ID: "below", Labels: []string{"Admission"}, Properties: admissionText(20 << 20)(0)})
			require.NoError(t, err, "a node of 20 MiB is written")
			require.Equal(t, 1, admissionStored(t, b))
			b = admissionReopen(t, b, dir, async, true)
			require.Equal(t, 1, admissionStored(t, b))
		})
	}
}

// A node with 18 MiB of text written by the storage engine's own CreateNode, not through
// the WAL engine, then takes a small write on four routes: an explicit and an
// implicit transaction through the WAL engine, the WAL engine's UpdateNode and
// the storage engine's UpdateNode. The four properties are stored, the store's
// streaming snapshot is written, and the text's length and the four properties
// are read again after crashClose and the recovery at open.
func TestLargeStoredNodeIsRewrittenOnFourRoutes(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			dir := t.TempDir()
			b := admissionEngine(t, dir, async, false)
			ctx := context.Background()
			// Written by the storage engine's own CreateNode, not through the WAL engine.
			_, err := b.CreateNode(ctx, &Node{ID: "test:admission-0", Labels: []string{"Admission"}, Properties: admissionText(18 << 20)(0)})
			require.NoError(t, err)
			touch := func(name string) func(context.Context, *BadgerTransaction) error {
				return func(ctx context.Context, tx *BadgerTransaction) error {
					node, err := tx.GetNode(ctx, "test:admission-0")
					if err != nil {
						return err
					}
					node = CopyNode(node)
					node.Properties[name] = true
					return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node})
				}
			}
			require.NoError(t, admissionCommit(t, b, CommitModeExplicit, "touch-explicit", touch("explicit")))
			require.NoError(t, admissionCommit(t, b, CommitModeImplicit, "touch-implicit", touch("implicit")))
			for name, engine := range map[string]Engine{"native": NewWALEngine(b, b.bufferedCommitWAL), "engine": b} {
				require.NoError(t, b.FlushWriteBehind(ctx))
				node, err := b.GetNode(ctx, "test:admission-0")
				require.NoError(t, err)
				node = CopyNode(node)
				node.Properties[name] = true
				require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: node}), name)
			}
			check := func(stage string) {
				require.NoError(t, b.FlushWriteBehind(ctx))
				node, err := b.GetNode(ctx, "test:admission-0")
				require.NoError(t, err, stage)
				require.Len(t, node.Properties["text"], 18<<20, stage)
				for _, name := range []string{"explicit", "implicit", "native", "engine"} {
					require.Equal(t, true, node.Properties[name], "%s: %s", stage, name)
				}
			}
			check("commit")
			require.NoError(t, SaveStreamingSnapshot(ctx, b, filepath.Join(dir, "large.nds"), SnapshotOptions{}))
			b = admissionReopen(t, b, dir, async, false)
			check("after crashClose and the recovery at open")
		})
	}
}
