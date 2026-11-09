package storage

// WAL readers retain request/job provenance in diagnostics without changing
// valid reads, recoverable tail handling, or format errors.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// walDiagnosticProvenanceKey marks the operation context a request or job hands
// to the recovery reader. Its value stands in for the request/trace identity the
// recovery record must retain, so the assertion stays true whether the owner
// threads its context unchanged or detaches cancellation with
// context.WithoutCancel (which keeps values).
type walDiagnosticProvenanceKey struct{}

const walDiagnosticProvenance = "job-7f2c4a:wal-recovery"

func walDiagnosticOperationContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, walDiagnosticProvenanceKey{}, walDiagnosticProvenance)
}

// capturedWALDiagnostic is one record as the handler received it, including the
// context the owner emitted it on.
type capturedWALDiagnostic struct {
	ctx   context.Context
	level slog.Level
	msg   string
	attrs map[string]string
}

// walDiagnosticRecorder captures what the WAL recovery owner emits.
type walDiagnosticRecorder struct {
	mu       sync.Mutex
	captured []capturedWALDiagnostic
}

func (r *walDiagnosticRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *walDiagnosticRecorder) Handle(ctx context.Context, record slog.Record) error {
	captured := capturedWALDiagnostic{
		ctx:   ctx,
		level: record.Level,
		msg:   record.Message,
		attrs: make(map[string]string, record.NumAttrs()),
	}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value.String()
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.captured = append(r.captured, captured)
	return nil
}

// WithAttrs and WithGroup return the recorder itself: the recovery owner emits
// through LogAttrs with no pre-bound attributes or open group, so neither may
// hide a record from the assertions.
func (r *walDiagnosticRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *walDiagnosticRecorder) WithGroup(string) slog.Handler      { return r }

func (r *walDiagnosticRecorder) logger() *slog.Logger { return slog.New(r) }

func (r *walDiagnosticRecorder) records() []capturedWALDiagnostic {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]capturedWALDiagnostic(nil), r.captured...)
}

// byEventID returns every record carrying the supplied stable event_id.
func (r *walDiagnosticRecorder) byEventID(id localization.EventID) []capturedWALDiagnostic {
	var matched []capturedWALDiagnostic
	for _, record := range r.records() {
		if record.attrs["event_id"] == string(id) {
			matched = append(matched, record)
		}
	}
	return matched
}

// walReadDiagnostics writes content as an active WAL and reads it through the
// canonical recovery reader with an operation context and a recording logger.
func walReadDiagnostics(t *testing.T, content []byte) (entries []WALEntry, err error, recorder *walDiagnosticRecorder, operationCtx context.Context) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	recorder = &walDiagnosticRecorder{}
	operationCtx = walDiagnosticOperationContext(t.Context())
	// Approved reader/recovery context migration: this read takes operationCtx
	// as its first argument. The recovery diagnostics below are emitted from the
	// same owner, so their record must carry that operation context.
	entries, err = ReadWALEntries(operationCtx, path, recorder.logger())
	return entries, err, recorder, operationCtx
}

// requireOperationProvenance asserts the required behaviour: the recovery
// diagnostic reached the handler on the caller's operation context, so request
// and job provenance survives into the record.
func requireOperationProvenance(t *testing.T, operationCtx context.Context, record capturedWALDiagnostic) {
	t.Helper()
	require.NotNil(t, record.ctx, "the recovery record carries the context its owner emitted it on")
	require.Equal(t, operationCtx.Value(walDiagnosticProvenanceKey{}), record.ctx.Value(walDiagnosticProvenanceKey{}),
		"WAL recovery diagnostics must be emitted on the operation context that owns the read")
}

// walDiagnosticValidRecord is one readable entry the readers must keep reporting
// whatever happens to the tail.
func walDiagnosticValidRecord(t *testing.T) []byte {
	t.Helper()
	data := []byte(`{"node":{"id":"n1"}}`)
	return buildAtomicWALRecord(t, WALEntry{
		Sequence:  1,
		Timestamp: time.Unix(0, 1).UTC(),
		Operation: OpCreateNode,
		Data:      data,
		Checksum:  crc32Checksum(data),
	})
}

// TestWALRecoveryDiagnosticContextCarriesOperationContext is the fail-first
// regression for the recovery diagnostic's lost provenance.
func TestWALRecoveryDiagnosticContextCarriesOperationContext(t *testing.T) {
	valid := walDiagnosticValidRecord(t)

	t.Run("valid read stays diagnostics-free", func(t *testing.T) {
		entries, err, recorder, _ := walReadDiagnostics(t, valid)
		require.NoError(t, err, "a readable WAL is not an error")
		require.Len(t, entries, 1)
		require.Empty(t, recorder.records(), "a complete WAL emits no recovery diagnostic")
	})

	t.Run("torn tail carries the operation context", func(t *testing.T) {
		// A frame announced as 32 payload bytes that stops after 4: an
		// incomplete write at the end of the active file.
		torn := append(append([]byte{}, valid...), atomicHeader(walFormatVersionCurrent, 32)...)
		torn = append(torn, []byte("torn")...)

		entries, err, recorder, operationCtx := walReadDiagnostics(t, torn)
		require.NoError(t, err, "a torn tail is a recoverable tail, not a read error")
		require.Len(t, entries, 1, "the complete record before the tail is still returned")

		matched := recorder.byEventID(localization.EventStorageWALIncompleteWrite)
		require.Len(t, matched, 1, "the torn tail emits exactly one incomplete-write diagnostic")
		record := matched[0]
		require.Equal(t, slog.LevelWarn, record.level)
		require.Equal(t, "crash_recovery", record.attrs["reason"])
		require.Equal(t, "atomic", record.attrs["format"])
		requireOperationProvenance(t, operationCtx, record)
	})

	t.Run("skipped corrupt embeddings carry the operation context", func(t *testing.T) {
		embeddingData := []byte(`{"node":{"id":"n1"},"embedding":[0.1,0.2]}`)
		corruptEmbedding := buildAtomicWALRecord(t, WALEntry{
			Sequence:  2,
			Timestamp: time.Unix(0, 2).UTC(),
			Operation: OpUpdateEmbedding,
			Data:      embeddingData,
			Checksum:  crc32Checksum(embeddingData) + 1, // regenerable record, corrupt checksum
		})

		entries, err, recorder, operationCtx := walReadDiagnostics(t, append(append([]byte{}, valid...), corruptEmbedding...))
		require.NoError(t, err, "a corrupt regenerable embedding is skipped, not fatal")
		require.Len(t, entries, 1, "the skipped embedding is not reported as an entry")

		matched := recorder.byEventID(localization.EventStorageWALCorruptedEmbeddingsSkipped)
		require.Len(t, matched, 1, "the skipped embedding emits exactly one diagnostic")
		record := matched[0]
		require.Equal(t, slog.LevelWarn, record.level)
		require.Equal(t, "1", record.attrs["skipped_embeddings"])
		require.Equal(t, "atomic", record.attrs["format"])
		require.Equal(t, "will_regenerate", record.attrs["action"])
		requireOperationProvenance(t, operationCtx, record)
	})

	t.Run("unsupported format keeps its error and emits no diagnostic", func(t *testing.T) {
		payload := mustWalEntryPayload(t, 1, OpCreateNode, []byte(`{"node":{"id":"n1"}}`), crc32Checksum([]byte(`{"node":{"id":"n1"}}`)))
		unsupported := buildAtomicRecordRaw(payload, walFormatVersionCurrent-1, crc32Checksum(payload), walTrailer, true)

		_, err, recorder, _ := walReadDiagnostics(t, unsupported)
		require.ErrorIs(t, err, ErrWALUnsupportedVersion)
		require.Empty(t, recorder.records(), "a refused format is an error, not a recovery diagnostic")
	})
}

// TestWALDiagnosticEmitterPreservesOperationContext pins the canonical emitter
// contract the recovery owner must feed: logStorageEvent already emits on the
// context it is given, so the lost provenance is only the recovery call site's
// substitution.
func TestWALDiagnosticEmitterPreservesOperationContext(t *testing.T) {
	recorder := &walDiagnosticRecorder{}
	operationCtx := walDiagnosticOperationContext(t.Context())

	logStorageEvent(operationCtx, recorder.logger(), slog.LevelWarn, localization.StorageWALIncompleteWriteEvent())

	matched := recorder.byEventID(localization.EventStorageWALIncompleteWrite)
	require.Len(t, matched, 1)
	requireOperationProvenance(t, operationCtx, matched[0])
}

// Recovering acknowledged data must not inherit a canceled initiating request.
func TestWALRecoveryCompletesAfterInitiatorCancellation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := newTestMemoryEngine(t)
	_, err := source.CreateNode(t.Context(), &Node{ID: "test:snapshot", Labels: []string{"Doc"}})
	require.NoError(t, err)
	snapshot := filepath.Join(t.TempDir(), "snapshot.nds")
	require.NoError(t, SaveStreamingSnapshot(t.Context(), source, snapshot, SnapshotOptions{}))
	walDir := t.TempDir()
	wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "wal", Labels: []string{"Doc"}}}, "test"))
	require.NoError(t, wal.Close())
	destination := newTestMemoryEngine(t)
	ctx, cancel := context.WithCancel(walDiagnosticOperationContext(t.Context()))
	cancel()
	result, status, err := RecoverIntoEngine(ctx, destination, walDir, snapshot)
	require.NoError(t, err)
	require.Zero(t, result.Failed)
	require.Equal(t, 1, result.Applied)
	require.Equal(t, uint64(1), status.WALEntries)
	require.Equal(t, uint64(1), status.SnapshotNodes)
	node, err := destination.GetNode(t.Context(), "test:snapshot")
	require.NoError(t, err)
	require.Equal(t, []string{"Doc"}, node.Labels)
	_, err = destination.GetNode(t.Context(), "test:wal")
	require.NoError(t, err)
	recovered, replay, err := fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(ctx, walDir, snapshot))
	require.NoError(t, err)
	require.Zero(t, replay.Failed)
	require.Equal(t, 1, replay.Applied)
	for _, id := range []NodeID{"test:snapshot", "test:wal"} {
		_, err = recovered.GetNode(t.Context(), id)
		require.NoError(t, err)
	}
}

func TestWALCompensationCompletesAfterInitiatorCancellation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	t.Run("public replay and undo", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		data, err := marshalWALRecoveryPayload(nil, WALNodeData{Node: &Node{ID: "temporary", Labels: []string{"Doc"}}})
		require.NoError(t, err)
		entry := WALEntry{Sequence: 1, Operation: OpCreateNode, Database: "test", Data: data, Checksum: crc32Checksum(data)}
		require.NoError(t, ReplayWALEntry(ctx, engine, entry))
		_, err = engine.GetNode(t.Context(), "test:temporary")
		require.NoError(t, err)
		require.NoError(t, UndoWALEntry(ctx, engine, entry))
		_, err = engine.GetNode(t.Context(), "test:temporary")
		require.ErrorIs(t, err, ErrNotFound)
	})
	t.Run("incomplete transaction recovery", func(t *testing.T) {
		dir := t.TempDir()
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "accepted", Labels: []string{"Doc"}}}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OpTxBegin, WALTxData{TxID: "incomplete"}, "test"))
		require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: &Node{ID: "temporary", Labels: []string{"Doc"}}, TxID: "incomplete"}, "test"))
		require.NoError(t, wal.Close())
		recovered, result, err := fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(ctx, dir, ""))
		require.NoError(t, err)
		require.False(t, result.HasErrors())
		require.Equal(t, 1, result.NonTxApplied)
		require.Equal(t, 1, result.RolledBackTransactions)
		_, err = recovered.GetNode(t.Context(), "test:accepted")
		require.NoError(t, err)
		_, err = recovered.GetNode(t.Context(), "test:temporary")
		require.ErrorIs(t, err, ErrNotFound)
	})
}
