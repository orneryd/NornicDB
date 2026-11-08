package localization

import (
	"log/slog"
	"time"
)

const (
	EventStorageWALIncompleteWrite            EventID   = "storage.wal.incomplete_write"
	EventStorageWALCorruptedEmbeddingsSkipped EventID   = "storage.wal.corrupted_embeddings_skipped"
	MessageStorageLogWALIncompleteWrite       MessageID = "storage-log.log.wal_incomplete_write"
	MessageStorageLogWALEmbeddingsSkipped     MessageID = "storage-log.log.wal_embeddings_skipped"
)

const (
	EventStorageLockWaitProlonged      EventID   = "storage.transaction.lock_wait_prolonged"
	MessageStorageLogLockWaitProlonged MessageID = "storage-log.log.lock_wait_prolonged"
)

// StorageLogLockWaitProlonged describes a MERGE still waiting for a key lock
// another transaction holds.
func StorageLogLockWaitProlonged() Message {
	return Message{ID: MessageStorageLogLockWaitProlonged, Fallback: "transaction is still waiting for a key lock another transaction holds"}
}

// StorageLockWaitProlongedEvent reports a MERGE whose wait for the lock on a
// label.property key has lasted waited: the waiting transaction, the
// transaction holding the key, and the timeout that will end the wait (zero
// when none does).
func StorageLockWaitProlongedEvent(database, transactionID, holderTransactionID, label, property string, waited, timeout time.Duration) LogEvent {
	return LogEvent{ID: EventStorageLockWaitProlonged, Message: StorageLogLockWaitProlonged(), Attrs: []slog.Attr{
		slog.String("database", database),
		slog.String("transaction_id", transactionID),
		slog.String("holder_transaction_id", holderTransactionID),
		slog.String("label", label),
		slog.String("property", property),
		slog.Int64("waited_ms", waited.Milliseconds()),
		slog.Int64("timeout_ms", timeout.Milliseconds()),
	}}
}

// StorageLogWALIncompleteWrite describes an incomplete atomic WAL tail.
func StorageLogWALIncompleteWrite() Message {
	return Message{ID: MessageStorageLogWALIncompleteWrite, Fallback: "wal recovery: detected incomplete write at end"}
}

// StorageLogWALEmbeddingsSkipped describes recoverable corrupted embeddings.
func StorageLogWALEmbeddingsSkipped() Message {
	return Message{ID: MessageStorageLogWALEmbeddingsSkipped, Fallback: "wal recovery: skipped corrupted embedding entries"}
}

// StorageWALIncompleteWriteEvent describes an incomplete atomic WAL tail.
func StorageWALIncompleteWriteEvent() LogEvent {
	return LogEvent{ID: EventStorageWALIncompleteWrite, Message: StorageLogWALIncompleteWrite(), Attrs: []slog.Attr{slog.String("reason", "crash_recovery"), slog.String("format", "atomic")}}
}

// StorageWALCorruptedEmbeddingsSkippedEvent describes recoverable corrupted embeddings.
func StorageWALCorruptedEmbeddingsSkippedEvent(skipped int, format string) LogEvent {
	return LogEvent{ID: EventStorageWALCorruptedEmbeddingsSkipped, Message: StorageLogWALEmbeddingsSkipped(), Attrs: []slog.Attr{slog.Int("skipped_embeddings", skipped), slog.String("format", format), slog.String("action", "will_regenerate")}}
}
