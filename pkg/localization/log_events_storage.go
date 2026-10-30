package localization

import "log/slog"

const (
	EventStorageWALIncompleteWrite            EventID   = "storage.wal.incomplete_write"
	EventStorageWALCorruptedEmbeddingsSkipped EventID   = "storage.wal.corrupted_embeddings_skipped"
	MessageStorageLogWALIncompleteWrite       MessageID = "storage-log.log.wal_incomplete_write"
	MessageStorageLogWALEmbeddingsSkipped     MessageID = "storage-log.log.wal_embeddings_skipped"
)

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
