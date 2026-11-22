package lifecycle

// Root-cause diagnostic, v1.5.2 verification campaign, lane B (test-only).
// G-mvcc-reader-registry (E53.20): ReaderRegistry keys entries by reader ID
// alone (reader_registry.go:30-44).

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCBReaderRegistrySameIDRefresh(t *testing.T) {
	r := NewReaderRegistry()
	// Transaction refresh order (badger_transaction.go:481-488): acquire the
	// new registration, then release the old one; both carry the same ID.
	_, releaseOld := r.Register(storage.SnapshotReaderInfo{ReaderID: "tx-1", Namespace: "test"})
	_, releaseNew := r.Register(storage.SnapshotReaderInfo{ReaderID: "tx-1", Namespace: "test"})
	t.Logf("RCBREG same-id after second Register: active=%d", r.ActiveCount())
	releaseOld()
	t.Logf("RCBREG same-id after releasing the OLD registration: active=%d (the new one is still held)", r.ActiveCount())
	releaseNew()

	// Variant: the same sequence with distinct IDs keeps the new entry.
	_, releaseA := r.Register(storage.SnapshotReaderInfo{ReaderID: "tx-2a", Namespace: "test"})
	_, releaseB := r.Register(storage.SnapshotReaderInfo{ReaderID: "tx-2b", Namespace: "test"})
	releaseA()
	t.Logf("RCBREG distinct-id variant after releasing the old registration: active=%d", r.ActiveCount())
	releaseB()

	// Composite member: one reader ID registered for two namespaces.
	_, releaseDBA := r.Register(storage.SnapshotReaderInfo{ReaderID: "ei3-reader", Namespace: "dba"})
	_, releaseDBB := r.Register(storage.SnapshotReaderInfo{ReaderID: "ei3-reader", Namespace: "dbb"})
	t.Logf("RCBREG one ID over two namespaces: active=%d", r.ActiveCount())
	releaseDBA()
	t.Logf("RCBREG after releasing the first namespace's registration: active=%d", r.ActiveCount())
	releaseDBB()
}
