package storage_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// This file can be copied unchanged onto the release base. It deliberately
// uses only public APIs: no bufferedCommit, dictionary bindings, private
// importer, crash-close helper, or AppendBufferedCommit signature dependency.
// One benchmark operation is 32 real write-behind commits or recovered records.
// Use a bounded sample (for example -benchtime=8x); setup, validation and file
// removal are untimed. Every store, WAL and backup is on verified /dev/shm tmpfs.
const walPublicBatch = 32

func BenchmarkWALPublicAppend(b *testing.B) {
	walPublicBench(b, "append")
}

func BenchmarkWALPublicReopenRecovery(b *testing.B) {
	walPublicBench(b, "reopen")
}

func BenchmarkWALPublicImport(b *testing.B) {
	walPublicBench(b, "import")
}

func walPublicBench(b *testing.B, mode string) {
	for _, keys := range []int{1, 160} {
		b.Run(fmt.Sprintf("Keys%d", keys), func(b *testing.B) {
			b.StopTimer()
			b.Cleanup(config.WithWALEnabled())
			root := walPublicTmpfs(b)
			properties := make(map[string]any, keys)
			for i := 0; i < keys; i++ {
				properties[fmt.Sprintf("property_%03d", i)] = int64(i + 1)
			}
			nodes := make([]*storage.Node, walPublicBatch)
			for i := range nodes {
				nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("walbench:n%03d", i)), Properties: properties, CreatedAt: time.Now()}
			}
			var walDir, backup string
			if mode != "append" {
				// Reopen uses already-persisted names in a paired physical
				// preimage. Import creates the names in the first logged commit,
				// so the base writer's natural drain binds the complete WAL.
				walDir, backup = walPublicSource(b, filepath.Join(root, "source"), nodes, keys, mode == "reopen", false)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "append" {
					path := filepath.Join(root, "append")
					walPublicSource(b, path, nodes, keys, true, true)
					walPublicCheck(b, os.RemoveAll(path))
				} else {
					walPublicRecover(b, filepath.Join(root, "target"), walDir, backup, nodes, keys, mode)
				}
			}
			b.ReportMetric(walPublicBatch, "records/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*walPublicBatch), "ns/record")
		})
	}
}

func walPublicTmpfs(b *testing.B) string {
	b.Helper()
	mounts, err := os.ReadFile("/proc/mounts")
	walPublicCheck(b, err)
	found := false
	for _, line := range strings.Split(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[1] == "/dev/shm" && fields[2] == "tmpfs" {
			found = true
		}
	}
	if !found {
		b.Fatal("WAL public benchmarks require /dev/shm mounted as tmpfs; no disk fallback")
	}
	root, err := os.MkdirTemp("/dev/shm", "nornic-wal-public-")
	walPublicCheck(b, err)
	b.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func walPublicOpen(b *testing.B, path string) *storage.BadgerEngine {
	store, err := fixtureowner.OwnResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: path, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 2 * walPublicBatch,
	}))
	walPublicCheck(b, err)
	return store
}

func walPublicCheck(b *testing.B, err error) {
	if err != nil {
		b.Helper()
		b.Fatal(err)
	}
}

func walPublicCommit(b *testing.B, store *storage.BadgerEngine, nodes []*storage.Node) {
	for _, node := range nodes {
		tx, err := store.BeginTransaction()
		walPublicCheck(b, err)
		walPublicCheck(b, tx.SetNamespace("walbench"))
		walPublicCheck(b, tx.SetImplicit(true))
		_, err = tx.CreateNode(node)
		walPublicCheck(b, err)
		walPublicCheck(b, tx.Commit())
		if !tx.AcknowledgedBuffered() {
			b.Fatal("benchmark commit did not use write-behind")
		}
	}
}

func walPublicValidate(b *testing.B, store *storage.BadgerEngine, nodes []*storage.Node, keys int) {
	for _, node := range nodes {
		got, err := store.GetNode(node.ID)
		walPublicCheck(b, err)
		if len(got.Properties) != keys {
			b.Fatalf("%s: got %d properties, want %d", node.ID, len(got.Properties), keys)
		}
		for i := 0; i < keys; i++ {
			name := fmt.Sprintf("property_%03d", i)
			if got.Properties[name] != int64(i+1) {
				b.Fatalf("%s: property %s = %v, want %d", node.ID, name, got.Properties[name], i+1)
			}
		}
	}
}

func walPublicSource(b *testing.B, root string, nodes []*storage.Node, keys int, persistedNames, timed bool) (string, string) {
	storeDir := filepath.Join(root, "store")
	store := walPublicOpen(b, storeDir)
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	seed := &storage.Node{ID: "walbench:seed"}
	if persistedNames {
		seed.Properties = nodes[0].Properties
	}
	_, err := store.CreateNode(seed)
	walPublicCheck(b, err)
	walPublicCheck(b, store.FlushWriteBehind(nil))
	backup := filepath.Join(root, "before-wal.backup")
	if !timed {
		walPublicCheck(b, store.Backup(backup))
	}
	walDir := filepath.Join(root, "wal")
	wal, err := storage.NewWAL(walDir, storage.DefaultWALConfig())
	walPublicCheck(b, err)
	defer func() { _ = wal.Close() }()
	journal := storage.NewWALEngine(store, wal)
	if timed {
		b.StartTimer()
	}
	walPublicCommit(b, store, nodes)
	if timed {
		b.StopTimer()
	}
	walPublicCheck(b, store.FlushWriteBehind(nil))
	walPublicValidate(b, store, nodes, keys)
	walPublicCheck(b, journal.Close())
	store = nil
	entries, err := storage.ReadWALEntriesFromDir(walDir)
	walPublicCheck(b, err)
	if len(entries) != walPublicBatch {
		b.Fatalf("WAL has %d records, want %d", len(entries), walPublicBatch)
	}
	for _, entry := range entries {
		if entry.Operation != storage.OpBufferedCommit {
			b.Fatal("expected physical write-behind record")
		}
	}
	walPublicCheck(b, os.RemoveAll(storeDir))
	return walDir, backup
}

func walPublicRecover(b *testing.B, dir, walDir, backup string, nodes []*storage.Node, keys int, mode string) {
	// Graceful Close flushes write-behind. Restore a public native backup of
	// the pre-WAL state, then close it, so the timed reopen must actually
	// recover all 32 records rather than merely skip already-flushed markers.
	store := walPublicOpen(b, dir)
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	walPublicCheck(b, store.Restore(backup))
	walPublicCheck(b, store.Close())
	store = nil
	var wal *storage.WAL
	defer func() {
		if wal != nil {
			_ = wal.Close()
		}
	}()
	b.StartTimer()
	store = walPublicOpen(b, dir)
	applied := 0
	if mode == "reopen" {
		var err error
		wal, err = storage.NewWAL(walDir, storage.DefaultWALConfig())
		walPublicCheck(b, err)
		applied, err = storage.RecoverBufferedCommits(store, wal)
		walPublicCheck(b, err)
	} else {
		result, _, err := storage.RecoverIntoEngine(store, walDir, "")
		walPublicCheck(b, err)
		if result.Failed != 0 {
			b.Fatal(result.Summary())
		}
		applied = result.Applied
	}
	b.StopTimer()
	if applied != walPublicBatch {
		b.Fatalf("recovered %d records, want %d", applied, walPublicBatch)
	}
	walPublicValidate(b, store, nodes, keys)
	b.StartTimer()
	walPublicCheck(b, store.Close())
	store = nil
	if wal != nil {
		walPublicCheck(b, wal.Close())
		wal = nil
	}
	b.StopTimer()
	walPublicCheck(b, os.RemoveAll(dir))
}
