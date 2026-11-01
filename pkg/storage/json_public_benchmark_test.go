package storage_test

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Copy this file unchanged to the release base. All writers, readers and engine
// operations are public APIs. One op is a fixed batch of 64 records; b.N counts
// batches, including retained recovery. Setup, validation, close, GC and removal
// are untimed. Each write batch gets a fresh WAL/store, each recovery gets a fresh
// destination, and directories are removed per batch rather than at benchmark
// exit. All files (including the immutable buffered-recovery preimage) use tmpfs.
const jsonPublicStorageBatch = 64

func BenchmarkJSONPublicWALAppendBounded(b *testing.B) {
	for _, syncMode := range []string{"none", "immediate"} {
		b.Run(syncMode, func(b *testing.B) {
			jsonPublicWALWriteBench(b, syncMode, false)
		})
	}
}

func BenchmarkJSONPublicWALEngineCreateNodeBounded(b *testing.B) {
	jsonPublicWALWriteBench(b, "none", true)
}

func jsonPublicWALWriteBench(b *testing.B, syncMode string, create bool) {
	b.StopTimer()
	b.Cleanup(config.WithWALEnabled())
	root := jsonPublicStorageTmpfs(b)
	nodes := jsonPublicStorageNodes(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jsonPublicWALWriteBatch(b, filepath.Join(root, "batch"), nodes, syncMode, create)
	}
	b.ReportMetric(jsonPublicStorageBatch, "records/op")
}

func jsonPublicWALWriteBatch(b *testing.B, dir string, nodes []*storage.Node, syncMode string, create bool) {
	defer func() {
		b.StopTimer()
		jsonPublicStorageCheck(b, os.RemoveAll(dir))
	}()
	wal := jsonPublicStorageWAL(b, dir, syncMode)
	engine := storage.NewWALEngine(testfixture.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed), wal)
	defer func() { jsonPublicStorageCheck(b, engine.Close()) }()
	b.StartTimer()
	for _, node := range nodes {
		if create {
			_, err := engine.CreateNode(node)
			jsonPublicStorageCheck(b, err)
		} else {
			jsonPublicStorageCheck(b, wal.Append(storage.OpCreateNode, storage.WALNodeData{Node: node}))
		}
	}
	b.StopTimer()
	jsonPublicStorageCheck(b, wal.Sync())
	entries, err := storage.ReadWALEntriesFromDir(dir)
	jsonPublicStorageCheck(b, err)
	if len(entries) != len(nodes) {
		b.Fatalf("WAL has %d records, want %d", len(entries), len(nodes))
	}
	if create {
		jsonPublicStorageValidate(b, engine, nodes)
	}
}

func BenchmarkJSONPublicBufferedRecoveryBounded(b *testing.B) {
	b.StopTimer()
	b.Cleanup(config.WithWALEnabled())
	root := jsonPublicStorageTmpfs(b)
	nodes := jsonPublicStorageNodes(8 << 10)
	walDir, backup := jsonPublicBufferedSource(b, filepath.Join(root, "source"), nodes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jsonPublicBufferedRecoverBatch(b, filepath.Join(root, "target"), walDir, backup, nodes)
	}
	b.ReportMetric(jsonPublicStorageBatch, "records/op")
}

func jsonPublicBufferedSource(b *testing.B, root string, nodes []*storage.Node) (string, string) {
	store := jsonPublicBufferedOpen(b, filepath.Join(root, "store"))
	defer func() { jsonPublicStorageCheck(b, store.Close()) }()
	// Seed and flush the property dictionary before taking a physical preimage.
	// A graceful source close can then flush normally: restoring this preimage
	// gives recovery the original dictionary without any of the 64 flush markers.
	_, err := store.CreateNode(&storage.Node{ID: "jsonbench:seed", Properties: nodes[0].Properties})
	jsonPublicStorageCheck(b, err)
	jsonPublicStorageCheck(b, store.FlushWriteBehind(nil))
	backup := filepath.Join(root, "before-wal.backup")
	jsonPublicStorageCheck(b, store.Backup(backup))
	walDir := filepath.Join(root, "wal")
	wal := jsonPublicStorageWAL(b, walDir, "none")
	journal := storage.NewWALEngine(store, wal)
	defer func() { jsonPublicStorageCheck(b, journal.Close()) }()
	for _, node := range nodes {
		tx, err := store.BeginTransaction()
		jsonPublicStorageCheck(b, err)
		jsonPublicStorageCheck(b, tx.SetNamespace("jsonbench"))
		jsonPublicStorageCheck(b, tx.SetImplicit(true))
		_, err = tx.CreateNode(node)
		jsonPublicStorageCheck(b, err)
		jsonPublicStorageCheck(b, tx.Commit())
		if !tx.AcknowledgedBuffered() {
			b.Fatal("fixture commit bypassed write-behind")
		}
	}
	jsonPublicStorageCheck(b, store.FlushWriteBehind(nil))
	jsonPublicStorageValidate(b, store, nodes)
	jsonPublicStorageCheck(b, wal.Sync())
	entries, err := storage.ReadWALEntriesFromDir(walDir)
	jsonPublicStorageCheck(b, err)
	if len(entries) != len(nodes) {
		b.Fatalf("source has %d WAL entries, want %d", len(entries), len(nodes))
	}
	for _, entry := range entries {
		if entry.Operation != storage.OpBufferedCommit {
			b.Fatal("fixture must contain real public write-behind commits")
		}
	}
	return walDir, backup
}

func jsonPublicBufferedRecoverBatch(b *testing.B, dir, walDir, backup string, nodes []*storage.Node) {
	defer func() {
		b.StopTimer()
		jsonPublicStorageCheck(b, os.RemoveAll(dir))
	}()
	store := jsonPublicBufferedOpen(b, dir)
	defer func() { jsonPublicStorageCheck(b, store.Close()) }()
	jsonPublicStorageCheck(b, store.Restore(backup))
	wal := jsonPublicStorageWAL(b, walDir, "none")
	defer func() { jsonPublicStorageCheck(b, wal.Close()) }()
	b.StartTimer()
	replayed, err := storage.RecoverBufferedCommits(store, wal)
	b.StopTimer()
	jsonPublicStorageCheck(b, err)
	if replayed != len(nodes) {
		b.Fatalf("recovered %d commits, want %d", replayed, len(nodes))
	}
	jsonPublicStorageValidate(b, store, nodes)
}

// This replaces the private retained-decode fixture with public WAL recovery
// into a fresh in-memory store. The real writer produces the recovery payload;
// there is no hand-encoded control bypassing the reader under measurement.
// retained-B/entity is a noisy post-GC heap delta, separate from timed B/op.
func BenchmarkJSONPublicRecoveryRetainedDocBounded(b *testing.B) {
	b.StopTimer()
	b.Cleanup(config.WithWALEnabled())
	root := jsonPublicStorageTmpfs(b)
	nodes := jsonPublicStorageNodes(0)
	wal := jsonPublicStorageWAL(b, filepath.Join(root, "source"), "none")
	for _, node := range nodes {
		// Pin the public replay namespace instead of inheriting the process's
		// default database and measuring environment/config resolution per row.
		jsonPublicStorageCheck(b, wal.AppendWithDatabase(storage.OpCreateNode, storage.WALNodeData{Node: node}, "jsonbench"))
	}
	jsonPublicStorageCheck(b, wal.Close())
	entries, err := storage.ReadWALEntriesFromDir(filepath.Join(root, "source"))
	jsonPublicStorageCheck(b, err)
	if len(entries) != len(nodes) {
		b.Fatalf("source has %d records, want %d", len(entries), len(nodes))
	}
	b.ReportAllocs()
	b.ResetTimer()
	var retainedBytes int64
	for i := 0; i < b.N; i++ {
		retainedBytes += jsonPublicRetainedRecoverBatch(b, entries, nodes)
	}
	b.ReportMetric(float64(retainedBytes)/float64(b.N)/jsonPublicStorageBatch, "retained-B/entity")
	b.ReportMetric(jsonPublicStorageBatch, "records/op")
}

func jsonPublicRetainedRecoverBatch(b *testing.B, entries []storage.WALEntry, nodes []*storage.Node) int64 {
	engine := testfixture.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer func() { jsonPublicStorageCheck(b, engine.Close()) }()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.StartTimer()
	var result storage.ReplayResult
	for _, entry := range entries {
		result.Record(entry, storage.ReplayWALEntry(engine, entry))
	}
	b.StopTimer()
	if result.Failed != 0 || result.Applied != len(nodes) || result.Skipped != 0 {
		b.Fatal(result.Summary())
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	jsonPublicStorageValidate(b, engine, nodes)
	runtime.KeepAlive(engine)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func jsonPublicStorageTmpfs(b *testing.B) string {
	b.Helper()
	mounts, err := os.ReadFile("/proc/mounts")
	jsonPublicStorageCheck(b, err)
	found := false
	for _, line := range strings.Split(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[1] == "/dev/shm" && fields[2] == "tmpfs" {
			found = true
		}
	}
	if !found {
		b.Fatal("public WAL benchmarks require /dev/shm tmpfs; no disk fallback")
	}
	root, err := os.MkdirTemp("/dev/shm", "nornic-json-wal-public-")
	jsonPublicStorageCheck(b, err)
	b.Cleanup(func() { jsonPublicStorageCheck(b, os.RemoveAll(root)) })
	return root
}

func jsonPublicStorageCheck(b *testing.B, err error) {
	if err != nil {
		b.Helper()
		b.Fatal(err)
	}
}

func jsonPublicStorageWAL(b *testing.B, dir, syncMode string) *storage.WAL {
	wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: syncMode})
	jsonPublicStorageCheck(b, err)
	return wal
}

func jsonPublicBufferedOpen(b *testing.B, dir string) *storage.BadgerEngine {
	store, err := testfixture.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 2 * jsonPublicStorageBatch,
	}))
	jsonPublicStorageCheck(b, err)
	return store
}

func jsonPublicStorageNodes(payloadBytes int) []*storage.Node {
	nodes := make([]*storage.Node, jsonPublicStorageBatch)
	created := time.Date(2026, 1, 2, 3, 4, 5, 123, time.UTC)
	for i := range nodes {
		properties := map[string]any{
			"integer": int64(9007199254740993), "whole": float64(2), "fraction": 0.125,
			"subnormal": math.SmallestNonzeroFloat64, "nested": map[string]any{"count": int64(11)},
		}
		if payloadBytes != 0 {
			properties["payload"] = strings.Repeat("x", payloadBytes)
		}
		nodes[i] = &storage.Node{
			ID: storage.NodeID(fmt.Sprintf("jsonbench:n%03d", i)), Labels: []string{"JSONPublicDoc"}, Properties: properties,
			CreatedAt: created, UpdatedAt: created.Add(time.Hour),
			NamedEmbeddings: map[string][]float32{"title": {1, 2}}, ChunkEmbeddings: [][]float32{{3, 4}},
			EmbedMeta: map[string]any{"chunk_count": int64(1), "has_embedding": true},
		}
		nodes[i].SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: true})
	}
	return nodes
}

func jsonPublicStorageValidate(b *testing.B, engine storage.Engine, nodes []*storage.Node) {
	b.Helper()
	provider, ok := engine.(storage.NamespaceLabelStatsProvider)
	if !ok {
		b.Fatal("fixture engine must provide public namespace label counts")
	}
	count, err := provider.NodeCountByLabelInNamespace("jsonbench", "JSONPublicDoc")
	jsonPublicStorageCheck(b, err)
	if count != int64(len(nodes)) {
		b.Fatalf("recovered %d labelled nodes, want %d", count, len(nodes))
	}
	for _, node := range nodes {
		got, err := engine.GetNode(node.ID)
		jsonPublicStorageCheck(b, err)
		// Shape checks work on both base and head, without requiring the base's
		// known numeric precision/type defects to pass corrected assertions.
		if len(got.Properties) != len(node.Properties) || got.Properties["integer"] == nil || got.Properties["whole"] == nil {
			b.Fatalf("%s: recovered property shape differs", node.ID)
		}
	}
}
