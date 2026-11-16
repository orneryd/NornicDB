//go:build linux || darwin

package storage

// Production-size cost qualification for the automatic snapshot compaction
// that B29's pinned read point made overlap-safe (docs/performance/v150-pinned-compaction.md).
//
// The deterministic overlap tests (compaction_snapshot_overlap_test.go,
// pinned_snapshot_dictionary_test.go) establish barrier ownership and recovery
// correctness. This file measures the same production compaction entry
// (WALEngine.createSnapshotAndCompact with the canonical
// saveCompactionSnapshot writer, the exact pair the auto-compaction loop calls)
// on synthetic populations built through the public WAL-backed write path.
//
// One benchmark operation is one full snapshot plus its WAL truncation. The
// fixture is built, warmed up and validated outside the timed region, and each
// named population is measured twice on fresh stores holding that population:
// with no foreground writes (mode=idle-foreground, the control) and with
// bounded-rate concurrent foreground writes (mode=overlap). Both modes report:
//
//   - snapshot duration and the bytes the snapshot file actually holds on disk;
//   - foreground write latency percentiles sampled during the snapshot, and the
//     same percentiles measured with no compaction in flight (baseline);
//   - cumulative allocations, peak Go heap (sampled), post-GC retained heap,
//     process peak RSS (ru_maxrss high-water) and its growth across the
//     operation, and GC cycle/pause deltas;
//   - disk: the store directory (Badger, including the versions a live pinned
//     read point keeps alive) before, at peak during the operation, and after;
//     the WAL directory and the snapshot directory;
//   - exact recovery: a fresh store recovered from that snapshot plus the WAL
//     that remains after its cut is compared entity by entity with the live
//     population, and any difference fails the run.
//
// These are the harness's own limits, not qualified behaviour: the transport
// (HTTP/Bolt/Cypher) is not exercised; the compaction runs in-process on one
// machine with a temp `b.TempDir()` (APFS here), not on a production data
// directory; foreground writes are the storage engine's public CreateNode and
// UpdateNode calls at a bounded pace, so this is not a foreground throughput
// benchmark. Process peak RSS is a monotonic high-water mark, so it reports the
// operation's own peak only when the operation raised it; the Go heap peak is a
// 100 ms sample. No ns/op target is asserted or implied.
//
// Run (one measured snapshot per population and mode; -benchtime=1x is the
// intended sample size because each operation is a full production-size
// snapshot):
//
//	go test -tags=noui,nolocalllm ./pkg/storage -run '^$' -v \
//		-bench '^BenchmarkSnapshotCompactionQualification$' -benchtime=1x -timeout 90m
//
// Profiles on the named populations:
//
//	go test -tags=noui,nolocalllm ./pkg/storage -run '^$' -bench \
//		'BenchmarkSnapshotCompactionQualification/fixture=properties-40000x20480B/mode=overlap' \
//		-benchtime=1x -cpuprofile /tmp/b29-cpu.prof -memprofile /tmp/b29-heap.prof
//	go tool pprof -top -nodecount=20 /tmp/b29-cpu.prof
//	go tool pprof -sample_index=alloc_space -top -nodecount=20 /tmp/b29-heap.prof

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/metrics"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
)

const (
	// snapshotQualificationWriters is the number of concurrent foreground
	// writers in mode=overlap and in the no-compaction baseline.
	snapshotQualificationWriters = 4
	// snapshotQualificationWritePace bounds the foreground write rate per
	// writer so the probe measures write latency under snapshot pressure
	// instead of becoming a write-throughput benchmark.
	snapshotQualificationWritePace = 20 * time.Millisecond
	// snapshotQualificationBaselineWrites is the number of foreground writes
	// measured with no compaction in flight (per writer it is this divided by
	// snapshotQualificationWriters).
	snapshotQualificationBaselineWrites = 200
	// snapshotQualificationSampleInterval is how often the operation's Go heap
	// and its directory sizes are sampled.
	snapshotQualificationSampleInterval = 100 * time.Millisecond
	// snapshotQualificationEdgeBatch bounds one public bulk-create call.
	snapshotQualificationEdgeBatch = 1000
	// snapshotQualificationCreateEvery makes every Nth foreground write a
	// create, so the post-cut WAL tail holds both update and create records.
	snapshotQualificationCreateEvery = 25
)

// snapshotQualificationFixture is one named synthetic population. Nodes carry
// one property of propertyBytes bytes; a vector fixture also gives every node
// managed vectors stored beside its body (the sidecar the snapshot must carry).
type snapshotQualificationFixture struct {
	name          string
	nodes         int
	edges         int
	propertyBytes int
	vectorChunks  int
	vectorDims    int
	batch         int
}

// snapshotQualificationFixtures names the populations this qualification
// covers. The first is the production-size population the pinned-compaction
// document admits is unqualified (40,000 nodes with a ~20 KB property); the
// second is the bounded vector-heavy case.
func snapshotQualificationFixtures() []snapshotQualificationFixture {
	return []snapshotQualificationFixture{
		{name: "properties-40000x20480B", nodes: 40000, edges: 40000, propertyBytes: 20480, batch: 250},
		{name: "vectors-4000x8chunks-256dim", nodes: 4000, edges: 1000, propertyBytes: 256, vectorChunks: 8, vectorDims: 256, batch: 250},
	}
}

// snapshotQualification owns one throwaway store: a native Badger engine behind
// the WAL-backed engine, so the population is written and journalled through
// the same public path production uses.
type snapshotQualification struct {
	spec        snapshotQualificationFixture
	storeDir    string
	walDir      string
	snapshotDir string
	recoveryDir string
	engine      *BadgerEngine
	journal     *WALEngine
	ids         []NodeID
}

func openSnapshotQualification(b *testing.B, spec snapshotQualificationFixture) *snapshotQualification {
	b.Helper()
	root := b.TempDir()
	q := &snapshotQualification{
		spec:        spec,
		storeDir:    filepath.Join(root, "store"),
		walDir:      filepath.Join(root, "wal"),
		snapshotDir: filepath.Join(root, "snapshots"),
		recoveryDir: filepath.Join(root, "recovered"),
	}
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(q.storeDir))
	if err != nil {
		b.Fatalf("open qualification store: %v", err)
	}
	q.engine = engine
	if err := os.MkdirAll(q.snapshotDir, 0o755); err != nil {
		b.Fatalf("create snapshot directory: %v", err)
	}
	wal, err := NewWAL(q.walDir, DefaultWALConfig())
	if err != nil {
		b.Fatalf("open qualification journal: %v", err)
	}
	fixtureowner.OwnChecked(b, wal)
	q.journal = fixtureowner.OwnChecked(b, NewWALEngine(engine, wal))
	// The auto-compaction loop is what normally sets this; the loop itself
	// stays disabled so this harness times exactly one compaction.
	q.journal.snapshotDir = q.snapshotDir
	return q
}

// snapshotQualificationPayload is the deterministic large property value of
// node index.
func snapshotQualificationPayload(index, size int) string {
	head := fmt.Sprintf("node-%08d;", index)
	if size <= len(head) {
		return head[:max(size, 0)]
	}
	const filler = "0123456789abcdefghijklmnopqrstuvwxyz"
	return head + strings.Repeat(filler, (size-len(head))/len(filler)+1)[:size-len(head)]
}

// ingest builds the named population through the public WAL-backed writes and
// leaves it quiescent. It is never part of a timed region.
func (q *snapshotQualification) ingest(b *testing.B) {
	b.Helper()
	ctx := b.Context()
	started := time.Now()
	q.ids = make([]NodeID, q.spec.nodes)
	for i := range q.ids {
		q.ids[i] = NodeID(fmt.Sprintf("bench:%08d", i))
	}
	for base := 0; base < q.spec.nodes; base += q.spec.batch {
		end := min(base+q.spec.batch, q.spec.nodes)
		batch := make([]*Node, 0, end-base)
		for i := base; i < end; i++ {
			batch = append(batch, &Node{
				ID:     q.ids[i],
				Labels: []string{"Bench"},
				Properties: map[string]any{
					"payload": snapshotQualificationPayload(i, q.spec.propertyBytes),
					"index":   int64(i),
				},
			})
		}
		if err := q.journal.BulkCreateNodes(ctx, batch); err != nil {
			b.Fatalf("ingest nodes %d..%d: %v", base, end, err)
		}
		if err := q.attachVectors(b, batch); err != nil {
			b.Fatalf("ingest vectors %d..%d: %v", base, end, err)
		}
	}
	for base := 0; base < q.spec.edges; base += snapshotQualificationEdgeBatch {
		end := min(base+snapshotQualificationEdgeBatch, q.spec.edges)
		batch := make([]*Edge, 0, end-base)
		for i := base; i < end; i++ {
			batch = append(batch, &Edge{
				ID:        EdgeID(fmt.Sprintf("bench:e%08d", i)),
				StartNode: q.ids[i%q.spec.nodes],
				EndNode:   q.ids[(i*7+3)%q.spec.nodes],
				Type:      "LINK",
				Properties: map[string]any{
					"weight": float64(i%97) / 97,
					"label":  fmt.Sprintf("edge-%08d", i),
				},
			})
		}
		if err := q.journal.BulkCreateEdges(ctx, batch); err != nil {
			b.Fatalf("ingest edges %d..%d: %v", base, end, err)
		}
	}
	b.Logf("fixture=%s ingest: %s; store %d MiB, wal %d MiB",
		q.spec.name, time.Since(started).Round(time.Millisecond),
		snapshotDirBytes(q.storeDir)>>20, snapshotDirBytes(q.walDir)>>20)
}

// attachVectors gives every node of batch the managed vectors of the named
// vector fixture, through the storage call the embedding worker uses.
func (q *snapshotQualification) attachVectors(b *testing.B, batch []*Node) error {
	if q.spec.vectorChunks == 0 {
		return nil
	}
	vectors := snapshotSidecarFixtureVectors(q.spec.vectorChunks, q.spec.vectorDims)
	for _, node := range batch {
		stored, err := q.journal.GetNode(b.Context(), node.ID)
		if err != nil {
			return err
		}
		stored.ChunkEmbeddings = vectors
		stored.EmbedMeta = map[string]any{
			"has_embedding":        true,
			"chunk_count":          q.spec.vectorChunks,
			"embedding_dimensions": q.spec.vectorDims,
			"embedding_model":      "synthetic",
		}
		if err := q.journal.UpdateNodeEmbeddingSidecar(b.Context(), stored); err != nil {
			return err
		}
	}
	return nil
}

// snapshotNames is the set of snapshot files that already exist, so the
// operation's own file can be identified by difference.
func (q *snapshotQualification) snapshotNames(b testing.TB) map[string]bool {
	b.Helper()
	entries, err := os.ReadDir(q.snapshotDir)
	if err != nil {
		b.Fatalf("read snapshot directory: %v", err)
	}
	names := make(map[string]bool, len(entries))
	for _, entry := range entries {
		names[entry.Name()] = true
	}
	return names
}

// newSnapshot returns the one snapshot file written since before, its size and
// its record counts, and fails when the operation wrote none or more than one.
func (q *snapshotQualification) newSnapshot(b testing.TB, before map[string]bool) (string, int64, SnapshotMetadata) {
	b.Helper()
	entries, err := os.ReadDir(q.snapshotDir)
	if err != nil {
		b.Fatalf("read snapshot directory: %v", err)
	}
	var path string
	for _, entry := range entries {
		if before[entry.Name()] || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if path != "" {
			b.Fatalf("compaction wrote more than one snapshot: %s and %s", path, entry.Name())
		}
		path = filepath.Join(q.snapshotDir, entry.Name())
	}
	if path == "" {
		b.Fatal("compaction wrote no snapshot file")
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatalf("stat snapshot: %v", err)
	}
	streaming, err := IsStreamingSnapshot(path)
	if err != nil {
		b.Fatalf("inspect snapshot: %v", err)
	}
	if !streaming {
		b.Fatalf("%s is not a streaming snapshot", path)
	}
	return path, info.Size(), snapshotRecordMetadata(b, path)
}

// snapshotRecordMetadata counts the frames the written snapshot holds, so the
// snapshot file's contents are evidence and not only its size.
func snapshotRecordMetadata(b testing.TB, path string) SnapshotMetadata {
	b.Helper()
	file, err := os.Open(path)
	if err != nil {
		b.Fatalf("open snapshot for read-back: %v", err)
	}
	defer file.Close()
	metadata, err := ReadSnapshot(b.Context(), file, snapshotDiscardVisitor{})
	if err != nil {
		b.Fatalf("read back snapshot: %v", err)
	}
	return metadata
}

// snapshotDiscardVisitor counts frames without retaining them, so reading back a
// production-size snapshot does not build a second copy of the population.
type snapshotDiscardVisitor struct{}

func (snapshotDiscardVisitor) VisitNode(context.Context, *Node) error { return nil }
func (snapshotDiscardVisitor) VisitEdge(context.Context, *Edge) error { return nil }

// compact runs one real compaction (snapshot file, WAL truncation, pruning) and
// returns its duration, the file it wrote and that file's record counts. It
// fails when the compaction wrote no new snapshot, because an unadvanced cut
// would make the measurement meaningless.
func (q *snapshotQualification) compact(b testing.TB, before map[string]bool) (time.Duration, string, int64, SnapshotMetadata) {
	b.Helper()
	statsBefore, _ := q.journal.GetSnapshotStats()
	started := time.Now()
	if err := q.journal.createSnapshotAndCompact(b.Context(), saveCompactionSnapshot); err != nil {
		b.Fatalf("compaction: %v", err)
	}
	elapsed := time.Since(started)
	statsAfter, _ := q.journal.GetSnapshotStats()
	if statsAfter != statsBefore+1 {
		b.Fatalf("compaction recorded %d snapshots, want %d (no snapshot was written)", statsAfter, statsBefore+1)
	}
	path, size, metadata := q.newSnapshot(b, before)
	return elapsed, path, size, metadata
}

// snapshotMemoryCounters is the allocation side of one operation sample.
type snapshotMemoryCounters struct {
	totalAlloc uint64
	heapAlloc  uint64
	mallocs    uint64
	numGC      uint32
	pauseTotal uint64
}

func readSnapshotMemoryCounters() snapshotMemoryCounters {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return snapshotMemoryCounters{
		totalAlloc: stats.TotalAlloc,
		heapAlloc:  stats.HeapAlloc,
		mallocs:    stats.Mallocs,
		numGC:      stats.NumGC,
		pauseTotal: stats.PauseTotalNs,
	}
}

// readSnapshotLiveHeap returns the reachable heap after a collection, which is
// what "retained" means here: what the operation made stay alive.
func readSnapshotLiveHeap() uint64 {
	runtime.GC()
	return readSnapshotMemoryCounters().heapAlloc
}

// snapshotProcessPeakRSS is the process high-water resident set: bytes on
// Darwin, kilobytes on Linux.
func snapshotProcessPeakRSS() (uint64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	if runtime.GOOS == "darwin" {
		return uint64(usage.Maxrss), nil
	}
	return uint64(usage.Maxrss) * 1024, nil
}

// snapshotDirBytes is the size of the regular files under dir. It is a
// best-effort sample of a live directory, so a file that disappears between
// listing and stat (Badger compaction and value-log GC do that) is skipped
// rather than failing the measurement.
func snapshotDirBytes(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// snapshotStoreData is what Badger's LSM holds: the table bytes of every level
// and the stale bytes those tables still carry. A live pinned read point keeps
// the versions it can still read, so stale bytes that compaction cannot drop
// are the retained versions of a snapshot, measured independently of the
// directory's preallocated files.
type snapshotStoreData struct {
	lsm   int64
	stale int64
}

func snapshotStoreDataBytes(engine *BadgerEngine) snapshotStoreData {
	var data snapshotStoreData
	for _, level := range engine.db.Levels() {
		data.lsm += level.Size
		data.stale += level.StaleDatSize
	}
	return data
}

// snapshotResourcePeaks is what the sampler saw while the operation ran.
type snapshotResourcePeaks struct {
	storeBytes      int64
	storeDataBytes  int64
	storeStaleBytes int64
	walBytes        int64
	snapshotBytes   int64
	heapBytes       uint64
	heapTotalBytes  uint64
}

// snapshotResourceSampler samples the Go heap and the three directories so the
// operation's peak is distinguished from its end state.
type snapshotResourceSampler struct {
	engine                *BadgerEngine
	store, wal, snapshots string
	stop, done            chan struct{}
	once                  sync.Once
	peaks                 snapshotResourcePeaks
}

func startSnapshotResourceSampler(engine *BadgerEngine, store, wal, snapshots string) *snapshotResourceSampler {
	sampler := &snapshotResourceSampler{
		engine: engine, store: store, wal: wal, snapshots: snapshots,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go sampler.run()
	return sampler
}

func (s *snapshotResourceSampler) run() {
	defer close(s.done)
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
	}
	ticker := time.NewTicker(snapshotQualificationSampleInterval)
	defer ticker.Stop()
	record := func() {
		stored := snapshotStoreDataBytes(s.engine)
		s.peaks.storeDataBytes = max(s.peaks.storeDataBytes, stored.lsm)
		s.peaks.storeStaleBytes = max(s.peaks.storeStaleBytes, stored.stale)
		s.peaks.storeBytes = max(s.peaks.storeBytes, snapshotDirBytes(s.store))
		s.peaks.walBytes = max(s.peaks.walBytes, snapshotDirBytes(s.wal))
		s.peaks.snapshotBytes = max(s.peaks.snapshotBytes, snapshotDirBytes(s.snapshots))
		metrics.Read(samples)
		s.peaks.heapBytes = max(s.peaks.heapBytes, samples[0].Value.Uint64())
		s.peaks.heapTotalBytes = max(s.peaks.heapTotalBytes, samples[1].Value.Uint64())
	}
	record()
	for {
		select {
		case <-s.stop:
			record()
			return
		case <-ticker.C:
			record()
		}
	}
}

func (s *snapshotResourceSampler) finish() snapshotResourcePeaks {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	return s.peaks
}

// snapshotForegroundSamples is the foreground write cost measured in one window.
type snapshotForegroundSamples struct {
	ops       int
	creates   int
	latencies []time.Duration
	window    time.Duration
}

// snapshotForegroundRun is a set of running foreground writers.
type snapshotForegroundRun struct {
	q       *snapshotQualification
	ctx     context.Context
	stop    chan struct{}
	wrote   chan error
	wg      sync.WaitGroup
	once    sync.Once
	mu      sync.Mutex
	samples snapshotForegroundSamples
	errs    []error
	start   time.Time
	limit   int
}

// startSnapshotForeground starts writers paced at snapshotQualificationWritePace.
// A positive limit makes each writer perform that many writes and return;
// otherwise they run until the window is closed. Every writer is joined by
// stopAndCollect, so none outlives the window it belongs to.
func startSnapshotForeground(b *testing.B, q *snapshotQualification, era string, limit int) *snapshotForegroundRun {
	b.Helper()
	run := &snapshotForegroundRun{
		q: q, ctx: b.Context(), stop: make(chan struct{}),
		wrote: make(chan error, snapshotQualificationWriters),
		start: time.Now(), limit: limit,
	}
	run.wg.Add(snapshotQualificationWriters)
	b.Cleanup(run.stopAndJoin)
	for writer := 0; writer < snapshotQualificationWriters; writer++ {
		go run.write(writer, era, limit)
	}
	if limit <= 0 {
		// A snapshot only advances once the journal holds a record past the
		// previous cut, so overlap mode waits for each writer's first write.
		for writer := 0; writer < snapshotQualificationWriters; writer++ {
			select {
			case err := <-run.wrote:
				if err != nil {
					b.Fatalf("foreground first write: %v", err)
				}
			case <-time.After(time.Minute):
				b.Fatal("foreground writer did not complete its first write")
			}
		}
	}
	return run
}

func (run *snapshotForegroundRun) write(writer int, era string, limit int) {
	defer run.wg.Done()
	latencies := make([]time.Duration, 0, 1024)
	creates := 0
	var errs []error
	signalled := false
	shard := len(run.q.ids) / snapshotQualificationWriters
	for seq := 0; limit <= 0 || seq < limit; seq++ {
		select {
		case <-run.stop:
			run.record(latencies, creates, errs)
			return
		default:
		}
		latency, created, err := run.q.foregroundWrite(run.ctx, writer, seq, shard, era)
		if err != nil {
			errs = append(errs, fmt.Errorf("writer %d write %d: %w", writer, seq, err))
			break
		}
		latencies = append(latencies, latency)
		if created {
			creates++
		}
		if !signalled {
			signalled = true
			run.wrote <- nil
		}
		if limit > 0 && seq == limit-1 {
			break
		}
		select {
		case <-run.stop:
			run.record(latencies, creates, errs)
			return
		case <-time.After(snapshotQualificationWritePace):
		}
	}
	if !signalled && len(errs) > 0 {
		run.wrote <- errs[0]
	}
	run.record(latencies, creates, errs)
}

func (run *snapshotForegroundRun) record(latencies []time.Duration, creates int, errs []error) {
	run.mu.Lock()
	defer run.mu.Unlock()
	run.samples.latencies = append(run.samples.latencies, latencies...)
	run.samples.ops += len(latencies)
	run.samples.creates += creates
	run.errs = append(run.errs, errs...)
}

// stopAndJoin is also registered before startup, so failed admission or a
// later fatal assertion cannot leave writers using the fixture during cleanup.
func (run *snapshotForegroundRun) stopAndJoin() {
	run.once.Do(func() { close(run.stop) })
	run.wg.Wait()
}

// stopAndCollect ends the window, joins the writers and returns what they saw.
func (run *snapshotForegroundRun) stopAndCollect(b *testing.B) snapshotForegroundSamples {
	b.Helper()
	if run.limit <= 0 {
		// A window that runs until stopped is ended here, after which the
		// writers exit on their own.
		run.stopAndJoin()
	} else {
		run.wg.Wait()
	}
	run.samples.window = time.Since(run.start)
	run.mu.Lock()
	defer run.mu.Unlock()
	if len(run.errs) != 0 {
		b.Fatalf("foreground writes failed: %v", run.errs[0])
	}
	out := run.samples
	out.latencies = append([]time.Duration(nil), run.samples.latencies...)
	return out
}

// foregroundWrite performs one public foreground write and returns the latency
// of the storage mutation call itself. GetNode is excluded: the probe measures
// the write path the snapshot must not stall, not the read that produces the
// observed image. Writer shards never touch the same node, so every sampled
// write is a real public write and not a transaction conflict.
func (q *snapshotQualification) foregroundWrite(ctx context.Context, writer, seq, shard int, era string) (latency time.Duration, created bool, err error) {
	if snapshotQualificationCreateEvery > 0 && seq > 0 && seq%snapshotQualificationCreateEvery == 0 {
		started := time.Now()
		_, err := q.journal.CreateNode(ctx, &Node{
			ID:         NodeID(fmt.Sprintf("bench-fg:%s:%d:%06d", era, writer, seq)),
			Labels:     []string{"Foreground"},
			Properties: map[string]any{"written": int64(seq)},
		})
		return time.Since(started), true, err
	}
	id := q.ids[writer+snapshotQualificationWriters*(seq%shard)]
	node, err := q.journal.GetNode(ctx, id)
	if err != nil {
		return 0, false, err
	}
	node.Properties["touch"] = fmt.Sprintf("%s-%d-%d", era, writer, seq)
	started := time.Now()
	err = q.journal.UpdateNode(ctx, NodeUpdateRequest{After: node})
	return time.Since(started), false, err
}

// snapshotLatencySummary returns nearest-rank percentiles and the maximum.
func snapshotLatencySummary(samples []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(percent int) time.Duration {
		return sorted[(len(sorted)-1)*percent/100]
	}
	return at(50), at(95), at(99), sorted[len(sorted)-1]
}

// snapshotOperationResult is everything one measured operation produced.
type snapshotOperationResult struct {
	duration          time.Duration
	snapshotPath      string
	snapshotBytes     int64
	snapshotRecords   SnapshotMetadata
	snapshotDir       int64
	snapshotDirPeak   int64
	storeBefore       int64
	storeAfter        int64
	storePeak         int64
	storeDataBefore   int64
	storeDataAfter    int64
	storeDataPeak     int64
	staleBefore       int64
	staleAfter        int64
	stalePeak         int64
	walBefore         int64
	walAfter          int64
	walPeak           int64
	heapLiveBefore    uint64
	heapLiveAfter     uint64
	heapPeak          uint64
	heapTotalPeak     uint64
	cumulativeAlloc   uint64
	cumulativeMallocs uint64
	gcCycles          uint32
	gcPause           time.Duration
	rssBefore         uint64
	rssAfter          uint64
	writes            snapshotForegroundSamples
}

// runOperation measures one full compaction. b.StopTimer/b.StartTimer bound the
// reported ns/op to the compaction call itself: fixture state, foreground
// writer start-up, writer join and every resource sample are outside it.
func (q *snapshotQualification) runOperation(b *testing.B, overlap bool) snapshotOperationResult {
	b.Helper()
	b.StopTimer()
	existing := q.snapshotNames(b)
	statsBefore, _ := q.journal.GetSnapshotStats()
	heapLiveBefore := readSnapshotLiveHeap()
	storeBefore, walBefore := snapshotDirBytes(q.storeDir), snapshotDirBytes(q.walDir)
	storedBefore := snapshotStoreDataBytes(q.engine)
	var run *snapshotForegroundRun
	if overlap {
		run = startSnapshotForeground(b, q, "overlap", 0)
	} else {
		// The control needs its own journal record past the previous cut: an
		// idle cut that equals the compacted sequence performs no snapshot at
		// all (TestAutoCompactionSkipsSnapshotWhenWALHasNotAdvanced).
		if _, _, err := q.foregroundWrite(b.Context(), 0, 0, 1, "idle"); err != nil {
			b.Fatalf("advance journal cut: %v", err)
		}
	}
	rssBefore, err := snapshotProcessPeakRSS()
	if err != nil {
		b.Fatalf("read process peak RSS: %v", err)
	}
	sampler := startSnapshotResourceSampler(q.engine, q.storeDir, q.walDir, q.snapshotDir)
	defer sampler.finish()
	before := readSnapshotMemoryCounters()
	b.StartTimer()
	started := time.Now()
	err = q.journal.createSnapshotAndCompact(b.Context(), saveCompactionSnapshot)
	elapsed := time.Since(started)
	b.StopTimer()
	// Capture operation costs before read-back validation decodes the full file.
	after := readSnapshotMemoryCounters()
	rssAfter, rssErr := snapshotProcessPeakRSS()
	peaks := sampler.finish()
	var writes snapshotForegroundSamples
	if run != nil {
		writes = run.stopAndCollect(b)
	}
	if err != nil {
		b.Fatalf("compaction: %v", err)
	}
	if statsAfter, _ := q.journal.GetSnapshotStats(); statsAfter != statsBefore+1 {
		b.Fatalf("compaction recorded %d snapshots, want %d", statsAfter, statsBefore+1)
	}
	if rssErr != nil {
		b.Fatalf("read process peak RSS: %v", rssErr)
	}
	heapLiveAfter := readSnapshotLiveHeap()
	path, size, records := q.newSnapshot(b, existing)
	storedAfter := snapshotStoreDataBytes(q.engine)
	return snapshotOperationResult{
		duration:          elapsed,
		snapshotPath:      path,
		snapshotBytes:     size,
		snapshotRecords:   records,
		snapshotDir:       snapshotDirBytes(q.snapshotDir),
		snapshotDirPeak:   peaks.snapshotBytes,
		storeBefore:       storeBefore,
		storeAfter:        snapshotDirBytes(q.storeDir),
		storePeak:         peaks.storeBytes,
		storeDataBefore:   storedBefore.lsm,
		storeDataAfter:    storedAfter.lsm,
		storeDataPeak:     peaks.storeDataBytes,
		staleBefore:       storedBefore.stale,
		staleAfter:        storedAfter.stale,
		stalePeak:         peaks.storeStaleBytes,
		walBefore:         walBefore,
		walAfter:          snapshotDirBytes(q.walDir),
		walPeak:           peaks.walBytes,
		heapLiveBefore:    heapLiveBefore,
		heapLiveAfter:     heapLiveAfter,
		heapPeak:          peaks.heapBytes,
		heapTotalPeak:     peaks.heapTotalBytes,
		cumulativeAlloc:   after.totalAlloc - before.totalAlloc,
		cumulativeMallocs: after.mallocs - before.mallocs,
		gcCycles:          after.numGC - before.numGC,
		gcPause:           time.Duration(after.pauseTotal - before.pauseTotal),
		rssBefore:         rssBefore,
		rssAfter:          rssAfter,
		writes:            writes,
	}
}

// snapshotEntityStream pulls one stored entity at a time from a callback
// stream, so two populations can be compared element by element without
// materializing either of them.
type snapshotEntityStream[T any] struct {
	items   chan *T
	failure chan error
	done    chan struct{}
	cancel  context.CancelFunc
}

func streamSnapshotEntities[T any](ctx context.Context, stream func(context.Context, func(*T) error) error) *snapshotEntityStream[T] {
	streamCtx, cancel := context.WithCancel(ctx)
	s := &snapshotEntityStream[T]{
		items: make(chan *T), failure: make(chan error, 1),
		done: make(chan struct{}), cancel: cancel,
	}
	go func() {
		defer close(s.done)
		err := stream(streamCtx, func(entity *T) error {
			select {
			case s.items <- entity:
				return nil
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			s.failure <- err
		}
	}()
	return s
}

// next returns the next entity, or nil once the stream has ended.
func (s *snapshotEntityStream[T]) next() (*T, error) {
	select {
	case entity := <-s.items:
		return entity, nil
	case <-s.done:
		select {
		case err := <-s.failure:
			return nil, err
		default:
			return nil, nil
		}
	}
}

func (s *snapshotEntityStream[T]) close() {
	s.cancel()
	<-s.done
}

// snapshotStoredNodeStream is the stored-inclusive node stream a snapshot and
// the live store both answer with, embeddings included.
func snapshotStoredNodeStream(ctx context.Context, engine Engine) *snapshotEntityStream[Node] {
	return streamSnapshotEntities(ctx, func(ctx context.Context, visit func(*Node) error) error {
		return StreamStoredNodes(ctx, engine, StreamNodesOptions{WithEmbeddings: true}, visit)
	})
}

func snapshotStoredEdgeStream(ctx context.Context, engine Engine) *snapshotEntityStream[Edge] {
	return streamSnapshotEntities(ctx, func(ctx context.Context, visit func(*Edge) error) error {
		return StreamStoredEdgesByType(ctx, engine, "", visit)
	})
}

// snapshotStoredEqual reports whether two stored property values are the same
// value, using the storage layer's own stored-value equality (sameStoredValue):
// numeric values compare exactly whatever their width, temporal values compare
// as instants, and NaN compares by bits.
func snapshotStoredEqual(tb testing.TB, what string, want, got any) {
	tb.Helper()
	if !sameStoredValue(want, got) {
		tb.Fatalf("%s: want %T=%v, got %T=%v", what, want, want, got, got)
	}
}

func snapshotStoredValuesEqual(tb testing.TB, what string, want, got map[string]any) {
	tb.Helper()
	if len(want) != len(got) {
		tb.Fatalf("%s: want %d values %v, got %d values %v", what, len(want), want, len(got), got)
	}
	for key, value := range want {
		other, ok := got[key]
		if !ok {
			tb.Fatalf("%s: recovered value set is missing %q", what, key)
		}
		snapshotStoredEqual(tb, what+" "+key, value, other)
	}
}

// compareSnapshotNode compares one recovered node with the node the live store
// held: properties by stored value, managed and named vectors bit for bit, and
// the semantic embedding metadata. Node timestamps are outside the compared
// contract, as they are for requireSameNode.
func compareSnapshotNode(tb testing.TB, want, got *Node) {
	tb.Helper()
	if want.ID != got.ID {
		tb.Fatalf("node population order differs: want %s, got %s", want.ID, got.ID)
	}
	if !slices.Equal(want.Labels, got.Labels) {
		tb.Fatalf("node %s labels: want %v, got %v", want.ID, want.Labels, got.Labels)
	}
	snapshotStoredValuesEqual(tb, fmt.Sprintf("node %s properties", want.ID), want.Properties, got.Properties)
	if !reflect.DeepEqual(rrvBits(want.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings)) {
		tb.Fatalf("node %s managed vectors: want %d chunks, got %d chunks", want.ID, len(want.ChunkEmbeddings), len(got.ChunkEmbeddings))
	}
	if !reflect.DeepEqual(rrvNamedBits(want.NamedEmbeddings), rrvNamedBits(got.NamedEmbeddings)) {
		tb.Fatalf("node %s named vectors differ", want.ID)
	}
	for _, key := range []string{"has_embedding", "chunk_count", "embedding_dimensions", "embedding_model"} {
		wantValue, wantOK := want.EmbedMeta[key]
		gotValue, gotOK := got.EmbedMeta[key]
		if wantOK != gotOK {
			tb.Fatalf("node %s embedding metadata %q: want present %v, got present %v", want.ID, key, wantOK, gotOK)
		}
		if wantOK {
			snapshotStoredEqual(tb, fmt.Sprintf("node %s embedding metadata %q", want.ID, key), wantValue, gotValue)
		}
	}
}

func compareSnapshotEdge(tb testing.TB, want, got *Edge) {
	tb.Helper()
	if want.ID != got.ID || want.StartNode != got.StartNode || want.EndNode != got.EndNode || want.Type != got.Type {
		tb.Fatalf("edge %s: want %s->%s %s, got %s->%s %s", want.ID, want.StartNode, want.EndNode, want.Type, got.StartNode, got.EndNode, got.Type)
	}
	snapshotStoredValuesEqual(tb, fmt.Sprintf("edge %s properties", want.ID), want.Properties, got.Properties)
}

// compareSnapshotPopulations streams both stores and compares every stored node
// and relationship the live store holds with the recovered one, returning how
// many it compared. A missing, extra or different entity fails the benchmark.
func compareSnapshotPopulations(tb testing.TB, live, recovered Engine) (nodes, edges, managedChunks uint64) {
	tb.Helper()
	ctx := tb.Context()
	liveNodes, recoveredNodes := snapshotStoredNodeStream(ctx, live), snapshotStoredNodeStream(ctx, recovered)
	defer liveNodes.close()
	defer recoveredNodes.close()
	for {
		want, err := liveNodes.next()
		if err != nil {
			tb.Fatalf("stream live nodes: %v", err)
		}
		got, err := recoveredNodes.next()
		if err != nil {
			tb.Fatalf("stream recovered nodes: %v", err)
		}
		if want == nil || got == nil {
			if want != nil || got != nil {
				tb.Fatalf("recovered node population ends at %d nodes (live continues)", nodes)
			}
			break
		}
		compareSnapshotNode(tb, want, got)
		managedChunks += uint64(len(want.ChunkEmbeddings))
		nodes++
	}
	liveEdges, recoveredEdges := snapshotStoredEdgeStream(ctx, live), snapshotStoredEdgeStream(ctx, recovered)
	defer liveEdges.close()
	defer recoveredEdges.close()
	for {
		want, err := liveEdges.next()
		if err != nil {
			tb.Fatalf("stream live edges: %v", err)
		}
		got, err := recoveredEdges.next()
		if err != nil {
			tb.Fatalf("stream recovered edges: %v", err)
		}
		if want == nil || got == nil {
			if want != nil || got != nil {
				tb.Fatalf("recovered edge population ends at %d edges (live continues)", edges)
			}
			break
		}
		compareSnapshotEdge(tb, want, got)
		edges++
	}
	return nodes, edges, managedChunks
}

// snapshotRecoveryResult is the exact-recovery evidence of one operation.
type snapshotRecoveryResult struct {
	duration      time.Duration
	walEntries    uint64
	nodes         uint64
	edges         uint64
	managedChunks uint64
	snapshotNodes uint64
	snapshotEdges uint64
}

// recoverAndCompare recovers a fresh store from the quiescent journal and
// snapshot, compares fixture fields with the still-open live store, then closes
// the measured store. It is not a killed-process recovery check.
func (q *snapshotQualification) recoverAndCompare(b *testing.B, result snapshotOperationResult) snapshotRecoveryResult {
	b.Helper()
	b.StopTimer()
	destination, err := fixtureowner.OwnResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(q.recoveryDir))
	if err != nil {
		b.Fatalf("open recovery store: %v", err)
	}
	started := time.Now()
	replayed, status, err := RecoverIntoEngine(b.Context(), destination, q.walDir, result.snapshotPath)
	elapsed := time.Since(started)
	if err != nil {
		b.Fatalf("recover into fresh store: %v", err)
	}
	if replayed.Failed != 0 {
		b.Fatalf("recovery failed %d records: %v", replayed.Failed, replayed.Errors)
	}
	// The journal is quiescent (every writer is joined) and its records are
	// flushed, so this recovery reads the same bytes a sealed journal holds.
	nodes, edges, managedChunks := compareSnapshotPopulations(b, q.journal, destination)
	if q.spec.vectorChunks > 0 && managedChunks == 0 {
		b.Fatal("vector qualification compared no populated managed-vector chunks")
	}
	b.Logf("fixture=%s compared managed-vector chunks: %d", q.spec.name, managedChunks)
	if err := q.journal.Close(); err != nil {
		b.Fatalf("close measured store: %v", err)
	}
	b.Logf("fixture=%s recovery: %s; exactly recovered %d nodes/%d edges (%d snapshot records, %d journal records), store %.1f MiB",
		q.spec.name, elapsed.Round(time.Millisecond), nodes, edges,
		status.SnapshotNodes, status.WALEntries, float64(snapshotDirBytes(q.storeDir))/(1<<20))
	return snapshotRecoveryResult{
		duration: elapsed, walEntries: status.WALEntries,
		nodes: nodes, edges: edges, managedChunks: managedChunks,
		snapshotNodes: status.SnapshotNodes, snapshotEdges: status.SnapshotEdges,
	}
}

// reportSnapshotQualification reports one operation's metrics. ReportMetric
// keeps the last iteration's value, which is the intended reading for the
// one-operation (-benchtime=1x) sample this harness is written for.
func reportSnapshotQualification(b *testing.B, spec snapshotQualificationFixture, overlap bool, warmup time.Duration, result snapshotOperationResult, baseline snapshotForegroundSamples) {
	mode := "idle-foreground"
	if overlap {
		mode = "overlap"
	}
	p50, p95, p99, maxLatency := snapshotLatencySummary(result.writes.latencies)
	base50, base95, base99, baseMax := snapshotLatencySummary(baseline.latencies)
	mib := func(bytes int64) float64 { return float64(bytes) / (1 << 20) }
	b.ReportMetric(float64(warmup.Milliseconds()), "warmup-ms")
	b.ReportMetric(float64(result.duration.Milliseconds()), "snapshot-ms")
	b.ReportMetric(mib(result.snapshotBytes), "snapshot-MiB")
	b.ReportMetric(float64(result.snapshotRecords.NodeCount), "snapshot-nodes")
	b.ReportMetric(float64(result.snapshotRecords.EdgeCount), "snapshot-edges")
	b.ReportMetric(mib(result.snapshotDir), "snapshotdir-MiB")
	b.ReportMetric(mib(result.snapshotDirPeak), "snapshotdir-peak-MiB")
	b.ReportMetric(mib(result.storeBefore), "store-before-MiB")
	b.ReportMetric(mib(result.storeAfter), "store-after-MiB")
	b.ReportMetric(mib(result.storePeak-result.storeBefore), "store-peak-extra-MiB")
	b.ReportMetric(mib(result.storeDataBefore), "storedata-before-MiB")
	b.ReportMetric(mib(result.storeDataAfter), "storedata-after-MiB")
	b.ReportMetric(mib(result.storeDataPeak-result.storeDataBefore), "storedata-peak-extra-MiB")
	b.ReportMetric(mib(result.staleBefore), "storedata-stale-before-MiB")
	b.ReportMetric(mib(result.staleAfter), "storedata-stale-after-MiB")
	b.ReportMetric(mib(result.stalePeak-result.staleBefore), "storedata-stale-peak-extra-MiB")
	b.ReportMetric(mib(result.walBefore), "wal-before-MiB")
	b.ReportMetric(mib(result.walAfter), "wal-after-MiB")
	b.ReportMetric(mib(result.walPeak-result.walBefore), "wal-peak-extra-MiB")
	b.ReportMetric(float64(result.heapPeak)/(1<<20), "peak-heap-MiB")
	b.ReportMetric(float64(result.heapTotalPeak)/(1<<20), "peak-heap-total-MiB")
	b.ReportMetric(float64(int64(result.heapLiveAfter)-int64(result.heapLiveBefore))/(1<<20), "retained-heap-MiB")
	b.ReportMetric(float64(result.cumulativeAlloc)/(1<<20), "cumul-alloc-MiB")
	b.ReportMetric(float64(result.cumulativeMallocs), "cumul-allocs")
	b.ReportMetric(float64(result.gcCycles), "gc-cycles")
	b.ReportMetric(float64(result.gcPause.Microseconds())/1e3, "gc-pause-ms")
	b.ReportMetric(float64(result.rssAfter)/(1<<20), "process-peak-rss-MiB")
	b.ReportMetric(float64(result.rssAfter-result.rssBefore)/(1<<20), "peak-rss-growth-MiB")
	b.ReportMetric(float64(result.writes.ops), "fg-ops")
	b.ReportMetric(float64(result.writes.creates), "fg-creates")
	b.ReportMetric(float64(p50.Nanoseconds())/1e3, "fg-p50-us")
	b.ReportMetric(float64(p95.Nanoseconds())/1e3, "fg-p95-us")
	b.ReportMetric(float64(p99.Nanoseconds())/1e3, "fg-p99-us")
	b.ReportMetric(float64(maxLatency.Nanoseconds())/1e3, "fg-max-us")
	b.ReportMetric(float64(base50.Nanoseconds())/1e3, "baseline-p50-us")
	b.ReportMetric(float64(base95.Nanoseconds())/1e3, "baseline-p95-us")
	b.ReportMetric(float64(base99.Nanoseconds())/1e3, "baseline-p99-us")
	b.ReportMetric(float64(baseMax.Nanoseconds())/1e3, "baseline-max-us")
	b.Logf("fixture=%s mode=%s (%d nodes/%d edges, one operation): snapshot %s, file %.1f MiB; foreground %d writes in %s p50 %s p95 %s p99 %s max %s; no-compaction baseline %d writes p99 %s max %s",
		spec.name, mode, spec.nodes, spec.edges,
		result.duration.Round(time.Millisecond), mib(result.snapshotBytes),
		result.writes.ops, result.writes.window.Round(time.Millisecond),
		p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), maxLatency.Round(time.Microsecond),
		baseline.ops, base99.Round(time.Microsecond), baseMax.Round(time.Microsecond))
}

// benchmarkSnapshotCompaction measures one named population in one mode. Each
// mode seeds and warms its own fresh store. This measures warm compaction,
// not a cold-filesystem comparison.
func benchmarkSnapshotCompaction(b *testing.B, spec snapshotQualificationFixture, overlap bool) {
	b.Helper()
	b.Cleanup(config.WithWALEnabled())
	q := openSnapshotQualification(b, spec)
	q.ingest(b)

	// Warmup: one complete compaction of this population before anything is
	// measured. Its cost is reported as warmup-ms and is not the operation.
	warmup, _, warmupBytes, warmupRecords := q.compact(b, q.snapshotNames(b))
	b.Logf("fixture=%s warmup snapshot (not measured): %s, file %.1f MiB, %d nodes/%d edges",
		spec.name, warmup.Round(time.Millisecond), float64(warmupBytes)/(1<<20),
		warmupRecords.NodeCount, warmupRecords.EdgeCount)

	// The control for foreground write latency: the same paced writers, same
	// count per writer, with no compaction in flight. One untimed write per
	// writer warms the path so the sample does not report first-write cost.
	if _, _, err := q.foregroundWrite(b.Context(), 0, 0, 1, "warm"); err != nil {
		b.Fatalf("foreground warmup: %v", err)
	}
	baselineRun := startSnapshotForeground(b, q, "baseline", snapshotQualificationBaselineWrites/snapshotQualificationWriters)
	baseline := baselineRun.stopAndCollect(b)
	_, _, baseP99, baseMax := snapshotLatencySummary(baseline.latencies)
	b.Logf("fixture=%s foreground baseline (no compaction): %d writes, p99 %s max %s",
		spec.name, baseline.ops, baseP99.Round(time.Microsecond), baseMax.Round(time.Microsecond))

	b.ResetTimer()
	var last snapshotOperationResult
	for i := 0; i < b.N; i++ {
		last = q.runOperation(b, overlap)
		reportSnapshotQualification(b, spec, overlap, warmup, last, baseline)
	}
	b.StopTimer()

	recovery := q.recoverAndCompare(b, last)
	b.ReportMetric(float64(recovery.duration.Milliseconds()), "recovery-ms")
	b.ReportMetric(float64(recovery.walEntries), "recovered-wal-records")
	b.ReportMetric(float64(recovery.nodes), "recovered-nodes")
	b.ReportMetric(float64(recovery.edges), "recovered-edges")
	b.ReportMetric(float64(recovery.managedChunks), "recovered-managed-chunks")
}

// BenchmarkSnapshotCompactionQualification is the B29 production-size
// qualification: each named population is measured with and without concurrent
// foreground writes, and each measured snapshot must recover the live
// population exactly.
func BenchmarkSnapshotCompactionQualification(b *testing.B) {
	for _, spec := range snapshotQualificationFixtures() {
		for _, mode := range []struct {
			name    string
			overlap bool
		}{{"idle-foreground", false}, {"overlap", true}} {
			b.Run(fmt.Sprintf("fixture=%s/mode=%s", spec.name, mode.name), func(b *testing.B) {
				benchmarkSnapshotCompaction(b, spec, mode.overlap)
			})
		}
	}
}
