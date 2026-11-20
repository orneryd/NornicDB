package bolt

// Part D (real disk) additions: an untracked fixture, byte-identical on both arms,
// shipped with zz_whole_head_ab_bench_test.go and the large-node fixture
// zz_large_node_update_ab_bench_test.go, whose helpers it uses.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ---- Part D (real disk) additions: storage facts, peak resident memory ----

var rdOnce sync.Once

// rdStoreDir is where every store of this binary goes (b.TempDir follows
// TMPDIR): a directory on the lab's ZFS dataset when the container has one
// (/work/verify, then /work/tnrun), else the runner's TMPDIR unchanged. The
// runner sets TMPDIR and GOTMPDIR itself (to its tmpfs); testing.TempDir uses
// GOTMPDIR, and an A/B's --env TMPDIR does not reach the benchmark process,
// so the fixture sets both here.
var rdStoreDir = func() string {
	for _, base := range []string{"/work/verify", "/work/tnrun"} {
		if st, err := os.Stat(base); err != nil || !st.IsDir() {
			continue
		}
		dir := filepath.Join(base, "zz-partd-tmp")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			continue
		}
		if f, err := os.CreateTemp(dir, "probe-*"); err == nil {
			name := f.Name()
			_ = f.Close()
			_ = os.Remove(name)
			_ = os.Setenv("TMPDIR", dir)
			_ = os.Setenv("GOTMPDIR", dir) // testing.TempDir uses GOTMPDIR (the runner sets it to /tmp)
			return dir
		}
	}
	return os.TempDir()
}()

func rdStatfs(dir string) (int64, uint64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(dir, &s); err != nil {
		return 0, 0, err
	}
	return int64(s.Type), uint64(s.Bavail) * uint64(s.Bsize), nil
}

func rdFsName(t int64) string {
	switch t {
	case 0x01021994:
		return "tmpfs"
	case 0x2fc12fc1:
		return "zfs"
	case 0x794c7630:
		return "overlayfs"
	case 0xef53:
		return "ext4"
	case 0x58465342:
		return "xfs"
	case 0x9123683e:
		return "btrfs"
	}
	return fmt.Sprintf("0x%x", t)
}

// rdMount is the /proc/self/mountinfo line of the mount that holds dir.
func rdMount(dir string) string {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "?"
	}
	abs, err := filepath.EvalSymlinks(dir)
	if err != nil {
		abs = dir
	}
	best, bestLen := "", -1
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		mp := f[4]
		if (abs == mp || mp == "/" || strings.HasPrefix(abs, strings.TrimSuffix(mp, "/")+"/")) && len(mp) > bestLen {
			best, bestLen = line, len(mp)
		}
	}
	return best
}

// rdFsync appends size bytes and fsyncs, n times, to a new file in dir: median,
// minimum and maximum microseconds per write plus fsync.
func rdFsync(dir string, size, n int) (float64, float64, float64, error) {
	f, err := os.CreateTemp(dir, "rdfsync-*")
	if err != nil {
		return 0, 0, 0, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i*7 + 1)
	}
	samples := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, err := f.Write(buf); err != nil {
			return 0, 0, 0, err
		}
		if err := f.Sync(); err != nil {
			return 0, 0, 0, err
		}
		samples = append(samples, float64(time.Since(t0).Nanoseconds())/1000)
	}
	sort.Float64s(samples)
	return samples[len(samples)/2], samples[0], samples[len(samples)-1], nil
}

// BenchmarkZZRDStorage: what the benchmark's TMPDIR (where b.TempDir puts every
// store) is, and what a synchronous write costs there. It prints the candidate
// directories once, and reports TMPDIR's write+fsync latency each round.
func BenchmarkZZRDStorage(b *testing.B) {
	tmp := os.TempDir()
	rdOnce.Do(func() {
		host, _ := os.Hostname()
		fmt.Printf("RDSTORAGE host=%s TMPDIR=%q tempdir=%s storeDir=%s GOMAXPROCS=%d NumCPU=%d\n", host, os.Getenv("TMPDIR"), tmp, rdStoreDir, runtime.GOMAXPROCS(0), runtime.NumCPU())
		for _, f := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.high", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
			if data, err := os.ReadFile(f); err == nil {
				fmt.Printf("RDSTORAGE %s=%s\n", f, strings.TrimSpace(string(data)))
			}
		}
		for _, dir := range []string{tmp, "/tmp", "/dev/shm", "/work/tnrun", "/work/verify"} {
			t, free, err := rdStatfs(dir)
			if err != nil {
				fmt.Printf("RDSTORAGE dir=%s err=%v\n", dir, err)
				continue
			}
			fmt.Printf("RDSTORAGE dir=%s fstype=%s freeGiB=%.1f mount=%q\n", dir, rdFsName(t), float64(free)/(1<<30), rdMount(dir))
			for _, sz := range []struct{ size, n int }{{416, 50}, {1024, 50}, {256 << 10, 20}, {12 << 20, 5}} {
				med, lo, hi, err := rdFsync(dir, sz.size, sz.n)
				if err != nil {
					fmt.Printf("RDFSYNC dir=%s bytes=%d err=%v\n", dir, sz.size, err)
					break
				}
				fmt.Printf("RDFSYNC dir=%s bytes=%d n=%d medianUs=%.1f minUs=%.1f maxUs=%.1f\n", dir, sz.size, sz.n, med, lo, hi)
			}
		}
	})
	t, _, _ := rdStatfs(tmp)
	// Measured once per run (outside the timer), whatever b.N is.
	b.StopTimer()
	m416, _, _, _ := rdFsync(tmp, 416, 20)
	m12, _, _, _ := rdFsync(tmp, 12<<20, 3)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
	}
	isTmpfs := 0.0
	if rdFsName(t) == "tmpfs" {
		isTmpfs = 1
	}
	b.ReportMetric(isTmpfs, "tmpdirIsTmpfs")
	b.ReportMetric(m416, "fsync416B-us")
	b.ReportMetric(m12, "fsync12MiB-us")
}

func rdRSSNow() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return -1
	}
	f := strings.Fields(string(data))
	if len(f) < 2 {
		return -1
	}
	pages, _ := strconv.ParseInt(f[1], 10, 64)
	return pages * int64(os.Getpagesize())
}

// rdHWM is VmHWM (the kernel's peak resident set size) in bytes, or -1.
func rdHWM() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				kb, _ := strconv.ParseInt(f[1], 10, 64)
				return kb << 10
			}
		}
	}
	return -1
}

// rdPeakDuring runs fn while sampling the resident set size every 2 ms, after a
// forced GC; it resets the kernel's peak first (clear_refs 5) when allowed.
// It returns the RSS before fn, the sampled peak and the kernel's peak (-1 when
// the reset was refused).
func rdPeakDuring(fn func() error) (before, sampled, kernel int64, err error) {
	runtime.GC()
	debug.FreeOSMemory()
	reset := os.WriteFile("/proc/self/clear_refs", []byte("5"), 0) == nil
	before = rdRSSNow()
	var peak atomic.Int64
	peak.Store(before)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				if v := rdRSSNow(); v > peak.Load() {
					peak.Store(v)
				}
			}
		}
	}()
	err = fn()
	close(stop)
	<-done
	if v := rdRSSNow(); v > peak.Load() {
		peak.Store(v)
	}
	kernel = -1
	if reset {
		kernel = rdHWM()
	}
	return before, peak.Load(), kernel, err
}

// BenchmarkZZRDArchive1000 and BenchmarkZZRDArchive8800: the collection archive of BenchmarkZZWHArchive, with
// peak resident memory. Each statement starts after a forced GC (outside the
// timer); the RSS sampler runs during the statement.
func BenchmarkZZRDArchive1000(b *testing.B) { rdArchive(b, 1000) }

func BenchmarkZZRDArchive8800(b *testing.B) { rdArchive(b, 8800) }

func rdArchive(b *testing.B, size int) {
	for _, members := range []int{size} {
		b.Run(fmt.Sprintf("docs=%d", members), func(b *testing.B) {
			l := whOpen(b, b.TempDir(), 2048)
			whCollection(b, l, members, true)
			const archive = "MATCH (:Collection {id: 'c'})<-[:MEMBER_OF]-(d:Document) SET d.archived = $flag"
			l.must(b, archive, map[string]any{"flag": true})
			before := whJournalBytes(l.root)
			var maxPeak, maxKernel, baseAtMax int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				flag := i%2 == 1
				var base, sampled, kernel int64
				var err error
				started := false
				base, sampled, kernel, err = rdPeakDuring(func() error {
					b.StartTimer()
					started = true
					_, runErr := l.run(archive, map[string]any{"flag": flag})
					b.StopTimer()
					return runErr
				})
				if !started || err != nil {
					b.Fatalf("archive: %v", err)
				}
				if sampled > maxPeak {
					maxPeak, baseAtMax = sampled, base
				}
				if kernel > maxKernel {
					maxKernel = kernel
				}
			}
			whJournal(b, l, before)
			b.ReportMetric(float64(maxPeak)/(1<<20), "peakRSS-MiB")
			b.ReportMetric(float64(maxPeak-baseAtMax)/(1<<20), "peakRSSgrowth-MiB")
			b.ReportMetric(float64(maxKernel)/(1<<20), "vmHWM-MiB")
			want := i64(b.N%2 == 0) * int64(members)
			if got := l.scalar(b, "MATCH (d:Document {archived: true}) RETURN count(d)", nil); got != want {
				b.Fatalf("archived=%d want %d", got, want)
			}
		})
	}
}

// ---- Part D: concurrent writers, and start-up of a large store after a clean stop and after a kill ----

// rdOpen is whOpen for any testing.TB, with room for 16 concurrent writers.
func rdOpen(tb testing.TB, root string, dims int) *whLab {
	tb.Helper()
	db, err := nornicdb.Open(root, whConfig(dims))
	if err != nil {
		tb.Fatal(err)
	}
	manager, err := whNewManager(db.GetBaseStorageForManager())
	if err != nil {
		_ = db.Close()
		tb.Fatal(err)
	}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 64}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		tb.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	if err != nil {
		_ = db.Close()
		tb.Fatal(err)
	}
	return &whLab{root: root, db: db, d: driver, s: server, done: done}
}

func rdMust(tb testing.TB, l *whLab, query string, params map[string]any) {
	tb.Helper()
	if _, err := l.run(query, params); err != nil {
		tb.Fatalf("%s: %v", query, err)
	}
}

// rdCollections creates n collections c0..c(n-1) of `members` documents each
// (the archive shape: about 1.7 KB of properties, one kept chunk vector of
// `dims` values stored as an embedding sidecar).
func rdCollections(tb testing.TB, l *whLab, n, members, dims int) {
	tb.Helper()
	text := strings.Repeat("lorem ipsum dolor sit amet ", 60)
	for c := 0; c < n; c++ {
		rdMust(tb, l, "CREATE (:Collection {id: $c, name: 'archive me'})", map[string]any{"c": fmt.Sprintf("c%d", c)})
		for start := 0; start < members; start += 500 {
			end := start + 500
			if end > members {
				end = members
			}
			rows := make([]any, 0, end-start)
			for j := start; j < end; j++ {
				rows = append(rows, map[string]any{"id": fmt.Sprintf("doc-%d-%d", c, j), "title": fmt.Sprintf("Document %d of collection %d", j, c), "text": text})
			}
			rdMust(tb, l, "MATCH (c:Collection {id: $c}) UNWIND $rows AS row CREATE (d:Document {id: row.id, title: row.title, text: row.text, archived: false})-[:MEMBER_OF]->(c)", map[string]any{"c": fmt.Sprintf("c%d", c), "rows": rows})
		}
	}
	docs, err := whNodesByLabel(l.db.GetStorage(), "Document")
	if err != nil || len(docs) != n*members {
		tb.Fatalf("documents=%d err=%v", len(docs), err)
	}
	inner := l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine()
	vector := make([]float32, dims)
	for i, d := range docs {
		for k := range vector {
			vector[k] = float32(i+1) + float32(k)/float32(dims)
		}
		embedded := storage.CopyNode(d)
		embedded.ID = storage.NodeID("nornic:" + string(d.ID))
		embedded.ChunkEmbeddings = [][]float32{append([]float32(nil), vector...)}
		embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1, "embedding_dimensions": dims, "embedding_model": "synthetic"}
		if err := whSidecar(inner, embedded); err != nil {
			tb.Fatal(err)
		}
	}
}

type rdOp struct {
	name  string
	k     int // statements per writer per benchmark operation
	dims  int
	setup func(b *testing.B, l *whLab, writers int)
	stmt  func(w, i int) (string, map[string]any)
}

func rdOps() []rdOp {
	rows := func(w, i int) []any {
		out := make([]any, 1000)
		for j := range out {
			out[j] = map[string]any{"id": fmt.Sprintf("b-%d-%d-%d", w, i, j), "v": int64(j)}
		}
		return out
	}
	return []rdOp{
		{name: "create-node", k: 50, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) { rdMust(b, l, "CREATE (:Item {id: 'warm', name: 'n', v: 0})", nil) },
			stmt: func(w, i int) (string, map[string]any) {
				return "CREATE (:Item {id: $id, name: 'n', v: $v})", map[string]any{"id": fmt.Sprintf("w%d-%d", w, i), "v": int64(i)}
			}},
		{name: "update-node", k: 50, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) {
				for w := 0; w < writers; w++ {
					rdMust(b, l, "CREATE (:Item {id: $id, name: 'n', v: 0})", map[string]any{"id": fmt.Sprintf("u%d", w)})
				}
			},
			stmt: func(w, i int) (string, map[string]any) {
				return "MATCH (n:Item {id: $id}) SET n.v = $v", map[string]any{"id": fmt.Sprintf("u%d", w), "v": int64(i)}
			}},
		{name: "create-rel", k: 50, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) {
				for w := 0; w < writers; w++ {
					rdMust(b, l, "CREATE (:Item {id: $a}), (:Item {id: $b})", map[string]any{"a": fmt.Sprintf("a%d", w), "b": fmt.Sprintf("b%d", w)})
				}
			},
			stmt: func(w, i int) (string, map[string]any) {
				return "MATCH (a:Item {id: $a}), (c:Item {id: $b}) CREATE (a)-[:REL {v: $v}]->(c)", map[string]any{"a": fmt.Sprintf("a%d", w), "b": fmt.Sprintf("b%d", w), "v": int64(i)}
			}},
		{name: "update-rel", k: 50, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) {
				for w := 0; w < writers; w++ {
					rdMust(b, l, "CREATE (:Item {id: $a})-[:REL {v: 0}]->(:Item {id: $b})", map[string]any{"a": fmt.Sprintf("a%d", w), "b": fmt.Sprintf("b%d", w)})
				}
			},
			stmt: func(w, i int) (string, map[string]any) {
				return "MATCH (:Item {id: $a})-[r:REL]->(:Item {id: $b}) SET r.v = $v", map[string]any{"a": fmt.Sprintf("a%d", w), "b": fmt.Sprintf("b%d", w), "v": int64(i)}
			}},
		{name: "bulk", k: 3, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) {},
			stmt: func(w, i int) (string, map[string]any) {
				return "UNWIND $rows AS row CREATE (:Bulk {id: row.id, v: row.v})", map[string]any{"rows": rows(w, i)}
			}},
		{name: "archive-1000", k: 2, dims: 2048,
			setup: func(b *testing.B, l *whLab, writers int) { rdCollections(b, l, writers, 1000, 2048) },
			stmt: func(w, i int) (string, map[string]any) {
				return "MATCH (:Collection {id: $c})<-[:MEMBER_OF]-(d:Document) SET d.archived = $flag", map[string]any{"c": fmt.Sprintf("c%d", w), "flag": i%2 == 0}
			}},
		{name: "large-node-1800", k: 2, dims: 1024,
			setup: func(b *testing.B, l *whLab, writers int) {
				for w := 0; w < writers; w++ {
					rdMust(b, l, "CREATE (:Doc {id: $id, text: 'alpha', flag: 0})", map[string]any{"id": fmt.Sprintf("d%d", w)})
				}
				docs, err := whNodesByLabel(l.db.GetStorage(), "Doc")
				if err != nil || len(docs) != writers {
					b.Fatalf("docs=%d err=%v", len(docs), err)
				}
				inner := l.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine()
				for _, d := range docs {
					embedded := storage.CopyNode(d)
					embedded.ID = storage.NodeID("nornic:" + string(d.ID))
					embedded.ChunkEmbeddings = abVectors(1800, 1024)
					embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1800, "embedding_dimensions": 1024, "embedding_model": "synthetic"}
					if err := whSidecar(inner, embedded); err != nil {
						b.Fatal(err)
					}
				}
			},
			stmt: func(w, i int) (string, map[string]any) {
				return "MATCH (n:Doc {id: $id}) SET n.flag = $flag", map[string]any{"id": fmt.Sprintf("d%d", w), "flag": int64(i + 1)}
			}},
	}
}

// BenchmarkZZRDConcSmall and BenchmarkZZRDConcHeavy: the write paths with 4 and 16 concurrent writers,
// each on its own session and its own data. One operation is every writer
// running k statements; statements are timed one by one.
func BenchmarkZZRDConcSmall(b *testing.B) {
	rdConcurrent(b, "create-node", "update-node", "create-rel", "update-rel")
}

func BenchmarkZZRDConcHeavy(b *testing.B) {
	rdConcurrent(b, "bulk", "archive-1000", "large-node-1800")
}

func rdConcurrent(b *testing.B, names ...string) {
	only := map[string]bool{}
	for _, name := range names {
		only[name] = true
	}
	for _, op := range rdOps() {
		op := op
		if len(only) > 0 && !only[op.name] {
			continue
		}
		for _, writers := range []int{4, 16} {
			writers := writers
			b.Run(fmt.Sprintf("%s/writers=%d", op.name, writers), func(b *testing.B) {
				l := rdOpen(b, b.TempDir(), op.dims)
				b.Cleanup(l.close)
				op.setup(b, l, writers)
				before := whJournalBytes(l.root)
				var mu sync.Mutex
				lat := make([]float64, 0, writers*op.k*b.N)
				var errs atomic.Int64
				firstErr := map[string]int{}
				var peak atomic.Int64
				stop, sampled := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(sampled)
					tick := time.NewTicker(5 * time.Millisecond)
					defer tick.Stop()
					for {
						select {
						case <-stop:
							return
						case <-tick.C:
							if v := rdRSSNow(); v > peak.Load() {
								peak.Store(v)
							}
						}
					}
				}()
				b.ResetTimer()
				start := time.Now()
				for n := 0; n < b.N; n++ {
					var wg sync.WaitGroup
					for w := 0; w < writers; w++ {
						wg.Add(1)
						go func(w int) {
							defer wg.Done()
							for i := 0; i < op.k; i++ {
								q, p := op.stmt(w, n*op.k+i)
								t0 := time.Now()
								_, err := l.run(q, p)
								d := float64(time.Since(t0).Microseconds()) / 1000
								mu.Lock()
								lat = append(lat, d)
								if err != nil {
									msg := err.Error()
									if len(msg) > 160 {
										msg = msg[:160]
									}
									firstErr[msg]++
								}
								mu.Unlock()
								if err != nil {
									errs.Add(1)
								}
							}
						}(w)
					}
					wg.Wait()
				}
				elapsed := time.Since(start)
				b.StopTimer()
				close(stop)
				<-sampled
				total := writers * op.k * b.N
				sort.Float64s(lat)
				q := func(p float64) float64 { return lat[int(p*float64(len(lat)-1))] }
				b.ReportMetric(float64(total)/elapsed.Seconds(), "stmt/s")
				b.ReportMetric(q(0.5), "p50-ms")
				b.ReportMetric(q(0.95), "p95-ms")
				b.ReportMetric(lat[len(lat)-1], "max-ms")
				b.ReportMetric(float64(errs.Load()), "errors")
				b.ReportMetric(float64(whJournalBytes(l.root)-before)/float64(total), "journalB/stmt")
				b.ReportMetric(float64(peak.Load())/(1<<20), "peakRSS-MiB")
				for msg, c := range firstErr {
					fmt.Printf("RDCONC op=%s writers=%d errors=%d msg=%q\n", op.name, writers, c, msg)
				}
			})
		}
	}
}

// TestZZRDStoreChild is the child process of BenchmarkZZRDStartupLarge; it does
// nothing unless RD_CHILD is set.
func TestZZRDStoreChild(t *testing.T) {
	mode := os.Getenv("RD_CHILD")
	if mode == "" {
		t.Skip("child process of BenchmarkZZRDStartupLarge only")
	}
	root := os.Getenv("RD_ROOT")
	collections, _ := strconv.Atoi(os.Getenv("RD_COLLECTIONS"))
	l := rdOpen(t, root, 1024)
	switch mode {
	case "seed":
		t0 := time.Now()
		rdCollections(t, l, collections, 1000, 1024)
		fmt.Printf("RDCHILD seeded collections=%d documents=%d seedMs=%d journalBytes=%d\n", collections, collections*1000, time.Since(t0).Milliseconds(), whJournalBytes(root))
		l.close()
		fmt.Printf("RDCHILD closed\n")
	case "writes":
		for i := 0; ; i++ {
			if _, err := l.run("CREATE (:Recent {i: $i, text: 'written before the kill'})", map[string]any{"i": int64(i)}); err != nil {
				fmt.Printf("RDCHILD writeErr=%v\n", err)
				os.Exit(3)
			}
			if i == 200 {
				fmt.Printf("RDREADY\n")
			}
		}
	}
}

func rdChild(b *testing.B, mode, root string, collections int, kill bool) {
	b.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestZZRDStoreChild$", "-test.bench=^$", "-test.v")
	cmd.Env = append(os.Environ(), "RD_CHILD="+mode, "RD_ROOT="+root, "RD_COLLECTIONS="+strconv.Itoa(collections))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		b.Fatal(err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		b.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var tail []string
	killed := false
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "RD") || strings.Contains(line, "FAIL") {
			fmt.Printf("child %s: %s\n", mode, line)
		}
		tail = append(tail, line)
		if len(tail) > 20 {
			tail = tail[1:]
		}
		if kill && line == "RDREADY" {
			time.Sleep(50 * time.Millisecond) // writes still in flight
			_ = cmd.Process.Kill()                // SIGKILL
			killed = true
			break
		}
	}
	_, _ = io.Copy(io.Discard, stdout)
	werr := cmd.Wait()
	if kill && !killed {
		b.Fatalf("child %s ended before it was killed: %v\n%s", mode, werr, strings.Join(tail, "\n"))
	}
	if !kill && werr != nil {
		b.Fatalf("child %s: %v\n%s", mode, werr, strings.Join(tail, "\n"))
	}
}

func rdDirBytes(root string) int64 {
	var total int64
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func rdTimedOpen(b *testing.B, root string) (float64, int64) {
	b.Helper()
	runtime.GC()
	debug.FreeOSMemory()
	b.StartTimer()
	t0 := time.Now()
	db, err := nornicdb.Open(root, whConfig(1024))
	ms := float64(time.Since(t0).Microseconds()) / 1000
	b.StopTimer()
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	var recent int64 = -1
	if nodes, err := whNodesByLabel(db.GetStorage(), "Recent"); err == nil {
		recent = int64(len(nodes))
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	return ms, recent
}

// BenchmarkZZRDStartupLarge: open a large store (RD_COLLECTIONS x 1,000
// documents of the archive shape, 1,024-dimension vectors; default 20) after a
// clean stop, and again after a process was killed while writing to it. The
// store is built by a child process through Bolt; the timed opens are in this
// process (ns/op is the sum of the two opens).
func BenchmarkZZRDStartupLarge(b *testing.B) {
	collections := 20
	if v, err := strconv.Atoi(os.Getenv("RD_COLLECTIONS")); err == nil && v > 0 {
		collections = v
	}
	rdStartupLarge(b, collections)
}

// BenchmarkZZRDStartupLarger: the same with 100 collections (100,000 documents).
func BenchmarkZZRDStartupLarger(b *testing.B) { rdStartupLarge(b, 100) }

func rdStartupLarge(b *testing.B, collections int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		root := filepath.Join(b.TempDir(), "store")
		rdChild(b, "seed", root, collections, false)
		cleanBytes, cleanJournal := rdDirBytes(root), whJournalBytes(root)
		cleanMs, _ := rdTimedOpen(b, root)
		rdChild(b, "writes", root, collections, true)
		killBytes, killJournal := rdDirBytes(root), whJournalBytes(root)
		killMs, recent := rdTimedOpen(b, root)
		fmt.Printf("RDSTARTUP collections=%d documents=%d storeBytes=%d journalBytes=%d cleanOpenMs=%.1f afterKillStoreBytes=%d afterKillJournalBytes=%d killOpenMs=%.1f recentAfterKill=%d\n", collections, collections*1000, cleanBytes, cleanJournal, cleanMs, killBytes, killJournal, killMs, recent)
		b.ReportMetric(cleanMs, "cleanOpen-ms")
		b.ReportMetric(killMs, "killOpen-ms")
		b.ReportMetric(float64(cleanBytes)/1e6, "storeMB")
		b.ReportMetric(float64(cleanJournal)/1e6, "journalMB")
	}
}
