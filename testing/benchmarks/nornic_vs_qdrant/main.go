package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	var (
		qdrantAddr = flag.String("qdrant-grpc-addr", "127.0.0.1:6334", "Qdrant gRPC address")
		points     = flag.Int("points", 20_000, "Synthetic points to load (ignored if -dataset is set)")
		dim        = flag.Int("dim", 128, "Vector dimension (required)")
		k          = flag.Int("k", 10, "Top-K results")
		hnswEf     = flag.Int("hnsw-ef", 0, "Optional Qdrant SearchPoints.Params.hnsw_ef override (0 = unset)")
		concurrent = flag.Int("concurrency", runtime.GOMAXPROCS(0), "Concurrent clients")
		seconds    = flag.Int("seconds", 10, "Benchmark duration per target")
		warmup     = flag.Int("warmup-seconds", 2, "Warmup duration per target")
		loadTO     = flag.Int("load-timeout-seconds", 600, "Timeout for loading dataset into each target")
		dataset    = flag.String("dataset", "", "Optional JSONL dataset path (one JSON object per line with fields: id, vector, payload)")
		collection = flag.String("collection", "bench_col", "Collection name to use in both systems")
		outCSV     = flag.String("csv", "testing/benchmarks/nornic_vs_qdrant/results.csv", "CSV output path")
	)
	flag.Parse()

	if *dim <= 0 || *k <= 0 || *hnswEf < 0 || *concurrent <= 0 || *seconds <= 0 || *warmup < 0 {
		fatalf("invalid args")
	}
	if *dataset == "" && *points <= 0 {
		fatalf("invalid args: points must be >0 when dataset is empty")
	}

	root, err := repoRoot()
	if err != nil {
		fatalf("repo root: %v", err)
	}

	// Ensure local Qdrant gRPC is reachable.
	waitTCP(*qdrantAddr, 3*time.Second)

	dataDir, err := os.MkdirTemp("", "nornicdb-vs-qdrant.*")
	if err != nil {
		fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(dataDir)

	httpPort := pickPort()
	boltPort := pickPort()
	nornicGRPCPort := pickPort()
	nornicAddr := fmt.Sprintf("127.0.0.1:%d", nornicGRPCPort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := startNornicDB(ctx, root, dataDir, httpPort, boltPort, nornicGRPCPort)
	if err != nil {
		fatalf("start nornicdb: %v", err)
	}
	defer func() { _ = server.stop() }()

	waitTCP(nornicAddr, 45*time.Second)

	spec := datasetSpec{
		path:   *dataset,
		points: *points,
		dim:    *dim,
	}
	logf("Loading dataset into both targets: %s", spec.describe(*collection))
	if _, err := loadDatasetIntoTarget(nornicAddr, *collection, spec, time.Duration(*loadTO)*time.Second); err != nil {
		fatalf("load nornicdb: %v", err)
	}
	if _, err := loadDatasetIntoTarget(*qdrantAddr, *collection, spec, time.Duration(*loadTO)*time.Second); err != nil {
		fatalf("load qdrant: %v", err)
	}

	queryVec := makeDeterministicQuery(*dim)

	runCfg := runConfig{
		concurrency: *concurrent,
		seconds:     time.Duration(*seconds) * time.Second,
		warmup:      time.Duration(*warmup) * time.Second,
	}

	logf("Running benchmark: NornicDB (Qdrant gRPC compat)")
	nornicSum := runBenchmark("nornicdb", runCfg, func() (workerFn, func(), error) {
		return newSearchWorker(nornicAddr, *collection, queryVec, uint64(*k), *hnswEf)
	})
	printSummary("NornicDB", nornicSum)

	logf("Running benchmark: Qdrant (local)")
	qdrantSum := runBenchmark("qdrant", runCfg, func() (workerFn, func(), error) {
		return newSearchWorker(*qdrantAddr, *collection, queryVec, uint64(*k), *hnswEf)
	})
	printSummary("Qdrant", qdrantSum)

	csvPath := *outCSV
	if !filepath.IsAbs(csvPath) {
		csvPath = filepath.Join(root, csvPath)
	}
	rows := []csvRow{
		rowFromSummary("nornicdb", spec.pointsForCSV(), *dim, *k, *concurrent, nornicSum),
		rowFromSummary("qdrant", spec.pointsForCSV(), *dim, *k, *concurrent, qdrantSum),
	}
	if err := appendCSV(csvPath, rows); err != nil {
		fatalf("write csv: %v", err)
	}
	logf("CSV appended: %s", csvPath)
}

type serverProc struct {
	cmd  *exec.Cmd
	logf *os.File
}

func (s *serverProc) stop() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	_ = s.cmd.Process.Signal(os.Interrupt)

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()

	select {
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	case <-done:
	}

	if s.logf != nil {
		_ = s.logf.Close()
	}
	return nil
}

func startNornicDB(ctx context.Context, repoRoot, dataDir string, httpPort, boltPort, grpcPort int) (*serverProc, error) {
	logPath := filepath.Join(dataDir, "server.log")
	f, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create server log: %w", err)
	}

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/nornicdb", "serve",
		"--data-dir", dataDir,
		"--address", "127.0.0.1",
		"--http-port", strconv.Itoa(httpPort),
		"--bolt-port", strconv.Itoa(boltPort),
		"--no-auth",
		"--headless",
	)
	cmd.Dir = repoRoot
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(),
		"NORNICDB_QDRANT_GRPC_ENABLED=true",
		fmt.Sprintf("NORNICDB_QDRANT_GRPC_LISTEN_ADDR=127.0.0.1:%d", grpcPort),
		"NORNICDB_EMBEDDING_ENABLED=false", // allow vector mutations
		"NORNICDB_MCP_ENABLED=false",
		"NORNICDB_HEIMDALL_ENABLED=false",
	)

	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start: %w", err)
	}
	return &serverProc{cmd: cmd, logf: f}, nil
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for i := 0; i < 15; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	return "", fmt.Errorf("could not find go.mod from %s", wd)
}

func pickPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("pickPort listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitTCP(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fatalf("timeout waiting for %s (is gRPC port exposed?)", addr)
}

// =============================================================================
// Dataset loading (shared)
// =============================================================================

type datasetSpec struct {
	path   string
	points int
	dim    int
}

func (s datasetSpec) pointsForCSV() int {
	if s.path != "" {
		return 0
	}
	return s.points
}

func (s datasetSpec) describe(col string) string {
	if s.path != "" {
		return fmt.Sprintf("jsonl=%s dim=%d col=%s", s.path, s.dim, col)
	}
	return fmt.Sprintf("synthetic points=%d dim=%d col=%s", s.points, s.dim, col)
}

type datasetRow struct {
	ID      string         `json:"id"`
	Vector  []float32      `json:"vector"`
	Payload map[string]any `json:"payload"`
}

func loadDatasetIntoTarget(grpcAddr, collection string, spec datasetSpec, timeout time.Duration) (int, error) {
	if timeout <= 0 {
		timeout = 600 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return 0, fmt.Errorf("grpc dial %s: %w", grpcAddr, err)
	}
	defer conn.Close()

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)

	_, _ = collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: collection})
	_, err = collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: collection,
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: uint64(spec.dim), Distance: qpb.Distance_Cosine},
			},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("create collection %q @ %s: %w", collection, grpcAddr, err)
	}

	const batch = 256
	upsertBatch := func(buf []*qpb.PointStruct) error {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 60*time.Second)
		defer rpcCancel()
		_, err := points.Upsert(rpcCtx, &qpb.UpsertPoints{CollectionName: collection, Points: buf})
		return err
	}

	if spec.path != "" {
		f, err := os.Open(spec.path)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

		buf := make([]*qpb.PointStruct, 0, batch)
		total := 0
		line := 0
		for sc.Scan() {
			line++
			var row datasetRow
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
				return 0, fmt.Errorf("dataset parse line %d: %w", line, err)
			}
			if row.ID == "" || len(row.Vector) == 0 {
				return 0, fmt.Errorf("dataset line %d: missing id/vector", line)
			}
			if len(row.Vector) != spec.dim {
				return 0, fmt.Errorf("dataset line %d: vector dim mismatch: got %d expected %d", line, len(row.Vector), spec.dim)
			}
			normalizeInPlace(row.Vector)

			payload := make(map[string]*qpb.Value, len(row.Payload))
			for k, v := range row.Payload {
				payload[k] = anyToQdrantValue(v)
			}
			pid := pointIDFromString(row.ID)
			if pid.GetUuid() == "" && pid.GetNum() == 0 {
				payload["_nornic_original_id"] = anyToQdrantValue(row.ID)
			}

			buf = append(buf, &qpb.PointStruct{
				Id: pid,
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: row.Vector}}},
					},
				},
				Payload: payload,
			})

			if len(buf) >= batch {
				if err := upsertBatch(buf); err != nil {
					return 0, err
				}
				total += len(buf)
				buf = buf[:0]
			}
		}
		if err := sc.Err(); err != nil {
			return 0, err
		}
		if len(buf) > 0 {
			if err := upsertBatch(buf); err != nil {
				return 0, err
			}
			total += len(buf)
		}
		if err := waitForPointCount(ctx, points, collection, total); err != nil {
			return total, err
		}
		return total, nil
	}

	rnd := rand.New(rand.NewSource(1))
	for off := 0; off < spec.points; off += batch {
		n := batch
		if off+n > spec.points {
			n = spec.points - off
		}
		buf := make([]*qpb.PointStruct, 0, n)
		for i := 0; i < n; i++ {
			vec := make([]float32, spec.dim)
			for j := 0; j < spec.dim; j++ {
				vec[j] = float32(rnd.NormFloat64())
			}
			normalizeInPlace(vec)
			id := uint64(off + i)
			buf = append(buf, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: id}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vec}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"i": {Kind: &qpb.Value_IntegerValue{IntegerValue: int64(off + i)}},
				},
			})
		}
		if err := upsertBatch(buf); err != nil {
			return 0, fmt.Errorf("upsert batch@%d: %w", off, err)
		}
	}
	if err := waitForPointCount(ctx, points, collection, spec.points); err != nil {
		return spec.points, err
	}
	return spec.points, nil
}

func anyToQdrantValue(v any) *qpb.Value {
	switch t := v.(type) {
	case nil:
		return &qpb.Value{Kind: &qpb.Value_NullValue{NullValue: qpb.NullValue_NULL_VALUE}}
	case string:
		return &qpb.Value{Kind: &qpb.Value_StringValue{StringValue: t}}
	case bool:
		return &qpb.Value{Kind: &qpb.Value_BoolValue{BoolValue: t}}
	case float64:
		if math.Trunc(t) == t {
			return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: int64(t)}}
		}
		return &qpb.Value{Kind: &qpb.Value_DoubleValue{DoubleValue: t}}
	case int:
		return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: int64(t)}}
	case int64:
		return &qpb.Value{Kind: &qpb.Value_IntegerValue{IntegerValue: t}}
	case []any:
		out := make([]*qpb.Value, 0, len(t))
		for _, item := range t {
			out = append(out, anyToQdrantValue(item))
		}
		return &qpb.Value{Kind: &qpb.Value_ListValue{ListValue: &qpb.ListValue{Values: out}}}
	case map[string]any:
		out := make(map[string]*qpb.Value, len(t))
		for k, item := range t {
			out[k] = anyToQdrantValue(item)
		}
		return &qpb.Value{Kind: &qpb.Value_StructValue{StructValue: &qpb.Struct{Fields: out}}}
	default:
		return &qpb.Value{Kind: &qpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

func waitForPointCount(ctx context.Context, points qpb.PointsClient, collection string, want int) error {
	if want <= 0 {
		return nil
	}
	exact := true
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := points.Count(ctx, &qpb.CountPoints{
			CollectionName: collection,
			Exact:          &exact,
		})
		if err == nil && resp != nil && resp.Result != nil && int(resp.Result.Count) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for points count=%d in %q", want, collection)
}

func pointIDFromString(id string) *qpb.PointId {
	if id == "" {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 0}}
	}
	if n, err := strconv.ParseUint(id, 10, 64); err == nil {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: n}}
	}
	if _, err := uuid.Parse(id); err == nil {
		return &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}}
	}

	// Qdrant only supports UUID or numeric point IDs; if the dataset uses arbitrary
	// strings, map to a stable numeric ID for benchmarking.
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: h.Sum64()}}
}

// =============================================================================
// Benchmark runner
// =============================================================================

type workerFn func(ctx context.Context) error

type runConfig struct {
	concurrency int
	seconds     time.Duration
	warmup      time.Duration
}

type summary struct {
	label        string
	totalOps     int
	totalSeconds float64
	latencies    []time.Duration
}

func runBenchmark(label string, cfg runConfig, newWorker func() (workerFn, func(), error)) summary {
	doRun := func(d time.Duration) (int, []time.Duration) {
		if d <= 0 {
			return 0, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()

		var (
			mu     sync.Mutex
			count  int
			latAll []time.Duration
		)

		var wg sync.WaitGroup
		wg.Add(cfg.concurrency)
		for i := 0; i < cfg.concurrency; i++ {
			fn, cleanup, err := newWorker()
			if err != nil {
				wg.Done()
				continue
			}
			go func(fn workerFn, cleanup func()) {
				defer wg.Done()
				defer cleanup()
				local := make([]time.Duration, 0, 1024)
				for {
					if ctx.Err() != nil {
						break
					}
					start := time.Now()
					err := fn(ctx)
					dur := time.Since(start)
					if err != nil {
						if ctx.Err() != nil || isContextDoneErr(err) {
							break
						}
						break
					}
					local = append(local, dur)
				}
				mu.Lock()
				count += len(local)
				latAll = append(latAll, local...)
				mu.Unlock()
			}(fn, cleanup)
		}
		wg.Wait()
		return count, latAll
	}

	_, _ = doRun(cfg.warmup)
	start := time.Now()
	n, lat := doRun(cfg.seconds)
	elapsed := time.Since(start).Seconds()
	return summary{label: label, totalOps: n, totalSeconds: elapsed, latencies: lat}
}

func isContextDoneErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled
	}
	return false
}

func newSearchWorker(grpcAddr, collection string, query []float32, k uint64, hnswEf int) (workerFn, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	client := qpb.NewPointsClient(conn)
	req := &qpb.SearchPoints{
		CollectionName: collection,
		Vector:         query,
		Limit:          k,
	}
	if hnswEf > 0 {
		ef := uint64(hnswEf)
		req.Params = &qpb.SearchParams{HnswEf: &ef}
	}
	fn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := client.Search(ctx, req)
		return err
	}
	return fn, func() { _ = conn.Close() }, nil
}

func printSummary(name string, s summary) {
	ops := float64(s.totalOps) / s.totalSeconds
	p50, p95, p99, min, max, mean := latencyStats(s.latencies)

	logf("%s: ops=%d secs=%.3f ops/sec=%.2f", name, s.totalOps, s.totalSeconds, ops)
	logf("%s: latency ms: min=%.3f p50=%.3f p95=%.3f p99=%.3f max=%.3f mean=%.3f",
		name,
		min.Seconds()*1000,
		p50.Seconds()*1000,
		p95.Seconds()*1000,
		p99.Seconds()*1000,
		max.Seconds()*1000,
		mean.Seconds()*1000,
	)
}

func latencyStats(durs []time.Duration) (p50, p95, p99, min, max, mean time.Duration) {
	if len(durs) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	cp := make([]time.Duration, len(durs))
	copy(cp, durs)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })

	min = cp[0]
	max = cp[len(cp)-1]
	var sum time.Duration
	for _, d := range cp {
		sum += d
	}
	mean = time.Duration(int64(sum) / int64(len(cp)))
	p50 = cp[int(float64(len(cp)-1)*0.50)]
	p95 = cp[int(float64(len(cp)-1)*0.95)]
	p99 = cp[int(float64(len(cp)-1)*0.99)]
	return
}

// =============================================================================
// CSV
// =============================================================================

type csvRow struct {
	Timestamp string
	Target    string
	Points    int
	Dim       int
	K         int
	Conc      int
	Ops       int
	Seconds   float64
	OpsPerSec float64
	P50ms     float64
	P95ms     float64
	P99ms     float64
	Meanms    float64
	Minms     float64
	Maxms     float64
}

func rowFromSummary(target string, points, dim, k, conc int, s summary) csvRow {
	p50, p95, p99, min, max, mean := latencyStats(s.latencies)
	return csvRow{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Target:    target,
		Points:    points,
		Dim:       dim,
		K:         k,
		Conc:      conc,
		Ops:       s.totalOps,
		Seconds:   s.totalSeconds,
		OpsPerSec: float64(s.totalOps) / s.totalSeconds,
		P50ms:     p50.Seconds() * 1000,
		P95ms:     p95.Seconds() * 1000,
		P99ms:     p99.Seconds() * 1000,
		Meanms:    mean.Seconds() * 1000,
		Minms:     min.Seconds() * 1000,
		Maxms:     max.Seconds() * 1000,
	}
}

func appendCSV(path string, rows []csvRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	needHeader := false
	if st, err := os.Stat(path); err != nil || st.Size() == 0 {
		needHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	defer func() {
		// Ensure CSV writer buffer is flushed before closing file
		w.Flush()
		if flushErr := w.Error(); flushErr != nil && err == nil {
			err = flushErr
		}
		// Close file after ensuring all data is flushed
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if needHeader {
		if err := w.Write([]string{
			"timestamp", "target", "points", "dim", "k", "concurrency",
			"ops", "seconds", "ops_per_sec",
			"p50_ms", "p95_ms", "p99_ms", "mean_ms", "min_ms", "max_ms",
		}); err != nil {
			return err
		}
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.Timestamp,
			r.Target,
			strconv.Itoa(r.Points),
			strconv.Itoa(r.Dim),
			strconv.Itoa(r.K),
			strconv.Itoa(r.Conc),
			strconv.Itoa(r.Ops),
			fmt.Sprintf("%.6f", r.Seconds),
			fmt.Sprintf("%.6f", r.OpsPerSec),
			fmt.Sprintf("%.6f", r.P50ms),
			fmt.Sprintf("%.6f", r.P95ms),
			fmt.Sprintf("%.6f", r.P99ms),
			fmt.Sprintf("%.6f", r.Meanms),
			fmt.Sprintf("%.6f", r.Minms),
			fmt.Sprintf("%.6f", r.Maxms),
		}); err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// Vector helpers
// =============================================================================

func makeDeterministicQuery(dim int) []float32 {
	v := make([]float32, dim)
	for i := 0; i < dim; i++ {
		v[i] = float32(math.Sin(float64(i+1)) * 0.5)
	}
	normalizeInPlace(v)
	return v
}

func normalizeInPlace(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

// =============================================================================
// Logging
// =============================================================================

func logf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func fatalf(format string, args ...any) {
	logf(format, args...)
	os.Exit(1)
}
