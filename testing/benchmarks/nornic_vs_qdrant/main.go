package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"hash/fnv"
	"math/rand"
	"net"
	"os"
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
		qdrantAddr = flag.String("qdrant-grpc-addr", "127.0.0.1:6334", "Qdrant gRPC address")
		nornicAddr = flag.String("nornic-grpc-addr", "127.0.0.1:6336", "NornicDB gRPC address (Qdrant-compatible)")
		skipQdrant = flag.Bool("skip-qdrant", false, "If set, only benchmark NornicDB")
	)
	flag.Parse()

	if *dim <= 0 || *k <= 0 || *hnswEf < 0 || *concurrent <= 0 || *seconds <= 0 || *warmup < 0 {
		fatalf("invalid args")
	}
	if *dataset == "" && *points <= 0 {
		fatalf("invalid args: points must be >0 when dataset is empty")
	}

	// Docker services are presumed already running.
	// Defaults:
	//   - Qdrant gRPC: 127.0.0.1:6334
	//   - NornicDB gRPC: 127.0.0.1:6336 (override with -nornic-grpc-addr)

	// Verify services are reachable (quick check, services should already be up)
	logf("Verifying services are reachable...")
	if !*skipQdrant {
		waitTCP(*qdrantAddr, 5*time.Second)
	}
	waitTCP(*nornicAddr, 5*time.Second)
	if *skipQdrant {
		logf("Services verified: NornicDB gRPC=%s", *nornicAddr)
	} else {
		logf("Services verified: Qdrant gRPC=%s, NornicDB gRPC=%s", *qdrantAddr, *nornicAddr)
	}

	spec := datasetSpec{
		path:   *dataset,
		points: *points,
		dim:    *dim,
	}
	logf("Loading dataset into both targets: %s", spec.describe(*collection))
	logf("Loading into NornicDB...")
	nornicCount, err := loadDatasetIntoTarget(*nornicAddr, *collection, spec, time.Duration(*loadTO)*time.Second)
	if err != nil {
		fatalf("load nornicdb: %v", err)
	}
	logf("Loaded %d points into NornicDB", nornicCount)
	if !*skipQdrant {
		logf("Loading into Qdrant...")
		qdrantCount, err := loadDatasetIntoTarget(*qdrantAddr, *collection, spec, time.Duration(*loadTO)*time.Second)
		if err != nil {
			fatalf("load qdrant: %v", err)
		}
		logf("Loaded %d points into Qdrant", qdrantCount)
	}

	queryVec := makeDeterministicQuery(*dim)

	runCfg := runConfig{
		concurrency: *concurrent,
		seconds:     time.Duration(*seconds) * time.Second,
		warmup:      time.Duration(*warmup) * time.Second,
	}

	logf("Running benchmark: NornicDB (Qdrant gRPC compat)")
	nornicSum := runBenchmark("nornicdb", runCfg, func() (workerFn, func(), error) {
		return newSearchWorker(*nornicAddr, *collection, queryVec, uint64(*k), *hnswEf)
	})
	printSummary("NornicDB", nornicSum)

	var qdrantSum summary
	if !*skipQdrant {
		logf("Running benchmark: Qdrant (Docker)")
		qdrantSum = runBenchmark("qdrant", runCfg, func() (workerFn, func(), error) {
			return newSearchWorker(*qdrantAddr, *collection, queryVec, uint64(*k), *hnswEf)
		})
		printSummary("Qdrant", qdrantSum)
	}

	csvPath := *outCSV
	if !filepath.IsAbs(csvPath) {
		// Try to find repo root for relative paths
		root, err := repoRoot()
		if err == nil {
			csvPath = filepath.Join(root, csvPath)
		}
		// If we can't find repo root, use the path as-is (might be absolute or relative to CWD)
	}
	rows := []csvRow{
		rowFromSummary("nornicdb", spec.pointsForCSV(), *dim, *k, *concurrent, nornicSum),
	}
	if !*skipQdrant {
		rows = append(rows, rowFromSummary("qdrant", spec.pointsForCSV(), *dim, *k, *concurrent, qdrantSum))
	}
	if err := appendCSV(csvPath, rows); err != nil {
		fatalf("write csv: %v", err)
	}
	logf("CSV appended: %s", csvPath)
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

	logf("  Connecting to %s (timeout: 10s)...", grpcAddr)
	// Use a shorter timeout for the initial connection
	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connCancel()

	conn, err := grpc.DialContext(connCtx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		if connCtx.Err() == context.DeadlineExceeded {
			return 0, fmt.Errorf("grpc dial %s: connection timeout after 10s (service may not be running, port not accessible, or gRPC not ready)", grpcAddr)
		}
		return 0, fmt.Errorf("grpc dial %s: %w", grpcAddr, err)
	}
	logf("  Connected successfully")
	defer conn.Close()

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)

	logf("  Deleting existing collection %q (if any)...", collection)
	_, _ = collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: collection})

	logf("  Creating collection %q with dim=%d...", collection, spec.dim)
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
	logf("  Collection created, starting data load...")

	const batch = 256
	upsertBatch := func(buf []*qpb.PointStruct) error {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 60*time.Second)
		defer rpcCancel()
		_, err := points.Upsert(rpcCtx, &qpb.UpsertPoints{CollectionName: collection, Points: buf})
		if err != nil {
			return fmt.Errorf("upsert batch of %d points: %w", len(buf), err)
		}
		return nil
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
					return 0, fmt.Errorf("upsert batch at line %d: %w", line, err)
				}
				total += len(buf)
				if total%1000 == 0 {
					logf("  Loaded %d points...", total)
				}
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
		logf("  Finished loading %d points, verifying count...", total)
		if err := waitForPointCount(ctx, points, collection, total); err != nil {
			return total, fmt.Errorf("waitForPointCount: %w", err)
		}
		logf("  Verified %d points in collection", total)
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
		if (off+batch)%1000 == 0 || off+batch >= spec.points {
			logf("  Loaded %d/%d points...", off+batch, spec.points)
		}
	}
	logf("  Finished loading %d points, verifying count...", spec.points)
	if err := waitForPointCount(ctx, points, collection, spec.points); err != nil {
		return spec.points, fmt.Errorf("waitForPointCount: %w", err)
	}
	logf("  Verified %d points in collection", spec.points)
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
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		resp, err := points.Count(ctx, &qpb.CountPoints{
			CollectionName: collection,
			Exact:          &exact,
		})
		if err != nil {
			if attempts%10 == 0 {
				logf("    Count RPC error (attempt %d): %v", attempts, err)
			}
		} else if resp != nil && resp.Result != nil {
			current := int(resp.Result.Count)
			if attempts%5 == 0 {
				logf("    Current count: %d/%d", current, want)
			}
			if current >= want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for points count=%d in %q (after %d attempts)", want, collection, attempts)
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
