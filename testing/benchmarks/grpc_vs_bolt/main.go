package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	var (
		points     = flag.Int("points", 20_000, "Number of points to load (ignored if -dataset is set)")
		dim        = flag.Int("dim", 128, "Vector dimension (required for synthetic; validated for dataset)")
		k          = flag.Int("k", 10, "Top-K results")
		concurrent = flag.Int("concurrency", runtime.GOMAXPROCS(0), "Concurrent clients")
		seconds    = flag.Int("seconds", 10, "Benchmark duration per scenario")
		warmup     = flag.Int("warmup-seconds", 2, "Warmup duration per scenario")
		scenarios  = flag.String("scenarios", "grpc_ann,grpc_bruteforce,bolt_vector,bolt_overhead", "Comma-separated scenarios to run")
		dataset    = flag.String("dataset", "", "Optional JSONL dataset path (one JSON object per line with fields: id, vector, payload)")
		baseCol    = flag.String("collection", "bench_col", "Base collection name")
		outCSV     = flag.String("csv", "testing/benchmarks/grpc_vs_bolt/results.csv", "CSV output path")
	)
	flag.Parse()

	if *points <= 0 || *dim <= 0 || *k <= 0 || *concurrent <= 0 || *seconds <= 0 || *warmup < 0 {
		fatalf("invalid args")
	}

	root, err := repoRoot()
	if err != nil {
		fatalf("repo root: %v", err)
	}

	dataDir, err := os.MkdirTemp("", "nornicdb-grpc-vs-bolt.*")
	if err != nil {
		fatalf("mktemp: %v", err)
	}
	defer os.RemoveAll(dataDir)

	httpPort := pickPort()
	boltPort := pickPort()
	grpcPort := pickPort()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := startNornicDB(ctx, root, dataDir, httpPort, boltPort, grpcPort)
	if err != nil {
		fatalf("start server: %v", err)
	}
	defer func() {
		_ = server.stop()
	}()

	waitTCP(fmt.Sprintf("127.0.0.1:%d", boltPort), 45*time.Second)
	waitTCP(fmt.Sprintf("127.0.0.1:%d", grpcPort), 45*time.Second)

	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	boltAddr := fmt.Sprintf("127.0.0.1:%d", boltPort)

	vectorName := "v"
	collectionNamed := *baseCol + "_named"
	collectionUnnamed := *baseCol + "_unnamed"

	loadSpec := datasetSpec{
		path:   *dataset,
		points: *points,
		dim:    *dim,
	}
	logf("Loading dataset: %s", loadSpec.describe(collectionNamed, collectionUnnamed))
	if err := loadDataset(grpcAddr, collectionNamed, collectionUnnamed, vectorName, loadSpec); err != nil {
		fatalf("load dataset: %v", err)
	}

	queryVec := makeDeterministicQuery(*dim)

	runCfg := runConfig{
		concurrency: *concurrent,
		seconds:     time.Duration(*seconds) * time.Second,
		warmup:      time.Duration(*warmup) * time.Second,
	}

	plan := parseScenarios(*scenarios)
	if len(plan) == 0 {
		fatalf("no scenarios selected")
	}

	// Bolt setup: create a vector index to ensure the Cypher procedure filters to just the benchmark label.
	// This keeps Bolt "best case" closer to real usage.
	if contains(plan, scenarioBoltVector) || contains(plan, scenarioBoltOverhead) {
		setupCypher := fmt.Sprintf(
			"CREATE VECTOR INDEX default IF NOT EXISTS FOR (n:%s) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}",
			collectionNamed,
			*dim,
		)
		if err := boltSetupOnce(boltAddr, setupCypher); err != nil {
			fatalf("bolt setup: %v", err)
		}
	}

	var outRows []csvRow

	for _, sc := range plan {
		switch sc {
		case scenarioGRPCANN:
			logf("Running gRPC best-case benchmark (ANN via search.Service)...")
			sum := runBenchmark("grpc_ann", runCfg, func() (workerFn, func(), error) {
				return newGRPCWorkerANN(grpcAddr, collectionUnnamed, queryVec, uint64(*k))
			})
			printSummary("gRPC (ANN)", sum)
			outRows = append(outRows, rowFromSummary("grpc", string(sc), loadSpec.pointsForCSV(), *dim, *k, *concurrent, sum))
		case scenarioGRPCBruteforce:
			logf("Running gRPC brute-force benchmark (named vector)...")
			sum := runBenchmark("grpc_bruteforce", runCfg, func() (workerFn, func(), error) {
				return newGRPCWorkerBruteforce(grpcAddr, collectionNamed, vectorName, queryVec, uint64(*k))
			})
			printSummary("gRPC (bruteforce)", sum)
			outRows = append(outRows, rowFromSummary("grpc", string(sc), loadSpec.pointsForCSV(), *dim, *k, *concurrent, sum))
		case scenarioBoltVector:
			logf("Running Bolt vector benchmark (CALL db.index.vector.queryNodes...)...")
			boltCypher := fmt.Sprintf(
				"CALL db.index.vector.queryNodes('default', %d, [%s]) YIELD node, score RETURN node.id AS id, score, $nonce AS nonce",
				*k,
				float32ListLiteral(queryVec),
			)
			sum := runBenchmark("bolt_vector", runCfg, func() (workerFn, func(), error) {
				return newBoltWorker(boltAddr, boltCypher)
			})
			printSummary("Bolt (vector proc)", sum)
			outRows = append(outRows, rowFromSummary("bolt", string(sc), loadSpec.pointsForCSV(), *dim, *k, *concurrent, sum))
		case scenarioBoltOverhead:
			logf("Running Bolt overhead benchmark (RETURN 1)...")
			overheadQuery := "RETURN 1 AS n, $nonce AS nonce"
			sum := runBenchmark("bolt_overhead", runCfg, func() (workerFn, func(), error) {
				return newBoltWorker(boltAddr, overheadQuery)
			})
			printSummary("Bolt (overhead)", sum)
			outRows = append(outRows, rowFromSummary("bolt", string(sc), loadSpec.pointsForCSV(), *dim, *k, *concurrent, sum))
		default:
			fatalf("unknown scenario: %s", sc)
		}
	}

	csvPath := *outCSV
	if !filepath.IsAbs(csvPath) {
		csvPath = filepath.Join(root, csvPath)
	}
	if err := appendCSV(csvPath, outRows); err != nil {
		fatalf("write csv: %v", err)
	}

	logf("CSV appended: %s", csvPath)
}

type serverProc struct {
	cmd  *exec.Cmd
	logf *os.File
}

type scenario string

const (
	scenarioGRPCANN        scenario = "grpc_ann"
	scenarioGRPCBruteforce scenario = "grpc_bruteforce"
	scenarioBoltVector     scenario = "bolt_vector"
	scenarioBoltOverhead   scenario = "bolt_overhead"
)

func parseScenarios(s string) []scenario {
	raw := strings.Split(s, ",")
	out := make([]scenario, 0, len(raw))
	seen := make(map[scenario]struct{})
	for _, r := range raw {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		sc := scenario(r)
		switch sc {
		case scenarioGRPCANN, scenarioGRPCBruteforce, scenarioBoltVector, scenarioBoltOverhead:
		default:
			fatalf("unknown scenario %q", r)
		}
		if _, ok := seen[sc]; ok {
			continue
		}
		seen[sc] = struct{}{}
		out = append(out, sc)
	}
	return out
}

func contains(list []scenario, v scenario) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
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
		"NORNICDB_EMBEDDING_ENABLED=false",   // allow vector mutations
		"NORNICDB_QUERY_CACHE_ENABLED=false", // avoid hiding work in Bolt/Cypher benchmarks
		"NORNICDB_MCP_ENABLED=false",
		"NORNICDB_HEIMDALL_ENABLED=false",
	)

	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start: %w", err)
	}

	return &serverProc{cmd: cmd, logf: f}, nil
}

func waitTCP(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	fatalf("timeout waiting for %s", addr)
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

type datasetSpec struct {
	path   string
	points int
	dim    int
}

func (s datasetSpec) pointsForCSV() int {
	if s.path != "" {
		// Unknown without reading the dataset file; use 0 to indicate external dataset.
		return 0
	}
	return s.points
}

func (s datasetSpec) describe(namedCol, unnamedCol string) string {
	if s.path != "" {
		return fmt.Sprintf("jsonl=%s collections=[%s,%s]", s.path, namedCol, unnamedCol)
	}
	return fmt.Sprintf("synthetic points=%d dim=%d collections=[%s,%s]", s.points, s.dim, namedCol, unnamedCol)
}

type datasetRow struct {
	ID      string                 `json:"id"`
	Vector  []float32              `json:"vector"`
	Payload map[string]any         `json:"payload"`
	Meta    map[string]interface{} `json:"meta,omitempty"`
}

func loadDataset(grpcAddr, collectionNamed, collectionUnnamed, vectorName string, spec datasetSpec) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}
	defer conn.Close()

	collections := qpb.NewCollectionsClient(conn)
	pts := qpb.NewPointsClient(conn)

	if err := recreateCollection(ctx, collections, collectionNamed, spec.dim); err != nil {
		return err
	}
	if err := recreateCollection(ctx, collections, collectionUnnamed, spec.dim); err != nil {
		return err
	}

	const batch = 256
	upsertBatch := func(ctx context.Context, collection string, points []*qpb.PointStruct) error {
		_, err := pts.Upsert(ctx, &qpb.UpsertPoints{CollectionName: collection, Points: points})
		return err
	}

	if spec.path != "" {
		f, err := os.Open(spec.path)
		if err != nil {
			return err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

		var (
			bufNamed   = make([]*qpb.PointStruct, 0, batch)
			bufUnnamed = make([]*qpb.PointStruct, 0, batch)
			line       = 0
			seenDim    = 0
		)
		flush := func() error {
			if len(bufNamed) > 0 {
				if err := upsertBatch(ctx, collectionNamed, bufNamed); err != nil {
					return err
				}
				bufNamed = bufNamed[:0]
			}
			if len(bufUnnamed) > 0 {
				if err := upsertBatch(ctx, collectionUnnamed, bufUnnamed); err != nil {
					return err
				}
				bufUnnamed = bufUnnamed[:0]
			}
			return nil
		}

		for sc.Scan() {
			line++
			var row datasetRow
			if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
				return fmt.Errorf("dataset parse line %d: %w", line, err)
			}
			if row.ID == "" || len(row.Vector) == 0 {
				return fmt.Errorf("dataset line %d: missing id/vector", line)
			}
			if seenDim == 0 {
				seenDim = len(row.Vector)
				if spec.dim != seenDim {
					return fmt.Errorf("dataset dim mismatch: flag dim=%d, dataset dim=%d", spec.dim, seenDim)
				}
			}
			if len(row.Vector) != spec.dim {
				return fmt.Errorf("dataset line %d: vector dim mismatch: got %d expected %d", line, len(row.Vector), spec.dim)
			}
			normalizeInPlace(row.Vector)

			payload := make(map[string]*qpb.Value, len(row.Payload))
			for k, v := range row.Payload {
				payload[k] = anyToQdrantValue(v)
			}

			bufNamed = append(bufNamed, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: row.ID}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								vectorName: {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: row.Vector}}},
							},
						},
					},
				},
				Payload: payload,
			})

			bufUnnamed = append(bufUnnamed, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: row.ID}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: row.Vector}}},
					},
				},
				Payload: payload,
			})

			if len(bufNamed) >= batch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := sc.Err(); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
		return nil
	}

	rnd := rand.New(rand.NewSource(1))
	for off := 0; off < spec.points; off += batch {
		n := batch
		if off+n > spec.points {
			n = spec.points - off
		}
		bufNamed := make([]*qpb.PointStruct, 0, n)
		bufUnnamed := make([]*qpb.PointStruct, 0, n)
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("p%08d", off+i)
			vec := make([]float32, spec.dim)
			for j := 0; j < spec.dim; j++ {
				vec[j] = float32(rnd.NormFloat64())
			}
			normalizeInPlace(vec)

			bufNamed = append(bufNamed, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								vectorName: {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vec}}},
							},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"i": {Kind: &qpb.Value_IntegerValue{IntegerValue: int64(off + i)}},
				},
			})

			bufUnnamed = append(bufUnnamed, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}},
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
		if err := upsertBatch(ctx, collectionNamed, bufNamed); err != nil {
			return fmt.Errorf("upsert named batch@%d: %w", off, err)
		}
		if err := upsertBatch(ctx, collectionUnnamed, bufUnnamed); err != nil {
			return fmt.Errorf("upsert unnamed batch@%d: %w", off, err)
		}
	}

	return nil
}

func recreateCollection(ctx context.Context, c qpb.CollectionsClient, name string, dim int) error {
	_, _ = c.Delete(ctx, &qpb.DeleteCollection{CollectionName: name})
	_, err := c.Create(ctx, &qpb.CreateCollection{
		CollectionName: name,
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: uint64(dim), Distance: qpb.Distance_Cosine},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("create collection %q: %w", name, err)
	}
	return nil
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
		// encoding/json decodes numbers as float64 by default.
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

func boltSetupOnce(addr, cypher string) error {
	fn, cleanup, err := newBoltWorker(addr, cypher)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return fn(ctx)
}

func newGRPCWorker(grpcAddr, collection, vectorName string, query []float32, k uint64) (workerFn, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	client := qpb.NewPointsClient(conn)

	fn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := client.Search(ctx, &qpb.SearchPoints{
			CollectionName: collection,
			Vector:         query,
			Limit:          k,
			VectorName:     ptrString(vectorName), // named vector => brute-force, no search.Service shortcut
			// Leave WithPayload/WithVectors nil to avoid additional per-hit GetNode calls.
		})
		return err
	}
	cleanup := func() { _ = conn.Close() }
	return fn, cleanup, nil
}

func newGRPCWorkerANN(grpcAddr, collection string, query []float32, k uint64) (workerFn, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	client := qpb.NewPointsClient(conn)

	fn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := client.Search(ctx, &qpb.SearchPoints{
			CollectionName: collection,
			Vector:         query,
			Limit:          k,
			// Leave VectorName unset so we can use the default vector and hit the ANN path.
		})
		return err
	}
	cleanup := func() { _ = conn.Close() }
	return fn, cleanup, nil
}

func newGRPCWorkerBruteforce(grpcAddr, collection, vectorName string, query []float32, k uint64) (workerFn, func(), error) {
	return newGRPCWorker(grpcAddr, collection, vectorName, query, k)
}

// =============================================================================
// Benchmark runner
// =============================================================================

type runConfig struct {
	concurrency int
	seconds     time.Duration
	warmup      time.Duration
}

type workerFn func(ctx context.Context) error

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
				logf("[%s] worker init error: %v", label, err)
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
						// Treat deadline/cancel as end-of-run (includes gRPC status codes).
						if ctx.Err() != nil || isContextDoneErr(err) {
							break
						}
						// If the server exits mid-run, abort quickly.
						logf("[%s] op error: %v", label, err)
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

	// Warmup (discard)
	_, _ = doRun(cfg.warmup)

	start := time.Now()
	n, lat := doRun(cfg.seconds)
	elapsed := time.Since(start).Seconds()

	return summary{
		label:        label,
		totalOps:     n,
		totalSeconds: elapsed,
		latencies:    lat,
	}
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
	logf("%s: histogram (ms)", name)
	printHistogram(s.latencies)
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

func printHistogram(durs []time.Duration) {
	if len(durs) == 0 {
		logf("  (no samples)")
		return
	}

	// Log2 buckets on milliseconds, plus a <1ms bucket.
	buckets := make([]int, 0, 20)
	buckets = append(buckets, 0) // <1ms
	for i := 0; i < 16; i++ {
		buckets = append(buckets, 0)
	}

	for _, d := range durs {
		ms := d.Seconds() * 1000
		if ms < 1 {
			buckets[0]++
			continue
		}
		b := int(math.Floor(math.Log2(ms))) + 1
		if b < 1 {
			b = 1
		}
		if b >= len(buckets) {
			b = len(buckets) - 1
		}
		buckets[b]++
	}

	total := len(durs)
	for i, c := range buckets {
		if c == 0 {
			continue
		}
		var lo, hi string
		if i == 0 {
			lo, hi = "0", "1"
		} else {
			lo = fmt.Sprintf("%.0f", math.Pow(2, float64(i-1)))
			hi = fmt.Sprintf("%.0f", math.Pow(2, float64(i)))
		}
		logf("  [%s,%s)ms: %d (%.2f%%)", lo, hi, c, 100*float64(c)/float64(total))
	}
}

// =============================================================================
// Bolt (minimal client)
// =============================================================================

func newBoltWorker(addr, cypher string) (workerFn, func(), error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	if err := boltHandshake(conn); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := boltSend(conn, boltHello()); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := boltDrainUntil(conn, msgSuccess, msgFailure); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	var nonce int64
	fn := func(ctx context.Context) error {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nonce++
		if err := boltSend(conn, boltRun(cypher, encodePSMap1Int("nonce", nonce))); err != nil {
			return err
		}
		if err := boltDrainUntil(conn, msgSuccess, msgFailure); err != nil {
			return err
		}

		if err := boltSend(conn, boltPullAll()); err != nil {
			return err
		}
		// Drain records until SUCCESS (or FAILURE).
		for {
			typ, _, err := boltRead(conn)
			if err != nil {
				return err
			}
			switch typ {
			case msgRecord:
				continue
			case msgSuccess:
				return nil
			case msgFailure:
				return fmt.Errorf("bolt failure")
			default:
				// ignore
			}
		}
	}

	cleanup := func() { _ = conn.Close() }
	return fn, cleanup, nil
}

// boltQueryOnce is retained for ad-hoc manual debugging. It is not used by the harness.

const (
	msgHello   = 0x01
	msgRun     = 0x10
	msgPull    = 0x3F
	msgRecord  = 0x71
	msgSuccess = 0x70
	msgFailure = 0x7F
)

func boltHandshake(conn net.Conn) error {
	// Magic + versions (Bolt 4.4..4.1).
	handshake := []byte{
		0x60, 0x60, 0xB0, 0x17,
		0x00, 0x00, 0x04, 0x04,
		0x00, 0x00, 0x04, 0x03,
		0x00, 0x00, 0x04, 0x02,
		0x00, 0x00, 0x04, 0x01,
	}
	if _, err := conn.Write(handshake); err != nil {
		return err
	}
	resp := make([]byte, 4)
	_, err := io.ReadFull(conn, resp)
	return err
}

func boltHello() []byte {
	// Struct(1) + HELLO + empty map.
	return []byte{0xB1, msgHello, 0xA0}
}

func boltRun(query string, params []byte) []byte {
	// Struct(3) + RUN + query + {} + {}.
	out := []byte{0xB3, msgRun}
	out = append(out, encodePSString(query)...)
	if len(params) == 0 {
		out = append(out, 0xA0) // empty params
	} else {
		out = append(out, params...)
	}
	out = append(out, 0xA0) // metadata
	return out
}

func boltPullAll() []byte {
	// Struct(1) + PULL + {}.
	return []byte{0xB1, msgPull, 0xA0}
}

func boltSend(conn net.Conn, payload []byte) error {
	// Chunked framing: [size:2][chunk...][0,0]
	const maxChunk = 0xFFFF
	rem := payload
	for len(rem) > 0 {
		n := len(rem)
		if n > maxChunk {
			n = maxChunk
		}
		if _, err := conn.Write([]byte{byte(n >> 8), byte(n)}); err != nil {
			return err
		}
		if _, err := conn.Write(rem[:n]); err != nil {
			return err
		}
		rem = rem[n:]
	}
	_, err := conn.Write([]byte{0x00, 0x00})
	return err
}

func boltRead(conn net.Conn) (byte, []byte, error) {
	var full []byte
	var header [2]byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return 0, nil, err
		}
		size := int(header[0])<<8 | int(header[1])
		if size == 0 {
			break
		}
		old := len(full)
		full = append(full, make([]byte, size)...)
		if _, err := io.ReadFull(conn, full[old:old+size]); err != nil {
			return 0, nil, err
		}
	}
	if len(full) < 2 {
		return 0, nil, fmt.Errorf("short message")
	}
	if full[0] >= 0xB0 && full[0] <= 0xBF {
		return full[1], full[2:], nil
	}
	return full[0], full[1:], nil
}

func boltDrainUntil(conn net.Conn, okType, errType byte) error {
	for {
		typ, _, err := boltRead(conn)
		if err != nil {
			return err
		}
		if typ == okType {
			return nil
		}
		if typ == errType {
			return fmt.Errorf("bolt failure")
		}
	}
}

func encodePSString(s string) []byte {
	b := []byte(s)
	if len(b) < 16 {
		return append([]byte{byte(0x80 + len(b))}, b...)
	}
	if len(b) < 256 {
		return append([]byte{0xD0, byte(len(b))}, b...)
	}
	if len(b) < 65536 {
		return append([]byte{0xD1, byte(len(b) >> 8), byte(len(b))}, b...)
	}
	panic("string too long")
}

func encodePSMap1Int(key string, val int64) []byte {
	// Map with 1 entry: tiny map marker 0xA1.
	out := []byte{0xA1}
	out = append(out, encodePSString(key)...)
	out = append(out, encodePSInt(val)...)
	return out
}

func encodePSInt(v int64) []byte {
	// PackStream integer encoding (subset).
	if v >= -16 && v <= 127 {
		return []byte{byte(int8(v))}
	}
	if v >= -128 && v <= 127 {
		return []byte{0xC8, byte(int8(v))}
	}
	if v >= -32768 && v <= 32767 {
		return []byte{0xC9, byte(v >> 8), byte(v)}
	}
	if v >= -2147483648 && v <= 2147483647 {
		return []byte{0xCA, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
	return []byte{0xCB, byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// =============================================================================
// CSV
// =============================================================================

type csvRow struct {
	Timestamp string
	Protocol  string
	Scenario  string
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

func rowFromSummary(protocol, scenario string, points, dim, k, conc int, s summary) csvRow {
	p50, p95, p99, min, max, mean := latencyStats(s.latencies)
	return csvRow{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Protocol:  protocol,
		Scenario:  scenario,
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
			"timestamp", "protocol", "scenario", "points", "dim", "k", "concurrency",
			"ops", "seconds", "ops_per_sec",
			"p50_ms", "p95_ms", "p99_ms", "mean_ms", "min_ms", "max_ms",
		}); err != nil {
			return err
		}
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.Timestamp,
			r.Protocol,
			r.Scenario,
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
// Small helpers
// =============================================================================

func makeDeterministicQuery(dim int) []float32 {
	// Stable query vector for regression tracking.
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

func float32ListLiteral(v []float32) string {
	// Keep this tight; it is not on the measured hot path.
	var sb strings.Builder
	for i, x := range v {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(x), 'f', 6, 32))
	}
	return sb.String()
}

func ptrString(s string) *string { return &s }

func logf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
}

func fatalf(format string, args ...any) {
	logf(format, args...)
	os.Exit(1)
}

// Ensure any unexpected server issues are visible when running the harness.
func tailFile(path string, lines int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var all []string
	for sc.Scan() {
		all = append(all, sc.Text())
		if len(all) > lines {
			all = all[len(all)-lines:]
		}
	}
	return strings.Join(all, "\n")
}
