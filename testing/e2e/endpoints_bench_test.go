//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This is an end-to-end harness you can run on any branch to:
//   - insert data via Bolt (Neo4j compatibility surface),
//   - query it via Bolt + Neo4j HTTP + GraphQL + Nornic REST,
//   - optionally (if available) insert/query via Qdrant gRPC,
//   - benchmark each endpoint for regression detection.
//
// Run:
//
//	go test ./testing/e2e -tags=e2e -run TestEndpointParityAndBenchmark -count=1
//
// Optional env knobs:
//   - NORNICDB_E2E_SECONDS (default 3)
//   - NORNICDB_E2E_WARMUP_SECONDS (default 1)
//   - NORNICDB_E2E_CONCURRENCY (default GOMAXPROCS)
//   - NORNICDB_E2E_POINTS_BOLT (default 1000)
//   - NORNICDB_E2E_POINTS_GRPC (default 5000)
func TestEndpointParityAndBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e benchmark in -short")
	}

	reportf := func(format string, args ...any) {
		t.Helper()
		msg := fmt.Sprintf(format, args...)
		t.Log(msg)
		// Always print, even without `-v`, so this test can be used as a branch-to-branch
		// performance/regression harness with simple `go test` output.
		fmt.Fprintln(os.Stdout, msg)
	}

	repoRoot := mustRepoRoot(t)
	dataDir := t.TempDir()
	binPath := buildNornicBinary(t, repoRoot)

	httpPort := pickPort(t)
	boltPort := pickPort(t)
	grpcPort := pickPort(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proc := startNornicDB(t, ctx, binPath, dataDir, httpPort, boltPort, grpcPort)
	defer proc.stop(t)

	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	boltAddr := fmt.Sprintf("127.0.0.1:%d", boltPort)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)

	waitTCP(t, httpAddr, 30*time.Second)
	waitTCP(t, boltAddr, 30*time.Second)

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     60 * time.Second,
		},
	}

	defaultDB := discoverDefaultDatabase(t, httpClient, httpAddr)
	reportf("default database: %s", defaultDB)

	// -------------------------------------------------------------------------
	// 1) Insert data via Bolt
	// -------------------------------------------------------------------------
	boltPoints := envInt("NORNICDB_E2E_POINTS_BOLT", 1000)
	benchLabel := "BenchE2E"

	driver := newBoltDriver(t, boltAddr)
	defer func() { _ = driver.Close(context.Background()) }()

	stepStart := time.Now()
	insertViaBolt(t, driver, benchLabel, boltPoints)
	reportf("insert via bolt: n=%d took=%s", boltPoints, time.Since(stepStart))

	// Verify via Bolt
	stepStart = time.Now()
	got := countViaBolt(t, driver, "", benchLabel)
	require.Equal(t, boltPoints, got)
	reportf("verify via bolt: count=%d took=%s", got, time.Since(stepStart))

	// Verify via Neo4j HTTP
	stepStart = time.Now()
	got = countViaNeo4jHTTP(t, httpClient, httpAddr, defaultDB, benchLabel)
	require.Equal(t, boltPoints, got)
	reportf("verify via neo4j http: count=%d took=%s", got, time.Since(stepStart))

	// Verify via GraphQL
	stepStart = time.Now()
	got = countViaGraphQL(t, httpClient, httpAddr, benchLabel)
	require.Equal(t, boltPoints, got)
	reportf("verify via graphql: count=%d took=%s", got, time.Since(stepStart))

	// Verify via Nornic REST search (text-only).
	stepStart = time.Now()
	require.NotEmpty(t, searchViaNornicREST(t, httpClient, httpAddr, "hello", []string{benchLabel}, 5))
	reportf("verify via /nornicdb/search: ok took=%s", time.Since(stepStart))

	// -------------------------------------------------------------------------
	// 2) Optional: Qdrant gRPC E2E (insert via gRPC, read via Bolt/Cypher)
	// -------------------------------------------------------------------------
	grpcAvailable := tcpReachable(grpcAddr, 250*time.Millisecond)
	grpcEnabled := envInt("NORNICDB_E2E_GRPC_ENABLED", 1) != 0
	if !grpcEnabled {
		reportf("qdrant grpc: disabled via NORNICDB_E2E_GRPC_ENABLED=0")
	}

	if grpcAvailable {
		reportf("qdrant grpc: detected addr=%s", grpcAddr)

		grpcPoints := envInt("NORNICDB_E2E_POINTS_GRPC", 5000)
		collection := "bench_col_e2e"
		dim := 128

		if grpcEnabled {
			qconn := dialGRPC(t, grpcAddr)
			defer qconn.Close()

			stepStart = time.Now()
			upsertViaQdrantGRPC(t, qconn, collection, dim, grpcPoints)
			reportf("insert via qdrant grpc: col=%s dim=%d n=%d took=%s", collection, dim, grpcPoints, time.Since(stepStart))

			// Read via Bolt/Graph surfaces: each collection is its own
			// database (named after the collection), and its points are nodes
			// labelled :QdrantPoint:Point there.
			qdrantLabel := "QdrantPoint"
			stepStart = time.Now()
			gotQdrantPoints := countViaBolt(t, driver, collection, qdrantLabel)
			require.GreaterOrEqual(t, gotQdrantPoints, grpcPoints)
			reportf("verify qdrant points via bolt: label=%s count=%d took=%s", qdrantLabel, gotQdrantPoints, time.Since(stepStart))

			stepStart = time.Now()
			gotQdrantPoints = countViaNeo4jHTTP(t, httpClient, httpAddr, collection, qdrantLabel)
			require.GreaterOrEqual(t, gotQdrantPoints, grpcPoints)
			reportf("verify qdrant points via neo4j http: label=%s count=%d took=%s", qdrantLabel, gotQdrantPoints, time.Since(stepStart))
		}
	} else {
		reportf("qdrant grpc: not available (skipping)")
	}

	// -------------------------------------------------------------------------
	// 3) Benchmarks (NornicDB only)
	// -------------------------------------------------------------------------
	runSeconds := time.Duration(envInt("NORNICDB_E2E_SECONDS", 3)) * time.Second
	warmupSeconds := time.Duration(envInt("NORNICDB_E2E_WARMUP_SECONDS", 1)) * time.Second
	concurrency := envInt("NORNICDB_E2E_CONCURRENCY", runtime.GOMAXPROCS(0))
	if concurrency <= 0 {
		concurrency = 1
	}

	reportf("benchmark config: concurrency=%d warmup=%s run=%s", concurrency, warmupSeconds, runSeconds)

	t.Run("bench:bolt", func(t *testing.T) {
		sum := runBench(t, concurrency, warmupSeconds, runSeconds, func() (func(context.Context) error, func()) {
			// Per worker: new session
			sess := driver.NewSession(context.Background(), neo4j.SessionConfig{})
			return func(ctx context.Context) error {
				_, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
					res, err := tx.Run(ctx, "MATCH (n:"+benchLabel+") RETURN count(n) AS c", nil)
					if err != nil {
						return nil, err
					}
					if !res.Next(ctx) {
						return nil, res.Err()
					}
					_, _ = res.Record().Get("c")
					return nil, res.Err()
				})
				return err
			}, func() { _ = sess.Close(context.Background()) }
		})
		reportf("%s", sum.String())
	})

	t.Run("bench:neo4j-http", func(t *testing.T) {
		stmt := "MATCH (n:" + benchLabel + ") RETURN count(n) AS c"
		sum := runBench(t, concurrency, warmupSeconds, runSeconds, func() (func(context.Context) error, func()) {
			return func(ctx context.Context) error {
				_, err := neo4jHTTPCommit(ctx, httpClient, httpAddr, defaultDB, stmt)
				return err
			}, func() {}
		})
		reportf("%s", sum.String())
	})

	t.Run("bench:graphql", func(t *testing.T) {
		query := fmt.Sprintf(`query { nodeCount(label: %q) }`, benchLabel)
		sum := runBench(t, concurrency, warmupSeconds, runSeconds, func() (func(context.Context) error, func()) {
			return func(ctx context.Context) error {
				_, err := graphqlQuery(ctx, httpClient, httpAddr, query)
				return err
			}, func() {}
		})
		reportf("%s", sum.String())
	})

	t.Run("bench:nornic-search-rest", func(t *testing.T) {
		sum := runBench(t, concurrency, warmupSeconds, runSeconds, func() (func(context.Context) error, func()) {
			return func(ctx context.Context) error {
				_, err := nornicSearch(ctx, httpClient, httpAddr, "hello", []string{benchLabel}, 10)
				return err
			}, func() {}
		})
		reportf("%s", sum.String())
	})

	if grpcAvailable && grpcEnabled {
		t.Run("bench:qdrant-grpc", func(t *testing.T) {
			qconn := dialGRPC(t, grpcAddr)
			defer qconn.Close()
			points := qpb.NewPointsClient(qconn)

			collection := "bench_col_e2e"
			dim := 128
			queryVec := make([]float32, dim)
			rnd := rand.New(rand.NewSource(1))
			for i := 0; i < dim; i++ {
				queryVec[i] = float32(rnd.NormFloat64())
			}

			req := &qpb.SearchPoints{
				CollectionName: collection,
				Vector:         queryVec,
				Limit:          10,
			}

			sum := runBench(t, concurrency, warmupSeconds, runSeconds, func() (func(context.Context) error, func()) {
				return func(ctx context.Context) error {
					_, err := points.Search(ctx, req)
					return err
				}, func() {}
			})
			reportf("%s", sum.String())
		})
	}
}

type serverProc struct {
	cmd  *exec.Cmd
	logf *os.File
}

func (p *serverProc) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	case <-done:
	}

	if p.logf != nil {
		_ = p.logf.Close()
	}
}

func buildNornicBinary(t *testing.T, repoRoot string) string {
	t.Helper()
	binName := "nornicdb-e2e"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	out := filepath.Join(t.TempDir(), binName)
	cmd := exec.Command("go", "build", "-o", out, "./cmd/nornicdb")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
	return out
}

func startNornicDB(t *testing.T, ctx context.Context, binPath, dataDir string, httpPort, boltPort, grpcPort int) *serverProc {
	t.Helper()

	logPath := filepath.Join(dataDir, "server.log")
	f, err := os.Create(logPath)
	require.NoError(t, err)

	cmd := exec.CommandContext(ctx, binPath, "serve",
		"--data-dir", dataDir,
		"--address", "127.0.0.1",
		"--http-port", strconv.Itoa(httpPort),
		"--bolt-port", strconv.Itoa(boltPort),
		"--no-auth",
		"--headless",
		"--mcp-enabled=false",
	)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(),
		// Prevent loading ~/.nornicdb/config.yaml (keep the test hermetic).
		"HOME="+filepath.Join(dataDir, "home"),
		// Keep vector dimensions consistent across endpoints (avoid indexing mismatches when gRPC uses 128-dim vectors).
		"NORNICDB_EMBEDDING_DIMENSIONS=128",
		"NORNICDB_EMBEDDING_ENABLED=false",
		// If the branch supports qdrantgrpc, it will bind this port.
		"NORNICDB_QDRANT_GRPC_ENABLED=true",
		fmt.Sprintf("NORNICDB_QDRANT_GRPC_LISTEN_ADDR=127.0.0.1:%d", grpcPort),
		"NORNICDB_MCP_ENABLED=false",
		"NORNICDB_HEIMDALL_ENABLED=false",
	)
	require.NoError(t, cmd.Start())
	return &serverProc{cmd: cmd, logf: f}
}

func mustRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	dir := wd
	for i := 0; i < 20; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	t.Fatalf("could not find repo root from %s", wd)
	return ""
}

func pickPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tcpReachable(addr, 250*time.Millisecond) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", addr)
}

func tcpReachable(addr string, timeout time.Duration) bool {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

// =============================================================================
// Bolt (Neo4j driver)
// =============================================================================

func newBoltDriver(t *testing.T, addr string) neo4j.DriverWithContext {
	t.Helper()
	// Use direct Bolt rather than routing; this server does not expose routing table discovery.
	uri := "bolt://" + addr
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.VerifyConnectivity(ctx))
	return driver
}

func insertViaBolt(t *testing.T, driver neo4j.DriverWithContext, label string, n int) {
	t.Helper()
	if n <= 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Provide an explicit embedding so the search service can build vector indexes
	// deterministically without relying on an external embedder.
	const dim = 128
	emb := make([]float64, dim)
	rnd := rand.New(rand.NewSource(1))
	var norm float64
	for i := 0; i < dim; i++ {
		v := rnd.NormFloat64()
		emb[i] = v
		norm += v * v
	}
	norm = 1.0 / (math.Sqrt(norm) + 1e-12)
	for i := range emb {
		emb[i] *= norm
	}

	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, int64(i))
	}

	sess := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = sess.Close(ctx) }()

	_, err := sess.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, "UNWIND $ids AS i CREATE (n:"+label+" {i: i, text: $text, embedding: $embedding})", map[string]any{
			"ids":       ids,
			"text":      "hello",
			"embedding": emb,
		})
		return nil, err
	})
	require.NoError(t, err)
}

// countViaBolt counts label's nodes in database ("" for the default one).
func countViaBolt(t *testing.T, driver neo4j.DriverWithContext, database, label string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
	defer func() { _ = sess.Close(ctx) }()

	outAny, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, "MATCH (n:"+label+") RETURN count(n) AS c", nil)
		if err != nil {
			return nil, err
		}
		if !res.Next(ctx) {
			return nil, res.Err()
		}
		v, ok := res.Record().Get("c")
		if !ok {
			return 0, nil
		}
		switch vv := v.(type) {
		case int64:
			return int(vv), nil
		case int:
			return vv, nil
		case float64:
			return int(vv), nil
		default:
			return 0, nil
		}
	})
	require.NoError(t, err)
	if outAny == nil {
		return 0
	}
	return outAny.(int)
}

// =============================================================================
// Neo4j HTTP transaction endpoint
// =============================================================================

func countViaNeo4jHTTP(t *testing.T, c *http.Client, httpAddr, db, label string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body, err := neo4jHTTPCommit(ctx, c, httpAddr, db, "MATCH (n:"+label+") RETURN count(n) AS c")
	require.NoError(t, err)

	// Minimal parse of Neo4j-style response:
	// results[0].data[0].row[0] is the scalar.
	var resp struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Errors, 0)
	require.Len(t, resp.Results, 1)
	require.Len(t, resp.Results[0].Data, 1)
	require.Len(t, resp.Results[0].Data[0].Row, 1)
	switch v := resp.Results[0].Data[0].Row[0].(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

func discoverDefaultDatabase(t *testing.T, c *http.Client, httpAddr string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+httpAddr+"/", nil)
	require.NoError(t, err)
	resp, err := c.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := ioReadAll(resp.Body, 1<<20)
	require.Equal(t, http.StatusOK, resp.StatusCode, "discovery status=%d body=%s", resp.StatusCode, string(body))

	var disc struct {
		DefaultDatabase string `json:"default_database"`
	}
	require.NoError(t, json.Unmarshal(body, &disc))
	if disc.DefaultDatabase == "" {
		return "nornic"
	}
	return disc.DefaultDatabase
}

func neo4jHTTPCommit(ctx context.Context, c *http.Client, httpAddr, db, statement string) ([]byte, error) {
	reqBody := map[string]any{
		"statements": []map[string]any{
			{"statement": statement},
		},
	}
	raw, _ := json.Marshal(reqBody)
	url := fmt.Sprintf("http://%s/db/%s/tx/commit", httpAddr, db)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := ioReadAll(resp.Body, 10<<20)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("neo4j http status=%d body=%s", resp.StatusCode, string(body))
	}
	return body, nil
}

// =============================================================================
// GraphQL
// =============================================================================

func countViaGraphQL(t *testing.T, c *http.Client, httpAddr, label string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := fmt.Sprintf(`query { nodeCount(label: %q) }`, label)
	body, err := graphqlQuery(ctx, c, httpAddr, query)
	require.NoError(t, err)

	var resp struct {
		Data struct {
			NodeCount int `json:"nodeCount"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Errors, 0)
	return resp.Data.NodeCount
}

func graphqlQuery(ctx context.Context, c *http.Client, httpAddr, query string) ([]byte, error) {
	reqBody := map[string]any{"query": query}
	raw, _ := json.Marshal(reqBody)
	url := fmt.Sprintf("http://%s/graphql", httpAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := ioReadAll(resp.Body, 10<<20)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("graphql status=%d body=%s", resp.StatusCode, string(body))
	}
	return body, nil
}

// =============================================================================
// Nornic REST (/nornicdb/search)
// =============================================================================

func searchViaNornicREST(t *testing.T, c *http.Client, httpAddr, query string, labels []string, limit int) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body, err := nornicSearch(ctx, c, httpAddr, query, labels, limit)
	require.NoError(t, err)
	var out []map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func nornicSearch(ctx context.Context, c *http.Client, httpAddr, query string, labels []string, limit int) ([]byte, error) {
	reqBody := map[string]any{
		"query":  query,
		"labels": labels,
		"limit":  limit,
	}
	raw, _ := json.Marshal(reqBody)
	url := fmt.Sprintf("http://%s/nornicdb/search", httpAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := ioReadAll(resp.Body, 10<<20)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nornic search status=%d body=%s", resp.StatusCode, string(body))
	}
	return body, nil
}

// =============================================================================
// Qdrant gRPC (optional)
// =============================================================================

func dialGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	return conn
}

func upsertViaQdrantGRPC(t *testing.T, conn *grpc.ClientConn, collection string, dim int, points int) {
	t.Helper()
	require.Greater(t, dim, 0)
	require.Greater(t, points, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	collections := qpb.NewCollectionsClient(conn)
	pts := qpb.NewPointsClient(conn)

	_, _ = collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: collection})
	_, err := collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: collection,
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: uint64(dim), Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.NoError(t, err)

	const batch = 256
	rnd := rand.New(rand.NewSource(1))
	for off := 0; off < points; off += batch {
		n := batch
		if off+n > points {
			n = points - off
		}
		buf := make([]*qpb.PointStruct, 0, n)
		for i := 0; i < n; i++ {
			vec := make([]float32, dim)
			for j := 0; j < dim; j++ {
				vec[j] = float32(rnd.NormFloat64())
			}
			id := uint64(off + i)
			buf = append(buf, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: id}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vec}}},
					},
				},
			})
		}
		_, err := pts.Upsert(ctx, &qpb.UpsertPoints{CollectionName: collection, Points: buf})
		require.NoError(t, err)
	}
}

// =============================================================================
// Benchmark runner
// =============================================================================

type benchSummary struct {
	ops  int
	dur  time.Duration
	lat  []time.Duration
	name string
}

func (s benchSummary) String() string {
	if s.dur <= 0 || len(s.lat) == 0 {
		return fmt.Sprintf("%s: ops=%d secs=%.3f ops/sec=%.2f", s.name, s.ops, s.dur.Seconds(), float64(s.ops)/s.dur.Seconds())
	}
	p50, p95, p99 := percentile(s.lat, 0.50), percentile(s.lat, 0.95), percentile(s.lat, 0.99)
	min, max := s.lat[0], s.lat[0]
	var sum time.Duration
	for _, d := range s.lat {
		sum += d
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	mean := time.Duration(int64(sum) / int64(len(s.lat)))
	return fmt.Sprintf("%s: ops=%d secs=%.3f ops/sec=%.2f latency_ms: min=%.3f p50=%.3f p95=%.3f p99=%.3f max=%.3f mean=%.3f",
		s.name, s.ops, s.dur.Seconds(), float64(s.ops)/s.dur.Seconds(),
		float64(min)/float64(time.Millisecond),
		float64(p50)/float64(time.Millisecond),
		float64(p95)/float64(time.Millisecond),
		float64(p99)/float64(time.Millisecond),
		float64(max)/float64(time.Millisecond),
		float64(mean)/float64(time.Millisecond),
	)
}

func runBench(t *testing.T, concurrency int, warmup time.Duration, dur time.Duration, newWorker func() (func(context.Context) error, func())) benchSummary {
	t.Helper()

	doRun := func(d time.Duration) (int, []time.Duration) {
		if d <= 0 {
			return 0, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()

		var (
			mu  sync.Mutex
			ops int
			all []time.Duration
			wg  sync.WaitGroup
		)
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			fn, cleanup := newWorker()
			go func(fn func(context.Context) error, cleanup func()) {
				defer wg.Done()
				defer cleanup()
				local := make([]time.Duration, 0, 1024)
				for ctx.Err() == nil {
					start := time.Now()
					err := fn(ctx)
					lat := time.Since(start)
					if err != nil {
						break
					}
					local = append(local, lat)
				}
				mu.Lock()
				ops += len(local)
				all = append(all, local...)
				mu.Unlock()
			}(fn, cleanup)
		}
		wg.Wait()
		return ops, all
	}

	_, _ = doRun(warmup)
	start := time.Now()
	ops, lat := doRun(dur)
	elapsed := time.Since(start)
	// Sort once for percentiles.
	sortDurations(lat)
	return benchSummary{ops: ops, dur: elapsed, lat: lat, name: t.Name()}
}

func sortDurations(d []time.Duration) {
	if len(d) < 2 {
		return
	}
	// Small inline quicksort to avoid pulling in sort for this file.
	var qs func(lo, hi int)
	qs = func(lo, hi int) {
		if lo >= hi {
			return
		}
		p := d[(lo+hi)/2]
		i, j := lo, hi
		for i <= j {
			for d[i] < p {
				i++
			}
			for d[j] > p {
				j--
			}
			if i <= j {
				d[i], d[j] = d[j], d[i]
				i++
				j--
			}
		}
		if lo < j {
			qs(lo, j)
		}
		if i < hi {
			qs(i, hi)
		}
	}
	qs(0, len(d)-1)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func ioReadAll(r io.Reader, limit int64) ([]byte, error) {
	var buf bytes.Buffer
	if limit <= 0 {
		limit = 10 << 20
	}
	_, err := buf.ReadFrom(io.LimitReader(r, limit))
	return buf.Bytes(), err
}
