//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

const largeRRFEnabledEnv = "NORNICDB_LARGE_RRF_E2E"

func TestLargeDatasetRRFRoundTrip_BoltVsHTTP(t *testing.T) {
	if os.Getenv(largeRRFEnabledEnv) != "1" {
		t.Skip("set " + largeRRFEnabledEnv + "=1 to run the 200k-embedding BGE-M3 benchmark")
	}
	if testing.Short() {
		t.Skip("skipping large RRF round-trip benchmark in -short")
	}

	repoRoot := mustRepoRoot(t)
	modelPath := envOrDefaultPath(repoRoot, "NORNICDB_LARGE_RRF_MODEL_PATH", "models/bge-m3.gguf")
	httpAddr := strings.TrimSpace(os.Getenv("NORNICDB_LARGE_RRF_HTTP_ADDR"))
	boltAddr := strings.TrimSpace(os.Getenv("NORNICDB_LARGE_RRF_BOLT_ADDR"))
	var proc *serverProc
	if httpAddr == "" {
		sourceDataDir := envOrDefaultPath(repoRoot, "NORNICDB_LARGE_RRF_DATA_DIR", "data/test-200kembed")
		requireDirectory(t, sourceDataDir)
		requireRegularFile(t, modelPath)
		workRoot := t.TempDir()
		dataDir := filepath.Join(workRoot, "data")
		cloneLargeBadgerFixture(t, sourceDataDir, dataDir)
		binPath := buildLocalEmbeddingNornicBinary(t, repoRoot)
		httpPort := pickPort(t)
		boltPort := pickPort(t)
		telemetryPort := pickPort(t)
		ctx, cancel := context.WithCancel(context.Background())
		proc = startLargeRRFServer(t, ctx, binPath, workRoot, dataDir, modelPath, httpPort, boltPort, telemetryPort)
		defer func() {
			cancel()
			proc.stop(t)
		}()
		httpAddr = fmt.Sprintf("127.0.0.1:%d", httpPort)
		boltAddr = fmt.Sprintf("127.0.0.1:%d", boltPort)
	} else {
		require.NotEmpty(t, boltAddr, "NORNICDB_LARGE_RRF_BOLT_ADDR is required with an external HTTP address")
	}
	waitTraversalTCP(t, httpAddr, 30*time.Minute, proc)
	waitTCP(t, boltAddr, 30*time.Minute)

	httpClient := newTraversalHTTPClient("", "")
	httpClient.Timeout = 60 * time.Second
	driver := newBoltDriverWithAuth(t, boltAddr, "", "")
	defer func() { _ = driver.Close(context.Background()) }()
	dbName := strings.TrimSpace(os.Getenv("NORNICDB_LARGE_RRF_DATABASE"))
	if dbName == "" {
		dbName = discoverLargestUserDatabase(t, driver)
	}
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: dbName,
	})
	defer func() { _ = session.Close(context.Background()) }()

	waitForLargeRRFSearchReady(t, httpClient, httpAddr, dbName)
	nodeCount := largeRRFCount(t, session, "MATCH (n) RETURN count(n)")
	nativeEdgeCount := largeRRFCount(t, session, "MATCH ()-[r]->() RETURN count(r)")
	require.GreaterOrEqual(t, nodeCount, int64(100_000), "large benchmark requires a production-scale node corpus")
	targetEdges := envInt("NORNICDB_LARGE_RRF_EDGES", 1_000_000)
	require.Greater(t, targetEdges, 0)
	require.LessOrEqual(t, targetEdges, 1_000_000, "large RRF fixture is capped at one million generated edges")
	generatedEdges := seedLargeRRFRelationships(t, driver, dbName, nodeCount, targetEdges)
	seedQuery := largeRRFSeedQuery(t, session)

	iterations := envInt("NORNICDB_LARGE_RRF_ITERS", 800)
	warmupIterations := envInt("NORNICDB_LARGE_RRF_WARMUP", 3)
	require.Positive(t, iterations)
	require.GreaterOrEqual(t, warmupIterations, 0)
	t.Logf("large RRF fixture: database=%s nodes=%d native_edges=%d generated_edges=%d model=%s dimensions=1024 warmup=%d measured=%d", dbName, nodeCount, nativeEdgeCount, generatedEdges, modelPath, warmupIterations, iterations)

	type shapeSpec struct {
		name  string
		query string
	}
	shapes := []shapeSpec{
		{
			name: "large_rrf_retrieval",
			query: `
CALL db.retrieve($request)
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
RETURN elementId(node), score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
ORDER BY rrf_score DESC
LIMIT 1`,
		},
		{
			name: "large_rrf_retrieval_1hop",
			query: `
CALL db.retrieve($request)
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
MATCH (node)-[:BENCH_ENTITY_LINK]->(neighbor)
RETURN elementId(node), elementId(neighbor), score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
ORDER BY rrf_score DESC
LIMIT 1`,
		},
	}

	runFingerprint := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	sequence := 0
	nextParams := func() map[string]any {
		sequence++
		return map[string]any{"request": map[string]any{
			"query": largeRRFUncachedQuery(seedQuery, runFingerprint, sequence),
			"limit": int64(10),
		}}
	}
	rows := make([]traversalTableRow, 0, len(shapes)*2)
	for _, shape := range shapes {
		assertRow := func(t *testing.T, row []any) {
			t.Helper()
			offset := 0
			if shape.name == "large_rrf_retrieval_1hop" {
				require.Len(t, row, 8)
				require.NotEmpty(t, normalizeElementIDE2E(fmt.Sprintf("%v", row[1])))
				offset = 1
			} else {
				require.Len(t, row, 7)
			}
			require.Positive(t, rowAsFloat64E2E(t, row[2+offset]), "RRF score must be populated")
			vectorRank := rowAsInt64(t, row[3+offset])
			bm25Rank := rowAsInt64(t, row[4+offset])
			require.True(t, vectorRank > 0 || bm25Rank > 0, "top RRF row must originate from at least one retrieval branch")
			require.Equal(t, "rrf_hybrid", row[5+offset])
			require.Equal(t, false, row[6+offset])
		}

		boltSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
			row, err := runBoltSingleRow(ctx, session, shape.query, nextParams())
			if err == nil {
				assertRow(t, row)
			}
			return err
		})
		require.NoError(t, err, "shape=%s protocol=bolt", shape.name)
		rows = append(rows, summarizeTableRow(shape.name, "-", 0, "bolt", boltSummary))

		httpSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
			row, err := neo4jHTTPCommitSingleRow(ctx, httpClient, httpAddr, dbName, shape.query, nextParams())
			if err == nil {
				assertRow(t, row)
			}
			return err
		})
		require.NoError(t, err, "shape=%s protocol=http", shape.name)
		rows = append(rows, summarizeTableRow(shape.name, "-", 0, "http", httpSummary))
	}
	reportTraversalRows(func(format string, args ...any) {
		fmt.Fprintf(os.Stdout, format+"\n", args...)
	}, rows)
}

func envOrDefaultPath(repoRoot, key, relativeDefault string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if filepath.IsAbs(value) {
			return value
		}
		return filepath.Join(repoRoot, value)
	}
	return filepath.Join(repoRoot, relativeDefault)
}

func requireDirectory(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Skipf("optional large RRF dataset is absent: %s", path)
	}
	require.NoError(t, err, "required directory %s", path)
	require.True(t, info.IsDir(), "required path is not a directory: %s", path)
}

func requireRegularFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Skipf("optional BGE-M3 model is absent: %s", path)
	}
	require.NoError(t, err, "required file %s", path)
	require.True(t, info.Mode().IsRegular(), "required path is not a regular file: %s", path)
}

func cloneLargeBadgerFixture(t *testing.T, source, destination string) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("automatic large-fixture cloning currently requires macOS APFS cp -c; provide a platform clone implementation before running")
	}
	require.NoError(t, os.MkdirAll(destination, 0o755))
	cmd := exec.Command("cp", "-cR", filepath.Join(source, "."), destination)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "copy-on-write clone failed: %s", strings.TrimSpace(string(output)))
}

func buildLocalEmbeddingNornicBinary(t *testing.T, repoRoot string) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "nornicdb-large-rrf-e2e")
	cmd := exec.Command("go", "build", "-tags", "localllm", "-o", out, "./cmd/nornicdb")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	require.NoError(t, cmd.Run(), "build localllm-enabled NornicDB binary")
	return out
}

func startLargeRRFServer(t *testing.T, ctx context.Context, binPath, workRoot, dataDir, modelPath string, httpPort, boltPort, telemetryPort int) *serverProc {
	t.Helper()
	logFile, err := os.Create(filepath.Join(workRoot, "server.log"))
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binPath, "serve",
		"--data-dir", dataDir,
		"--upgrade-storage",
		"--address", "127.0.0.1",
		"--http-port", strconv.Itoa(httpPort),
		"--bolt-port", strconv.Itoa(boltPort),
		"--embedding-provider", "local",
		"--embedding-model", strings.TrimSuffix(filepath.Base(modelPath), filepath.Ext(modelPath)),
		"--embedding-dim", "1024",
		"--embedding-enabled",
		"--embedding-cache", "0",
		"--embedding-gpu-layers", "-1",
		"--no-auth",
		"--headless",
		"--mcp-enabled=false",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = append(os.Environ(),
		"HOME="+filepath.Join(workRoot, "home"),
		"NORNICDB_MODELS_DIR="+filepath.Dir(modelPath),
		"NORNICDB_EMBEDDING_ENABLED=true",
		"NORNICDB_EMBEDDING_PROVIDER=local",
		"NORNICDB_EMBEDDING_DIMENSIONS=1024",
		"NORNICDB_EMBEDDING_CACHE_SIZE=0",
		"NORNICDB_EMBED_WORKER_NUM_WORKERS=0",
		fmt.Sprintf("NORNICDB_TELEMETRY_LISTEN=127.0.0.1:%d", telemetryPort),
		"NORNICDB_QDRANT_GRPC_ENABLED=false",
		"NORNICDB_MCP_ENABLED=false",
		"NORNICDB_HEIMDALL_ENABLED=false",
	)
	require.NoError(t, cmd.Start())
	return &serverProc{cmd: cmd, logf: logFile}
}

func largeRRFCount(t *testing.T, session neo4j.SessionWithContext, query string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	row, err := runBoltSingleRow(ctx, session, query, nil)
	require.NoError(t, err)
	require.Len(t, row, 1)
	return rowAsInt64(t, row[0])
}

func discoverLargestUserDatabase(t *testing.T, driver neo4j.DriverWithContext) string {
	t.Helper()
	systemSession := driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: "system"})
	defer func() { _ = systemSession.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := systemSession.Run(ctx, "SHOW DATABASES", nil)
	require.NoError(t, err)
	var names []string
	for result.Next(ctx) {
		name, _ := result.Record().Get("name")
		if value, ok := name.(string); ok && value != "" && value != "system" {
			names = append(names, value)
		}
	}
	require.NoError(t, result.Err())
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, names, "no user databases found")

	largestName := ""
	var largestCount int64
	for _, name := range names {
		session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: name})
		count := largeRRFCount(t, session, "MATCH (n) RETURN count(n)")
		require.NoError(t, session.Close(context.Background()))
		if count > largestCount {
			largestName = name
			largestCount = count
		}
	}
	require.NotEmpty(t, largestName)
	t.Logf("selected largest user database: name=%s nodes=%d", largestName, largestCount)
	return largestName
}

func seedLargeRRFRelationships(t *testing.T, driver neo4j.DriverWithContext, dbName string, nodeCount int64, targetEdges int) int64 {
	t.Helper()
	readSession := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead, DatabaseName: dbName})
	entities := collectLargeRRFEntities(t, readSession, nodeCount)
	require.NoError(t, readSession.Close(context.Background()))

	writeSession := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite, DatabaseName: dbName})
	defer func() { _ = writeSession.Close(context.Background()) }()
	existing := largeRRFCount(t, writeSession, "MATCH ()-[r:BENCH_ENTITY_LINK]->() RETURN count(r)")
	if existing == int64(targetEdges) {
		verifyLargeRRFRelationships(t, writeSession, int64(len(entities)), int64(targetEdges))
		t.Logf("reusing verified deterministic benchmark topology: edges=%d", existing)
		return existing
	}
	require.Zero(t, existing, "existing BENCH_ENTITY_LINK topology has an unexpected size; use a fresh cloned fixture")

	batchSize := envInt("NORNICDB_LARGE_RRF_EDGE_BATCH", 2_000)
	require.Greater(t, batchSize, 0)
	rows := make([]map[string]any, 0, batchSize)
	created := int64(0)
	flush := func() {
		if len(rows) == 0 {
			return
		}
		query := `
UNWIND $rows AS row
MATCH (source) WHERE elementId(source) = row.sourceID
MATCH (target) WHERE elementId(target) = row.targetID
CREATE (source)-[:BENCH_ENTITY_LINK]->(target)
RETURN count(source) AS created`
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		row, err := runBoltSingleRow(ctx, writeSession, query, map[string]any{"rows": rows})
		cancel()
		require.NoError(t, err, "seed element-ID relationship batch")
		require.Len(t, row, 1)
		batchCreated := rowAsInt64(t, row[0])
		require.Equal(t, int64(len(rows)), batchCreated, "every deterministic edge row must resolve both endpoints")
		created += batchCreated
		rows = rows[:0]
	}

	for edgeOrdinal := 0; edgeOrdinal < targetEdges; edgeOrdinal++ {
		sourceIndex, targetIndex := deterministicLargeRRFEndpoints(edgeOrdinal, len(entities))
		rows = append(rows, map[string]any{"sourceID": entities[sourceIndex], "targetID": entities[targetIndex]})
		if len(rows) >= batchSize {
			flush()
		}
	}
	flush()
	require.Equal(t, int64(targetEdges), created)
	verifyLargeRRFRelationships(t, writeSession, int64(len(entities)), int64(targetEdges))
	return created
}

func collectLargeRRFEntities(t *testing.T, session neo4j.SessionWithContext, nodeCount int64) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	clearResult, err := session.Run(ctx, "CALL db.clearQueryCaches()", nil)
	if err == nil {
		_, err = clearResult.Consume(ctx)
	}
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	result, err := session.Run(ctx, "MATCH (n) RETURN elementId(n) ORDER BY elementId(n)", nil)
	require.NoError(t, err)
	entities := make([]string, 0, nodeCount)
	seen := make(map[string]struct{}, nodeCount)
	for result.Next(ctx) {
		values := result.Record().Values
		require.Len(t, values, 1)
		id, ok := values[0].(string)
		require.True(t, ok && id != "", "every benchmark entity must have an element ID")
		_, duplicate := seen[id]
		require.False(t, duplicate, "benchmark entity element ID must be unique: %s", id)
		seen[id] = struct{}{}
		entities = append(entities, id)
	}
	require.NoError(t, result.Err())
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.Equal(t, nodeCount, int64(len(entities)), "all entities must participate in deterministic topology generation")
	require.Greater(t, len(entities), 1)
	return entities
}

func deterministicLargeRRFEndpoints(edgeOrdinal, entityCount int) (int, int) {
	source := edgeOrdinal % entityCount
	round := edgeOrdinal / entityCount
	offset := 1 + (round*7_919)%(entityCount-1)
	return source, (source + offset) % entityCount
}

func verifyLargeRRFRelationships(t *testing.T, session neo4j.SessionWithContext, entityCount, targetEdges int64) {
	t.Helper()
	expectedCoverage := targetEdges
	if expectedCoverage > entityCount {
		expectedCoverage = entityCount
	}
	checks := []struct {
		query string
		want  int64
	}{
		{"MATCH ()-[r:BENCH_ENTITY_LINK]->() RETURN count(r)", targetEdges},
		{"MATCH (source)-[:BENCH_ENTITY_LINK]->() RETURN count(DISTINCT source)", expectedCoverage},
		{"MATCH ()-[:BENCH_ENTITY_LINK]->(target) RETURN count(DISTINCT target)", expectedCoverage},
		{"MATCH (source)-[:BENCH_ENTITY_LINK]->(source) RETURN count(*)", 0},
	}
	for _, check := range checks {
		require.Equal(t, check.want, largeRRFCount(t, session, check.query), "topology verification failed for %s", check.query)
	}
	t.Logf("verified benchmark topology: edges=%d distinct_sources=%d distinct_targets=%d self_loops=0", targetEdges, expectedCoverage, expectedCoverage)
}

func largeRRFSeedQuery(t *testing.T, session neo4j.SessionWithContext) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := session.Run(ctx, "MATCH (source) RETURN properties(source) LIMIT 100", nil)
	require.NoError(t, err)
	var bootstrap string
	for result.Next(ctx) {
		properties, ok := result.Record().Values[0].(map[string]any)
		if !ok {
			continue
		}
		if candidate := largeRRFPropertySeed(properties); candidate != "" {
			bootstrap = candidate
			break
		}
	}
	require.NoError(t, result.Err())
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, bootstrap, "large fixture has no searchable string property")

	result, err = session.Run(ctx, `
CALL db.retrieve($request)
YIELD rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
RETURN vector_rank, bm25_rank, search_method, fallback_triggered
ORDER BY rrf_score DESC
	LIMIT 20`, map[string]any{"request": map[string]any{"query": bootstrap, "limit": int64(10)}})
	require.NoError(t, err)
	var vectorContributed, bm25Contributed bool
	for result.Next(ctx) {
		values := result.Record().Values
		require.Len(t, values, 4)
		vectorContributed = vectorContributed || rowAsInt64(t, values[0]) > 0
		bm25Contributed = bm25Contributed || rowAsInt64(t, values[1]) > 0
		require.Equal(t, "rrf_hybrid", values[2])
		require.Equal(t, false, values[3])
	}
	require.NoError(t, result.Err())
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	require.True(t, vectorContributed, "calibration query must produce vector candidates")
	require.True(t, bm25Contributed, "calibration query must produce BM25 candidates")
	return bootstrap
}

func largeRRFPropertySeed(properties map[string]any) string {
	var best []string
	for _, raw := range properties {
		value, ok := raw.(string)
		if !ok {
			continue
		}
		words := strings.FieldsFunc(value, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		})
		if len(words) > len(best) {
			best = words
		}
	}
	if len(best) < 8 {
		return ""
	}
	if len(best) > 24 {
		best = best[:24]
	}
	return strings.Join(best, " ")
}

func largeRRFUncachedQuery(seed, runFingerprint string, sequence int) string {
	hash := fnv.New64a()
	_, _ = fmt.Fprintf(hash, "%s:%d", runFingerprint, sequence)
	fingerprint := fmt.Sprintf("%064b", hash.Sum64())
	fingerprint = strings.NewReplacer("0", " ", "1", "\t").Replace(fingerprint)
	return seed + " " + fingerprint
}

func TestLargeRRFUncachedQueryIsUniqueAcrossRuns(t *testing.T) {
	seed := "production query"
	first := largeRRFUncachedQuery(seed, "run-a", 1)
	second := largeRRFUncachedQuery(seed, "run-b", 1)

	require.NotEqual(t, first, second)
	require.Equal(t, seed, strings.TrimSpace(first))
	require.Equal(t, seed, strings.TrimSpace(second))
}

type largeRRFSearchStatus struct {
	Name              string  `json:"name"`
	Status            string  `json:"status"`
	SearchReady       bool    `json:"searchReady"`
	SearchBuilding    bool    `json:"searchBuilding"`
	SearchInitialized bool    `json:"searchInitialized"`
	SearchStrategy    string  `json:"searchStrategy"`
	SearchPhase       string  `json:"searchPhase"`
	SearchProcessed   int64   `json:"searchProcessed"`
	SearchTotal       int64   `json:"searchTotal"`
	SearchRate        float64 `json:"searchRate"`
	SearchETASeconds  int64   `json:"searchEtaSeconds"`
}

func waitForLargeRRFSearchReady(t *testing.T, client *http.Client, httpAddr, dbName string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Minute)
	endpoint := "http://" + httpAddr + "/db/" + url.PathEscape(dbName)
	var last largeRRFSearchStatus
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err == nil {
			var resp *http.Response
			resp, err = client.Do(req)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					err = json.NewDecoder(resp.Body).Decode(&last)
				} else {
					err = fmt.Errorf("embed stats status %d", resp.StatusCode)
				}
				_ = resp.Body.Close()
			}
		}
		cancel()
		lastErr = err
		complete := last.SearchTotal > 0 && last.SearchProcessed >= last.SearchTotal
		if err == nil && last.Status == "online" && last.SearchInitialized && last.SearchReady && !last.SearchBuilding && last.SearchPhase == "ready" && complete {
			t.Logf("large RRF search ready: database=%s phase=%s strategy=%s processed=%d/%d", last.Name, last.SearchPhase, last.SearchStrategy, last.SearchProcessed, last.SearchTotal)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("large RRF search did not become ready: stats=%+v last_error=%v", last, lastErr)
}
