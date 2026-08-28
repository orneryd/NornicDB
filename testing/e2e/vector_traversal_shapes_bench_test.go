//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

type traversalBenchConfig struct {
	httpAddr  string
	boltAddr  string
	httpUser  string
	httpPass  string
	boltUser  string
	boltPass  string
	indexName string
}

type traversalTableRow struct {
	shape     string
	fanout    string
	depth     int
	protocol  string
	samples   int
	opsPerSec float64
	minMS     float64
	meanMS    float64
	p50MS     float64
	p95MS     float64
	p99MS     float64
	maxMS     float64
}

func TestVectorTraversalShapeMatrix_BoltVsHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping traversal matrix e2e benchmark in -short")
	}
	reportf := func(format string, args ...any) {
		t.Helper()
		msg := fmt.Sprintf(format, args...)
		fmt.Fprintln(os.Stdout, msg)
	}

	benchCfg := traversalBenchConfigFromEnv()
	var stopServer func()
	var localServer *serverProc
	if benchCfg.httpAddr == "" {
		repoRoot := mustRepoRoot(t)
		dataDir := t.TempDir()
		binPath := buildNornicBinary(t, repoRoot)
		httpPort := pickPort(t)
		boltPort := pickPort(t)
		grpcPort := pickPort(t)

		ctx, cancel := context.WithCancel(context.Background())
		proc := startNornicDB(t, ctx, binPath, dataDir, httpPort, boltPort, grpcPort)
		localServer = proc
		stopServer = func() {
			cancel()
			proc.stop(t)
		}
		benchCfg.httpAddr = fmt.Sprintf("127.0.0.1:%d", httpPort)
		benchCfg.boltAddr = fmt.Sprintf("127.0.0.1:%d", boltPort)
	}
	if stopServer != nil {
		defer stopServer()
	}

	waitTraversalTCP(t, benchCfg.httpAddr, 30*time.Second, localServer)
	waitTCP(t, benchCfg.boltAddr, 30*time.Second)

	httpClient := newTraversalHTTPClient(benchCfg.httpUser, benchCfg.httpPass)
	dbName := discoverDefaultDatabaseWithAuth(t, httpClient, benchCfg.httpAddr, benchCfg.httpUser, benchCfg.httpPass)
	driver := newBoltDriverWithAuth(t, benchCfg.boltAddr, benchCfg.boltUser, benchCfg.boltPass)
	defer func() { _ = driver.Close(context.Background()) }()

	seedVectorTraversalFixtureE2E(t, driver, benchCfg.indexName, httpClient, benchCfg.httpAddr)
	waitForEmbeddingQueueDrainE2E(t, httpClient, benchCfg.httpAddr, "before benchmark")
	rootIDs := fetchTraversalRootIDsE2E(t, driver)
	boltSession := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: dbName,
	})
	defer func() { _ = boltSession.Close(context.Background()) }()

	fanouts := parseFanoutsEnv("NORNICDB_TRAVERSAL_FANOUTS", []int{1, 2, 3})
	requestedIterations := envInt("NORNICDB_TRAVERSAL_MATRIX_ITERS", 0)
	minMeasuredIterations := envInt("NORNICDB_TRAVERSAL_MATRIX_MIN_SAMPLES", 101)
	if minMeasuredIterations < 3 {
		minMeasuredIterations = 3
	}
	warmupIterations := envInt("NORNICDB_TRAVERSAL_MATRIX_WARMUP", 2)
	if warmupIterations < 0 {
		warmupIterations = 0
	}
	iterations := requestedIterations
	if iterations <= 0 {
		iterations = minMeasuredIterations
	}
	if iterations < minMeasuredIterations {
		reportf("benchmark config: requested measured_iters=%d is too low for stable percentiles; using measured_iters=%d instead", requestedIterations, minMeasuredIterations)
		iterations = minMeasuredIterations
	}
	reportf("benchmark config: warmup_iters=%d measured_iters=%d", warmupIterations, iterations)
	pathCap := envInt("NORNICDB_TRAVERSAL_PATH_CAP", 5)

	rows := make([]traversalTableRow, 0, 128)
	rrfRequestSequence := 0
	nextRRFParams := func() map[string]any {
		rrfRequestSequence++
		return map[string]any{"request": map[string]any{
			"query":     fmt.Sprintf("chain baseline benchmark%d", rrfRequestSequence),
			"embedding": []any{0.95, 0.05, 0.0},
			"limit":     int64(5),
			"types":     []any{"OriginalText"},
		}}
	}
	rrfSpecs := []struct {
		name      string
		query     string
		assertRow func(t *testing.T, row []any)
	}{
		{
			name: "rrf_retrieval",
			query: `
CALL db.retrieve($request)
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
RETURN elementId(node) AS nodeID, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
ORDER BY rrf_score DESC
LIMIT 1
`,
			assertRow: func(t *testing.T, row []any) {
				require.Len(t, row, 7)
				require.Equal(t, normalizeElementIDE2E(rootIDs["chain-root"]), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Positive(t, rowAsFloat64E2E(t, row[2]), "RRF score must be populated")
				require.Positive(t, rowAsInt64(t, row[3]), "vector branch must contribute")
				require.Positive(t, rowAsInt64(t, row[4]), "BM25 branch must contribute")
				require.Equal(t, "rrf_hybrid", row[5])
				require.Equal(t, false, row[6])
			},
		},
		{
			name: "rrf_retrieval_1hop",
			query: `
CALL db.retrieve($request)
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
MATCH (node)-[:BENCH_HOP]->(neighbor:BenchmarkHop)
RETURN elementId(node) AS nodeID, elementId(neighbor) AS neighborID, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
ORDER BY rrf_score DESC
LIMIT 1
`,
			assertRow: func(t *testing.T, row []any) {
				require.Len(t, row, 8)
				require.Equal(t, normalizeElementIDE2E(rootIDs["chain-root"]), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.NotEmpty(t, normalizeElementIDE2E(fmt.Sprintf("%v", row[1])))
				require.Positive(t, rowAsFloat64E2E(t, row[3]), "RRF score must be populated")
				require.Positive(t, rowAsInt64(t, row[4]), "vector branch must contribute")
				require.Positive(t, rowAsInt64(t, row[5]), "BM25 branch must contribute")
				require.Equal(t, "rrf_hybrid", row[6])
				require.Equal(t, false, row[7])
			},
		},
	}
	for _, spec := range rrfSpecs {
		boltSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
			row, err := runBoltSingleRow(ctx, boltSession, spec.query, nextRRFParams())
			if err != nil {
				return err
			}
			spec.assertRow(t, row)
			return nil
		})
		require.NoError(t, err, "shape=%s protocol=bolt", spec.name)
		rows = append(rows, summarizeTableRow(spec.name, "-", 0, "bolt", boltSummary))

		httpSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
			row, err := neo4jHTTPCommitSingleRow(ctx, httpClient, benchCfg.httpAddr, dbName, spec.query, nextRRFParams())
			if err != nil {
				return err
			}
			spec.assertRow(t, row)
			return nil
		})
		require.NoError(t, err, "shape=%s protocol=http", spec.name)
		rows = append(rows, summarizeTableRow(spec.name, "-", 0, "http", httpSummary))
	}
	if os.Getenv("NORNICDB_TRAVERSAL_RRF_ONLY") == "1" {
		reportTraversalRows(reportf, rows)
		return
	}

	shapeSpecs := []struct {
		name      string
		fanoutSet []int
		queryFor  func(depth, fanout, pathCap int) (string, map[string]any, string)
		assertRow func(t *testing.T, row []any, depth, fanout, pathCap int, expectedNodeID string)
	}{
		{
			name:      "chain",
			fanoutSet: []int{1},
			queryFor: func(depth, _fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('%s', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP*1..%d]->(:BenchmarkHop)
WITH node, score, max(length(p)) AS maxDepth
RETURN elementId(node) AS nodeID, score, maxDepth
LIMIT $topK
`, benchCfg.indexName, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "query": []any{0.95, 0.05, 0.0}}, rootIDs["chain-root"]
			},
			assertRow: func(t *testing.T, row []any, depth, _fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(depth), rowAsInt64(t, row[2]))
			},
		},
		{
			name:      "branching",
			fanoutSet: fanouts,
			queryFor: func(depth, fanout, pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('%s', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:BENCH_HOP|REL_A|REL_B*1..%d]->(x)
WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0)
WITH node, score, p, length(p) AS d
ORDER BY d ASC
WITH node, score, collect(p)[0..$pathCap] AS paths
RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount
LIMIT $topK
`, benchCfg.indexName, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "pathCap": int64(pathCap), "query": branchingVectorForFanoutE2E(fanout)}, rootIDs[fmt.Sprintf("branch-f%d", fanout)]
			},
			assertRow: func(t *testing.T, row []any, depth, fanout, pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(minIntE2E(pathCap, geometricPathCountE2E(fanout, depth))), rowAsInt64(t, row[2]))
			},
		},
		{
			name:      "frontier",
			fanoutSet: fanouts,
			queryFor: func(depth, fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('%s', $vectorTopK, $query)
YIELD node, score
MATCH (node)-[:REL*1..%d]->(x)
WITH node, score, length(shortestPath((node)-[:REL*1..%d]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeID, score, nearest, reachable
LIMIT $topK
`, benchCfg.indexName, depth, depth), map[string]any{"vectorTopK": int64(1), "topK": int64(1), "query": frontierVectorForFanoutE2E(fanout)}, rootIDs[fmt.Sprintf("frontier-f%d", fanout)]
			},
			assertRow: func(t *testing.T, row []any, depth, fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 4)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(1), rowAsInt64(t, row[2]))
				require.Equal(t, int64(geometricPathCountE2E(fanout, depth)), rowAsInt64(t, row[3]))
			},
		},
		{
			name:      "constrained",
			fanoutSet: []int{2},
			queryFor: func(depth, _fanout, _pathCap int) (string, map[string]any, string) {
				return fmt.Sprintf(`
CALL db.index.vector.queryNodes('%s', $vectorTopK, $query)
YIELD node, score
MATCH p = (node)-[:REL*1..%d]->(x)
WHERE any(r IN relationships(p) WHERE r.weight >= $minWeight)
  AND any(n IN nodes(p) WHERE n.category IN $cats)
RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth
LIMIT $topK
`, benchCfg.indexName, depth), map[string]any{"vectorTopK": int64(2), "topK": int64(2), "query": []any{0.12, 0.84, 0.04}, "minWeight": 2.5, "cats": []string{"allowed"}}, rootIDs["constrained-strong"]
			},
			assertRow: func(t *testing.T, row []any, depth, _fanout, _pathCap int, expectedNodeID string) {
				require.Len(t, row, 3)
				require.Equal(t, normalizeElementIDE2E(expectedNodeID), normalizeElementIDE2E(fmt.Sprintf("%v", row[0])))
				require.Equal(t, int64(depth), rowAsInt64(t, row[2]))
			},
		},
	}

	for _, spec := range shapeSpecs {
		for _, fanout := range spec.fanoutSet {
			for depth := 1; depth <= 6; depth++ {
				query, params, expectedNodeID := spec.queryFor(depth, fanout, pathCap)
				boltSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
					row, err := runBoltSingleRow(ctx, boltSession, query, params)
					if err != nil {
						return err
					}
					spec.assertRow(t, row, depth, fanout, pathCap, expectedNodeID)
					return nil
				})
				require.NoError(t, err, "shape=%s fanout=%d depth=%d protocol=bolt", spec.name, fanout, depth)
				rows = append(rows, summarizeTableRow(spec.name, fanoutLabel(spec.name, fanout), depth, "bolt", boltSummary))

				httpSummary, err := runSerialBench(warmupIterations, iterations, func(ctx context.Context) error {
					row, err := neo4jHTTPCommitSingleRow(ctx, httpClient, benchCfg.httpAddr, dbName, query, params)
					if err != nil {
						return err
					}
					spec.assertRow(t, row, depth, fanout, pathCap, expectedNodeID)
					return nil
				})
				require.NoError(t, err, "shape=%s fanout=%d depth=%d protocol=http", spec.name, fanout, depth)
				rows = append(rows, summarizeTableRow(spec.name, fanoutLabel(spec.name, fanout), depth, "http", httpSummary))
			}
		}
	}

	reportTraversalRows(reportf, rows)
}

func waitTraversalTCP(t *testing.T, addr string, timeout time.Duration, server *serverProc) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tcpReachable(addr, 250*time.Millisecond) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if server != nil && server.logf != nil {
		_ = server.logf.Sync()
		if logBytes, err := os.ReadFile(server.logf.Name()); err == nil {
			t.Logf("local E2E server log:\n%s", logBytes)
		}
	}
	t.Fatalf("timeout waiting for %s", addr)
}

func reportTraversalRows(reportf func(string, ...any), rows []traversalTableRow) {
	reportf("| shape | fanout | depth | protocol | samples | throughput_ops_s | min_ms | mean_ms | p50_ms | p95_ms | p99_ms | max_ms |")
	reportf("| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for _, row := range rows {
		reportf("| %s | %s | %d | %s | %d | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f |", row.shape, row.fanout, row.depth, row.protocol, row.samples, row.opsPerSec, row.minMS, row.meanMS, row.p50MS, row.p95MS, row.p99MS, row.maxMS)
	}

	reportf("")
	reportf("| Transport | Throughput | Mean | P50 | P95 | P99 | Max |")
	reportf("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for _, protocol := range []string{"http", "bolt"} {
		agg := summarizeTransport(rows, protocol)
		reportf("| %s | %s ops/s | %s ms | %s ms | %s ms | %s ms | %s ms |", strings.ToUpper(protocol), formatFloatRange(agg.opsPerSec), formatFloatRange(agg.meanMS), formatFloatRange(agg.p50MS), formatFloatRange(agg.p95MS), formatFloatRange(agg.p99MS), formatFloatRange(agg.maxMS))
	}
}

func seedVectorTraversalFixtureE2E(t *testing.T, driver neo4j.DriverWithContext, indexName string, httpClient *http.Client, httpAddr string) {
	t.Helper()
	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{})
	defer func() { _ = sess.Close(context.Background()) }()
	waitForEmbeddingQueueDrainE2E(t, httpClient, httpAddr, "before fixture cleanup")

	runWrite := func(query string, params map[string]any) {
		t.Helper()
		maxAttempts := envInt("NORNICDB_TRAVERSAL_WRITE_RETRY_ATTEMPTS", 8)
		if maxAttempts < 1 {
			maxAttempts = 1
		}
		backoffMillis := envInt("NORNICDB_TRAVERSAL_WRITE_RETRY_BACKOFF_MS", 250)
		if backoffMillis < 0 {
			backoffMillis = 0
		}
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := sess.ExecuteWrite(writeCtx, func(tx neo4j.ManagedTransaction) (any, error) {
				_, err := tx.Run(writeCtx, query, params)
				return nil, err
			})
			cancel()
			if err == nil {
				return
			}
			lastErr = err
			if !isTraversalWriteConflictE2E(err) || attempt == maxAttempts {
				break
			}
			t.Logf("retrying traversal fixture write after conflict: attempt=%d/%d err=%v", attempt, maxAttempts, err)
			waitForEmbeddingQueueDrainE2E(t, httpClient, httpAddr, fmt.Sprintf("after write conflict retry %d", attempt))
			if backoffMillis > 0 {
				time.Sleep(time.Duration(backoffMillis) * time.Millisecond)
			}
		}
		require.NoError(t, lastErr)
	}

	runWrite(`
MATCH (n)
WHERE (n:OriginalText AND n.textKey IN $textKeys)
   OR ((n:BenchmarkHop OR n:BranchHop OR n:FrontierHop OR n:ConstrainedHop) AND n.rootKey IN $rootKeys)
DETACH DELETE n
`, map[string]any{"textKeys": traversalRootTextKeysE2E(), "rootKeys": traversalRootTextKeysE2E()})
	runWrite(fmt.Sprintf("CALL db.index.vector.createNodeIndex('%s', 'OriginalText', 'embedding', 3, 'cosine')", indexName), nil)
	runWrite(`
UNWIND $rows AS row
CREATE (o:OriginalText {textKey: row.textKey, originalText: row.originalText, embedding: row.embedding, nodeKey: row.textKey})
`, map[string]any{"rows": traversalRootRowsE2E()})
	embedStartupDelayMillis := envInt("NORNICDB_TRAVERSAL_EMBED_STARTUP_DELAY_MS", 2000)
	if embedStartupDelayMillis > 0 {
		time.Sleep(time.Duration(embedStartupDelayMillis) * time.Millisecond)
	}
	waitForEmbeddingQueueDrainE2E(t, httpClient, httpAddr, "after original text creation")
	runWrite(`
UNWIND $rows AS row
CREATE (h:BenchmarkHop {nodeKey: row.nodeKey, hopDepth: row.hopDepth, rootKey: row.rootKey})
`, map[string]any{"rows": chainNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:BranchHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": branchingNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:FrontierHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": frontierNodeRowsE2E()})
	runWrite(`
UNWIND $rows AS row
CREATE (h:ConstrainedHop {nodeKey: row.nodeKey, category: row.category, hopDepth: row.hopDepth, rootKey: row.rootKey, branchSlot: row.branchSlot})
`, map[string]any{"rows": constrainedNodeRowsE2E()})
	for edgeType, rows := range traversalEdgeRowsE2E() {
		query := fmt.Sprintf(`
UNWIND $rows AS row
MATCH (a {nodeKey: row.from}), (b {nodeKey: row.to})
CREATE (a)-[:%s {weight: row.weight, depth: row.depth}]->(b)
`, edgeType)
		runWrite(query, map[string]any{"rows": rows})
	}
}

func isTraversalWriteConflictE2E(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "changed after transaction start") ||
		strings.Contains(message, "commit failed: conflict:")
}

func runBoltSingleRow(ctx context.Context, session neo4j.SessionWithContext, query string, params map[string]any) ([]any, error) {
	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	if !result.Next(ctx) {
		return nil, result.Err()
	}
	row := append([]any{}, result.Record().Values...)
	if _, err := result.Consume(ctx); err != nil {
		return nil, err
	}
	return row, nil
}

func neo4jHTTPCommitSingleRow(ctx context.Context, c *http.Client, httpAddr, db, statement string, params map[string]any) ([]any, error) {
	reqBody := map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}
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
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("neo4j http status=%d body=%s", resp.StatusCode, string(body))
	}
	var parsed struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Errors) > 0 || len(parsed.Results) == 0 || len(parsed.Results[0].Data) == 0 {
		return nil, fmt.Errorf("unexpected neo4j http response: %s", string(body))
	}
	return parsed.Results[0].Data[0].Row, nil
}

func traversalBenchConfigFromEnv() traversalBenchConfig {
	httpAddr := strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_HTTP_ADDR"))
	boltAddr := strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_BOLT_ADDR"))
	if httpAddr != "" && boltAddr == "" {
		boltAddr = deriveBoltAddr(httpAddr)
	}
	user := strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_USER"))
	pass := os.Getenv("NORNICDB_TRAVERSAL_PASS")
	indexName := strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_INDEX_NAME"))
	if indexName == "" {
		indexName = fmt.Sprintf("idx_original_text_%d", time.Now().UnixNano())
	}
	return traversalBenchConfig{
		httpAddr:  httpAddr,
		boltAddr:  boltAddr,
		httpUser:  firstNonEmpty(strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_HTTP_USER")), user),
		httpPass:  firstNonEmpty(os.Getenv("NORNICDB_TRAVERSAL_HTTP_PASS"), pass),
		boltUser:  firstNonEmpty(strings.TrimSpace(os.Getenv("NORNICDB_TRAVERSAL_BOLT_USER")), user),
		boltPass:  firstNonEmpty(os.Getenv("NORNICDB_TRAVERSAL_BOLT_PASS"), pass),
		indexName: indexName,
	}
}

func newTraversalHTTPClient(user, pass string) *http.Client {
	return &http.Client{
		Timeout:   15 * time.Second,
		Transport: &traversalAuthTransport{base: http.DefaultTransport, user: user, pass: pass},
	}
}

type traversalAuthTransport struct {
	base http.RoundTripper
	user string
	pass string
}

func (t *traversalAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if t.user != "" {
		clone.SetBasicAuth(t.user, t.pass)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

func deriveBoltAddr(httpAddr string) string {
	host, port, ok := strings.Cut(httpAddr, ":")
	if !ok {
		return httpAddr
	}
	if port == "7474" {
		return host + ":7687"
	}
	return httpAddr
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func traversalRootTextKeysE2E() []string {
	return []string{"chain-root", "branch-f1", "branch-f2", "branch-f3", "frontier-f1", "frontier-f2", "frontier-f3", "constrained-strong", "constrained-weak"}
}

type embedStatsResponseE2E struct {
	Enabled      bool                 `json:"enabled"`
	PendingNodes int                  `json:"pending_nodes"`
	Stats        *embedWorkerStatsE2E `json:"stats"`
}

type embedWorkerStatsE2E struct {
	Running bool `json:"running"`
}

func (e embedStatsResponseE2E) workerRunning() bool {
	return e.Stats != nil && e.Stats.Running
}

func waitForEmbeddingQueueDrainE2E(t *testing.T, httpClient *http.Client, httpAddr, phase string) {
	t.Helper()
	if httpClient == nil || strings.TrimSpace(httpAddr) == "" {
		return
	}
	pollMillis := envInt("NORNICDB_TRAVERSAL_EMBED_WAIT_POLL_MS", 500)
	if pollMillis < 50 {
		pollMillis = 50
	}
	idlePollsBeforeRetrigger := envInt("NORNICDB_TRAVERSAL_EMBED_IDLE_POLLS_BEFORE_RETRIGGER", 2)
	if idlePollsBeforeRetrigger < 1 {
		idlePollsBeforeRetrigger = 1
	}
	progressSeconds := envInt("NORNICDB_TRAVERSAL_EMBED_PROGRESS_SECONDS", 5)
	if progressSeconds < 1 {
		progressSeconds = 1
	}
	maxFetchFailures := envInt("NORNICDB_TRAVERSAL_EMBED_MAX_FETCH_FAILURES", 10)
	if maxFetchFailures < 1 {
		maxFetchFailures = 1
	}
	pollInterval := time.Duration(pollMillis) * time.Millisecond
	progressInterval := time.Duration(progressSeconds) * time.Second

	var lastErr error
	announcedWait := false
	lastProgressLog := time.Time{}
	triggerAttempts := 0
	lastPending := -1
	idlePolls := 0
	fetchFailures := 0

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stats, err := fetchEmbedStatsE2E(ctx, httpClient, httpAddr)
		cancel()
		if err == nil {
			lastErr = nil
			fetchFailures = 0
			if !stats.Enabled {
				return
			}
			if stats.PendingNodes == 0 && !stats.workerRunning() {
				if announcedWait {
					t.Logf("embedding queue drained %s: pending=%d running=%t", phase, stats.PendingNodes, stats.workerRunning())
				}
				return
			}
			if stats.PendingNodes < 0 && !stats.workerRunning() {
				if announcedWait {
					t.Logf("embedding worker idle %s with unavailable pending count", phase)
				}
				return
			}
			if !announcedWait && (stats.PendingNodes > 0 || stats.workerRunning()) {
				t.Logf("waiting for embedding queue %s: pending=%d running=%t", phase, stats.PendingNodes, stats.workerRunning())
				announcedWait = true
			}
			if lastPending >= 0 {
				switch {
				case stats.PendingNodes < lastPending:
					idlePolls = 0
				case stats.PendingNodes > lastPending:
					idlePolls = 0
				default:
					idlePolls++
				}
			}
			lastPending = stats.PendingNodes
			if announcedWait && (lastProgressLog.IsZero() || time.Since(lastProgressLog) >= progressInterval) {
				t.Logf("embedding queue progress %s: pending=%d running=%t idle_polls=%d trigger_attempts=%d", phase, stats.PendingNodes, stats.workerRunning(), idlePolls, triggerAttempts)
				lastProgressLog = time.Now()
			}
			if stats.PendingNodes > 0 && !stats.workerRunning() && idlePolls >= idlePollsBeforeRetrigger {
				triggerAttempts++
				triggerCtx, triggerCancel := context.WithTimeout(context.Background(), 5*time.Second)
				triggerErr := triggerEmbeddingWorkerE2E(triggerCtx, httpClient, httpAddr)
				triggerCancel()
				if triggerErr != nil {
					t.Logf("embedding queue retrigger failed %s: pending=%d err=%v", phase, stats.PendingNodes, triggerErr)
					lastErr = triggerErr
				} else {
					t.Logf("embedding queue retriggered %s: pending=%d attempt=%d", phase, stats.PendingNodes, triggerAttempts)
					lastErr = nil
				}
				idlePolls = 0
			}
		} else {
			fetchFailures++
			lastErr = err
			if fetchFailures >= maxFetchFailures {
				t.Fatalf("failed waiting for embedding queue %s after %d fetch errors: last_error=%v", phase, fetchFailures, lastErr)
			}
		}
		time.Sleep(pollInterval)
	}
}

func triggerEmbeddingWorkerE2E(ctx context.Context, httpClient *http.Client, httpAddr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+httpAddr+"/nornicdb/embed/trigger", nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioReadAll(resp.Body, 1<<20)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("embed trigger status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func fetchEmbedStatsE2E(ctx context.Context, httpClient *http.Client, httpAddr string) (embedStatsResponseE2E, error) {
	var out embedStatsResponseE2E
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+httpAddr+"/nornicdb/embed/stats", nil)
	if err != nil {
		return out, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	body, err := ioReadAll(resp.Body, 1<<20)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("embed stats status=%d body=%s", resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func newBoltDriverWithAuth(t *testing.T, addr, user, pass string) neo4j.DriverWithContext {
	t.Helper()
	uri := "bolt://" + addr
	auth := neo4j.NoAuth()
	if user != "" {
		auth = neo4j.BasicAuth(user, pass, "")
	}
	driver, err := neo4j.NewDriverWithContext(uri, auth)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.VerifyConnectivity(ctx))
	return driver
}

func discoverDefaultDatabaseWithAuth(t *testing.T, c *http.Client, httpAddr, _user, _pass string) string {
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

func runSerialBench(warmupIterations, iterations int, fn func(context.Context) error) (benchSummary, error) {
	if iterations <= 0 {
		iterations = 1
	}
	if warmupIterations < 0 {
		warmupIterations = 0
	}
	runPhase := func(total int, record bool) ([]time.Duration, error) {
		lat := make([]time.Duration, 0, total)
		for i := 0; i < total; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			begin := time.Now()
			err := fn(ctx)
			elapsed := time.Since(begin)
			cancel()
			if err != nil {
				return lat, fmt.Errorf("iteration %d/%d failed: %w", i+1, total, err)
			}
			if record {
				lat = append(lat, elapsed)
			}
		}
		return lat, nil
	}
	if _, err := runPhase(warmupIterations, false); err != nil {
		return benchSummary{}, err
	}
	start := time.Now()
	lat, err := runPhase(iterations, true)
	if err != nil {
		return benchSummary{ops: len(lat), dur: time.Since(start), lat: lat}, err
	}
	sortDurations(lat)
	return benchSummary{ops: len(lat), dur: time.Since(start), lat: lat}, nil
}

func meanDuration(lat []time.Duration) time.Duration {
	if len(lat) == 0 {
		return 0
	}
	var total time.Duration
	for _, item := range lat {
		total += item
	}
	return time.Duration(int64(total) / int64(len(lat)))
}

func durationToMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func summarizeTableRow(shape, fanout string, depth int, protocol string, summary benchSummary) traversalTableRow {
	row := traversalTableRow{
		shape:     shape,
		fanout:    fanout,
		depth:     depth,
		protocol:  protocol,
		samples:   len(summary.lat),
		opsPerSec: benchOpsPerSecond(summary),
		meanMS:    durationToMS(meanDuration(summary.lat)),
		p50MS:     durationToMS(percentile(summary.lat, 0.50)),
		p95MS:     durationToMS(percentile(summary.lat, 0.95)),
		p99MS:     durationToMS(percentile(summary.lat, 0.99)),
	}
	if len(summary.lat) > 0 {
		row.minMS = durationToMS(summary.lat[0])
		row.maxMS = durationToMS(summary.lat[len(summary.lat)-1])
	}
	return row
}

type transportSummary struct {
	opsPerSec []float64
	meanMS    []float64
	p50MS     []float64
	p95MS     []float64
	p99MS     []float64
	maxMS     []float64
}

func summarizeTransport(rows []traversalTableRow, protocol string) transportSummary {
	out := transportSummary{}
	for _, row := range rows {
		if row.protocol != protocol {
			continue
		}
		out.opsPerSec = append(out.opsPerSec, row.opsPerSec)
		out.meanMS = append(out.meanMS, row.meanMS)
		out.p50MS = append(out.p50MS, row.p50MS)
		out.p95MS = append(out.p95MS, row.p95MS)
		out.p99MS = append(out.p99MS, row.p99MS)
		out.maxMS = append(out.maxMS, row.maxMS)
	}
	return out
}

func benchOpsPerSecond(summary benchSummary) float64 {
	if summary.dur <= 0 {
		return 0
	}
	return float64(summary.ops) / summary.dur.Seconds()
}

func formatFloatRange(values []float64) string {
	if len(values) == 0 {
		return "N/A"
	}
	minVal, maxVal := values[0], values[0]
	for _, value := range values[1:] {
		if value < minVal {
			minVal = value
		}
		if value > maxVal {
			maxVal = value
		}
	}
	if math.Abs(maxVal-minVal) < 0.0005 {
		return fmt.Sprintf("%.3f", minVal)
	}
	return fmt.Sprintf("%.3f-%.3f", minVal, maxVal)
}

func rowAsInt64(t *testing.T, value any) int64 {
	t.Helper()
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		v, err := typed.Int64()
		require.NoError(t, err)
		return v
	default:
		t.Fatalf("unexpected numeric type %T (%v)", value, value)
		return 0
	}
}

func rowAsFloat64E2E(t *testing.T, value any) float64 {
	t.Helper()
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int64:
		return float64(typed)
	case int:
		return float64(typed)
	case json.Number:
		v, err := typed.Float64()
		require.NoError(t, err)
		return v
	default:
		t.Fatalf("unexpected numeric type %T (%v)", value, value)
		return 0
	}
}

func fanoutLabel(shape string, fanout int) string {
	if shape == "chain" {
		return "-"
	}
	return strconv.Itoa(fanout)
}

func normalizeElementIDE2E(value string) string {
	parts := strings.Split(value, ":")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return value
}

func parseFanoutsEnv(key string, fallback []int) []int {
	raw := os.Getenv(key)
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func traversalRootRowsE2E() []map[string]any {
	return []map[string]any{
		{"textKey": "chain-root", "originalText": "chain baseline", "embedding": []any{0.95, 0.05, 0.0}},
		{"textKey": "branch-f1", "originalText": "branch fanout one", "embedding": branchingVectorForFanoutE2E(1)},
		{"textKey": "branch-f2", "originalText": "branch fanout two", "embedding": branchingVectorForFanoutE2E(2)},
		{"textKey": "branch-f3", "originalText": "branch fanout three", "embedding": branchingVectorForFanoutE2E(3)},
		{"textKey": "frontier-f1", "originalText": "frontier fanout one", "embedding": frontierVectorForFanoutE2E(1)},
		{"textKey": "frontier-f2", "originalText": "frontier fanout two", "embedding": frontierVectorForFanoutE2E(2)},
		{"textKey": "frontier-f3", "originalText": "frontier fanout three", "embedding": frontierVectorForFanoutE2E(3)},
		{"textKey": "constrained-strong", "originalText": "weighted allowed root", "embedding": []any{0.12, 0.84, 0.04}},
		{"textKey": "constrained-weak", "originalText": "filtered weak root", "embedding": []any{0.11, 0.79, 0.10}},
	}
}

func chainNodeRowsE2E() []map[string]any {
	rows := make([]map[string]any, 0, 6)
	for depth := 1; depth <= 6; depth++ {
		rows = append(rows, map[string]any{"nodeKey": fmt.Sprintf("chain-root:hop:%d", depth), "hopDepth": int64(depth), "rootKey": "chain-root"})
	}
	return rows
}

func branchingNodeRowsE2E() []map[string]any {
	rows := []map[string]any{}
	for _, fanout := range []int{1, 2, 3} {
		rows = append(rows, treeNodeRowsE2E(fmt.Sprintf("branch-f%d", fanout), fanout, 6, "allowed")...)
	}
	return rows
}

func frontierNodeRowsE2E() []map[string]any {
	rows := []map[string]any{}
	for _, fanout := range []int{1, 2, 3} {
		rows = append(rows, treeNodeRowsE2E(fmt.Sprintf("frontier-f%d", fanout), fanout, 6, "allowed")...)
	}
	return rows
}

func constrainedNodeRowsE2E() []map[string]any {
	rows := treeNodeRowsE2E("constrained-strong", 2, 6, "allowed")
	rows = append(rows, treeNodeRowsE2E("constrained-weak", 2, 6, "other")...)
	return rows
}

func treeNodeRowsE2E(rootKey string, fanout, maxDepth int, category string) []map[string]any {
	rows := []map[string]any{}
	nextOrdinal := 0
	levelCount := 1
	for depth := 1; depth <= maxDepth; depth++ {
		for parent := 0; parent < levelCount; parent++ {
			for childIdx := 0; childIdx < fanout; childIdx++ {
				nextOrdinal++
				rows = append(rows, map[string]any{
					"nodeKey":    fmt.Sprintf("%s:node:%02d:%04d", rootKey, depth, nextOrdinal),
					"category":   category,
					"hopDepth":   int64(depth),
					"rootKey":    rootKey,
					"branchSlot": int64(childIdx),
				})
			}
		}
		levelCount *= fanout
	}
	return rows
}

func traversalEdgeRowsE2E() map[string][]map[string]any {
	out := map[string][]map[string]any{
		"BENCH_HOP": {},
		"REL_A":     {},
		"REL_B":     {},
		"REL":       {},
	}
	for depth := 1; depth <= 6; depth++ {
		from := "chain-root"
		if depth > 1 {
			from = fmt.Sprintf("chain-root:hop:%d", depth-1)
		}
		to := fmt.Sprintf("chain-root:hop:%d", depth)
		out["BENCH_HOP"] = append(out["BENCH_HOP"], map[string]any{"from": from, "to": to, "weight": 1.0, "depth": int64(depth)})
	}
	for _, fanout := range []int{1, 2, 3} {
		appendTreeEdgesE2E(out, fmt.Sprintf("branch-f%d", fanout), fanout, 6, func(depth, childIdx int) string {
			if depth == 1 {
				return "BENCH_HOP"
			}
			if childIdx%2 == 0 {
				return "REL_A"
			}
			return "REL_B"
		}, 1.0)
		appendTreeEdgesE2E(out, fmt.Sprintf("frontier-f%d", fanout), fanout, 6, func(_depth, _childIdx int) string { return "REL" }, 1.0)
	}
	appendTreeEdgesE2E(out, "constrained-strong", 2, 6, func(_depth, _childIdx int) string { return "REL" }, 5.0)
	appendTreeEdgesE2E(out, "constrained-weak", 2, 6, func(_depth, _childIdx int) string { return "REL" }, 0.2)
	return out
}

func appendTreeEdgesE2E(dest map[string][]map[string]any, rootKey string, fanout, maxDepth int, edgeTypeFn func(depth, childIdx int) string, weight float64) {
	currentLevel := []string{rootKey}
	nextOrdinal := 0
	for depth := 1; depth <= maxDepth; depth++ {
		nextLevel := make([]string, 0, len(currentLevel)*fanout)
		for _, parentKey := range currentLevel {
			for childIdx := 0; childIdx < fanout; childIdx++ {
				nextOrdinal++
				childKey := fmt.Sprintf("%s:node:%02d:%04d", rootKey, depth, nextOrdinal)
				edgeType := edgeTypeFn(depth, childIdx)
				dest[edgeType] = append(dest[edgeType], map[string]any{"from": parentKey, "to": childKey, "weight": weight, "depth": int64(depth)})
				nextLevel = append(nextLevel, childKey)
			}
		}
		currentLevel = nextLevel
	}
}

func branchingVectorForFanoutE2E(fanout int) []any {
	switch fanout {
	case 1:
		return []any{1.0, 0.0, 0.0}
	case 2:
		return []any{0.0, 1.0, 0.0}
	default:
		return []any{0.0, 0.0, 1.0}
	}
}

func frontierVectorForFanoutE2E(fanout int) []any {
	switch fanout {
	case 1:
		return []any{0.75, 0.25, 0.0}
	case 2:
		return []any{0.25, 0.75, 0.0}
	default:
		return []any{0.25, 0.25, 0.5}
	}
}

func geometricPathCountE2E(fanout, depth int) int {
	if fanout == 1 {
		return depth
	}
	return int((math.Pow(float64(fanout), float64(depth+1)) - float64(fanout)) / float64(fanout-1))
}

func minIntE2E(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fetchTraversalRootIDsE2E(t *testing.T, driver neo4j.DriverWithContext) map[string]string {
	t.Helper()
	maxAttempts := envInt("NORNICDB_TRAVERSAL_ROOT_FETCH_ATTEMPTS", 6)
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	perAttemptTimeoutSeconds := envInt("NORNICDB_TRAVERSAL_ROOT_FETCH_TIMEOUT_SECONDS", 30)
	if perAttemptTimeoutSeconds < 1 {
		perAttemptTimeoutSeconds = 1
	}
	backoffMillis := envInt("NORNICDB_TRAVERSAL_ROOT_FETCH_BACKOFF_MS", 1000)
	if backoffMillis < 0 {
		backoffMillis = 0
	}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(perAttemptTimeoutSeconds)*time.Second)
		sess := driver.NewSession(ctx, neo4j.SessionConfig{})
		out, err := sess.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			res, err := tx.Run(ctx, "MATCH (o:OriginalText) RETURN o.textKey AS textKey, elementId(o) AS nodeID", nil)
			if err != nil {
				return nil, err
			}
			ids := map[string]string{}
			for res.Next(ctx) {
				textKey, _ := res.Record().Get("textKey")
				nodeID, _ := res.Record().Get("nodeID")
				ids[fmt.Sprintf("%v", textKey)] = fmt.Sprintf("%v", nodeID)
			}
			return ids, res.Err()
		})
		_ = sess.Close(ctx)
		cancel()
		if err == nil {
			ids, _ := out.(map[string]string)
			return ids
		}
		lastErr = err
		if attempt < maxAttempts {
			t.Logf("retrying traversal root id fetch: attempt=%d/%d err=%v", attempt, maxAttempts, err)
			if backoffMillis > 0 {
				time.Sleep(time.Duration(backoffMillis) * time.Millisecond)
			}
		}
	}
	require.NoError(t, lastErr)
	return nil
}
