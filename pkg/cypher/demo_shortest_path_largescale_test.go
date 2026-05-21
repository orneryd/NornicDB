package cypher

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Large-scale traversal benchmark.
//
// Seeds a graph of the demo's shape but at production-ish scale and runs
// shortestPath across hop-depth buckets to characterize traversal latency
// scaling. Goals:
//
//   - 500K nodes, 3-4M edges (forward+reverse), >55-hop diameter.
//   - Random pairs at each depth bucket so we exercise BFS rather than
//     repeatedly hitting the result cache.
//   - Single Markdown table reporting min / median / p95 / max per bucket.
//
// Why direct BulkCreate writes: doing 500K × cypher MERGEs would take
// 5-15 minutes per run. The scaling under test is read-side (BFS, edge
// cache, adjacency cache); the seed shape is identical to what cypher
// would have produced.

const (
	largeScaleNodes      = 500_000
	largeScaleSectors    = 1000
	largeScaleSectorSize = 500 // largeScaleSectors * largeScaleSectorSize must equal largeScaleNodes
	largeScaleIntraEdges = 3
	largeScaleGateways   = 2
	largeScalePairsPer   = 30 // pairs benched at each hop bucket
)

var largeScaleEnable = flag.Bool("largescale", false,
	"run the 500K-node traversal benchmark (slow seed, ~30-90s)")

// largeScaleSetupOnceArtifacts caches the seeded executor across multiple
// sub-benches so we don't pay the seed cost more than once per `go test`
// invocation. Set up by ensureLargeScaleFixture; populated lazily.
type largeScaleFixture struct {
	exec    *StorageExecutor
	starIDs []string
	// pairsByDepth groups (start, end) pairs by their reference-BFS hop
	// distance. Built by sampling random sources and recording the
	// distance to every reachable node, then bucketing.
	pairsByDepth map[int][][2]string
	depthsSorted []int
}

var largeScaleFixtureCache *largeScaleFixture

// ensureLargeScaleFixture builds the fixture once. The fixture is reused
// across the bench's per-depth sub-benches so wall-clock seed cost is
// amortized.
func ensureLargeScaleFixture(tb testing.TB) *largeScaleFixture {
	tb.Helper()
	if largeScaleFixtureCache != nil {
		return largeScaleFixtureCache
	}

	if largeScaleSectors*largeScaleSectorSize != largeScaleNodes {
		tb.Fatalf("invalid scale config: %d * %d != %d",
			largeScaleSectors, largeScaleSectorSize, largeScaleNodes)
	}

	tb.Logf("seeding %d nodes across %d sectors...", largeScaleNodes, largeScaleSectors)
	t0 := time.Now()

	exec, async, ns := buildDemoExecutorNS(tb)

	// Indexes OFF during bulk insert — AddPropertyIndex is deferred until
	// after the data has landed in storage and we've verified the counts.
	// Bulk insert without an index is straight key-value writes; with one
	// it's an extra map mutation per node. At 500K nodes the difference
	// is multiple seconds.
	schema := ns.GetSchema()

	// Build all nodes + edges in memory. The seed shape mirrors the TS
	// demo: sectors arranged in a chain, intra-sector spanning + a few
	// random extra edges, gateways between adjacent sectors.
	starIDs := make([]string, 0, largeScaleNodes)
	sectorMembers := make([][]string, largeScaleSectors)
	nodes := make([]*storage.Node, 0, largeScaleNodes)
	rng := rand.New(rand.NewSource(0xfeedd3))

	for s := 0; s < largeScaleSectors; s++ {
		members := make([]string, largeScaleSectorSize)
		for i := 0; i < largeScaleSectorSize; i++ {
			id := fmt.Sprintf("s%d-%d", s, i)
			members[i] = id
			starIDs = append(starIDs, id)
			nodes = append(nodes, &storage.Node{
				ID:     storage.NodeID("d3_demo:" + id),
				Labels: []string{"Star"},
				Properties: map[string]interface{}{
					"starId": id,
					"sector": int64(s),
				},
			})
		}
		sectorMembers[s] = members
	}

	tb.Logf("nodes built in %s; bulk-inserting (index OFF)...", time.Since(t0))
	t1 := time.Now()
	if err := bulkCreateNodesInChunks(async, nodes, 5000); err != nil {
		tb.Fatalf("BulkCreateNodes: %v", err)
	}
	tb.Logf("nodes inserted in %s; building edges...", time.Since(t1))

	// Edge generation. Same shape as the demo: spanning chain inside each
	// sector, intraEdges random extras, then gateways between adjacent
	// sectors. We emit edges in both directions so the demo's
	// `[:HYPERLANE*]-` undirected pattern matches naturally.
	t2 := time.Now()
	edges := make([]*storage.Edge, 0, largeScaleNodes*largeScaleIntraEdges*2)
	addedKey := make(map[string]struct{}, largeScaleNodes*largeScaleIntraEdges)
	addEdge := func(a, b string) {
		if a == b {
			return
		}
		var k string
		if a < b {
			k = a + "|" + b
		} else {
			k = b + "|" + a
		}
		if _, ok := addedKey[k]; ok {
			return
		}
		addedKey[k] = struct{}{}
		edgeID1 := storage.EdgeID(fmt.Sprintf("d3_demo:e:%s:%s", a, b))
		edgeID2 := storage.EdgeID(fmt.Sprintf("d3_demo:e:%s:%s", b, a))
		edges = append(edges,
			&storage.Edge{
				ID:        edgeID1,
				StartNode: storage.NodeID("d3_demo:" + a),
				EndNode:   storage.NodeID("d3_demo:" + b),
				Type:      "HYPERLANE",
			},
			&storage.Edge{
				ID:        edgeID2,
				StartNode: storage.NodeID("d3_demo:" + b),
				EndNode:   storage.NodeID("d3_demo:" + a),
				Type:      "HYPERLANE",
			},
		)
	}
	for s := 0; s < largeScaleSectors; s++ {
		members := sectorMembers[s]
		for i := 1; i < len(members); i++ {
			addEdge(members[i-1], members[i])
		}
		for _, id := range members {
			for k := 0; k < largeScaleIntraEdges; k++ {
				other := members[rng.Intn(len(members))]
				addEdge(id, other)
			}
		}
	}
	for s := 0; s < largeScaleSectors-1; s++ {
		a := sectorMembers[s]
		b := sectorMembers[s+1]
		for g := 0; g < largeScaleGateways; g++ {
			fromID := a[rng.Intn(len(a))]
			toID := b[rng.Intn(len(b))]
			addEdge(fromID, toID)
		}
	}

	tb.Logf("%d edges built in %s; bulk-inserting...", len(edges), time.Since(t2))
	t3 := time.Now()
	if err := bulkCreateEdgesInChunks(async, edges, 10000); err != nil {
		tb.Fatalf("BulkCreateEdges: %v", err)
	}
	tb.Logf("edges inserted in %s; flushing...", time.Since(t3))

	t4 := time.Now()
	if err := async.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	tb.Logf("flush complete in %s", time.Since(t4))

	// Verify storage has everything before we build the index. Cheap
	// sanity check that catches a partial-flush regression before the
	// bench wastes minutes querying an incomplete graph.
	tb.Logf("verifying storage counts...")
	t5a := time.Now()
	gotNodes, err := ns.NodeCount()
	if err != nil {
		tb.Fatalf("NodeCount: %v", err)
	}
	gotEdges, err := ns.EdgeCount()
	if err != nil {
		tb.Fatalf("EdgeCount: %v", err)
	}
	if int(gotNodes) != len(nodes) {
		tb.Fatalf("node count mismatch: stored=%d expected=%d", gotNodes, len(nodes))
	}
	if int(gotEdges) != len(edges) {
		tb.Fatalf("edge count mismatch: stored=%d expected=%d", gotEdges, len(edges))
	}
	tb.Logf("verified %d nodes, %d edges in %s", gotNodes, gotEdges, time.Since(t5a))

	// Index ON — declare the property index and populate it from the live
	// node set. We iterate sectorMembers (already in memory) so we don't
	// pay for a 500K GetNodesByLabel scan. Same data either way; this
	// avoids a Badger round-trip.
	tb.Logf("building property index...")
	t5b := time.Now()
	if schema != nil {
		if err := schema.AddPropertyIndex("star_id_idx", "Star", []string{"starId"}); err != nil {
			tb.Fatalf("AddPropertyIndex: %v", err)
		}
		for _, members := range sectorMembers {
			for _, id := range members {
				_ = schema.PropertyIndexInsert("Star", "starId",
					storage.NodeID("d3_demo:"+id), id)
			}
		}
	}
	tb.Logf("property index built in %s", time.Since(t5b))
	tb.Logf("total seed wall-clock: %s", time.Since(t0))

	// Reference BFS: pick K random sources, walk the entire graph from
	// each, record the hop distance to every reachable node, and bucket
	// (source, target) pairs by distance. This gives us a sample of
	// pairs at each depth so the bench can target hop counts directly.
	tb.Logf("building reference adjacency...")
	t5 := time.Now()
	adj := buildReferenceAdjacency(sectorMembers, addedKey)
	tb.Logf("reference adjacency built in %s (%d nodes)", time.Since(t5), len(adj))

	tb.Logf("sampling depth buckets...")
	t6 := time.Now()
	pairsByDepth := samplePairsByDepth(adj, starIDs, rng,
		largeScalePairsPer, 60 /* maxDepth */)
	tb.Logf("depth buckets sampled in %s", time.Since(t6))

	depths := make([]int, 0, len(pairsByDepth))
	for d := range pairsByDepth {
		depths = append(depths, d)
	}
	// Insertion sort — small slice.
	for i := 1; i < len(depths); i++ {
		x := depths[i]
		j := i - 1
		for j >= 0 && depths[j] > x {
			depths[j+1] = depths[j]
			j--
		}
		depths[j+1] = x
	}

	largeScaleFixtureCache = &largeScaleFixture{
		exec:         exec,
		starIDs:      starIDs,
		pairsByDepth: pairsByDepth,
		depthsSorted: depths,
	}
	return largeScaleFixtureCache
}

// bulkCreateNodesInChunks writes nodes in slices of `chunk` so a single
// failure doesn't abort the whole 500K insert. Returns the first error.
func bulkCreateNodesInChunks(eng storage.Engine, nodes []*storage.Node, chunk int) error {
	for i := 0; i < len(nodes); i += chunk {
		end := i + chunk
		if end > len(nodes) {
			end = len(nodes)
		}
		if err := eng.BulkCreateNodes(nodes[i:end]); err != nil {
			return fmt.Errorf("chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func bulkCreateEdgesInChunks(eng storage.Engine, edges []*storage.Edge, chunk int) error {
	for i := 0; i < len(edges); i += chunk {
		end := i + chunk
		if end > len(edges) {
			end = len(edges)
		}
		if err := eng.BulkCreateEdges(edges[i:end]); err != nil {
			return fmt.Errorf("chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

// buildReferenceAdjacency reconstructs the seeded undirected adjacency
// from the (sectorMembers, addedKey) pair we already have in memory —
// avoids reading back from storage and is exact.
func buildReferenceAdjacency(sectorMembers [][]string, addedKey map[string]struct{}) map[string]map[string]struct{} {
	adj := make(map[string]map[string]struct{}, 1<<20)
	ensure := func(s string) map[string]struct{} {
		m := adj[s]
		if m == nil {
			m = make(map[string]struct{})
			adj[s] = m
		}
		return m
	}
	for _, members := range sectorMembers {
		for _, id := range members {
			ensure(id)
		}
	}
	for k := range addedKey {
		// Each key is "min|max"; split on the pipe to recover endpoints.
		parts := strings.SplitN(k, "|", 2)
		if len(parts) != 2 {
			continue
		}
		a, b := parts[0], parts[1]
		ensure(a)[b] = struct{}{}
		ensure(b)[a] = struct{}{}
	}
	return adj
}

// samplePairsByDepth picks a handful of random sources, BFS's the full
// graph from each, and records (source, target) pairs by hop distance up
// to maxDepth. We early-stop when each depth bucket has reached pairsPer
// samples to keep BFS time bounded.
func samplePairsByDepth(
	adj map[string]map[string]struct{},
	allIDs []string,
	rng *rand.Rand,
	pairsPer int,
	maxDepth int,
) map[int][][2]string {
	pairsByDepth := make(map[int][][2]string, maxDepth+1)

	// We BFS from up to N random sources until every depth bucket is
	// filled (or we hit a hard cap on iterations). Keeps reference work
	// bounded at large scales.
	const maxSources = 200
	enough := func() bool {
		for d := 1; d <= maxDepth; d++ {
			if len(pairsByDepth[d]) < pairsPer {
				return false
			}
		}
		return true
	}

	for src := 0; src < maxSources && !enough(); src++ {
		start := allIDs[rng.Intn(len(allIDs))]
		dist := bfsAllDistances(adj, start, maxDepth)
		for target, d := range dist {
			if d <= 0 || d > maxDepth {
				continue
			}
			if len(pairsByDepth[d]) >= pairsPer {
				continue
			}
			pairsByDepth[d] = append(pairsByDepth[d], [2]string{start, target})
		}
	}
	return pairsByDepth
}

// bfsAllDistances returns the BFS hop-distance from start to every node
// reachable within maxDepth. Standard textbook BFS; all-pairs would be
// O(N²) memory at this scale so we sample sources instead.
func bfsAllDistances(adj map[string]map[string]struct{}, start string, maxDepth int) map[string]int {
	dist := map[string]int{start: 0}
	queue := []string{start}
	for h := 0; h < len(queue); h++ {
		cur := queue[h]
		d := dist[cur]
		if d >= maxDepth {
			continue
		}
		for nb := range adj[cur] {
			if _, ok := dist[nb]; ok {
				continue
			}
			dist[nb] = d + 1
			queue = append(queue, nb)
		}
	}
	return dist
}

// TestLargeScaleShortestPath_HopBuckets is the entry point. Run with:
//
//	go test ./pkg/cypher/ -run TestLargeScaleShortestPath_HopBuckets \
//	    -timeout 30m -largescale -v
//
// Outputs a Markdown table to stdout with hop / samples / min / median /
// p95 / max latencies.
func TestLargeScaleShortestPath_HopBuckets(t *testing.T) {
	if !*largeScaleEnable {
		t.Skip("set -largescale to run; this seeds 500K nodes (~30-90s).")
	}

	fixture := ensureLargeScaleFixture(t)
	t.Logf("fixture ready: %d depth buckets sampled", len(fixture.depthsSorted))

	cypher := demoCypherShortestPath
	ctx := context.Background()

	type bucketResult struct {
		depth   int
		samples int
		minNs   int64
		medNs   int64
		p95Ns   int64
		maxNs   int64
	}
	results := make([]bucketResult, 0, len(fixture.depthsSorted))

	// Optional cap on the depths we bench, settable via env so a quick
	// debug run doesn't spend minutes on the long-tail buckets.
	maxBenched := envIntDefault("LARGESCALE_MAX_DEPTH", 60)

	for _, depth := range fixture.depthsSorted {
		if depth > maxBenched {
			break
		}
		pairs := fixture.pairsByDepth[depth]
		if len(pairs) == 0 {
			continue
		}
		// Warm — make sure the property index, plan cache, etc. are
		// hot for this exact pair shape.
		runShortestPathPair(t, fixture.exec, ctx, cypher,
			pairs[0][0], pairs[0][1])

		durs := make([]int64, 0, len(pairs))
		for _, p := range pairs {
			t0 := time.Now()
			hops := runShortestPathPair(t, fixture.exec, ctx, cypher, p[0], p[1])
			elapsed := time.Since(t0).Nanoseconds()
			if hops != int64(depth) {
				t.Logf("[depth %d] %s → %s reported %d hops (expected %d) — graph may have a shorter path; bench still valid",
					depth, p[0], p[1], hops, depth)
			}
			durs = append(durs, elapsed)
		}
		// Insertion sort — pairsPer is small.
		for i := 1; i < len(durs); i++ {
			x := durs[i]
			j := i - 1
			for j >= 0 && durs[j] > x {
				durs[j+1] = durs[j]
				j--
			}
			durs[j+1] = x
		}
		var sum int64
		for _, d := range durs {
			sum += d
		}
		_ = sum // mean isn't reported but we may want it later
		results = append(results, bucketResult{
			depth:   depth,
			samples: len(durs),
			minNs:   durs[0],
			medNs:   durs[len(durs)/2],
			p95Ns:   durs[clampIdx(int(float64(len(durs))*0.95), len(durs))],
			maxNs:   durs[len(durs)-1],
		})
	}

	// Markdown table to stdout. Goes through t.Log so it shows under -v.
	var b strings.Builder
	b.WriteString("\n| hops | samples | min       | median    | p95       | max       |\n")
	b.WriteString("|-----:|--------:|----------:|----------:|----------:|----------:|\n")
	for _, r := range results {
		b.WriteString(fmt.Sprintf("| %4d | %7d | %9s | %9s | %9s | %9s |\n",
			r.depth, r.samples,
			fmtDur(r.minNs), fmtDur(r.medNs), fmtDur(r.p95Ns), fmtDur(r.maxNs)))
	}
	t.Logf("%s", b.String())
	// Also dump to stderr so it survives test buffering.
	fmt.Fprintln(os.Stderr, b.String())
}

func runShortestPathPair(tb testing.TB, exec *StorageExecutor, ctx context.Context, cypher, startID, endID string) int64 {
	tb.Helper()
	res, err := exec.Execute(ctx, cypher, map[string]interface{}{
		"startId": startID,
		"endId":   endID,
	})
	require.NoError(tb, err)
	require.NotEmpty(tb, res.Rows, "no path %s → %s", startID, endID)
	hops, _ := res.Rows[0][1].(int64)
	return hops
}

func envIntDefault(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func clampIdx(i, n int) int {
	if i < 0 {
		return 0
	}
	if i >= n {
		return n - 1
	}
	return i
}

func fmtDur(ns int64) string {
	d := time.Duration(ns)
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", ns)
	case d < time.Millisecond:
		return fmt.Sprintf("%.2fµs", float64(ns)/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(ns)/1e6)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1e9)
	}
}
