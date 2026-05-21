package cypher

import (
	"context"
	"flag"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// demoBenchEnable gates the demo shortestPath Test* functions. The
// benchmarks (Benchmark*) gate themselves implicitly via `go test -bench`
// so they don't need a flag. Off by default so a plain `go test ./...`
// stays under a few seconds; pass -demobench to opt in. Matches the
// pattern used by the large-scale traversal bench (-largescale).
var demoBenchEnable = flag.Bool("demobench", false,
	"run the demo shortestPath Test* harnesses (~3-4s combined seed + traversals)")

// Mirror of the /demo route's procedural seed + shortestPath query.
// Lives next to the cypher executor so the same code path runs that the
// HTTP handler invokes — Badger → WAL → Async → Namespaced → Executor.
//
// Benchmarks:
//   BenchmarkDemoShortestPath_Cold  — fresh DB per iteration; measures the
//                                     setup-dominated path. Useful as a
//                                     sanity check; slow.
//   BenchmarkDemoShortestPath_Warm  — seed once, measure the hot query loop.
//                                     This is what the user is profiling
//                                     when the demo browser is open.
//
// Configurable via env:
//   DEMO_SECTORS, DEMO_STARS_PER_SECTOR, DEMO_INTRA_EDGES, DEMO_GATEWAYS

const (
	defaultDemoSectors        = 20
	defaultDemoStarsPerSector = 50
	defaultDemoIntraEdges     = 3
	defaultDemoGatewaysPerLnk = 2
	demoCypherCreateIndex     = `CREATE INDEX star_id_idx IF NOT EXISTS FOR (n:Star) ON (n.starId)`
	demoCypherShortestPath    = `MATCH (start:Star {starId: $startId}), (end:Star {starId: $endId})
MATCH p = shortestPath((start)-[:HYPERLANE*]-(end))
RETURN [n IN nodes(p) | n.starId] AS pathIds, length(p) AS hops
LIMIT 1`
)

type demoShape struct {
	sectors         int
	starsPerSector  int
	intraEdges      int
	gatewaysPerLink int
	starIDs         []string // ordered, sectored
	startStarID     string
	endStarID       string
}

func defaultDemoShape() demoShape {
	return demoShape{
		sectors:         defaultDemoSectors,
		starsPerSector:  defaultDemoStarsPerSector,
		intraEdges:      defaultDemoIntraEdges,
		gatewaysPerLink: defaultDemoGatewaysPerLnk,
	}
}

// pseudoRand mirrors the TS LCG so generated layouts are deterministic
// and reproducible across the JS demo and Go benches.
type pseudoRand struct{ s uint32 }

func newPseudoRand(seed uint32) *pseudoRand { return &pseudoRand{s: seed} }
func (r *pseudoRand) next() float64 {
	r.s = r.s*1664525 + 1013904223
	return float64(r.s) / float64(1<<32)
}

// buildDemoExecutor wires the same storage chain as the running server
// (Badger → Async → Namespaced) and returns a configured executor.
// Uses a temp dir so each call starts cold.
func buildDemoExecutor(tb testing.TB) (*StorageExecutor, *storage.AsyncEngine) {
	exec, async, _ := buildDemoExecutorNS(tb)
	return exec, async
}

// buildDemoExecutorNS is the variant the large-scale traversal bench uses
// so it can push direct BulkCreate writes through the namespaced engine
// without paying cypher parse + plan cost on every seeded node.
func buildDemoExecutorNS(tb testing.TB) (*StorageExecutor, *storage.AsyncEngine, *storage.NamespacedEngine) {
	tb.Helper()
	dir := tb.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	require.NoError(tb, err)
	async := storage.NewAsyncEngine(badger, nil)
	tb.Cleanup(func() { _ = async.Close() })
	ns := storage.NewNamespacedEngine(async, "d3_demo")
	exec := NewStorageExecutor(ns)
	return exec, async, ns
}

// seedDemoGalaxy seeds the same layout the TS demo seeds. Returns the
// fully-populated shape with a chosen start/end star ID at opposite ends
// of the sector chain.
func seedDemoGalaxy(tb testing.TB, exec *StorageExecutor, shape *demoShape) {
	tb.Helper()
	ctx := context.Background()
	rand := newPseudoRand(0xfeedd3)

	_, err := exec.Execute(ctx, demoCypherCreateIndex, nil)
	require.NoError(tb, err)

	sectorMembers := make([][]string, shape.sectors)
	totalStars := shape.sectors * shape.starsPerSector
	starRows := make([]map[string]any, 0, totalStars)
	for s := 0; s < shape.sectors; s++ {
		members := make([]string, 0, shape.starsPerSector)
		for i := 0; i < shape.starsPerSector; i++ {
			id := fmt.Sprintf("s%d-%d", s, i)
			members = append(members, id)
			starRows = append(starRows, map[string]any{
				"starId": id,
				"name":   id,
				"sector": int64(s),
				"hue":    int64((s * 17) % 360),
				"mass":   int64(1 + int(rand.next()*18)),
				"x":      rand.next() * 100,
				"y":      rand.next() * 100,
				"z":      rand.next() * 100,
			})
		}
		sectorMembers[s] = members
	}
	shape.starIDs = make([]string, 0, totalStars)
	for _, members := range sectorMembers {
		shape.starIDs = append(shape.starIDs, members...)
	}

	const seedBatch = 400
	createStars := `UNWIND $rows AS row
MERGE (n:Star {starId: row.starId})
SET n.name = row.name, n.sector = row.sector, n.hue = row.hue,
    n.mass = row.mass, n.x = row.x, n.y = row.y, n.z = row.z`
	for i := 0; i < len(starRows); i += seedBatch {
		end := i + seedBatch
		if end > len(starRows) {
			end = len(starRows)
		}
		_, err := exec.Execute(ctx, createStars, map[string]interface{}{"rows": starRows[i:end]})
		require.NoError(tb, err)
	}

	type edgeSpec struct{ from, to string }
	seen := make(map[string]struct{})
	addEdge := func(out *[]map[string]any, a, b string) {
		if a == b {
			return
		}
		var k string
		if a < b {
			k = a + "|" + b
		} else {
			k = b + "|" + a
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		*out = append(*out,
			map[string]any{"fromId": a, "toId": b, "distance": int64(1)},
			map[string]any{"fromId": b, "toId": a, "distance": int64(1)},
		)
	}
	edgeRows := make([]map[string]any, 0, totalStars*shape.intraEdges*2)
	for s := 0; s < shape.sectors; s++ {
		members := sectorMembers[s]
		for i := 1; i < len(members); i++ {
			addEdge(&edgeRows, members[i-1], members[i])
		}
		for _, id := range members {
			for k := 0; k < shape.intraEdges; k++ {
				other := members[int(rand.next()*float64(len(members)))]
				addEdge(&edgeRows, id, other)
			}
		}
	}
	for s := 0; s < shape.sectors-1; s++ {
		a, b := sectorMembers[s], sectorMembers[s+1]
		for g := 0; g < shape.gatewaysPerLink; g++ {
			fromID := a[int(rand.next()*float64(len(a)))]
			toID := b[int(rand.next()*float64(len(b)))]
			addEdge(&edgeRows, fromID, toID)
		}
	}
	createEdges := `UNWIND $rows AS row
MATCH (a:Star {starId: row.fromId})
MATCH (b:Star {starId: row.toId})
CREATE (a)-[:HYPERLANE {distance: row.distance}]->(b)`
	for i := 0; i < len(edgeRows); i += seedBatch {
		end := i + seedBatch
		if end > len(edgeRows) {
			end = len(edgeRows)
		}
		_, err := exec.Execute(ctx, createEdges, map[string]interface{}{"rows": edgeRows[i:end]})
		require.NoError(tb, err)
	}

	// Sectored chain endpoints — sector 0 ↔ last sector forces a deep path.
	shape.startStarID = sectorMembers[0][0]
	shape.endStarID = sectorMembers[shape.sectors-1][shape.starsPerSector-1]
}

// runShortestPath fires the demo's exact query once and returns hop count.
func runShortestPath(tb testing.TB, exec *StorageExecutor, startID, endID string) int {
	tb.Helper()
	res, err := exec.Execute(context.Background(), demoCypherShortestPath, map[string]interface{}{
		"startId": startID,
		"endId":   endID,
	})
	require.NoError(tb, err)
	require.NotEmpty(tb, res.Rows, "no path between %s and %s", startID, endID)
	hops, _ := res.Rows[0][1].(int64)
	return int(hops)
}

// TestDemoShortestPath_E2E mirrors what the demo browser does end-to-end
// against the same in-process storage chain the server uses. Confirms a
// path exists across the sector chain so subsequent benchmarks have a
// known-good fixture.
func TestDemoShortestPath_E2E(t *testing.T) {
	if !*demoBenchEnable {
		t.Skip("set -demobench to run the demo shortestPath harness")
	}
	exec, _ := buildDemoExecutor(t)
	shape := defaultDemoShape()
	seedDemoGalaxy(t, exec, &shape)

	hops := runShortestPath(t, exec, shape.startStarID, shape.endStarID)
	require.Greater(t, hops, 0, "expected non-zero hop count")
	t.Logf("demo seed: sectors=%d starsPerSector=%d intra=%d gw=%d → %d hops",
		shape.sectors, shape.starsPerSector, shape.intraEdges, shape.gatewaysPerLink, hops)
}

// BenchmarkDemoShortestPath_Warm is the user-facing bench: seed once, then
// run shortestPath in a loop. This isolates the per-request cost the user
// is hunting (~100ms post-warmup).
func BenchmarkDemoShortestPath_Warm(b *testing.B) {
	exec, async := buildDemoExecutor(b)
	shape := defaultDemoShape()
	seedDemoGalaxy(b, exec, &shape)
	require.NoError(b, async.Flush()) // ensure async cache is empty before measuring

	// Warm the hot path so plan/result caches are primed (mirrors what
	// the demo browser does after its first traversal).
	for i := 0; i < 3; i++ {
		runShortestPath(b, exec, shape.startStarID, shape.endStarID)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Vary endpoints slightly so we don't trivially hit the result cache.
		startID := shape.starIDs[i%len(shape.starIDs)]
		endID := shape.starIDs[(i*7+1)%len(shape.starIDs)]
		if startID == endID {
			continue
		}
		_, err := exec.Execute(context.Background(), demoCypherShortestPath, map[string]interface{}{
			"startId": startID,
			"endId":   endID,
		})
		require.NoError(b, err)
	}
}

// BenchmarkDemoShortestPath_FixedEndpoints repeats the SAME (start,end)
// pair so the result cache should hit every iteration after the first.
// If the per-request floor is real and bypasses the cache, this will
// look identical to _Warm. If it doesn't, we know the cache is masking
// the symptom.
func BenchmarkDemoShortestPath_FixedEndpoints(b *testing.B) {
	exec, async := buildDemoExecutor(b)
	shape := defaultDemoShape()
	seedDemoGalaxy(b, exec, &shape)
	require.NoError(b, async.Flush())

	for i := 0; i < 3; i++ {
		runShortestPath(b, exec, shape.startStarID, shape.endStarID)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(context.Background(), demoCypherShortestPath, map[string]interface{}{
			"startId": shape.startStarID,
			"endId":   shape.endStarID,
		})
		require.NoError(b, err)
	}
}

// BenchmarkDemoShortestPath_Cold pays the seed cost once but rebuilds the
// executor each iteration. Useful only for diagnosing setup overhead.
func BenchmarkDemoShortestPath_Cold(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		exec, _ := buildDemoExecutor(b)
		shape := defaultDemoShape()
		seedDemoGalaxy(b, exec, &shape)
		b.StartTimer()
		runShortestPath(b, exec, shape.startStarID, shape.endStarID)
	}
}

// TestDemoShortestPath_LatencyDistribution captures min/median/p95/p99
// over 100 runs at the demo's default shape. Reports as a t.Logf so we
// have an in-Go baseline that doesn't depend on a browser.
func TestDemoShortestPath_LatencyDistribution(t *testing.T) {
	if !*demoBenchEnable {
		t.Skip("set -demobench to run the demo latency distribution test")
	}
	exec, async := buildDemoExecutor(t)
	shape := defaultDemoShape()
	seedDemoGalaxy(t, exec, &shape)
	require.NoError(t, async.Flush())

	const samples = 100
	for i := 0; i < 5; i++ {
		runShortestPath(t, exec, shape.startStarID, shape.endStarID) // warmup
	}

	durs := make([]time.Duration, 0, samples)
	hopCounts := make([]int, 0, samples)
	for i := 0; i < samples; i++ {
		startID := shape.starIDs[i%len(shape.starIDs)]
		endID := shape.starIDs[(i*7+1)%len(shape.starIDs)]
		if startID == endID {
			continue
		}
		t0 := time.Now()
		res, err := exec.Execute(context.Background(), demoCypherShortestPath, map[string]interface{}{
			"startId": startID,
			"endId":   endID,
		})
		require.NoError(t, err)
		durs = append(durs, time.Since(t0))
		if len(res.Rows) > 0 {
			if h, ok := res.Rows[0][1].(int64); ok {
				hopCounts = append(hopCounts, int(h))
			}
		}
	}

	min, max := durs[0], durs[0]
	var sum time.Duration
	for _, d := range durs {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
		sum += d
	}
	mean := time.Duration(int64(sum) / int64(len(durs)))
	sortedDurs := append([]time.Duration(nil), durs...)
	sortDurations(sortedDurs)
	p50 := sortedDurs[len(sortedDurs)/2]
	p95 := sortedDurs[int(math.Floor(float64(len(sortedDurs))*0.95))]
	p99 := sortedDurs[int(math.Floor(float64(len(sortedDurs))*0.99))]
	minHop, maxHop := hopCounts[0], hopCounts[0]
	for _, h := range hopCounts {
		if h < minHop {
			minHop = h
		}
		if h > maxHop {
			maxHop = h
		}
	}
	t.Logf("shortestPath latency over %d samples: min=%s mean=%s p50=%s p95=%s p99=%s max=%s",
		len(durs), min, mean, p50, p95, p99, max)
	t.Logf("hop range over %d samples: min=%d max=%d", len(hopCounts), minHop, maxHop)
}

// TestDemoShortestPath_LatencyOverTime samples per-query latency in chunks
// to detect growing-state regressions: if a cache or map keeps appending
// per-query and never trims, later chunks should look slower than earlier
// chunks. The user reported "initial 42ms then warm 100+ms" — this is the
// shape that pattern produces.
func TestDemoShortestPath_LatencyOverTime(t *testing.T) {
	if !*demoBenchEnable {
		t.Skip("set -demobench to run the demo latency-over-time test")
	}
	exec, async := buildDemoExecutor(t)
	shape := defaultDemoShape()
	seedDemoGalaxy(t, exec, &shape)
	require.NoError(t, async.Flush())

	const total = 500
	const chunk = 100
	cypher := demoCypherShortestPath

	durs := make([]time.Duration, 0, total)
	for i := 0; i < total; i++ {
		startID := shape.starIDs[i%len(shape.starIDs)]
		endID := shape.starIDs[(i*7+1)%len(shape.starIDs)]
		if startID == endID {
			continue
		}
		t0 := time.Now()
		_, err := exec.Execute(context.Background(), cypher, map[string]interface{}{
			"startId": startID,
			"endId":   endID,
		})
		require.NoError(t, err)
		durs = append(durs, time.Since(t0))
	}

	for i := 0; i < len(durs); i += chunk {
		end := i + chunk
		if end > len(durs) {
			end = len(durs)
		}
		seg := durs[i:end]
		var sum time.Duration
		for _, d := range seg {
			sum += d
		}
		mean := time.Duration(int64(sum) / int64(len(seg)))
		sorted := append([]time.Duration(nil), seg...)
		sortDurations(sorted)
		p50 := sorted[len(sorted)/2]
		p95 := sorted[int(math.Floor(float64(len(sorted))*0.95))]
		t.Logf("chunk %d-%d (%d samples): mean=%s p50=%s p95=%s",
			i, end, len(seg), mean, p50, p95)
	}
}

func sortDurations(d []time.Duration) {
	// insertion sort — N is small (100ish)
	for i := 1; i < len(d); i++ {
		x := d[i]
		j := i - 1
		for j >= 0 && d[j] > x {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = x
	}
}
