// Author: Codex, NOR-160 implementation sub-agent, 2026-10-26.
package bolt

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const nor160OriginalRead = `UNWIND $rows AS row
MATCH (:Held {thing_id: row.i})-[co:COHELD]->(:Held {thing_id: row.t})
RETURN row.i AS i, row.t AS t, co.n AS value`

const nor160BoundRead = `UNWIND $rows AS row
MATCH (a:Held {thing_id: row.i})
MATCH (b:Held {thing_id: row.t})
MATCH (a)-[co:COHELD]->(b)
RETURN row.i AS i, row.t AS t, co.n AS value`

const nor160StoredRead = `UNWIND $ids AS id
MATCH (d:Document {id: id})
CALL (d) {
  MATCH (d)-[has:HAS]->(held:Held)
  RETURN collect({thing: held.thing_id,
    via: {link: has.link, item: has.item, kind: has.kind, source_id: has.source_id,
          cite_start: has.cite_start, cite_end: has.cite_end},
    identity: has.identity}) AS holdings
}
CALL (d) {
  MATCH (d)-[:CITES]->(source:SearchSource)
  RETURN collect(source.id) AS cites
}
RETURN d.id AS id, holdings, cites`

const nor160CountsRead = `UNWIND $ids AS id MATCH (h:Held {thing_id: id}) RETURN id, h.holders AS value`

type nor160Fixture struct {
	driver          neo4j.DriverWithContext
	params          map[string]interface{}
	docIDs, heldIDs []interface{}
	want            []string
}

type nor160Run func(context.Context, string, map[string]interface{}) (neo4j.ResultWithContext, error)

// The default is a small correctness test. NOR160_FULL=1 selects a deterministic
// synthetic graph with the reported global cardinalities; it is not a replay of
// the reporter's saved graph. Both routes fully consume the same rows over Bolt.
func TestNOR160RelationshipReadProfile(t *testing.T) {
	ctx := context.Background()
	f := nor160Setup(t, ctx)
	rounds := 2
	if value := os.Getenv("NOR160_ROUNDS"); value != "" {
		var err error
		rounds, err = strconv.Atoi(value)
		require.NoError(t, err)
		require.True(t, rounds >= 1 && rounds <= 6)
	}
	shapes := []struct{ name, query string }{{"original", nor160OriginalRead}, {"bound", nor160BoundRead}}
	for _, shape := range shapes {
		if chosen := os.Getenv("NOR160_SHAPE"); chosen != "" && chosen != shape.name {
			continue
		}
		t.Run(shape.name, func(t *testing.T) {
			totals := map[bool]time.Duration{}
			// Reverse route order on the second round. Each route gets a fresh
			// session; the explicit transaction is primed before its pair read.
			for round := 0; round < rounds; round++ {
				for _, explicit := range []bool{round%2 != 0, round%2 == 0} {
					mode := "autocommit"
					if explicit {
						mode = "explicit"
					}
					t.Run(fmt.Sprintf("round%d/%s", round, mode), func(t *testing.T) {
						run := f.open(t, ctx, explicit)
						params := nor160Params(f.params, shape.name+"/"+mode+"/"+strconv.Itoa(round))
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						start := time.Now()
						records := nor160Collect(t, ctx, run, shape.query, params)
						elapsed := time.Since(start)
						totals[explicit] += elapsed
						runtime.ReadMemStats(&after)
						require.Equal(t, f.want, nor160Rows(t, records), "complete row multiset, including duplicates and every value")
						nor160Log(t, "MEASURE", map[string]interface{}{
							"parser": os.Getenv("NORNICDB_PARSER"), "shape": shape.name, "mode": mode, "round": round,
							"seconds": elapsed.Seconds(), "rows": len(records), "allocated_bytes": after.TotalAlloc - before.TotalAlloc,
							"allocations": after.Mallocs - before.Mallocs, "gc_cycles": after.NumGC - before.NumGC,
						})
					})
				}
			}
			ratio := float64(totals[true]) / float64(totals[false])
			nor160Log(t, "RATIO", map[string]interface{}{"shape": shape.name, "explicit_over_autocommit": ratio})
			if limit := os.Getenv("NOR160_MAX_RATIO"); limit != "" {
				maximum, err := strconv.ParseFloat(limit, 64)
				require.NoError(t, err)
				require.LessOrEqual(t, ratio, maximum, "primed write-capable transaction read cost")
			}
			if os.Getenv("NOR160_PROFILE") == "1" {
				for _, explicit := range []bool{false, true} {
					mode := "autocommit"
					if explicit {
						mode = "explicit"
					}
					t.Run("profile/"+mode, func(t *testing.T) {
						run := f.open(t, ctx, explicit)
						nor160Profile(t, shape.name, mode, func(iteration int) {
							records := nor160Collect(t, ctx, run, shape.query, nor160Params(f.params, fmt.Sprintf("profile/%s/%s/%d", shape.name, mode, iteration)))
							require.Equal(t, f.want, nor160Rows(t, records))
						})
					})
				}
			}
		})
	}
}

func (f *nor160Fixture) open(t testing.TB, ctx context.Context, explicit bool) nor160Run {
	t.Helper()
	session := f.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nor160", AccessMode: neo4j.AccessModeWrite})
	t.Cleanup(func() { require.NoError(t, session.Close(ctx)) })
	run := nor160Run(func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
		return session.Run(ctx, query, params)
	})
	if explicit {
		tx, err := session.BeginTransaction(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tx.Rollback(ctx)) })
		run = tx.Run
	}
	// Prime both routes identically. In the explicit route these two reads and
	// the pair read share one write-capable transaction, without pending writes.
	require.Len(t, nor160Collect(t, ctx, run, nor160StoredRead, map[string]interface{}{"ids": f.docIDs}), len(f.docIDs))
	require.Len(t, nor160Collect(t, ctx, run, nor160CountsRead, map[string]interface{}{"ids": f.heldIDs}), len(f.heldIDs))
	return run
}

func nor160Setup(t testing.TB, ctx context.Context) *nor160Fixture {
	t.Helper()
	held, active, docs, totalNodes, totalEdges, requested, matched := 24, 12, 4, 64, 80, 32, 8
	if os.Getenv("NOR160_FULL") == "1" {
		held, active, docs, totalNodes, totalEdges, requested, matched = 10000, 106, 200, 70802, 96160, 5101, 1390
	}
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), SyncWrites: true})
	require.NoError(t, err)
	engine = fixtureowner.Own(t, fixtureowner.Track(t, engine, storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	nodes := make([]*storage.Node, 0, totalNodes)
	id := func(kind string, i int) storage.NodeID { return storage.NodeID(fmt.Sprintf("nor160:%s-%05d", kind, i)) }
	thing := func(i int) string { return fmt.Sprintf("thing-%05d", i) }
	f := &nor160Fixture{params: map[string]interface{}{}}
	for i := 0; i < held; i++ {
		nodes = append(nodes, &storage.Node{ID: id("held", i), Labels: []string{"Held"}, Properties: map[string]interface{}{"thing_id": thing(i), "holders": int64(i%7 + 1)}})
		if i < active {
			f.heldIDs = append(f.heldIDs, thing(i))
		}
	}
	for i := 0; i < docs; i++ {
		name := fmt.Sprintf("doc-%05d", i)
		f.docIDs = append(f.docIDs, name)
		nodes = append(nodes, &storage.Node{ID: id("doc", i), Labels: []string{"Document"}, Properties: map[string]interface{}{"id": name}})
		nodes = append(nodes, &storage.Node{ID: id("source", i), Labels: []string{"SearchSource"}, Properties: map[string]interface{}{"id": fmt.Sprintf("source-%05d", i)}})
	}
	noiseNodes := totalNodes - len(nodes)
	for i := 0; i < noiseNodes; i++ {
		nodes = append(nodes, &storage.Node{ID: id("noise", i), Labels: []string{"Noise"}, Properties: map[string]interface{}{"id": int64(i)}})
	}
	// Chunk the public storage bulk fixture loader; none of setup is timed.
	for lo := 0; lo < len(nodes); lo += 1000 {
		hi := min(lo+1000, len(nodes))
		require.NoError(t, engine.BulkCreateNodes(nodes[lo:hi]))
	}
	edges := make([]*storage.Edge, 0, totalEdges)
	addEdge := func(start, end storage.NodeID, kind string, props map[string]interface{}) {
		edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nor160:edge-%06d", len(edges))), StartNode: start, EndNode: end, Type: kind, Properties: props})
	}
	var rows []interface{}
	for i := 0; i < active && len(rows) < requested; i++ {
		for j := i + 1; j < active && len(rows) < requested; j++ {
			rows = append(rows, map[string]interface{}{"i": thing(i), "t": thing(j)})
			if len(rows) <= matched {
				value := int64(len(rows))
				addEdge(id("held", i), id("held", j), "COHELD", map[string]interface{}{"n": value})
				encoded, err := json.Marshal([]interface{}{thing(i), thing(j), value})
				require.NoError(t, err)
				f.want = append(f.want, string(encoded))
			}
		}
	}
	require.Len(t, rows, requested)
	for i := 0; i < docs; i++ {
		addEdge(id("doc", i), id("held", i%active), "HAS", map[string]interface{}{"link": "link", "item": int64(i), "kind": "probe", "source_id": "source", "cite_start": int64(0), "cite_end": int64(1), "identity": fmt.Sprint(i)})
		addEdge(id("doc", i), id("source", i), "CITES", nil)
	}
	for i := active; i < held; i++ {
		addEdge(id("held", i), id("held", active+(i-active+1)%(held-active)), "COHELD", map[string]interface{}{"n": int64(1)})
	}
	for i := 0; len(edges) < totalEdges; i++ {
		addEdge(id("noise", i%noiseNodes), id("noise", (i+1)%noiseNodes), "NOISE", map[string]interface{}{"n": int64(i)})
	}
	require.Len(t, edges, totalEdges)
	for lo := 0; lo < len(edges); lo += 1000 {
		hi := min(lo+1000, len(edges))
		require.NoError(t, engine.BulkCreateEdges(edges[lo:hi]))
	}
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(engine, "nor160"))
	f.driver, err = neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.driver.Close(ctx)) })
	session := f.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nor160"})
	defer session.Close(ctx)
	run := nor160Run(func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
		return session.Run(ctx, query, params)
	})
	for _, query := range []string{
		"CREATE INDEX nor160_held FOR (h:Held) ON (h.thing_id)",
		"CREATE INDEX nor160_doc FOR (d:Document) ON (d.id)",
	} {
		nor160Collect(t, ctx, run, query, nil)
	}
	f.params["rows"] = rows
	sort.Strings(f.want)
	encoded, err := json.Marshal(rows)
	require.NoError(t, err)
	nor160Log(t, "FIXTURE", map[string]interface{}{"nodes": totalNodes, "edges": totalEdges, "held": held, "active_endpoints": active, "request_rows": len(rows), "expected_rows": len(f.want), "params_sha256": fmt.Sprintf("%x", sha256.Sum256(encoded)), "sync_writes": true, "write_behind": false, "gomaxprocs": runtime.GOMAXPROCS(0), "synthetic": true})
	return f
}

func nor160Collect(t testing.TB, ctx context.Context, run nor160Run, query string, params map[string]interface{}) []*neo4j.Record {
	t.Helper()
	result, err := run(ctx, query, params)
	require.NoError(t, err, query)
	records, err := result.Collect(ctx)
	require.NoError(t, err, query)
	return records
}

func nor160Rows(t testing.TB, records []*neo4j.Record) []string {
	t.Helper()
	rows := make([]string, 0, len(records))
	for _, record := range records {
		encoded, err := json.Marshal(record.Values)
		require.NoError(t, err)
		rows = append(rows, string(encoded))
	}
	sort.Strings(rows)
	return rows
}

func nor160Params(base map[string]interface{}, nonce string) map[string]interface{} {
	// An unused changing parameter prevents the result cache from answering a
	// measured query. Requested pair rows remain byte-for-byte identical.
	return map[string]interface{}{"rows": base["rows"], "nonce": nonce}
}

func nor160Log(t testing.TB, kind string, value interface{}) {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	t.Logf("NOR160_%s %s", kind, encoded)
}

// Benchmark the identical public fixture and oracle, outside setup/BEGIN and
// priming. Each query carries a distinct nonce to bypass the result cache.
func BenchmarkNOR160RelationshipRead(b *testing.B) {
	ctx := context.Background()
	f := nor160Setup(b, ctx)
	for _, shape := range []struct{ name, query string }{{"original", nor160OriginalRead}, {"bound", nor160BoundRead}} {
		b.Run(shape.name, func(b *testing.B) {
			for _, explicit := range []bool{false, true} {
				mode := "autocommit"
				if explicit {
					mode = "explicit"
				}
				b.Run(mode, func(b *testing.B) {
					run := f.open(b, ctx, explicit)
					noncePrefix := fmt.Sprintf("%s/%d", b.Name(), time.Now().UnixNano())
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						records := nor160Collect(b, ctx, run, shape.query, nor160Params(f.params, fmt.Sprintf("%s/%d", noncePrefix, iteration)))
						b.StopTimer()
						require.Equal(b, f.want, nor160Rows(b, records))
						b.StartTimer()
					}
				})
			}
		})
	}
}
