package cypher

// The NOR-414 seed-choice fixtures and benchmark, kept apart from the tests so
// the same benchmark compiles against a base without this change (A/B runs).

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// newSeedBenchExecutor is an executor without a query result cache over a
// memory store holding statements' data.
func newSeedBenchExecutor(b *testing.B, statements ...string) *StorageExecutor {
	b.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "seed_choice"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, statement := range statements {
		if _, err := exec.Execute(context.Background(), statement, nil); err != nil {
			b.Fatal(statement, err)
		}
	}
	return exec
}

// The cases population: 3,000 Case nodes with unique ids c-0 … c-2999, every
// third one done and the rest pending, indexed on id and status. Twenty ids
// are looked up, thirteen of them pending.
const seedCaseCount = 3000

func seedCaseStatements() []string {
	return []string{
		"CREATE INDEX seed_case_id FOR (n:Case) ON (n.id)",
		"CREATE INDEX seed_case_status FOR (n:Case) ON (n.status)",
		fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:Case {id: 'c-' + toString(i), status: CASE WHEN i %% 3 = 0 THEN 'done' ELSE 'pending' END, again: i %% 50 = 1})", seedCaseCount-1),
	}
}

func seedCaseIDs() ([]interface{}, []string) {
	ids := make([]interface{}, 0, 20)
	var pending []string
	for i := 0; i < 20; i++ {
		n := i*50 + 1
		ids = append(ids, fmt.Sprintf("c-%d", n))
		if n%3 != 0 {
			pending = append(pending, fmt.Sprintf("c-%d", n))
		}
	}
	sort.Strings(pending)
	return ids, pending
}

// The multi-label population: 3,000 G:Ev nodes and 200 G:Id identities with
// survivors s0 … s49 (four each, every fourth without a status), indexed on
// Id(survivor) only, and 60 Sub nodes bound to identities.
func seedMultiLabelStatements() []string {
	return []string{
		"CREATE INDEX seed_id_survivor FOR (n:Id) ON (n.survivor)",
		"CREATE INDEX seed_g_id FOR (n:G) ON (n.id)",
		"UNWIND range(1, 3000) AS i CREATE (:G:Ev {id: 'e' + toString(i), status: 'x'})",
		"UNWIND range(0, 199) AS i CREATE (:G:Id {id: 'p' + toString(i), survivor: 's' + toString(i % 50), status: CASE WHEN i % 4 = 3 THEN null ELSE 'live' END})",
		"UNWIND range(0, 59) AS i CREATE (:G:Sub {id: 'sub' + toString(i), k: i})",
		"MATCH (s:Sub), (p:Id) WHERE p.id = 'p' + toString(s.k * 3 % 200) CREATE (s)-[:B]->(p)",
	}
}

// The label-only population: 3,000 G nodes, every 50th also Id (60), each Id
// with one L relationship.
func seedLabelOnlyStatements() []string {
	return []string{
		"UNWIND range(0, 2999) AS i CREATE (n:G {id: i}) WITH n, i WHERE i % 50 = 0 SET n:Id CREATE (n)-[:L]->(:Out {id: i})",
	}
}

// BenchmarkNodeSeedChoice times the NOR-414 lookup, the single-seek lookups
// whose plan is unchanged, a broad indexed read, a label scan, and the
// multi-label label-only and inline forms.
func BenchmarkNodeSeedChoice(b *testing.B) {
	cases := newSeedBenchExecutor(b, seedCaseStatements()...)
	ids, _ := seedCaseIDs()
	labels := newSeedBenchExecutor(b, append(seedMultiLabelStatements(), seedLabelOnlyStatements()...)...)
	for _, bench := range []struct {
		name   string
		exec   *StorageExecutor
		query  string
		params map[string]interface{}
	}{
		{"unwind_inline_where", cases, "UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id", map[string]interface{}{"ids": ids}},
		{"unwind_inline", cases, "UNWIND $ids AS id MATCH (c:Case {id: id}) RETURN c.id", map[string]interface{}{"ids": ids}},
		{"inline_lookup", cases, "MATCH (c:Case {id: $id}) RETURN c.id", map[string]interface{}{"id": "c-101"}},
		{"where_lookup", cases, "MATCH (c:Case) WHERE c.id = $id RETURN c.id", map[string]interface{}{"id": "c-101"}},
		{"broad_indexed", cases, "MATCH (c:Case) WHERE c.status = 'pending' RETURN count(c)", nil},
		{"label_scan", cases, "MATCH (c:Case) WHERE c.again RETURN count(c)", nil},
		{"multi_label_inline", labels, "MATCH (p:G:Id {survivor: $x}) RETURN p.id", map[string]interface{}{"x": "s9"}},
		{"multi_label_scan", labels, "MATCH (n:G:Id) RETURN count(n)", nil},
	} {
		b.Run(bench.name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := bench.exec.Execute(ctx, bench.query, bench.params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNodeSeedChoiceCompositeInTransaction times a lookup that names a
// composite index's properties and an indexed id inside an explicit
// transaction holding 4,000 pending nodes of the label: the MATCH form and
// the MERGE form, one statement each per iteration.
func BenchmarkNodeSeedChoiceCompositeInTransaction(b *testing.B) {
	const pending = 4000
	exec := newSeedBenchExecutor(b,
		"CREATE INDEX seed_ptx_kind_shard FOR (n:SeedPendingComposite) ON (n.kind, n.shard)",
		"CREATE INDEX seed_ptx_id FOR (n:SeedPendingComposite) ON (n.id)",
	)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	if _, err := exec.Execute(ctx, "UNWIND range(1, $n) AS i CREATE (:SeedPendingComposite {id: i, kind: 'k', shard: 7})", map[string]interface{}{"n": int64(pending)}); err != nil {
		b.Fatal(err)
	}
	for _, bench := range []struct{ name, query string }{
		{"match", "MATCH (n:SeedPendingComposite {kind: $k, shard: $s}) WHERE n.id = $id RETURN n.id"},
		{"merge", "MERGE (n:SeedPendingComposite {kind: $k, shard: $s, id: $id}) RETURN n.id"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				params := map[string]interface{}{"k": "k", "s": int64(7), "id": int64(i%pending + 1)}
				if _, err := exec.Execute(ctx, bench.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNodeSeedChoicePD times the PD shapes folded into NOR-414: the
// multi-label survivor read, the two-hop chain through a shared hub, the
// traversal from a bound end and from an indexed end, and a creating MERGE
// in a transaction that already holds about 6,000 pending nodes.
func BenchmarkNodeSeedChoicePD(b *testing.B) {
	ctx := context.Background()
	reads := newSeedBenchExecutor(b,
		"CREATE INDEX bench_pd_graph_id FOR (n:PDGraphNode) ON (n.id)",
		"CREATE INDEX bench_pd_survivor FOR (n:PDIdentity) ON (n.survivor)",
		"CREATE INDEX bench_pd_doc_id FOR (n:PDDocument) ON (n.id)",
		"CREATE INDEX bench_known_end_id FOR (n:KnownEndEntity) ON (n.id)",
		"UNWIND range(1, 3000) AS i CREATE (:PDGraphNode {id: 'g' + toString(i), status: 'live'})",
		"UNWIND range(1, 12) AS i CREATE (:PDGraphNode:PDIdentity {id: 'p' + toString(i), survivor: 's' + toString(i % 4), status: 'live'})",
		"CREATE (:PDDocument {id: 'doc'}), (:PDGraphNode {id: 'hub', document_id: 'other'})",
		"MATCH (d:PDDocument {id: 'doc'}), (hub:PDGraphNode {id: 'hub'}) UNWIND range(1, 256) AS i CREATE (d)-[:PD_RELATION]->(hub) CREATE (own:PDGraphNode {id: 'own' + toString(i), document_id: 'doc'}) CREATE (d)-[:PD_RELATION]->(own) CREATE (own)-[:PD_RELATION]->(hub)",
		"UNWIND range(1, 3000) AS i CREATE (:KnownEndHolder {id: 'h' + toString(i), x: i})",
		"UNWIND range(1, 3000) AS i CREATE (:KnownEndDoc {id: 'd' + toString(i)})",
		"CREATE (:KnownEndTarget {id: 't1'}), (:KnownEndEntity {id: 'e1'})",
		"MATCH (t:KnownEndTarget {id: 't1'}), (h:KnownEndHolder) WHERE h.id IN ['h1', 'h2'] CREATE (h)-[:KNOWN_END_R]->(t)",
		"MATCH (e:KnownEndEntity {id: 'e1'}), (d:KnownEndDoc) WHERE d.id IN ['d1', 'd2'] CREATE (d)-[:KNOWN_END_MENTIONS]->(e)",
	)
	for _, bench := range []struct {
		name   string
		query  string
		params map[string]interface{}
	}{
		{"survivor_multi_label", "UNWIND $ids AS id MATCH (p:PDGraphNode:PDIdentity {survivor: id}) WHERE p.status IS NOT NULL RETURN p.id", map[string]interface{}{"ids": []interface{}{"s1", "s2", "missing"}}},
		{"two_hop_hub", "MATCH (document:PDDocument {id: $id}) CALL (document) { MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode) WHERE own.document_id = document.id AND coalesce(shared.document_id, '') <> document.id RETURN count(*) AS c } RETURN c", map[string]interface{}{"id": "doc"}},
		{"bound_end", "MATCH (t:KnownEndTarget {id: 't1'}) OPTIONAL MATCH (holder:KnownEndHolder)-[:KNOWN_END_R]-(t) WHERE holder.x IS NOT NULL RETURN count(holder)", nil},
		{"indexed_end", "MATCH (d:KnownEndDoc)-[:KNOWN_END_MENTIONS]->(e:KnownEndEntity {id: $id}) RETURN d.id", map[string]interface{}{"id": "e1"}},
	} {
		b.Run(bench.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := reads.Execute(ctx, bench.query, bench.params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("pending_merge", func(b *testing.B) {
		exec := newSeedBenchExecutor(b, "CREATE CONSTRAINT bench_pd_merge_id FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE")
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
		if _, err := exec.Execute(ctx, "UNWIND range(1, 6000) AS i MERGE (x:PDGraphNode:PNGroup0 {id: 'seed-' + toString(i)}) SET x.ordinal = i", nil); err != nil {
			b.Fatal(err)
		}
		statement := "UNWIND $nodes AS n MERGE (x:PDGraphNode:PNGroup0 {id: n.id}) SET x.ordinal = n.ordinal"
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			params := map[string]interface{}{"nodes": []interface{}{map[string]interface{}{"id": fmt.Sprintf("new-%d", i), "ordinal": int64(i)}}}
			if _, err := exec.Execute(ctx, statement, params); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkNodeSeedChoiceLargeTransactionRead times an indexed read of
// about 10,000 to 11,000 nodes inside an explicit transaction, the first read
// of a transaction and a read repeated in one, as a count and as rows. The
// sizes sit on both sides of storage's node cache size (10,000) and of nine
// tenths of the label, where an earlier version of this change switched
// between the index and the label scan.
func BenchmarkNodeSeedChoiceLargeTransactionRead(b *testing.B) {
	for _, size := range []struct{ listed, label int }{{10000, 10500}, {10001, 10500}, {11030, 12256}, {11031, 12256}} {
		b.Run(fmt.Sprintf("%d_of_%d", size.listed, size.label), func(b *testing.B) {
			exec := newSeedBenchExecutor(b,
				"CREATE INDEX seed_large_k FOR (n:SeedLarge) ON (n.k)",
				fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (:SeedLarge {id: i, k: CASE WHEN i <= %d THEN 'hit' ELSE 'other' END})", size.label, size.listed),
			)
			ctx := context.Background()
			control := func(statement string) {
				if _, err := exec.Execute(ctx, statement, nil); err != nil {
					b.Fatal(err)
				}
			}
			for _, form := range []struct{ name, query string }{
				{"count", "MATCH (n:SeedLarge {k: 'hit'}) RETURN count(n)"},
				{"rows", "MATCH (n:SeedLarge {k: 'hit'}) RETURN n"},
			} {
				read := func() {
					result, err := exec.Execute(ctx, form.query, nil)
					if err != nil {
						b.Fatal(err)
					}
					if form.name == "rows" && len(result.Rows) != size.listed {
						b.Fatalf("%d rows", len(result.Rows))
					}
					if form.name == "count" && result.Rows[0][0] != int64(size.listed) {
						b.Fatalf("count %v", result.Rows)
					}
				}
				b.Run(form.name+"/first", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						control("BEGIN")
						b.StartTimer()
						read()
						b.StopTimer()
						control("ROLLBACK")
						b.StartTimer()
					}
				})
				b.Run(form.name+"/repeated", func(b *testing.B) {
					control("BEGIN")
					read()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						read()
					}
					b.StopTimer()
					control("ROLLBACK")
				})
			}
		})
	}
}
