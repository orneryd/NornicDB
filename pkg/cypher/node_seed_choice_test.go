package cypher

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// seedReadEngine counts the nodes the store hands a statement: point reads,
// batch reads and every node of a label or whole-store stream. An index seek
// reads the nodes its index lists; a label scan reads the whole label.
type seedReadEngine struct {
	*storage.MemoryEngine
	reads atomic.Int64
}

func (c *seedReadEngine) countNode(node *storage.Node) {
	if node != nil {
		c.reads.Add(1)
	}
}

func (c *seedReadEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := c.MemoryEngine.GetNode(ctx, id)
	c.countNode(node)
	return node, err
}

func (c *seedReadEngine) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := c.MemoryEngine.GetQueryNode(ctx, id)
	c.countNode(node)
	return node, err
}

func (c *seedReadEngine) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	nodes, err := c.MemoryEngine.BatchGetNodes(ctx, ids)
	c.reads.Add(int64(len(nodes)))
	return nodes, err
}

func (c *seedReadEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := c.MemoryEngine.GetNodesByLabel(ctx, label)
	c.reads.Add(int64(len(nodes)))
	return nodes, err
}

func (c *seedReadEngine) GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*storage.Node, error) {
	nodes, err := c.MemoryEngine.GetNodesByLabelInScope(ctx, scope, label)
	c.reads.Add(int64(len(nodes)))
	return nodes, err
}

func (c *seedReadEngine) StreamNodes(ctx context.Context, visit func(*storage.Node) error) error {
	return c.MemoryEngine.StreamNodes(ctx, func(node *storage.Node) error {
		c.countNode(node)
		return visit(node)
	})
}

func (c *seedReadEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return c.MemoryEngine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
		c.countNode(node)
		return visit(node)
	})
}

func (c *seedReadEngine) StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*storage.Node) error) error {
	return c.MemoryEngine.StreamNodesByLabelProjectedInScope(ctx, scope, label, properties, func(node *storage.Node) error {
		c.countNode(node)
		return visit(node)
	})
}

// seedFixture is an executor over a seedReadEngine.
type seedFixture struct {
	exec   *StorageExecutor
	engine *seedReadEngine
}

func newSeedFixture(t testing.TB, statements ...string) *seedFixture {
	t.Helper()
	engine := &seedReadEngine{MemoryEngine: newTestMemoryEngine(t)}
	// No query result cache: every statement reads the store.
	fixture := &seedFixture{exec: NewStorageExecutor(storage.NewNamespacedEngine(engine, "seed_choice"), QueryCachePolicy{MaxEntries: 0, TTL: 0}), engine: engine}
	fixture.run(t, statements...)
	return fixture
}

func (f *seedFixture) run(t testing.TB, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		_, err := f.exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
}

// query runs statement and returns its rows, sorted, and the nodes it read.
func (f *seedFixture) query(t *testing.T, statement string, params map[string]interface{}) ([]string, int64) {
	t.Helper()
	f.engine.reads.Store(0)
	result, err := f.exec.Execute(context.Background(), statement, params)
	require.NoError(t, err, statement)
	return seedRowKeys(result.Rows), f.engine.reads.Load()
}

func seedRowKeys(rows [][]interface{}) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, fmt.Sprint(row...))
	}
	sort.Strings(keys)
	return keys
}

// NOR-414: an equality on an indexed property in the WHERE no longer replaces
// the pattern's own selective index lookup with a read of every node the
// WHERE value matches. The nodes read stay a small multiple of the ids looked
// up, and do not grow when more nodes hold the WHERE value.
func TestNodeSeedChoiceReadsSelectiveIndexNotWhereEquality(t *testing.T) {
	fixture := newSeedFixture(t, seedCaseStatements()...)
	ids, pending := seedCaseIDs()
	require.Len(t, pending, 13)
	idRows := func(prefix string) []string {
		rows := make([]string, 0, len(pending))
		for _, id := range pending {
			rows = append(rows, prefix+id)
		}
		return rows
	}
	forms := []struct {
		name, query string
		rows        []string
	}{
		{"unwind_inline_where", "UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id", idRows("")},
		{"unwind_inline_where_and", "UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' AND c.again RETURN c.id", idRows("")},
		{"unwind_where_conjuncts", "UNWIND $ids AS id MATCH (c:Case) WHERE c.status = 'pending' AND c.id = id RETURN c.id", idRows("")},
		{"unwind_inline_where_in", "UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status IN ['pending', 'later'] RETURN c.id", idRows("")},
		{"unwind_optional", "UNWIND $ids AS id OPTIONAL MATCH (c:Case {id: id}) WHERE c.status = 'pending' WITH c WHERE c IS NOT NULL RETURN c.id", idRows("")},
		{"workaround_with", "UNWIND $ids AS id MATCH (c:Case {id: id}) WITH c WHERE c.status = 'pending' RETURN c.id", idRows("")},
	}
	params := map[string]interface{}{"ids": ids}
	reads := make(map[string]int64, len(forms))
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			rows, read := fixture.query(t, form.query, params)
			require.Equal(t, form.rows, rows)
			require.LessOrEqual(t, read, int64(3*len(ids)), "nodes read: an index lookup per id, not every pending node per id")
			reads[form.name] = read
		})
	}
	t.Run("single_id", func(t *testing.T) {
		total := int64(0)
		var rows []string
		for _, id := range ids {
			got, read := fixture.query(t, "MATCH (c:Case {id: $id}) WHERE c.status = 'pending' RETURN c.id", map[string]interface{}{"id": id})
			rows = append(rows, got...)
			total += read
		}
		sort.Strings(rows)
		require.Equal(t, idRows(""), rows)
		require.LessOrEqual(t, total, int64(3*len(ids)))
	})
	t.Run("single_id_count", func(t *testing.T) {
		rows, read := fixture.query(t, "MATCH (c:Case {id: $id}) WHERE c.status = 'pending' RETURN count(c)", map[string]interface{}{"id": "c-1"})
		require.Equal(t, []string{"1"}, rows)
		require.LessOrEqual(t, read, int64(3))
	})

	// Twice as many pending nodes: the same reads.
	fixture.run(t, fmt.Sprintf("UNWIND range(0, %d) AS i CREATE (:Case {id: 'x-' + toString(i), status: 'pending'})", seedCaseCount-1))
	for _, form := range forms {
		rows, read := fixture.query(t, form.query, params)
		require.Equal(t, form.rows, rows, form.name)
		require.Equal(t, reads[form.name], read, "%s: the nodes read must not depend on how many nodes are pending", form.name)
	}
}

// The same choice inside an explicit transaction: the inline id lookup seeds
// the MATCH (the hot-path trace records it), with the same rows.
func TestNodeSeedChoiceExplicitTransaction(t *testing.T) {
	fixture := newSeedFixture(t, seedCaseStatements()...)
	ids, pending := seedCaseIDs()
	ctx := context.Background()
	_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
	// A write in the transaction: the seek merges it.
	_, err = fixture.exec.Execute(ctx, "CREATE (:Case {id: 'tx-1', status: 'pending'})", nil)
	require.NoError(t, err)
	for _, statement := range []string{
		"UNWIND $ids AS id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id",
		"UNWIND $ids AS id OPTIONAL MATCH (c:Case {id: id}) WHERE c.status = 'pending' WITH c WHERE c IS NOT NULL RETURN c.id",
	} {
		result, err := fixture.exec.Execute(ctx, statement, map[string]interface{}{"ids": append(append([]interface{}{}, ids...), "tx-1")})
		require.NoError(t, err, statement)
		require.Equal(t, append(append([]string{}, pending...), "tx-1"), seedRowKeys(result.Rows), statement)
		trace := fixture.exec.LastHotPathTrace()
		require.True(t, trace.MergeSchemaLookupUsed, "%s: the inline id index must seed the MATCH", statement)
		require.False(t, trace.MergeScanFallbackUsed, statement)
	}
}

// A unique index wins over a non-unique one, and of two non-unique indexes
// the one listing fewer nodes for the value wins, wherever each predicate is
// written. Rows are the same whichever is read.
func TestNodeSeedChoiceRanksBySelectivity(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE CONSTRAINT seed_item_key FOR (n:Item) REQUIRE n.key IS UNIQUE",
		"CREATE INDEX seed_item_kind FOR (n:Item) ON (n.kind)",
		"CREATE INDEX seed_item_bucket FOR (n:Item) ON (n.bucket)",
		"UNWIND range(0, 2999) AS i CREATE (:Item {key: 'k-' + toString(i), kind: CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END, bucket: i % 300})",
	)
	for _, form := range []struct {
		name, query string
		rows        []string
		maxReads    int64
	}{
		{"unique_inline_over_where", "MATCH (n:Item {key: 'k-42'}) WHERE n.kind = 'even' RETURN n.key", []string{"k-42"}, 2},
		{"unique_where_over_inline", "MATCH (n:Item {kind: 'even'}) WHERE n.key = 'k-42' RETURN n.key", []string{"k-42"}, 2},
		{"unique_where_conjuncts", "MATCH (n:Item) WHERE n.kind = 'even' AND n.key = 'k-42' RETURN n.key", []string{"k-42"}, 2},
		{"selective_inline_over_where", "MATCH (n:Item {bucket: 42}) WHERE n.kind = 'even' RETURN count(n)", []string{"10"}, 12},
		{"selective_where_over_inline", "MATCH (n:Item {kind: 'even'}) WHERE n.bucket = 42 RETURN count(n)", []string{"10"}, 12},
		{"selective_where_conjuncts", "MATCH (n:Item) WHERE n.kind = 'even' AND n.bucket = 42 RETURN count(n)", []string{"10"}, 12},
		{"selective_in_over_where", "MATCH (n:Item) WHERE n.kind = 'even' AND n.bucket IN [42, 44] RETURN count(n)", []string{"20"}, 22},
		{"unique_absent", "MATCH (n:Item {key: 'none'}) WHERE n.kind = 'even' RETURN n.key", []string{}, 1},
	} {
		t.Run(form.name, func(t *testing.T) {
			rows, read := fixture.query(t, form.query, nil)
			require.Equal(t, form.rows, rows)
			require.LessOrEqual(t, read, form.maxReads)
		})
	}
}

// E1/E2: an inline property on a multi-label pattern is served by an index on
// any pattern label, not only the first written one, on every route that
// seeds a node pattern. Each form returns the same rows as the form that
// writes the indexed label first.
func TestNodeSeedChoiceUsesIndexOnAnyPatternLabel(t *testing.T) {
	fixture := newSeedFixture(t, seedMultiLabelStatements()...)
	survivors := []interface{}{"s1", "s9", "s17", "s_none"}
	params := map[string]interface{}{"ids": survivors, "x": "s9", "none": "s_none"}
	for _, form := range []struct {
		name, slow, swapped string
	}{
		{"unwind", "UNWIND $ids AS id MATCH (p:G:Id {survivor: id}) WHERE p.status IS NOT NULL RETURN p.id", "UNWIND $ids AS id MATCH (p:Id:G {survivor: id}) WHERE p.status IS NOT NULL RETURN p.id"},
		{"param", "MATCH (p:G:Id {survivor: $x}) RETURN p.id", "MATCH (p:Id:G {survivor: $x}) RETURN p.id"},
		{"optional", "UNWIND $ids AS id OPTIONAL MATCH (p:G:Id {survivor: id}) RETURN id, p.id", "UNWIND $ids AS id OPTIONAL MATCH (p:Id:G {survivor: id}) RETURN id, p.id"},
		{"traversal_start", "MATCH (p:G:Id {survivor: $x})<-[b:B]-(s:G) RETURN p.id, s.id", "MATCH (p:Id:G {survivor: $x})<-[b:B]-(s:G) RETURN p.id, s.id"},
		{"traversal_start_absent", "MATCH (p:G:Id {survivor: $none})<-[b:B]-(s:G) RETURN p.id, s.id", "MATCH (p:Id:G {survivor: $none})<-[b:B]-(s:G) RETURN p.id, s.id"},
		{"pattern_subquery", "UNWIND $ids AS id RETURN id, COUNT { (p:G:Id {survivor: id}) } AS n", "UNWIND $ids AS id RETURN id, COUNT { (p:Id:G {survivor: id}) } AS n"},
		{"count", "MATCH (p:G:Id {survivor: $x}) RETURN count(p)", "MATCH (p:Id:G {survivor: $x}) RETURN count(p)"},
	} {
		t.Run(form.name, func(t *testing.T) {
			want, _ := fixture.query(t, form.swapped, params)
			rows, read := fixture.query(t, form.slow, params)
			require.Equal(t, want, rows)
			require.LessOrEqual(t, read, int64(40), "nodes read: the Id(survivor) index, not a scan of every G node")
			// A scan that storage filters hands the counter only the matches;
			// the trace records whether the pattern's label was scanned.
			trace := fixture.exec.LastHotPathTrace()
			require.True(t, trace.MergeSchemaLookupUsed, "the Id(survivor) index must seed the pattern")
			require.False(t, trace.MergeScanFallbackUsed, "the first label must not be scanned")
		})
	}
}

// A multi-label pattern without properties scans the label with the fewest
// nodes, whichever is written first, and checks the other label on each node:
// plain, rows, traversal start and a traversal under LIMIT. Rows are the same
// either way.
func TestNodeSeedChoiceScansSmallestLabel(t *testing.T) {
	fixture := newSeedFixture(t, seedLabelOnlyStatements()...)
	for _, form := range []struct {
		name, slow, swapped string
		maxReads            int64
	}{
		{"count", "MATCH (n:G:Id) RETURN count(n)", "MATCH (n:Id:G) RETURN count(n)", 120},
		{"rows", "MATCH (n:G:Id) RETURN n.id", "MATCH (n:Id:G) RETURN n.id", 120},
		{"traversal_count", "MATCH (n:G:Id)-[r:L]->(x) RETURN count(r)", "MATCH (n:Id:G)-[r:L]->(x) RETURN count(r)", 240},
		{"traversal_rows", "MATCH (n:G:Id)-[r:L]->(x) RETURN n.id, x.id", "MATCH (n:Id:G)-[r:L]->(x) RETURN n.id, x.id", 240},
		{"traversal_limit", "MATCH (n:G:Id)-[r:L]->(x) RETURN x.id LIMIT 1000", "MATCH (n:Id:G)-[r:L]->(x) RETURN x.id LIMIT 1000", 240},
	} {
		t.Run(form.name, func(t *testing.T) {
			want, _ := fixture.query(t, form.swapped, nil)
			rows, read := fixture.query(t, form.slow, nil)
			require.Equal(t, want, rows)
			require.NotEmpty(t, rows)
			require.LessOrEqual(t, read, form.maxReads, "nodes read: the 60 Id nodes, not the 3,000 G nodes")
		})
	}
}

// Inside an explicit transaction, which reads through the transaction rather
// than the counted store, the label-only forms return the same rows and
// allocate for the small label's nodes, not for the 30,000 large-label nodes.
func TestNodeSeedChoiceScansSmallestLabelInTransaction(t *testing.T) {
	fixture := newSeedFixture(t,
		"UNWIND range(0, 29999) AS i CREATE (n:G {id: i, text: 'payload ' + toString(i)}) WITH n, i WHERE i % 500 = 0 SET n:Id CREATE (n)-[:L]->(:Out {id: i})",
	)
	ctx := context.Background()
	_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
	_, err = fixture.exec.Execute(ctx, "CREATE (:G:Id {id: -1})", nil)
	require.NoError(t, err)
	for _, pair := range [][2]string{
		{"MATCH (n:G:Id) RETURN count(n)", "MATCH (n:Id:G) RETURN count(n)"},
		{"MATCH (n:G:Id) RETURN n.id", "MATCH (n:Id:G) RETURN n.id"},
		{"MATCH (n:G:Id)-[r:L]->(x) RETURN n.id, x.id", "MATCH (n:Id:G)-[r:L]->(x) RETURN n.id, x.id"},
	} {
		swapped, err := fixture.exec.Execute(ctx, pair[1], nil)
		require.NoError(t, err)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		slow, err := fixture.exec.Execute(ctx, pair[0], nil)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		require.Equal(t, seedRowKeys(swapped.Rows), seedRowKeys(slow.Rows), pair[0])
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20), "%s: allocated %d bytes; the 61 Id nodes must seed the scan", pair[0], after.TotalAlloc-before.TotalAlloc)
	}
	count, err := fixture.exec.Execute(ctx, "MATCH (n:G:Id) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"61"}, seedRowKeys(count.Rows))
}

// The plans that do not compete keep their rules and rows: IN lists, OR,
// ranges, ORDER BY + LIMIT windows, composite indexes, and no index at all.
func TestNodeSeedChoiceKeepsOtherPlans(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX seed_doc_n FOR (n:Doc) ON (n.n)",
		"CREATE INDEX seed_doc_tag FOR (n:Doc) ON (n.tag)",
		"CREATE INDEX seed_doc_pair FOR (n:Doc) ON (n.a, n.b)",
		"UNWIND range(0, 999) AS i CREATE (:Doc {n: i, tag: 't' + toString(i % 10), a: i % 7, b: i % 11, free: i % 13})",
	)
	for _, form := range []struct {
		name, query string
		rows        []string
		maxReads    int64
	}{
		{"in_list", "MATCH (d:Doc) WHERE d.n IN [3, 4, 5] RETURN d.n", []string{"3", "4", "5"}, 3},
		{"in_param", "MATCH (d:Doc) WHERE d.n IN $ns RETURN d.n", []string{"7", "8"}, 2},
		{"or_equality", "MATCH (d:Doc) WHERE d.n = 1 OR d.n = 2 RETURN d.n", []string{"1", "2"}, 2},
		{"range", "MATCH (d:Doc) WHERE d.n >= 997 RETURN d.n", []string{"997", "998", "999"}, 3},
		{"inline_with_range", "MATCH (d:Doc {n: 5}) WHERE d.free > 0 RETURN d.n", []string{"5"}, 1},
		{"window", "MATCH (d:Doc) WHERE d.tag = 't1' RETURN d.n ORDER BY d.n DESC LIMIT 2", []string{"981", "991"}, 100},
		{"composite", "MATCH (d:Doc {a: 3, b: 5}) RETURN count(d)", []string{"13"}, 13},
		{"composite_with_where", "MATCH (d:Doc {a: 3, b: 5}) WHERE d.tag = 't1' RETURN count(d)", []string{"1"}, 13},
		{"no_index", "MATCH (d:Doc) WHERE d.free = 4 RETURN count(d)", []string{"77"}, 1000},
	} {
		t.Run(form.name, func(t *testing.T) {
			rows, read := fixture.query(t, form.query, map[string]interface{}{"ns": []interface{}{int64(7), int64(8)}})
			require.Equal(t, form.rows, rows)
			require.LessOrEqual(t, read, form.maxReads)
		})
	}
}

// The statement routes that chose a node pattern's seed outside the one
// access decision now go through it: a relationship pattern whose start node
// has a WHERE, CALL and pattern subqueries, and the OR-of-IN-lists read.
// Each reads the inline id index, not the 2,000 pending nodes, with the
// expected rows.
func TestNodeSeedChoiceRoutesShareOneDecision(t *testing.T) {
	ids, pending := seedCaseIDs()
	fixture := newSeedFixture(t, seedCaseStatements()...)
	// Each looked-up case has one NEXT step.
	_, err := fixture.exec.Execute(context.Background(), "MATCH (c:Case) WHERE c.id IN $ids CREATE (c)-[:NEXT]->(:Step {of: c.id})", map[string]interface{}{"ids": ids})
	require.NoError(t, err)
	params := map[string]interface{}{"ids": ids, "statuses": []interface{}{"pending"}, "more": []interface{}{"c-2"}}
	for _, form := range []struct {
		name, query string
		perID       bool
		rows        []string
		maxReads    int64
	}{
		{"traversal_start", "MATCH (c:Case {id: $id})-[:NEXT]->(s:Step) WHERE c.status = 'pending' RETURN s.of", true, pending, 6},
		{"traversal_start_unwind", "UNWIND $ids AS id MATCH (c:Case {id: id})-[:NEXT]->(s:Step) WHERE c.status = 'pending' RETURN s.of", false, pending, int64(6 * len(ids))},
		{"call_subquery", "UNWIND $ids AS id CALL { WITH id MATCH (c:Case {id: id}) WHERE c.status = 'pending' RETURN c.id AS cid } RETURN cid", false, pending, int64(3 * len(ids))},
		{"exists_subquery", "UNWIND $ids AS id WITH id WHERE EXISTS { MATCH (c:Case {id: id}) WHERE c.status = 'pending' } RETURN id", false, pending, int64(3 * len(ids))},
		{"or_in", "MATCH (c:Case {id: $id}) WHERE c.status IN $statuses OR c.id IN $more RETURN c.id", true, pending, 3},
	} {
		t.Run(form.name, func(t *testing.T) {
			var rows []string
			total := int64(0)
			if form.perID {
				for _, id := range ids {
					run := map[string]interface{}{"id": id}
					for k, v := range params {
						run[k] = v
					}
					got, read := fixture.query(t, form.query, run)
					rows = append(rows, got...)
					total += read
				}
				sort.Strings(rows)
				require.Equal(t, form.rows, rows)
				require.LessOrEqual(t, total, form.maxReads*int64(len(ids)), "nodes read: the inline id index per id, not the pending nodes")
				return
			}
			rows, total = fixture.query(t, form.query, params)
			require.Equal(t, form.rows, rows)
			require.LessOrEqual(t, total, form.maxReads, "nodes read: the inline id index per id, not the pending nodes")
		})
	}
}

// An access decision that finds no candidate is final: a relationship
// pattern whose start node's indexed WHERE lists no node reads no node,
// rather than scanning the start label, and so does an empty IN list.
func TestNodeSeedChoiceEmptySeekIsFinal(t *testing.T) {
	ids, _ := seedCaseIDs()
	fixture := newSeedFixture(t, seedCaseStatements()...)
	_, err := fixture.exec.Execute(context.Background(), "MATCH (c:Case) WHERE c.id IN $ids CREATE (c)-[:NEXT]->(:Step {of: c.id})", map[string]interface{}{"ids": ids})
	require.NoError(t, err)
	params := map[string]interface{}{"absent": []interface{}{"x-none", "y-none"}, "one": "x-none", "empty": []interface{}{}}
	for _, form := range []struct{ name, query string }{
		{"traversal_in_absent", "MATCH (c:Case)-[:NEXT]->(s:Step) WHERE c.id IN $absent RETURN s.of"},
		{"traversal_equality_absent", "MATCH (c:Case)-[:NEXT]->(s:Step) WHERE c.id = $one RETURN s.of"},
		{"traversal_in_empty", "MATCH (c:Case)-[:NEXT]->(s:Step) WHERE c.id IN $empty RETURN s.of"},
		{"traversal_in_empty_literal", "MATCH (c:Case)-[:NEXT]->(s:Step) WHERE c.id IN [] RETURN s.of"},
		{"traversal_count_absent", "MATCH (c:Case)-[:NEXT]->(s:Step) WHERE c.id IN $absent RETURN count(s)"},
		{"node_in_empty", "MATCH (c:Case) WHERE c.id IN $empty RETURN c.id"},
		{"node_in_absent", "MATCH (c:Case) WHERE c.id IN $absent RETURN c.id"},
	} {
		t.Run(form.name, func(t *testing.T) {
			rows, read := fixture.query(t, form.query, params)
			if form.name == "traversal_count_absent" {
				require.Equal(t, []string{"0"}, rows)
			} else {
				require.Empty(t, rows)
			}
			require.Zero(t, read, "nodes read: the index lists none, so nothing is scanned")
		})
	}
}

// An OR is a candidate source only when an index covers every branch: with
// only P(k) indexed, n.k = 1 OR n.u = 2 must still return the nodes that
// match only n.u = 2, on every statement route that reads a node pattern.
// Expected rows are Neo4j 5.26's.
func TestNodeSeedChoiceOrCoversEveryDisjunct(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX or_p_id FOR (n:P) ON (n.id)",
		"CREATE INDEX or_p_k FOR (n:P) ON (n.k)",
		"CREATE INDEX or_p_v FOR (n:P) ON (n.v)",
		"CREATE (:P {id: 'a', k: 1, u: 0, v: 0})-[:R]->(:Out {id: 'oa'}), (:P {id: 'b', k: 0, u: 2, v: 2})-[:R]->(:Out {id: 'ob'}), (:P {id: 'c', k: 0, u: 0, v: 0})-[:R]->(:Out {id: 'oc'}), (:P {id: 'd', k: 1, u: 2, v: 0})-[:R]->(:Out {id: 'od'})",
	)
	params := map[string]interface{}{"ks": []interface{}{int64(1)}, "us": []interface{}{int64(2)}, "vs": []interface{}{int64(2)}}
	routes := []struct{ name, prefix, suffix string }{
		{"match", "MATCH (n:P) WHERE ", " RETURN n.id"},
		{"match_inline", "MATCH (n:P {id: 'b'}) WHERE ", " RETURN n.id"},
		{"traversal_start", "MATCH (n:P)-[:R]->(m:Out) WHERE ", " RETURN n.id"},
		{"call_subquery", "CALL { MATCH (n:P) WHERE ", " RETURN n.id AS id } RETURN id"},
	}
	for _, predicate := range []struct {
		name, where  string
		rows, inline []string
	}{
		{"unindexed_right", "n.k = 1 OR n.u = 2", []string{"a", "b", "d"}, []string{"b"}},
		{"unindexed_left", "n.u = 2 OR n.k = 1", []string{"a", "b", "d"}, []string{"b"}},
		{"zero_hit_indexed", "n.k = 99 OR n.u = 2", []string{"b", "d"}, []string{"b"}},
		{"both_indexed", "n.k = 1 OR n.v = 2", []string{"a", "b", "d"}, []string{"b"}},
		{"in_unindexed_right", "n.k IN $ks OR n.u IN $us", []string{"a", "b", "d"}, []string{"b"}},
		{"in_both_indexed", "n.k IN $ks OR n.v IN $vs", []string{"a", "b", "d"}, []string{"b"}},
	} {
		for _, route := range routes {
			t.Run(predicate.name+"/"+route.name, func(t *testing.T) {
				rows, _ := fixture.query(t, route.prefix+predicate.where+route.suffix, params)
				want := predicate.rows
				if route.name == "match_inline" {
					want = predicate.inline
				}
				require.Equal(t, want, rows)
			})
		}
	}
}

// Review r1 of the change: the public matrix of node-pattern access shapes,
// with Neo4j 5.26's rows (reference run on the reviewer's dataset), including
// an OR whose unindexed branch must not let an incomplete index answer
// suppress a match on MATCH, OPTIONAL MATCH and a traversal start.
func TestNodeSeedChoiceReviewMatrix(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_id FOR (n:R1771) ON (n.id)",
		"CREATE INDEX r1771_k FOR (n:R1771) ON (n.k)",
		"CREATE INDEX r1771_small FOR (n:R1771Small) ON (n.id)",
		"CREATE (a:R1771:R1771Small {id:'a', k:1, u:0}), (b:R1771:R1771Small {id:'b', k:0, u:2}), (c:R1771 {id:'c', k:1, u:2}), (d:R1771 {id:'d', u:3}), (a)-[:R1771NEXT]->(:R1771Step {of:'a'}), (b)-[:R1771NEXT]->(:R1771Step {of:'b'}), (c)-[:R1771NEXT]->(:R1771Step {of:'c'})",
	)
	params := map[string]interface{}{"one": int64(1), "nil": nil, "empty": []interface{}{}, "ks": []interface{}{int64(1), nil}, "ids": []interface{}{"b"}}
	for _, c := range []struct {
		name, query string
		want        []string
	}{
		{"inline_where", "MATCH (n:R1771 {id:'a'}) WHERE n.k=1 RETURN n.id", []string{"a"}},
		{"multilabel_inline", "MATCH (n:R1771:R1771Small {id:'b'}) RETURN n.id", []string{"b"}},
		{"multilabel_where", "MATCH (n:R1771:R1771Small) WHERE n.id='b' RETURN n.id", []string{"b"}},
		{"multilabel_scan", "MATCH (n:R1771:R1771Small) RETURN n.id", []string{"a", "b"}},
		{"or_both_indexed", "MATCH (n:R1771) WHERE n.k=1 OR n.id='b' RETURN n.id", []string{"a", "b", "c"}},
		{"or_unindexed", "MATCH (n:R1771) WHERE n.k=1 OR n.u=2 RETURN n.id", []string{"a", "b", "c"}},
		{"or_inline_unindexed", "MATCH (n:R1771 {id:'b'}) WHERE n.k=99 OR n.u=2 RETURN n.id", []string{"b"}},
		{"or_in", "MATCH (n:R1771 {id:'a'}) WHERE n.k IN $ks OR n.id IN $ids RETURN n.id", []string{"a"}},
		{"or_in_missing_index", "MATCH (n:R1771) WHERE n.k IN $ks OR n.u IN [2] RETURN n.id", []string{"a", "b", "c"}},
		{"and_or", "MATCH (n:R1771) WHERE (n.k=1 OR n.u=2) AND n.id='b' RETURN n.id", []string{"b"}},
		{"in_null", "MATCH (n:R1771) WHERE n.k IN $ks RETURN n.id", []string{"a", "c"}},
		{"in_empty", "MATCH (n:R1771) WHERE n.k IN $empty RETURN n.id", []string{}},
		{"absent", "MATCH (n:R1771 {id:'absent'}) WHERE n.k=1 RETURN n.id", []string{}},
		{"equals_null", "MATCH (n:R1771) WHERE n.k=$nil RETURN n.id", []string{}},
		{"inline_null", "MATCH (n:R1771 {k:$nil}) RETURN n.id", []string{}},
		{"in_null_param", "MATCH (n:R1771) WHERE n.k IN $nil RETURN n.id", []string{}},
		{"optional_empty", "OPTIONAL MATCH (n:R1771 {id:'absent'}) WHERE n.k=1 RETURN n.id", []string{"<nil>"}},
		{"optional_or", "OPTIONAL MATCH (n:R1771 {id:'b'}) WHERE n.k=99 OR n.u=2 RETURN n.id", []string{"b"}},
		{"traversal_inline", "MATCH (n:R1771 {id:'a'})-[:R1771NEXT]->(s) WHERE n.k=1 RETURN s.of", []string{"a"}},
		{"traversal_or_unindexed", "MATCH (n:R1771)-[:R1771NEXT]->(s) WHERE n.k=1 OR n.u=2 RETURN s.of", []string{"a", "b", "c"}},
		{"traversal_or_inline", "MATCH (n:R1771 {id:'b'})-[:R1771NEXT]->(s) WHERE n.k=99 OR n.u=2 RETURN s.of", []string{"b"}},
		{"traversal_or_both", "MATCH (n:R1771 {id:'a'})-[:R1771NEXT]->(s) WHERE n.k=1 OR n.id='b' RETURN s.of", []string{"a"}},
		{"traversal_empty", "MATCH (n:R1771)-[:R1771NEXT]->(s) WHERE n.k IN $empty RETURN s.of", []string{}},
		{"call", "MATCH (n:R1771 {id:'a'}) WHERE n.k=1 CALL { WITH n RETURN n.id AS x } RETURN x", []string{"a"}},
		{"count_subquery", "MATCH (n:R1771 {id:'a'}) RETURN COUNT { MATCH (n)-[:R1771NEXT]->(s) }", []string{"1"}},
		{"range", "MATCH (n:R1771) WHERE n.k>=0 AND n.k<2 RETURN n.id", []string{"a", "b", "c"}},
		{"nonnull", "MATCH (n:R1771) WHERE n.k IS NOT NULL RETURN n.id", []string{"a", "b", "c"}},
		{"ordered", "MATCH (n:R1771) WHERE n.k IS NOT NULL RETURN n.id ORDER BY n.k,n.id LIMIT 2", []string{"a", "b"}},
		{"merge", "MERGE (n:R1771:R1771Small {id:'b'}) RETURN n.id", []string{"b"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rows, _ := fixture.query(t, c.query, params)
			require.Equal(t, c.want, rows)
		})
	}
}

// A WHERE equality, IN or OR on a multi-label pattern reads the smallest
// pattern label's posting, as the inline form does, not the union of every
// indexed label's.
func TestNodeSeedChoiceMultiLabelWhereReadsSmallestLabel(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_big_k FOR (n:R1771Big) ON (n.k)",
		"CREATE INDEX r1771_small_k FOR (n:R1771Tiny) ON (n.k)",
		"UNWIND range(1,1000) AS i CREATE (:R1771Big {k:1, id:i})",
		"CREATE (:R1771Big:R1771Tiny {k:1,id:1001}), (:R1771Big:R1771Tiny {k:1,id:1002})",
	)
	for _, query := range []string{
		"MATCH (n:R1771Big:R1771Tiny) WHERE n.k=1 RETURN n.id",
		"MATCH (n:R1771Big:R1771Tiny {k:1}) RETURN n.id",
		"MATCH (n:R1771Big:R1771Tiny) WHERE n.k IN [1] RETURN n.id",
		"MATCH (n:R1771Big:R1771Tiny) WHERE n.k = 1 OR n.k = 2 RETURN n.id",
		"MATCH (n:R1771Tiny:R1771Big) WHERE n.k=1 RETURN n.id",
	} {
		t.Run(query, func(t *testing.T) {
			rows, read := fixture.query(t, query, nil)
			require.Equal(t, []string{"1001", "1002"}, rows)
			require.LessOrEqual(t, read, int64(2), "nodes read: the Tiny index's 2 entries")
		})
	}
}

// Inside an explicit transaction the estimates count the transaction's own
// pending writes, as its lookups merge them: after 101 in-transaction writes
// of the WHERE value, the unique inline id still seeds the MATCH.
func TestNodeSeedChoiceTransactionEstimate(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_txc_id FOR (n:R1771TxnCost) ON (n.id)",
		"CREATE INDEX r1771_txc_k FOR (n:R1771TxnCost) ON (n.k)",
		"CREATE (:R1771TxnCost {id:'target', k:'old'})",
	)
	ctx := context.Background()
	for _, statement := range []string{
		"BEGIN",
		"MATCH (n:R1771TxnCost {id:'target'}) SET n.k='hot'",
		"UNWIND range(1,100) AS i CREATE (:R1771TxnCost {id:toString(i),k:'hot'})",
	} {
		_, err := fixture.exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
	result, err := fixture.exec.Execute(ctx, "MATCH (n:R1771TxnCost {id:'target'}) WHERE n.k='hot' RETURN n.id", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"target"}, seedRowKeys(result.Rows))
	require.True(t, fixture.exec.LastHotPathTrace().MergeSchemaLookupUsed, "the inline id must seed the MATCH")

	wrapper := fixture.exec.txContext.storageWrapper
	require.NotNil(t, wrapper)
	txe := NewStorageExecutor(wrapper, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	np := nodePatternInfo{variable: "n", labels: []string{"R1771TxnCost"}, properties: map[string]interface{}{"id": "target"}}
	where, ok, err := txe.planPropertyIndexEquality(ctx, np, "n.k='hot'", nil)
	require.NoError(t, err)
	require.True(t, ok)
	estimate, err := where.estimate(ctx, unknownIndexSeekEstimate)
	require.NoError(t, err)
	candidates, used, err := txe.collectNodeIndexSeek(ctx, &where)
	require.NoError(t, err)
	require.True(t, used)
	require.GreaterOrEqual(t, estimate, int64(len(candidates)), "the estimate is an upper bound of the transaction's lookup")
	inline, ok, err := txe.planPatternPropertySeek(ctx, np, wrapper)
	require.NoError(t, err)
	require.True(t, ok)
	var seeks nodeIndexSeekSet
	seeks.add(where)
	seeks.add(inline)
	chosen, err := bestNodeIndexSeek(ctx, &seeks)
	require.NoError(t, err)
	require.Equal(t, 1, chosen, "the unique inline id beats 101 pending WHERE values")
}

// The UNWIND batch prefetch (multi-MATCH CREATE, relationship MERGE) resolves
// each lookup value with the release's own lookup and keeps the release's
// rule for an index miss: the index is an accelerator, never proof that a
// node is absent. A key whose index entry is missing still resolves, by the
// batch's one label scan, and a key no node holds matches nothing.
func TestNodeSeedChoiceBatchVerifiesIndexMiss(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_batch_id FOR (n:R1771Batch) ON (n.id)",
		"UNWIND range(1,1000) AS i CREATE (:R1771Batch {id:i})",
	)
	ctx := context.Background()
	schema := fixture.exec.storage.GetSchema()
	ids, err := schema.PropertyIndexLookup(ctx, "R1771Batch", "id", int64(8))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.NoError(t, schema.PropertyIndexDelete("R1771Batch", "id", ids[0], int64(8)))

	create := "UNWIND $rows AS row MATCH (a:R1771Batch {id:row.id}) MATCH (b:R1771Batch {id:row.id+1}) CREATE (a)-[:R1771_BATCH_REL]->(b) RETURN count(*) AS created"
	rows, _ := fixture.query(t, create, map[string]interface{}{"rows": []interface{}{
		map[string]interface{}{"id": int64(7)}, map[string]interface{}{"id": int64(10001)}, map[string]interface{}{"id": int64(999)},
	}})
	require.Equal(t, []string{"2"}, rows, "7->8 resolves although 8's index entry is missing; 10001 matches nothing")
	require.True(t, fixture.exec.LastHotPathTrace().UnwindMultiMatchCreateBatch)

	// The prefetch, which the relationship-MERGE batch shares, verifies a miss
	// with one scan of the label, however many keys missed. (The release's
	// TestUnwindRelationshipMergeBatch_IncompleteIndexedMatchBucketKeepsFastPath
	// covers that batch's statement.)
	scanned, unique := seedBatchPrefetchScans(t, fixture, "R1771Batch", "id", int64(10001))
	require.True(t, unique)
	require.Equal(t, 1000, scanned, "one label scan verifies the miss")
}

// MERGE keeps the release's rule for a composite index that lists nothing:
// it is verified against the pattern's single-property index before MERGE
// concludes the node is absent. A composite index registered after the nodes
// exist lists none of them; MERGE still finds the node, through the
// translationId index, without a scan and without creating a duplicate, also
// when that index lists several nodes for the value.
func TestNodeSeedChoiceMergeVerifiesCompositeMiss(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE (:R1771Translated {translationId: 'src-1', language: 'es', text: 'hola'}), (:R1771Translated {translationId: 'src-1', language: 'fr', text: 'salut'}), (:R1771Translated {translationId: 'src-1', language: 'de', text: 'hallo'})",
		"CREATE INDEX r1771_tt_id FOR (n:R1771Translated) ON (n.translationId)",
	)
	require.NoError(t, fixture.exec.storage.GetSchema().AddCompositeIndex("r1771_tt_id_lang", "R1771Translated", []string{"translationId", "language"}))
	for _, statement := range []string{
		"MERGE (t:R1771Translated {translationId: 'src-1', language: 'es'}) ON MATCH SET t.text = 'hola-2' RETURN t.text",
		"UNWIND ['es'] AS lang MERGE (t:R1771Translated {translationId: 'src-1', language: lang}) ON MATCH SET t.text = 'hola-2' RETURN t.text",
	} {
		result, err := fixture.exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, []string{"hola-2"}, seedRowKeys(result.Rows), statement)
		require.False(t, fixture.exec.LastHotPathTrace().MergeScanFallbackUsed, statement)
	}
	rows, _ := fixture.query(t, "MATCH (t:R1771Translated) RETURN count(t)", nil)
	require.Equal(t, []string{"3"}, rows, "MERGE must not create a duplicate")
}

// MERGE keeps the release's lookup, which reads the first written label's
// indexes only. When that label has none, MERGE scans a label, as on the
// release, but the pattern label with the fewest nodes, not the first
// written one: the 2 Tiny nodes, not the 1,000 Big ones. Rows are unchanged
// and no duplicate is created.
func TestNodeSeedChoiceMergeScansSmallestLabel(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX merge_tiny_id FOR (n:Tiny) ON (n.id)",
		"UNWIND range(1, 1000) AS i CREATE (:Big {id: 'b' + toString(i)})",
		"CREATE (:Big:Tiny {id: 't1'}), (:Big:Tiny {id: 't2'}), (:Seed {ref: 't1'}), (:Seed {ref: 't2'})",
	)
	for _, form := range []struct {
		name, query string
		rows        []string
	}{
		{"merge", "MERGE (n:Big:Tiny {id: 't1'}) RETURN n.id", []string{"t1"}},
		{"merge_unwind", "UNWIND ['t1', 't2'] AS id MERGE (n:Big:Tiny {id: id}) RETURN n.id", []string{"t1", "t2"}},
		{"merge_after_match", "MATCH (s:Seed) MERGE (n:Big:Tiny {id: s.ref}) RETURN n.id", []string{"t1", "t2"}},
		{"merge_on_match", "MERGE (n:Big:Tiny {id: 't2'}) ON MATCH SET n.seen = true RETURN n.id", []string{"t2"}},
	} {
		t.Run(form.name, func(t *testing.T) {
			rows, read := fixture.query(t, form.query, nil)
			t.Logf("%s: rows=%v reads=%d", form.name, rows, read)
			require.Equal(t, form.rows, rows)
			require.Less(t, read, int64(50), "nodes read: the Tiny label, not the 1,000 Big nodes")
		})
	}
	rows, _ := fixture.query(t, "MATCH (n:Big:Tiny) RETURN count(n)", nil)
	require.Equal(t, []string{"2"}, rows, "MERGE must not create duplicates")
}

// seedDropIndexEntry removes one node's entry from label's property index
// under value and leaves the node itself as it is: an index that omits a
// node, as one added after its nodes or one missing an entry does. It is the
// fixture of the release's own incomplete-index guards.
func seedDropIndexEntry(t *testing.T, fixture *seedFixture, label, property string, value interface{}) {
	t.Helper()
	schema := fixture.exec.storage.GetSchema()
	ids, err := schema.PropertyIndexLookup(context.Background(), label, property, value)
	require.NoError(t, err)
	require.NotEmpty(t, ids, "%s(%s) lists no node for %v", label, property, value)
	require.NoError(t, schema.PropertyIndexDelete(label, property, ids[0], value))
}

// MERGE finds, in every state of an index answer, the nodes the release finds
// (reviews r3 and r4). Its lookup is the release's: only the first written
// label's own indexes can end the search, and an index on another label is
// not read. So an index on a later label that omits a node, lists one node of
// two, or lists only a decoy the pattern's other label or properties reject,
// neither hides an existing node nor makes MERGE create a duplicate. The
// expected rows and node counts are the release head's, measured cell by
// cell (the answer-state matrix).
func TestNodeSeedChoiceMergeMatchesReleaseInEveryIndexState(t *testing.T) {
	const later = "R1771Late"
	realNode := "CREATE (:R1771First:R1771Late {k: 'hit', id: 'real', u: 1})"
	second := "CREATE (:R1771First:R1771Late {k: 'hit', id: 'real2', u: 1})"
	for _, index := range []string{"later_label_only", "both_labels"} {
		ddl := []string{"CREATE INDEX r1771_state_late FOR (n:R1771Late) ON (n.k)"}
		if index == "both_labels" {
			ddl = append(ddl, "CREATE INDEX r1771_state_first FOR (n:R1771First) ON (n.k)")
		}
		for _, state := range []struct {
			name    string
			setup   []string
			pattern string
			rows    []string
			nodes   string
		}{
			{"complete", []string{realNode}, "{k: 'hit'}", []string{"real"}, "1"},
			{"absent", nil, "{k: 'hit'}", []string{"<nil>"}, "1"},
			{"entry_missing", []string{realNode}, "{k: 'hit'}", []string{"real"}, "1"},
			{"one_of_two_listed", []string{realNode, second}, "{k: 'hit'}", []string{"real", "real2"}, "2"},
			{"decoy_without_the_first_label", []string{realNode, "CREATE (:R1771Late {k: 'hit', id: 'decoy'})"}, "{k: 'hit'}", []string{"real"}, "1"},
			{"decoy_with_another_property", []string{realNode, "CREATE (:R1771First:R1771Late {k: 'hit', id: 'decoy', u: 2})"}, "{k: 'hit', u: 1}", []string{"real"}, "2"},
		} {
			for _, form := range []string{
				"MERGE (n:R1771First:R1771Late %s) RETURN n.id",
				"MERGE (n:R1771First:R1771Late %s) ON MATCH SET n.seen = true RETURN n.id",
				"UNWIND [1] AS one MERGE (n:R1771First:R1771Late %s) RETURN n.id",
			} {
				statement := fmt.Sprintf(form, state.pattern)
				t.Run(index+"/"+state.name+"/"+statement, func(t *testing.T) {
					fixture := newSeedFixture(t, append(append([]string{}, ddl...), state.setup...)...)
					if state.name != "complete" && state.name != "absent" {
						seedDropNodeIndexEntry(t, fixture, later, "k", "hit", "real")
					}
					result, err := fixture.exec.Execute(context.Background(), statement, nil)
					require.NoError(t, err)
					got := make([]string, 0, len(result.Rows))
					for _, row := range result.Rows {
						got = append(got, fmt.Sprint(row[0]))
					}
					sort.Strings(got)
					require.Equal(t, state.rows, got, "the rows the release returns")
					rows, _ := fixture.query(t, "MATCH (n:R1771First:R1771Late) RETURN count(n)", nil)
					require.Equal(t, []string{state.nodes}, rows, "the nodes the release leaves")
				})
			}
		}
	}
}

// seedDropNodeIndexEntry is seedDropIndexEntry for the entry of the node
// whose id property is id.
func seedDropNodeIndexEntry(t *testing.T, fixture *seedFixture, label, property string, value interface{}, id string) {
	t.Helper()
	ctx := context.Background()
	schema := fixture.exec.storage.GetSchema()
	ids, err := schema.PropertyIndexLookup(ctx, label, property, value)
	require.NoError(t, err)
	for _, nodeID := range ids {
		node, err := fixture.exec.storage.GetNode(ctx, nodeID)
		if err != nil || node == nil || fmt.Sprint(node.Properties["id"]) != id {
			continue
		}
		require.NoError(t, schema.PropertyIndexDelete(label, property, nodeID, value))
		return
	}
	t.Fatalf("%s(%s) lists no entry for node %s under %v", label, property, id, value)
}

// The UNWIND batch routes use an index as an accelerator, never as proof of
// which nodes hold a key (review r3 F6A). A key several nodes hold is
// verified by the batch's one label scan, as on the release, and the rows
// read the scanned nodes: with one of the key's index entries missing, every
// node still gets its relationship. The batch routes themselves stay
// declined for such a key, and a complete index gives the same rows.
func TestNodeSeedChoiceBatchVerifiesNonUniqueKey(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("index_entry_missing=%v", partial), func(t *testing.T) {
			fixture := newSeedFixture(t,
				"CREATE INDEX batch_r_key FOR (n:R) ON (n.key)",
				"UNWIND range(1, 1000) AS i CREATE (:R {key: 'k' + toString(i)})",
				"CREATE (:R {key: 'dup', id: 'a'}), (:R {key: 'dup', id: 'b'}), (:R {key: 'dup', id: 'c'})",
			)
			if partial {
				seedDropIndexEntry(t, fixture, "R", "key", "dup")
			}
			create := "UNWIND $rows AS row MATCH (a:R {key: row.a}) MATCH (b:R {key: row.b}) CREATE (a)-[:REL]->(b) RETURN count(*) AS created"
			rows, _ := fixture.query(t, create, map[string]interface{}{"rows": []interface{}{map[string]interface{}{"a": "dup", "b": "k1"}}})
			require.Equal(t, []string{"3"}, rows, "one relationship from each of the three dup nodes")
			require.False(t, fixture.exec.LastHotPathTrace().UnwindMultiMatchCreateBatch, "a key several nodes hold declines the batch")
			rows, _ = fixture.query(t, "MATCH (:R)-[r:REL]->(:R {key: 'k1'}) RETURN count(r)", nil)
			require.Equal(t, []string{"3"}, rows)

			merge := `UNWIND $rows AS row
MATCH (source:R {key: row.source_key})
MATCH (target:R {key: row.target_key})
MERGE (source)-[rel:MREL {uuid: row.uuid}]->(target)
SET rel = row
WITH rel, row CALL db.create.setRelationshipVectorProperty(rel, "embedding", row.embedding)
RETURN row.uuid AS uuid`
			result, err := fixture.exec.Execute(context.Background(), merge, map[string]interface{}{"rows": []map[string]interface{}{{
				"source_key": "dup", "target_key": "k2", "uuid": "edge-1", "embedding": []float64{1, 0, 0, 0},
			}}})
			require.NoError(t, err)
			require.Len(t, result.Rows, 3, "one row from each of the three dup nodes")
			require.False(t, fixture.exec.LastHotPathTrace().UnwindRelationshipMergeBatch, "a key several nodes hold declines the batch")
			rows, _ = fixture.query(t, "MATCH (:R)-[r:MREL]->(:R {key: 'k2'}) RETURN count(r)", nil)
			require.Equal(t, []string{"3"}, rows)

			// The prefetch verifies the key with one scan of the label,
			// whether or not the index happens to be complete.
			scanned, unique := seedBatchPrefetchScans(t, fixture, "R", "key", "dup")
			require.False(t, unique, "a key several nodes hold is not unique")
			require.Equal(t, 1003, scanned, "one label scan verifies the key")
		})
	}
}

// A lookup that lists several nodes reads them in an order that differs from
// one read to the next, as the release did (review r3 F5, r4 F6). Storage
// keeps 10,000 nodes cached; a 12,000-node read repeated inside a transaction
// in the same order never finds a node still cached and costs about 60
// allocations per node, against about 43 on the release head.
func TestNodeSeedChoiceRepeatedLargeReadKeepsReleaseCost(t *testing.T) {
	const listed = 12000
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_doc FOR (n:R1771Doc) ON (n.document)",
		fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (:R1771Doc:R1771Entity {id: i, document: 'target'})", listed),
		"UNWIND range(1, 256) AS i CREATE (:R1771Doc:R1771Entity {id: -i, document: 'other'})",
	)
	ctx := context.Background()
	_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
	read := func() {
		result, err := fixture.exec.Execute(ctx, "MATCH (m:R1771Doc:R1771Entity {document: $d}) RETURN count(m)", map[string]interface{}{"d": "target"})
		require.NoError(t, err)
		require.Equal(t, []string{fmt.Sprint(listed)}, seedRowKeys(result.Rows))
	}
	read()
	read()
	perNode := testing.AllocsPerRun(3, read) / float64(listed)
	t.Logf("repeated 12,000-node count inside a transaction: %.1f allocations per listed node", perNode)
	require.Less(t, perNode, 50.0, "the repeated read must not decode every listed node again")
}

// seedScopeCounter counts the nodes the batch prefetch's label scan reads
// (GetNodesByLabelInScope, what a namespaced store's GetNodesByLabel calls).
type seedScopeCounter struct {
	*storage.MemoryEngine
	scanned int
}

func (c *seedScopeCounter) GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*storage.Node, error) {
	nodes, err := c.MemoryEngine.GetNodesByLabelInScope(ctx, scope, label)
	c.scanned += len(nodes)
	return nodes, err
}

// seedBatchPrefetchScans runs the UNWIND batch prefetch for label's property
// = value over a store that counts its label scans, and returns the nodes
// scanned and whether the batch kept its one-node-per-key shortcut.
func seedBatchPrefetchScans(t *testing.T, fixture *seedFixture, label, property string, value interface{}) (int, bool) {
	t.Helper()
	counted := &seedScopeCounter{MemoryEngine: fixture.engine.MemoryEngine}
	store := storage.NewNamespacedEngine(counted, "seed_choice")
	rows := []map[string]interface{}{{"key": value}}
	_, unique, err := fixture.exec.buildRelationshipBatchNodeMatchIndex(context.Background(), store, rows, []matchClauseSpec{{variable: "a", label: label, propName: property, rowField: "key", lookupExpr: "row.key", bindingVar: "row"}})
	require.NoError(t, err)
	return counted.scanned, unique
}

// Inside an explicit transaction, a composite index candidate's estimate
// reads the transaction's pending-node index, not every pending node: with N
// pending nodes of a label indexed on (kind, shard) and on id, the work per
// MATCH or MERGE that names kind, shard and id does not grow with N (r2 F1).
// Work is the heap allocations per statement, after one warm-up statement
// that indexes the pending nodes once.
func TestNodeSeedChoiceCompositeEstimateInTransactionIndependentOfPending(t *testing.T) {
	perStatement := func(t *testing.T, pending int) float64 {
		fixture := newSeedFixture(t,
			"CREATE INDEX r1771_pc_kind_shard FOR (n:R1771PendingComposite) ON (n.kind, n.shard)",
			"CREATE INDEX r1771_pc_id FOR (n:R1771PendingComposite) ON (n.id)",
		)
		ctx := context.Background()
		_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
		_, err = fixture.exec.Execute(ctx, "UNWIND range(1, $n) AS i CREATE (:R1771PendingComposite {id: i, kind: 'k', shard: 7})", map[string]interface{}{"n": int64(pending)})
		require.NoError(t, err)
		statements := []string{
			"MATCH (n:R1771PendingComposite {kind: $k, shard: $s}) WHERE n.id = $id RETURN n.id",
			"MERGE (n:R1771PendingComposite {kind: $k, shard: $s, id: $id}) RETURN n.id",
		}
		run := func(id int64) {
			for _, statement := range statements {
				result, err := fixture.exec.Execute(ctx, statement, map[string]interface{}{"k": "k", "s": int64(7), "id": id})
				require.NoError(t, err, statement)
				require.Equal(t, []string{fmt.Sprint(id)}, seedRowKeys(result.Rows), statement)
			}
		}
		run(1)
		const lookups = 20
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for id := int64(2); id < 2+lookups; id++ {
			run(id)
		}
		runtime.ReadMemStats(&after)
		result, err := fixture.exec.Execute(ctx, "MATCH (n:R1771PendingComposite) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, []string{fmt.Sprint(pending)}, seedRowKeys(result.Rows), "MERGE must not create duplicates")
		return float64(after.Mallocs-before.Mallocs) / float64(lookups*len(statements))
	}
	small := perStatement(t, 200)
	large := perStatement(t, 4000)
	t.Logf("allocations per statement: %.0f with 200 pending nodes, %.0f with 4000", small, large)
	require.Less(t, large, small+200, "allocations per statement grew with the pending nodes: %.0f with 200, %.0f with 4000", small, large)
}

// Composite reads inside an explicit transaction list exactly the nodes the
// transaction sees: committed nodes it didn't touch, committed nodes it
// rewrote under their new values, and its own new nodes; a node it deleted or
// moved off the key is not listed. MERGE matches a node the transaction wrote
// instead of creating another. Rows equal Neo4j 5.26.
func TestNodeSeedChoiceCompositeInTransactionRows(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX r1771_ptx_kind_shard FOR (n:R1771TxComposite) ON (n.kind, n.shard)",
		"CREATE INDEX r1771_ptx_id FOR (n:R1771TxComposite) ON (n.id)",
		"UNWIND range(1, 6) AS i CREATE (:R1771TxComposite {id: i, kind: 'k', shard: i % 2})",
	)
	ctx := context.Background()
	for _, statement := range []string{
		"BEGIN",
		"UNWIND range(101, 120) AS i CREATE (:R1771TxComposite {id: i, kind: 'k', shard: i % 2})",
		"MATCH (n:R1771TxComposite {id: 2}) SET n.shard = 1",
		"MATCH (n:R1771TxComposite {id: 103}) SET n.shard = 0",
		"MATCH (n:R1771TxComposite {id: 3}) DETACH DELETE n",
		"MATCH (n:R1771TxComposite {id: 105}) DETACH DELETE n",
	} {
		_, err := fixture.exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
	for _, check := range []struct {
		query string
		rows  []string
	}{
		{"MATCH (n:R1771TxComposite {kind: 'k', shard: 1}) RETURN n.id ORDER BY n.id", []string{"1", "101", "107", "109", "111", "113", "115", "117", "119", "2", "5"}},
		{"MATCH (n:R1771TxComposite {kind: 'k', shard: 0}) RETURN n.id ORDER BY n.id", []string{"102", "103", "104", "106", "108", "110", "112", "114", "116", "118", "120", "4", "6"}},
		{"MATCH (n:R1771TxComposite {kind: 'k'}) RETURN count(n)", []string{"24"}},
		{"MATCH (n:R1771TxComposite {kind: 'k', shard: 1}) WHERE n.id = 113 RETURN n.id", []string{"113"}},
		{"MATCH (n:R1771TxComposite {kind: 'k', shard: 0}) WHERE n.id = 105 RETURN n.id", []string{}},
		{"MERGE (n:R1771TxComposite {kind: 'k', shard: 0, id: 103}) RETURN n.id", []string{"103"}},
		{"MERGE (n:R1771TxComposite {kind: 'k', shard: 0, id: 2}) RETURN n.id", []string{"2"}},
		{"MATCH (n:R1771TxComposite) RETURN count(n)", []string{"25"}},
	} {
		result, err := fixture.exec.Execute(ctx, check.query, nil)
		require.NoError(t, err, check.query)
		require.Equal(t, check.rows, seedRowKeys(result.Rows), check.query)
	}
}

// With asynchronous writes, a composite index candidate's estimate counts the
// acknowledged writes its lookup returns (r2 F2): with N acknowledged,
// undrained nodes under one (kind, shard) key and a singleton id, MATCH and
// MERGE read the id index, not the N-node composite answer, so the work per
// statement does not grow with N. Rows are unchanged.
func TestNodeSeedChoiceCompositeEstimateSeesAcknowledgedWrites(t *testing.T) {
	perStatement := func(t *testing.T, pending int) float64 {
		engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir:             t.TempDir(),
			WriteBehind:         true,
			WriteBehindInterval: time.Hour, // nothing drains during the test
		}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })
		exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
		ctx := context.Background()
		for _, statement := range []string{
			"CREATE INDEX r1771_ack_kind_shard FOR (n:R1771Acked) ON (n.kind, n.shard)",
			"CREATE INDEX r1771_ack_id FOR (n:R1771Acked) ON (n.id)",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		require.NoError(t, engine.FlushWriteBehind(ctx))
		_, err = exec.Execute(ctx, "UNWIND range(1, $n) AS i CREATE (:R1771Acked {id: i, kind: 'hot', shard: 0})", map[string]interface{}{"n": int64(pending)})
		require.NoError(t, err)
		statements := []string{
			"MATCH (n:R1771Acked {kind: $k, shard: $s}) WHERE n.id = $id RETURN n.id",
			"MERGE (n:R1771Acked {kind: $k, shard: $s, id: $id}) RETURN n.id",
		}
		run := func(id int64) {
			for _, statement := range statements {
				result, err := exec.Execute(ctx, statement, map[string]interface{}{"k": "hot", "s": int64(0), "id": id})
				require.NoError(t, err, statement)
				require.Equal(t, []string{fmt.Sprint(id)}, seedRowKeys(result.Rows), statement)
			}
		}
		run(1)
		const lookups = 20
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for id := int64(2); id < 2+lookups; id++ {
			run(id)
		}
		runtime.ReadMemStats(&after)
		result, err := exec.Execute(ctx, "MATCH (n:R1771Acked {kind: 'hot', shard: 0}) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, []string{fmt.Sprint(pending)}, seedRowKeys(result.Rows), "every acknowledged node is visible, and MERGE created none")
		return float64(after.Mallocs-before.Mallocs) / float64(lookups*len(statements))
	}
	small := perStatement(t, 100)
	large := perStatement(t, 2000)
	t.Logf("allocations per statement: %.0f with 100 acknowledged nodes, %.0f with 2000", small, large)
	require.Less(t, large, small+200, "allocations per statement grew with the acknowledged nodes: %.0f with 100, %.0f with 2000", small, large)
}

// PD's survivor-group read: UNWIND $ids AS id MATCH
// (p:PDGraphNode:PDIdentity {survivor: id}) WHERE p.status IS NOT NULL. Only
// PDIdentity indexes survivor, so the pattern reads that index, not every
// PDGraphNode, whatever the label order, and the rows are unchanged.
func TestNodeSeedChoicePDSurvivorGroupReadUsesIndexOnAnyLabel(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX pd_graph_id FOR (n:PDGraphNode) ON (n.id)",
		"CREATE INDEX pd_identity_survivor FOR (n:PDIdentity) ON (n.survivor)",
		"UNWIND range(1, 3000) AS i CREATE (:PDGraphNode {id: 'g' + toString(i), status: 'live'})",
		"UNWIND range(1, 12) AS i CREATE (:PDGraphNode:PDIdentity {id: 'p' + toString(i), survivor: 's' + toString(i % 4), status: CASE WHEN i = 9 THEN null ELSE 'live' END})",
	)
	params := map[string]interface{}{"ids": []interface{}{"s1", "s2", "missing"}}
	for _, labels := range []string{"PDGraphNode:PDIdentity", "PDIdentity:PDGraphNode"} {
		query := "UNWIND $ids AS id MATCH (p:" + labels + " {survivor: id}) WHERE p.status IS NOT NULL RETURN p.id"
		rows, read := fixture.query(t, query, params)
		require.Equal(t, []string{"p1", "p10", "p2", "p5", "p6"}, rows, labels)
		require.Less(t, read, int64(200), "%s: nodes read, the survivor index rather than the 3,012 PDGraphNode nodes", labels)
		// The same read for one survivor passed as a parameter, with and
		// without the WHERE.
		for where, want := range map[string][]string{" WHERE p.status IS NOT NULL": {"p1", "p5"}, "": {"p1", "p5", "p9"}} {
			query = "MATCH (p:" + labels + " {survivor: $id})" + where + " RETURN p.id"
			rows, read = fixture.query(t, query, map[string]interface{}{"id": "s1"})
			require.Equal(t, want, rows, query)
			require.Less(t, read, int64(200), "%s: nodes read for one survivor parameter", query)
			// Some scans hand nodes over without a counted read, so the
			// statement's allocations are checked too: a scan of the 3,012
			// PDGraphNode nodes allocates for each of them.
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			const runs = 5
			for i := 0; i < runs; i++ {
				_, err := fixture.exec.Execute(context.Background(), query, map[string]interface{}{"id": "s1"})
				require.NoError(t, err)
			}
			runtime.ReadMemStats(&after)
			perStatement := float64(after.Mallocs-before.Mallocs) / runs
			require.Less(t, perStatement, float64(1500), "%s: %.0f allocations per statement; a scan of the PDGraphNode label allocates for every node", query, perStatement)
		}
	}
}

// PD's publication MERGE: UNWIND batches of new nodes, each
// MERGE (x:PDGraphNode:<group> {id: n.id}) SET x += n.props, in one explicit
// transaction, with UNIQUE on PDGraphNode.id. The cost of a MERGE does not
// grow with the nodes the transaction already wrote (NOR-377): the
// allocations and the time per row of the last batch are about the same with
// 750 and with 6,000 pending nodes, and every node is written once.
func TestNodeSeedChoicePDPendingMergeIndependentOfPending(t *testing.T) {
	type cost struct {
		allocs float64
		nanos  float64
	}
	lastBatch := func(t *testing.T, total int) cost {
		fixture := newSeedFixture(t, "CREATE CONSTRAINT pd_graph_id_unique FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE")
		ctx := context.Background()
		_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "ROLLBACK", nil) })
		const batch = 250
		statement := "UNWIND $nodes AS n MERGE (x:PDGraphNode:PNGroup0 {id: n.id}) SET x += n.props"
		// The time is the fastest of the last three batches: one batch's
		// time also holds whatever the collector does for the tests that ran
		// before this one in the same process.
		measured := cost{nanos: math.MaxFloat64}
		for start := 0; start < total; start += batch {
			nodes := make([]interface{}, 0, batch)
			for i := start; i < start+batch; i++ {
				nodes = append(nodes, map[string]interface{}{"id": fmt.Sprintf("n-%06d", i), "props": map[string]interface{}{"name": fmt.Sprintf("node %d", i), "ordinal": int64(i)}})
			}
			last := start+batch >= total
			var before, after runtime.MemStats
			if last {
				runtime.ReadMemStats(&before)
			}
			began := time.Now()
			_, err := fixture.exec.Execute(ctx, statement, map[string]interface{}{"nodes": nodes})
			elapsed := time.Since(began)
			require.NoError(t, err)
			if start+3*batch >= total {
				measured.nanos = min(measured.nanos, float64(elapsed.Nanoseconds())/batch)
			}
			if last {
				runtime.ReadMemStats(&after)
				measured.allocs = float64(after.Mallocs-before.Mallocs) / batch
			}
		}
		result, err := fixture.exec.Execute(ctx, "MATCH (x:PDGraphNode) RETURN count(x)", nil)
		require.NoError(t, err)
		require.Equal(t, []string{fmt.Sprint(total)}, seedRowKeys(result.Rows), "every node is written once")
		return measured
	}
	small := lastBatch(t, 750)
	large := lastBatch(t, 6000)
	t.Logf("per MERGE row: %.0f allocations (last batch) / %.0f ns (fastest of the last three batches) with 750 pending nodes, %.0f / %.0f with 6,000", small.allocs, small.nanos, large.allocs, large.nanos)
	require.Less(t, large.allocs, small.allocs*1.5+20, "allocations per MERGE grew with the pending nodes")
	require.Less(t, large.nanos, small.nanos*3, "time per MERGE grew with the pending nodes")
}

// PD's holdings read: a two-hop chain from a bound document through its own
// nodes to shared ones, where a shared hub is also linked to the document N
// times and fails the intermediate node's own.document_id = document.id. The
// chain applies that condition when it reaches the intermediate node, so the
// work grows with the output (N rows) like the statement split at the
// intermediate node, not with the 2N² paths through the hub; the rows equal
// the split form's.
func TestNodeSeedChoicePDTwoHopChainFiltersIntermediate(t *testing.T) {
	chained := "MATCH (document:PDDocument {id: $id}) CALL (document) { " +
		"MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode) " +
		"WHERE own.document_id = document.id AND coalesce(shared.document_id, '') <> document.id " +
		"RETURN collect(own.id + '>' + shared.id) AS found } RETURN found"
	plain := "MATCH (document:PDDocument {id: $id})-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode) " +
		"WHERE own.document_id = document.id AND coalesce(shared.document_id, '') <> document.id " +
		"RETURN own.id + '>' + shared.id AS found"
	split := "MATCH (document:PDDocument {id: $id})-[l:PD_RELATION]-(own:PDGraphNode) WHERE own.document_id = document.id " +
		"WITH document, l, own MATCH (own)-[l2:PD_RELATION]-(shared:PDGraphNode) " +
		"WHERE l2 <> l AND coalesce(shared.document_id, '') <> document.id RETURN own.id + '>' + shared.id AS found"
	perStatement := func(t *testing.T, n int) float64 {
		fixture := newSeedFixture(t,
			"CREATE INDEX pd_doc_id FOR (n:PDDocument) ON (n.id)",
			"CREATE (:PDDocument {id: 'doc'}), (:PDGraphNode {id: 'hub', document_id: 'other'})",
		)
		ctx := context.Background()
		_, err := fixture.exec.Execute(ctx, "MATCH (d:PDDocument {id: 'doc'}), (hub:PDGraphNode {id: 'hub'}) "+
			"UNWIND range(1, $n) AS i CREATE (d)-[:PD_RELATION]->(hub) "+
			"CREATE (own:PDGraphNode {id: 'own' + toString(i), document_id: 'doc'}) CREATE (d)-[:PD_RELATION]->(own) CREATE (own)-[:PD_RELATION]->(hub)",
			map[string]interface{}{"n": int64(n)})
		require.NoError(t, err)
		params := map[string]interface{}{"id": "doc"}
		want, _ := fixture.query(t, split, params)
		require.Len(t, want, n)
		got, _ := fixture.query(t, plain, params)
		require.Equal(t, want, got, "plain chain rows equal the split form's")
		result, err := fixture.exec.Execute(ctx, chained, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		found := make([]string, 0, n)
		for _, value := range result.Rows[0][0].([]interface{}) {
			found = append(found, value.(string))
		}
		sort.Strings(found)
		require.Equal(t, want, found, "seeded chain rows equal the split form's")
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		const runs = 3
		for i := 0; i < runs; i++ {
			_, err := fixture.exec.Execute(ctx, chained, params)
			require.NoError(t, err)
		}
		runtime.ReadMemStats(&after)
		return float64(after.Mallocs-before.Mallocs) / runs
	}
	small := perStatement(t, 64)
	large := perStatement(t, 256)
	t.Logf("allocations per statement: %.0f with 64 hub links, %.0f with 256", small, large)
	require.Less(t, large, small*8, "work grew faster than the output: %.0f with 64 links, %.0f with 256", small, large)
}

// A chain's WHERE conjunct that holds a subquery expression is applied to the
// complete path only: the subquery's body may read a variable a later segment
// binds, which the conjunct's own variables do not show. Decided after the
// first hop, b.enabled OR NOT EXISTS { MATCH (c)-[:T]->() } ran the subquery
// with c unbound and dropped a path whose real end node passes. The rows are
// the release head's in every form.
func TestNodeSeedChoiceChainPruningWaitsForSubqueryVariables(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE (:SubChain {id: 'a'})-[:SUB_R]->(:SubChain {id: 'b', enabled: false})-[:SUB_S]->(:SubChain {id: 'c'})",
		"CREATE (:SubChain {id: 'a2'})-[:SUB_R]->(:SubChain {id: 'b2', enabled: false})-[:SUB_S]->(:SubChain {id: 'c2'})-[:SUB_T]->(:SubChain {id: 'd2'})",
		"CREATE (:SubChain {id: 'a3'})-[:SUB_R]->(:SubChain {id: 'b3', enabled: true})-[:SUB_S]->(:SubChain {id: 'c3'})-[:SUB_T]->(:SubChain {id: 'd3'})",
	)
	const chain = "MATCH (a:SubChain)-[:SUB_R]->(b)-[:SUB_S]->(c) WHERE "
	for _, form := range []struct {
		name, where string
		rows        []string
	}{
		{"or_not_exists", "b.enabled OR NOT EXISTS { MATCH (c)-[:SUB_T]->() }", []string{"c", "c3"}},
		{"or_exists", "b.enabled OR EXISTS { MATCH (c)-[:SUB_T]->() }", []string{"c2", "c3"}},
		{"or_count_zero", "b.enabled OR COUNT { MATCH (c)-[:SUB_T]->() } = 0", []string{"c", "c3"}},
		{"and_count", "NOT b.enabled AND COUNT { MATCH (c)-[:SUB_T]->() } = 1", []string{"c2"}},
		{"subquery_where_names_later_variable", "b.enabled OR NOT EXISTS { MATCH (x)-[:SUB_T]->() WHERE x.id = c.id }", []string{"c", "c3"}},
		{"subquery_on_bound_variable_only", "EXISTS { MATCH (b)-[:SUB_S]->() } AND NOT b.enabled", []string{"c", "c2"}},
		{"pattern_predicate", "b.enabled OR NOT (c)-[:SUB_T]->()", []string{"c", "c3"}},
	} {
		t.Run(form.name, func(t *testing.T) {
			result, err := fixture.exec.Execute(context.Background(), chain+form.where+" RETURN c.id", nil)
			require.NoError(t, err)
			got := seedRowKeys(result.Rows)
			sort.Strings(got)
			require.Equal(t, form.rows, got)
		})
	}
}

// A relationship pattern starts from its known end (NOR-160): from a bound
// end node even when the WHERE also constrains the other end (PD's context
// read), and from an end node an index answers when the start side has none.
// Rows are unchanged; the nodes read are the known end's neighbourhood, not
// the other side's whole label.
func TestNodeSeedChoiceTraversalStartsFromKnownEnd(t *testing.T) {
	fixture := newSeedFixture(t,
		"CREATE INDEX known_end_entity_id FOR (n:KnownEndEntity) ON (n.id)",
		"UNWIND range(1, 3000) AS i CREATE (:KnownEndHolder {id: 'h' + toString(i), x: i})",
		"UNWIND range(1, 3000) AS i CREATE (:KnownEndDoc {id: 'd' + toString(i)})",
		"CREATE (:KnownEndTarget {id: 't1'}), (:KnownEndTarget {id: 't2'}), (:KnownEndEntity {id: 'e1'}), (:KnownEndEntity {id: 'e2'})",
		"MATCH (t:KnownEndTarget {id: 't1'}), (h:KnownEndHolder) WHERE h.id IN ['h1', 'h2'] CREATE (h)-[:KNOWN_END_R]->(t)",
		"MATCH (e:KnownEndEntity {id: 'e1'}), (d:KnownEndDoc) WHERE d.id IN ['d1', 'd2'] CREATE (d)-[:KNOWN_END_MENTIONS]->(e)",
	)
	for _, check := range []struct {
		name, query string
		params      map[string]interface{}
		rows        []string
	}{
		{"bound_end_where_on_start", "MATCH (t:KnownEndTarget) OPTIONAL MATCH (holder:KnownEndHolder)-[:KNOWN_END_R]-(t) WHERE holder.x IS NOT NULL RETURN t.id, count(holder)", nil, []string{"t12", "t20"}},
		{"indexed_end", "MATCH (d:KnownEndDoc)-[:KNOWN_END_MENTIONS]->(e:KnownEndEntity {id: $id}) RETURN d.id", map[string]interface{}{"id": "e1"}, []string{"d1", "d2"}},
		{"indexed_end_where", "MATCH (d:KnownEndDoc)-[:KNOWN_END_MENTIONS]->(e:KnownEndEntity) WHERE e.id = $id RETURN d.id", map[string]interface{}{"id": "e1"}, []string{"d1", "d2"}},
		{"indexed_end_absent", "MATCH (d:KnownEndDoc)-[:KNOWN_END_MENTIONS]->(e:KnownEndEntity {id: $id}) RETURN count(d)", map[string]interface{}{"id": "e9"}, []string{"0"}},
	} {
		t.Run(check.name, func(t *testing.T) {
			rows, read := fixture.query(t, check.query, check.params)
			require.Equal(t, check.rows, rows)
			require.Less(t, read, int64(100), "nodes read: the known end's neighbourhood, not the 3,000-node other side")
		})
	}
}
