package cypher

// Regression and complexity coverage for NornicDB #562: a later MATCH whose
// pattern starts (or ends) at a node already bound by an earlier clause was
// expanded over every relationship in the store and only afterward joined to
// the bound node, instead of seeding the traversal from that node directly.
//
// TestIssue562LimitAfterBoundSecondMatch guards the correctness fix for #519
// (LIMIT must never be pushed into the second MATCH's scan before the join).
// The SeedsFromBoundNode tests guard the performance fix: the traversal must
// be seeded from the bound node (O(degree)), never expanded over the whole
// store (O(|E|)), including when the seeded traversal finds zero rows.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// streamNodeCountingEngine wraps storage.NamespacedEngine and counts every
// node handed to StreamNodes' visitor function. It embeds the concrete
// *storage.NamespacedEngine (not an interface) so every other optional
// interface it implements (ProjectedLabelNodeReader, temporal viewport
// checks, etc.) is promoted unchanged; only StreamNodes is intercepted.
type streamNodeCountingEngine struct {
	*storage.NamespacedEngine
	nodesVisited int64
	streamCalls  int64
}

func (s *streamNodeCountingEngine) StreamNodes(ctx context.Context, fn func(node *storage.Node) error) error {
	s.streamCalls++
	return s.NamespacedEngine.StreamNodes(ctx, func(node *storage.Node) error {
		s.nodesVisited++
		return fn(node)
	})
}

func (s *streamNodeCountingEngine) reset() {
	s.nodesVisited = 0
	s.streamCalls = 0
}

func newStreamCountingExecutor(t testing.TB) (*StorageExecutor, *streamNodeCountingEngine) {
	t.Helper()
	inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	spy := &streamNodeCountingEngine{NamespacedEngine: inner}
	return NewStorageExecutor(spy), spy
}

// seedSinkProbeGraph creates one Function --[:QUERIES_TABLE]--> SqlTable edge
// (the real shape) plus decoyCount unrelated (:X)-[:LINKS]->(:Y) pairs, the
// same repro shape used in the upstream draft issue B evidence.
func seedSinkProbeGraph(t testing.TB, exec *StorageExecutor, decoyCount int) {
	t.Helper()
	ctx := context.Background()
	run := func(q string, p map[string]any) {
		_, err := exec.Execute(ctx, q, p)
		require.NoError(t, err, q)
	}
	run(`CREATE (:Function {uid: 'fn'})`, nil)
	run(`CREATE (:SqlTable {name: 'orders'})`, nil)
	run(`MATCH (f:Function {uid: 'fn'}), (t:SqlTable {name: 'orders'}) CREATE (f)-[:QUERIES_TABLE]->(t)`, nil)
	seedDecoyLinksPairs(t, exec, decoyCount)
}

// decoySeedBatchSize caps each UNWIND CREATE batch so large decoy counts
// (e.g. 20,000) don't exceed the storage layer's single-transaction size
// limit ("Txn is too big to fit into one request").
const decoySeedBatchSize = 2000

// seedDecoyLinksPairs bulk-creates decoyCount unrelated (:X)-[:LINKS]->(:Y)
// pairs via batched UNWIND CREATE statements instead of 3*decoyCount
// individual CREATE calls, so large decoy counts seed in well under a
// second instead of minutes.
func seedDecoyLinksPairs(t testing.TB, exec *StorageExecutor, decoyCount int) {
	t.Helper()
	ctx := context.Background()
	run := func(q string, p map[string]any) {
		_, err := exec.Execute(ctx, q, p)
		require.NoError(t, err, q)
	}
	for start := 0; start < decoyCount; start += decoySeedBatchSize {
		end := start + decoySeedBatchSize
		if end > decoyCount {
			end = decoyCount
		}
		run(`UNWIND range($start, $end - 1) AS k CREATE (:X {uid: 'x' + toString(k)})-[:LINKS]->(:Y {uid: 'y' + toString(k)})`,
			map[string]any{"start": int64(start), "end": int64(end)})
	}
}

// TestIssue562LimitAfterBoundSecondMatch is the #519 correctness
// regression: LIMIT must be applied after the join with the earlier-bound
// variable, never physically pushed into the second MATCH's standalone scan.
func TestIssue562LimitAfterBoundSecondMatch(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	run := func(q string, p map[string]any) *ExecuteResult {
		res, err := exec.Execute(ctx, q, p)
		require.NoError(t, err, q)
		return res
	}
	repo := func(rid, path string) {
		run(`CREATE (:Repository {id: $r})`, map[string]any{"r": rid})
		run(`CREATE (:File {path: $p})`, map[string]any{"p": path})
		run(`MATCH (r:Repository {id: $r}), (f:File {path: $p}) CREATE (r)-[:REPO_CONTAINS]->(f)`, map[string]any{"r": rid, "p": path})
	}
	for i := 0; i < 10; i++ {
		repo(fmt.Sprintf("decoyA%d", i), fmt.Sprintf("a%d", i))
	}
	repo("target", "t")
	run(`CREATE (:Function {uid: 'fn'})`, nil)
	run(`MATCH (f:File {path: 't'}), (e:Function {uid: 'fn'}) CREATE (f)-[:CONTAINS]->(e)`, nil)
	for i := 0; i < 10; i++ {
		repo(fmt.Sprintf("decoyB%d", i), fmt.Sprintf("b%d", i))
	}
	q := `MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File) MATCH (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo`
	require.Len(t, run(q, nil).Rows, 1, "no LIMIT")
	for n := 1; n <= 21; n++ {
		require.Len(t, run(fmt.Sprintf("%s LIMIT %d", q, n), nil).Rows, 1, "LIMIT %d", n)
	}
	withQ := `MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File) WITH f MATCH (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo`
	for n := 1; n <= 21; n++ {
		require.Len(t, run(fmt.Sprintf("%s LIMIT %d", withQ, n), nil).Rows, 1, "WITH form LIMIT %d", n)
	}
	nodeQ := `MATCH (f:File {path: 't'}) MATCH (x:File) WHERE x.path = f.path RETURN x.path AS p`
	for n := 1; n <= 21; n++ {
		require.Len(t, run(fmt.Sprintf("%s LIMIT %d", nodeQ, n), nil).Rows, 1, "node-join LIMIT %d", n)
	}
}

// TestIssue562BoundHopSeedsFromBoundNode proves the performance fix: the
// second MATCH, whose start variable ("reached") is bound by the first
// MATCH, must seed its traversal from that single node instead of streaming
// every node in the store. With 2,000 unrelated decoy nodes present, a
// store-wide scan would visit thousands of nodes; a correctly seeded
// traversal visits none via StreamNodes at all (it walks the bound node's
// own outgoing edges instead).
func TestIssue562BoundHopSeedsFromBoundNode(t *testing.T) {
	exec, spy := newStreamCountingExecutor(t)
	seedSinkProbeGraph(t, exec, 2000)

	spy.reset()
	res, err := exec.Execute(context.Background(),
		`MATCH (reached:Function {uid:'fn'}) MATCH (reached)-[sinkRel]->(sinkNode) WHERE type(sinkRel) IN ['QUERIES_TABLE'] RETURN type(sinkRel) AS t`,
		nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "QUERIES_TABLE", res.Rows[0][0])

	t.Logf("nodesVisited=%d streamCalls=%d (2000 decoy pairs = 4000 decoy nodes)", spy.nodesVisited, spy.streamCalls)
	require.Less(t, spy.nodesVisited, int64(50),
		"second MATCH must seed from the bound 'reached' node (O(degree)); "+
			"visiting %d nodes means it streamed the store instead (O(|E|))", spy.nodesVisited)
}

// TestIssue562BoundEndSeedsFromBoundNode proves the same fix in the reverse
// direction: the pattern's END variable is the one already bound
// ("MATCH (a)-[:LINKS]->(bound)"), not the start. The traversal must reverse
// and seed from the bound endpoint, not scan the store.
func TestIssue562BoundEndSeedsFromBoundNode(t *testing.T) {
	exec, spy := newStreamCountingExecutor(t)
	ctx := context.Background()
	run := func(q string) {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	run(`CREATE (:Anchor {uid: 'anchor'})`)
	run(`CREATE (:Source {uid: 'src'})`)
	run(`MATCH (s:Source {uid: 'src'}), (a:Anchor {uid: 'anchor'}) CREATE (s)-[:LINKS]->(a)`)
	seedDecoyLinksPairs(t, exec, 2000)

	spy.reset()
	res, err := exec.Execute(ctx,
		`MATCH (a:Anchor {uid:'anchor'}) MATCH (src)-[:LINKS]->(a) RETURN src.uid AS uid`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "src", res.Rows[0][0])

	t.Logf("nodesVisited=%d streamCalls=%d", spy.nodesVisited, spy.streamCalls)
	require.Less(t, spy.nodesVisited, int64(50),
		"second MATCH must seed from the bound end node via reversed traversal (O(degree)); "+
			"visiting %d nodes means it streamed the store instead (O(|E|))", spy.nodesVisited)
}

// TestIssue562BoundHopVarlenZeroHopRetained proves the start-seeding fix
// composes correctly with variable-length zero-hop retention (upstream
// 8d25b246): seeding must not reintroduce the zero-hop-row-dropping bug it
// fixed.
func TestIssue562BoundHopVarlenZeroHopRetained(t *testing.T) {
	exec, _ := newStreamCountingExecutor(t)
	ctx := context.Background()
	run := func(q string) *ExecuteResult {
		res, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
		return res
	}
	run(`CREATE (:N {uid: 'src'})`)
	run(`CREATE (:N {uid: 'mid'})`)
	run(`CREATE (:N {uid: 'leaf'})`)
	run(`MATCH (a:N {uid: 'src'}), (b:N {uid: 'mid'}) CREATE (a)-[:CALLS]->(b)`)
	run(`MATCH (a:N {uid: 'mid'}), (b:N {uid: 'leaf'}) CREATE (a)-[:CALLS]->(b)`)

	res := run(`MATCH (src:N {uid: 'src'}) MATCH path = (src)-[:CALLS*0..4]->(reached) RETURN reached.uid AS uid ORDER BY uid`)
	got := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		got = append(got, row[0].(string))
	}
	require.ElementsMatch(t, []string{"src", "mid", "leaf"}, got, "zero-hop binding (reached=src) must be retained when the traversal is seeded from an earlier-bound node")
}

// TestIssue562SeededZeroRowsDoesNotScan proves that a seeded traversal that
// finds nothing stays seeded. An empty result (a WHERE that rejects every
// seeded path, or a bound node that fails the pattern's own labels or inline
// properties) must not be mistaken for "not seeded" and fall back to a
// store-wide scan.
func TestIssue562SeededZeroRowsDoesNotScan(t *testing.T) {
	exec, spy := newStreamCountingExecutor(t)
	ctx := context.Background()
	run := func(q string) {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	run(`CREATE (:Anchor {uid: 'anchor'})`)
	run(`CREATE (:Source {uid: 'src'})`)
	run(`MATCH (s:Source {uid: 'src'}), (a:Anchor {uid: 'anchor'}) CREATE (s)-[:LINKS {w: 1}]->(a)`)
	seedDecoyLinksPairs(t, exec, 2000)

	queries := map[string]string{
		"end_seed_where_rejects_all":      `MATCH (a:Anchor {uid:'anchor'}) MATCH (src)-[r:LINKS]->(a) WHERE r.w = 99 RETURN src.uid AS uid`,
		"end_seed_inline_prop_mismatch":   `MATCH (a:Anchor {uid:'anchor'}) MATCH (src)-[r:LINKS]->(a {uid:'other'}) RETURN src.uid AS uid`,
		"end_seed_extra_label_mismatch":   `MATCH (a:Anchor {uid:'anchor'}) MATCH (src)-[r:LINKS]->(a:Anchor:Missing) RETURN src.uid AS uid`,
		"start_seed_where_rejects_all":    `MATCH (s:Source {uid:'src'}) MATCH (s)-[r:LINKS]->(dst) WHERE r.w = 99 RETURN dst.uid AS uid`,
		"start_seed_inline_prop_mismatch": `MATCH (s:Source {uid:'src'}) MATCH (s {uid:'other'})-[r:LINKS]->(dst) RETURN dst.uid AS uid`,
	}
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			spy.reset()
			res, err := exec.Execute(ctx, q, nil)
			require.NoError(t, err, q)
			require.Empty(t, res.Rows, q)
			t.Logf("nodesVisited=%d streamCalls=%d", spy.nodesVisited, spy.streamCalls)
			require.Less(t, spy.nodesVisited, int64(50),
				"a seeded traversal with zero results must not fall back to a store scan; visited %d nodes", spy.nodesVisited)
		})
	}
}
