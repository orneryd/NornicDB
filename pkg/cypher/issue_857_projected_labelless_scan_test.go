package cypher

// NornicDB #857: a label-less match whose equality is in WHERE (MATCH (a)
// WHERE a.id = $id), or that starts a relationship pattern
// ((a {id: …})-[r]->(b {id: …}), graphify's stale-edge delete), decoded every
// node in full; only the plain node pattern took the projected scan.

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// projectionProbeEngine records the projections of the node streams a
// statement runs (nil for a full-decode stream).
type projectionProbeEngine struct {
	*storage.MemoryEngine
	mu          sync.Mutex
	projections [][]string
}

func (e *projectionProbeEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	e.record(opts.Projection)
	return e.MemoryEngine.StreamNodesWithOptions(ctx, opts, fn)
}

func (e *projectionProbeEngine) StreamNodes(ctx context.Context, fn func(*storage.Node) error) error {
	e.record(nil)
	return e.MemoryEngine.StreamNodes(ctx, fn)
}

func (e *projectionProbeEngine) record(projection []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sorted := append([]string(nil), projection...)
	sort.Strings(sorted)
	if projection == nil {
		sorted = nil
	}
	e.projections = append(e.projections, sorted)
}

func (e *projectionProbeEngine) take() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := e.projections
	e.projections = nil
	return out
}

func TestIssue857LabellessScansAreProjected(t *testing.T) {
	probe := &projectionProbeEngine{MemoryEngine: newTestMemoryEngine(t)}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(probe, "i857"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (:Code {id: 'c1', name: 'one'})-[:IMPORTS]->(:Code {id: 'c2', name: 'two'}),
		(:Document {id: 'd1', name: 'doc'}), ({id: 'c1', name: 'unlabeled'})`, nil)
	require.NoError(t, err)
	probe.take()

	for _, tc := range []struct {
		query      string
		params     map[string]interface{}
		rows       [][]interface{}
		projection []string // nil: a full-decode stream
	}{
		{query: "MATCH (a) WHERE a.id = $id RETURN a.name ORDER BY a.name", params: map[string]interface{}{"id": "c1"},
			rows: [][]interface{}{{"one"}, {"unlabeled"}}, projection: []string{"id"}},
		{query: "MATCH (a) WHERE a.id = $id AND a.name STARTS WITH 'u' RETURN a.name", params: map[string]interface{}{"id": "c1"},
			rows: [][]interface{}{{"unlabeled"}}, projection: []string{"id"}},
		{query: "MATCH (a {name: 'doc'}) WHERE a.id = 'd1' RETURN a.name", rows: [][]interface{}{{"doc"}}, projection: []string{"id", "name"}},
		{query: "MATCH (a) WHERE a.id = $id RETURN count(a)", params: map[string]interface{}{"id": nil},
			rows: [][]interface{}{{int64(0)}}, projection: []string{"id"}},
		{query: "MATCH (a {id: $src})-[r]->(b {id: $tgt}) RETURN type(r)", params: map[string]interface{}{"src": "c1", "tgt": "c2"},
			rows: [][]interface{}{{"IMPORTS"}}, projection: []string{"id"}},
		// An OR is not a top-level equality: the WHERE is evaluated on every node.
		{query: "MATCH (a) WHERE a.id = 'c2' OR a.id = 'd1' RETURN a.name ORDER BY a.name",
			rows: [][]interface{}{{"doc"}, {"two"}}, projection: nil},
	} {
		res, err := exec.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, res.Rows, tc.query)
		projections := probe.take()
		require.NotEmpty(t, projections, tc.query)
		require.Equal(t, tc.projection, projections[0], tc.query)
	}
}

// The same statements on the server's storage stack, in auto-commit and in an
// explicit transaction, return what a full scan does.
func TestIssue857LabellessWhereAndRelationshipPatterns(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			run := func(q string, params map[string]interface{}) [][]interface{} {
				t.Helper()
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() {
						_, err := exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}()
				}
				res, err := exec.Execute(ctx, q, params)
				require.NoError(t, err, q)
				return res.Rows
			}
			run(`CREATE (:Code {id: 'c1'})-[:IMPORTS]->(:Code {id: 'c2'}), (:Code {id: 'c3'})-[:IMPORTS]->(:Code {id: 'c2'}),
				({id: 'c1'})`, nil)
			require.Equal(t, [][]interface{}{{int64(2)}}, run("MATCH (a) WHERE a.id = $id RETURN count(a)", map[string]interface{}{"id": "c1"}))
			require.Equal(t, [][]interface{}{{int64(1)}}, run("UNWIND $rows AS row MATCH (a {id: row.src})-[r]->(b {id: row.tgt}) RETURN count(r)",
				map[string]interface{}{"rows": []interface{}{map[string]interface{}{"src": "c3", "tgt": "c2"}, map[string]interface{}{"src": "c1", "tgt": "c3"}}}))
			// Graphify's stale-edge delete.
			run("UNWIND $rows AS row MATCH (a {id: row.src})-[r]->(b {id: row.tgt}) WHERE type(r) = row.rel DELETE r",
				map[string]interface{}{"rows": []interface{}{map[string]interface{}{"src": "c1", "tgt": "c2", "rel": "IMPORTS"}}})
			require.Equal(t, [][]interface{}{{"c3"}}, run("MATCH (a)-[:IMPORTS]->(:Code {id: 'c2'}) RETURN a.id", nil))
		})
	}
}

// A MATCH with an outer WITH before a procedure CALL loads its nodes with the
// pattern's properties (loadPatternNodes, #857).
func TestIssue857MatchWithCallProcedureLoadsPatternNodes(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Code {id: 'c1'}), (:Code {id: 'c2'}), ({id: 'c1'})", nil)
	require.NoError(t, err)
	res, err := exec.executeMatchWithCallProcedure(ctx, "MATCH (n {id: 'c1'}) WITH n CALL db.labels() YIELD label RETURN label")
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	_, err = exec.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	res, err = exec.executeMatchWithCallProcedure(ctx, "MATCH (n {id: 'c1'}) WITH n CALL db.labels() YIELD label RETURN label")
	require.NoError(t, err)
	require.Empty(t, res.Rows, "no node matches the pattern")
}

// A label-less match on a string compares the stored bytes (#857): only a
// stored string with exactly those characters matches, as in Neo4j 5.26.30;
// an integer, a list, a date, another case or a longer string does not.
func TestIssue857LabellessStringMatchComparesExactly(t *testing.T) {
	long := strings.Repeat("k", 300)
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			run := func(q string, params map[string]interface{}) [][]interface{} {
				t.Helper()
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() {
						_, err := exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}()
				}
				res, err := exec.Execute(ctx, q, params)
				require.NoError(t, err, q)
				return res.Rows
			}
			run(`CREATE ({id: 'n7', name: 'hit'}), ({id: 'N7'}), ({id: 'n7x'}), ({id: 7}), ({id: ['n7']}),
				({id: date('2020-01-01')}), ({name: 'n7'}), ({id: ''}), ({id: $long}), ({id: $long + 'x'})`, map[string]interface{}{"long": long})
			for _, tc := range []struct {
				query  string
				params map[string]interface{}
				count  int64
			}{
				{"MATCH (a {id: 'n7'}) RETURN count(a)", nil, 1},
				{"MATCH (a) WHERE a.id = $id RETURN count(a)", map[string]interface{}{"id": "n7"}, 1},
				{"MATCH (a {id: $id}) RETURN count(a)", map[string]interface{}{"id": long}, 1},
				{"MATCH (a {id: ''}) RETURN count(a)", nil, 1},
				{"MATCH (a {id: '7'}) RETURN count(a)", nil, 0},
				{"MATCH (a {id: 7}) RETURN count(a)", nil, 1},
				{"MATCH (a {id: '2020-01-01'}) RETURN count(a)", nil, 0},
				{"MATCH (a {id: 'n7', name: 'hit'}) RETURN count(a)", nil, 1},
				{"MATCH (a {id: 'n7', name: 'miss'}) RETURN count(a)", nil, 0},
			} {
				require.Equal(t, [][]interface{}{{tc.count}}, run(tc.query, tc.params), tc.query)
			}
		})
	}
}
