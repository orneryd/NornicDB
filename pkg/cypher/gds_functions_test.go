package cypher

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// GDS's link-prediction functions, gds.util helpers and gds.version() give
// GDS 2.13.12's values on the same graph (#1161): five :P nodes a–e with six
// :K relationships and a :Q node q linked from a by :L; then a second a→b and
// a self-loop on b.
func TestGDSFunctionsMatchGDS(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds_functions"))
	ctx := context.Background()
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	run(`CREATE (a:P {n:'a'}),(b:P {n:'b'}),(c:P {n:'c'}),(d:P {n:'d'}),(e:P {n:'e'}),(x:Q {n:'q'}),
		(a)-[:K]->(b),(a)-[:K]->(c),(b)-[:K]->(c),(b)-[:K]->(d),(c)-[:K]->(d),(d)-[:K]->(e),(a)-[:L]->(x)`)
	scores := func(left, right, config string) []float64 {
		t.Helper()
		rows := run("MATCH (x {n:'" + left + "'}), (y {n:'" + right + "'}) RETURN " +
			"gds.alpha.linkprediction.commonNeighbors(x, y" + config + "), " +
			"gds.alpha.linkprediction.adamicAdar(x, y" + config + "), " +
			"gds.alpha.linkprediction.resourceAllocation(x, y" + config + "), " +
			"gds.alpha.linkprediction.preferentialAttachment(x, y" + config + "), " +
			"gds.alpha.linkprediction.totalNeighbors(x, y" + config + ")")
		require.Len(t, rows, 1)
		values := make([]float64, len(rows[0]))
		for index, value := range rows[0] {
			values[index] = value.(float64)
		}
		return values
	}
	inf := math.Inf(1)
	for _, check := range []struct {
		left, right, config string
		want                []float64
	}{
		{"a", "d", "", []float64{2, 1.8204784532536746, 0.6666666666666666, 9, 4}},
		{"a", "d", ", {relationshipQuery: 'K'}", []float64{2, 1.8204784532536746, 0.6666666666666666, 6, 3}},
		{"a", "d", ", {direction: 'OUTGOING'}", []float64{0, 0, 0, 3, 4}},
		{"a", "d", ", {direction: 'INCOMING'}", []float64{0, 0, 0, 0, 2}},
		{"a", "d", ", {relationshipQuery: 'K', direction: 'OUTGOING'}", []float64{0, 0, 0, 2, 3}},
		// A common neighbour's degree reads the same direction.
		{"b", "c", ", {direction: 'INCOMING'}", []float64{1, 0, inf, 2, 2}},
		{"b", "c", ", {direction: 'OUTGOING'}", []float64{1, inf, 1, 2, 2}},
		// Other keys are ignored; another direction reads outgoing
		// relationships, as GDS does; an unknown type reads nothing.
		{"a", "d", ", {bogus: 1}", []float64{2, 1.8204784532536746, 0.6666666666666666, 9, 4}},
		{"a", "d", ", {direction: 'sideways'}", []float64{0, 0, 0, 3, 4}},
		{"a", "d", ", {relationshipQuery: 'NOPE'}", []float64{0, 0, 0, 0, 0}},
	} {
		got := scores(check.left, check.right, check.config)
		for index := range check.want {
			if math.IsInf(check.want[index], 1) {
				require.True(t, math.IsInf(got[index], 1), "%v %v %d", check, got, index)
				continue
			}
			require.InDelta(t, check.want[index], got[index], 1e-12, "%v %v %d", check, got, index)
		}
	}

	// A parallel relationship counts again in a degree, a self-loop once; a
	// neighbour counts once; a node and itself share no neighbours.
	run("MATCH (a {n:'a'}), (b {n:'b'}) CREATE (a)-[:K]->(b), (b)-[:K]->(b)")
	require.InDeltaSlice(t, []float64{2, 1.531574161186449, 0.5333333333333333, 12, 4}, scores("a", "d", ""), 1e-12)
	require.InDeltaSlice(t, []float64{2, 1.631586747071319, 0.5833333333333333, 15, 4}, scores("b", "c", ""), 1e-12)
	require.InDeltaSlice(t, []float64{0, 0, 0, 16, 3}, scores("a", "a", ""), 1e-12)
	require.InDeltaSlice(t, []float64{0, 0, 0, 25, 3}, scores("b", "b", ""), 1e-12)

	run("MATCH (a {n:'a'}), (b {n:'b'}) SET a.community = 1, b.community = 1, b.grp = 2")
	require.Equal(t, [][]interface{}{{1.0, 0.0, 0.0, 0.0, 1.0}}, run(`MATCH (a {n:'a'}), (b {n:'b'}), (c {n:'c'})
		RETURN gds.alpha.linkprediction.sameCommunity(a, b), gds.alpha.linkprediction.sameCommunity(a, c),
		gds.alpha.linkprediction.sameCommunity(a, b, 'grp'), gds.alpha.linkprediction.sameCommunity(c, c),
		gds.alpha.linkprediction.sameCommunity(a, a)`))
	// The row evaluator reads them alike, in a WHERE.
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (a {n:'a'}), (d {n:'d'}) WITH a, d WHERE gds.alpha.linkprediction.commonNeighbors(a, d) = 2.0 RETURN 1"))
	require.Equal(t, [][]interface{}{{nil}}, run("MATCH (a {n:'a'}) RETURN gds.alpha.linkprediction.commonNeighbors(a, null)"))

	// gds.util, as GDS answers: isInfinite is true for anything not finite.
	require.Equal(t, [][]interface{}{{false, true, false, true, true, true, false, false}}, run(`RETURN
		gds.util.isFinite(null), gds.util.isInfinite(null), gds.util.isFinite(gds.util.NaN()), gds.util.isInfinite(gds.util.NaN()),
		gds.util.isFinite(1), gds.util.isInfinite(gds.util.infinity()), gds.util.isFinite(-gds.util.infinity()), gds.util.isInfinite(2.5)`))
	require.Equal(t, [][]interface{}{{"a", nil, int64(1), []interface{}{}}}, run(`MATCH (n {n:'a'})
		RETURN gds.util.asNode(id(n)).n, gds.util.asNode('nope'), size(gds.util.asNodes([id(n), 'nope'])), gds.util.asNodes([])`))

	// gds.version(): the function and GDS's gdsVersion column.
	require.Equal(t, run("CALL gds.version() YIELD gdsVersion RETURN gdsVersion"), run("RETURN gds.version()"))

	// SHOW FUNCTIONS lists them as Neo4j with GDS does.
	require.Equal(t, [][]interface{}{{"gds.alpha.linkprediction.adamicAdar(node1 :: NODE, node2 :: NODE, config = {} :: MAP) :: FLOAT", false}},
		run("SHOW FUNCTIONS YIELD name, signature, isBuiltIn WHERE name = 'gds.alpha.linkprediction.adamicAdar' RETURN signature, isBuiltIn"))
}

// GDS functions' argument errors, a null node and a context without
// storage, through the function registry both evaluators call.
func TestGDSFunctionArgumentErrors(t *testing.T) {
	errArgument := errors.New("argument failed")
	call := func(name string, values ...interface{}) (interface{}, error) {
		args := make([]string, len(values))
		for index := range values {
			args[index] = strconv.Itoa(index)
		}
		store := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = store.Close() })
		value, found, err := cypherfn.EvaluateFunction(name, args, cypherfn.Context{Eval: func(expression string) (interface{}, error) {
			index, _ := strconv.Atoi(expression)
			if values[index] == errArgument {
				return nil, errArgument
			}
			return values[index], nil
		}})
		require.True(t, found, name)
		return value, err
	}
	node := &storage.Node{ID: "n", Properties: map[string]interface{}{}}
	for _, check := range []struct {
		name   string
		values []interface{}
		err    string
	}{
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{node}, "Insufficient parameters"},
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{node, errArgument}, "argument failed"},
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{"x", node}, "expects a node as argument 1"},
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{node, node, int64(1)}, "expects a config map"},
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{node, node, map[string]interface{}{"relationshipQuery": int64(1)}}, "relationshipQuery must be"},
		{"gds.alpha.linkprediction.commonNeighbors", []interface{}{node, node, map[string]interface{}{"direction": int64(1)}}, "direction must be"},
		{"gds.alpha.linkprediction.sameCommunity", []interface{}{node}, "Insufficient parameters"},
		{"gds.alpha.linkprediction.sameCommunity", []interface{}{node, errArgument}, "argument failed"},
		{"gds.alpha.linkprediction.sameCommunity", []interface{}{node, node, int64(1)}, "expects a property name"},
		{"gds.util.asnode", nil, "Insufficient parameters"},
		{"gds.util.asnode", []interface{}{errArgument}, "argument failed"},
		{"gds.util.asnodes", []interface{}{nil, nil}, "Too many parameters"},
		{"gds.util.asnodes", []interface{}{errArgument}, "argument failed"},
		{"gds.util.asnodes", []interface{}{int64(1)}, "expects a list of node ids"},
		{"gds.util.isfinite", nil, "Insufficient parameters"},
		{"gds.version", []interface{}{int64(1)}, "Too many parameters"},
		{"gds.util.nan", []interface{}{int64(1)}, "Too many parameters"},
		{"gds.util.infinity", []interface{}{int64(1)}, "Too many parameters"},
		{"gds.util.isfinite", []interface{}{errArgument}, "argument failed"},
		{"gds.util.isinfinite", []interface{}{"x"}, "expected a number"},
	} {
		_, err := call(check.name, check.values...)
		require.ErrorContains(t, err, check.err, check.name)
	}
	// A null node gives null; no storage gives no neighbours and no node.
	for _, name := range []string{"gds.alpha.linkprediction.commonNeighbors", "gds.alpha.linkprediction.sameCommunity"} {
		value, err := call(name, node, nil)
		require.NoError(t, err)
		require.Nil(t, value)
	}
	value, err := call("gds.alpha.linkprediction.preferentialAttachment", node, node)
	require.NoError(t, err)
	require.Equal(t, 0.0, value)
	value, err = call("gds.util.asnode", "n")
	require.NoError(t, err)
	require.Nil(t, value)
	value, err = call("gds.util.asnodes", nil)
	require.NoError(t, err)
	require.Nil(t, value)
}

// failingEdgeEngine fails the relationship reads a test switches on.
type failingEdgeEngine struct {
	storage.Engine
	failOutgoing, failIncoming bool
	// only limits the failures to one node's reads, when set.
	only storage.NodeID
}

var errEdgeRead = errors.New("edge read failed")

func (e *failingEdgeEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.failOutgoing && (e.only == "" || e.only == id) {
		return nil, errEdgeRead
	}
	return e.Engine.GetOutgoingEdges(id)
}

func (e *failingEdgeEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.failIncoming && (e.only == "" || e.only == id) {
		return nil, errEdgeRead
	}
	return e.Engine.GetIncomingEdges(id)
}

// A failed relationship read, in either direction, is the statement's error,
// not an empty neighbourhood.
func TestGDSLinkPredictionReportsFailedReads(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gds_failed_reads")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ac", StartNode: "a", EndNode: "c", Type: "K"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "K"}))
	a, err := store.GetNode("a")
	require.NoError(t, err)
	b, err := store.GetNode("b")
	require.NoError(t, err)
	engine := &failingEdgeEngine{Engine: store}
	call := func(name string) (interface{}, error) {
		values := []interface{}{a, b}
		value, _, err := cypherfn.EvaluateFunction(name, []string{"0", "1"}, cypherfn.Context{Storage: engine, Eval: func(expression string) (interface{}, error) {
			index, _ := strconv.Atoi(expression)
			return values[index], nil
		}})
		return value, err
	}
	value, err := call("gds.alpha.linkprediction.adamicAdar")
	require.NoError(t, err)
	require.InDelta(t, 1/math.Log(2), value, 1e-12)
	// Each direction, and each node read: the first, the second, a common
	// neighbour (whose degree Adamic-Adar reads).
	for _, failure := range []struct {
		outgoing, incoming bool
		only               storage.NodeID
		name               string
	}{
		{true, false, "", "gds.alpha.linkprediction.commonNeighbors"},
		{false, true, "", "gds.alpha.linkprediction.commonNeighbors"},
		{true, false, "b", "gds.alpha.linkprediction.commonNeighbors"},
		{false, true, "c", "gds.alpha.linkprediction.adamicAdar"},
	} {
		engine.failOutgoing, engine.failIncoming, engine.only = failure.outgoing, failure.incoming, failure.only
		_, err := call(failure.name)
		require.ErrorIs(t, err, errEdgeRead, "%+v", failure)
	}

	// An empty argument list may arrive as one empty argument.
	value, _, err = cypherfn.EvaluateFunction("gds.version", []string{""}, cypherfn.Context{Eval: func(string) (interface{}, error) { return nil, nil }})
	require.NoError(t, err)
	require.Equal(t, gdsVersion, value)
}
