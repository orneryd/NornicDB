package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// gdsNodeFamilyFile reports whether a pkg/cypher source file belongs to the
// GDS and similarity family whose node handling gds_node_identity.go owns.
func gdsNodeFamilyFile(name string) bool {
	return strings.HasPrefix(name, "gds_") || name == "fastrp.go" || name == "linkprediction.go"
}

// gdsNodeTypeTestAllowed are the family functions that test a value's node
// type for something other than its identity, with the reason.
var gdsNodeTypeTestAllowed = map[string]string{
	"gdsJavaType": "names a value's type for an error message",
}

// TestGDSNodeIdentityOwner: every GDS and similarity procedure and function
// is listed in gdsNodeProcedures / gdsNodeFunctions (and nothing else is),
// every stream that returns node ids renders them through the owner, and no
// family source outside gds_node_identity.go tests a value for a node type,
// so a new procedure or reader can't bypass the one reader unnoticed.
func TestGDSNodeIdentityOwner(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	family := func(name string) bool {
		return strings.HasPrefix(name, "gds.") || strings.HasPrefix(name, "nornicdb.similarity.")
	}
	registered := map[string]bool{}
	for _, spec := range ListRegisteredProcedures() {
		if !family(spec.Name) {
			continue
		}
		registered[spec.Name] = true
		entry, ok := gdsNodeProcedures[spec.Name]
		require.True(t, ok, "%s is not in gdsNodeProcedures", spec.Name)
		// A stream with node-id columns renders them through the owner, and
		// declares each as ANY (an id may be an INTEGER or a STRING) unless
		// it only ever shows strings.
		returns := map[string]string{}
		for _, column := range spec.Returns {
			returns[column.Name] = column.Type
		}
		for _, column := range entry.columns {
			require.Contains(t, returns, column, "%s has no %s column", spec.Name, column)
			if entry.rendering == gdsRenderProjection && column != "shared" {
				require.Equal(t, "ANY", returns[column], "%s.%s can be an INTEGER or a STRING", spec.Name, column)
			}
		}
		if len(entry.columns) > 0 {
			require.NotEqual(t, gdsRenderNone, entry.rendering, spec.Name)
		}
	}
	for name := range gdsNodeProcedures {
		require.True(t, registered[name], "gdsNodeProcedures lists %s, which is not registered", name)
	}
	catalog := map[string]bool{}
	for _, spec := range cypherFunctionCatalog {
		if strings.HasPrefix(spec.name, "gds.") {
			catalog[spec.name] = true
			require.Contains(t, gdsNodeFunctions, spec.name, "%s is not in gdsNodeFunctions", spec.name)
		}
	}
	for name := range gdsNodeFunctions {
		require.True(t, catalog[name], "gdsNodeFunctions lists %s, which is not in the function catalog", name)
	}

	// Source: no family file but the owner tests a value for *storage.Node
	// or storage.RemoteEntityValue, outside the allowed functions.
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var offenders []string
	for _, path := range files {
		name := filepath.Base(path)
		if strings.HasSuffix(name, "_test.go") || !gdsNodeFamilyFile(name) || name == "gds_node_identity.go" {
			continue
		}
		source, err := os.ReadFile(path)
		require.NoError(t, err)
		file, err := parser.ParseFile(fset, path, source, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if _, allowed := gdsNodeTypeTestAllowed[fn.Name.Name]; allowed {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				var types []ast.Expr
				switch node := n.(type) {
				case *ast.TypeAssertExpr:
					types = append(types, node.Type)
				case *ast.CaseClause:
					types = append(types, node.List...)
				}
				for _, expr := range types {
					if text := gdsExprText(expr); text == "*storage.Node" || text == "storage.RemoteEntityValue" {
						offenders = append(offenders, fmt.Sprintf("%s: %s tests for %s", fset.Position(expr.Pos()), fn.Name.Name, text))
					}
				}
				return true
			})
		}
	}
	require.Empty(t, offenders, "node identity is read only by readGDSNode (gds_node_identity.go)")
}

// gdsExprText is a type expression's source text, for the simple forms
// the owner test looks for.
func gdsExprText(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return "*" + gdsExprText(e.X)
	case *ast.SelectorExpr:
		return gdsExprText(e.X) + "." + e.Sel.Name
	case *ast.Ident:
		return e.Name
	}
	return ""
}

// gdsNodeInputCase is one node-valued input, run with a value of each kind.
type gdsNodeInputCase struct {
	query string
	// nullWhenRefused: the input refuses a kind by giving null, not an
	// error (gds.util.asNode/asNodes).
	nullWhenRefused bool
}

// TestGDSNodeInputKinds: every node-valued input of every GDS and
// similarity procedure and function, given a value of each kind, accepts
// exactly the kinds gdsNodeProcedures / gdsNodeFunctions list for it. A
// listed input with no case here fails the test.
func TestGDSNodeInputKinds(t *testing.T) {
	gdsFollowUpCatalog(t)
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "kinds")
	// "1" is a stored id too, so gds.util.asNode(1) finds a node.
	for _, id := range []string{"a", "b", "c", "1"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}, Properties: map[string]interface{}{"community": int64(1)}})
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	run := func(query string, params map[string]interface{}) (*ExecuteResult, error) {
		return exec.Execute(ctx, query, params)
	}
	nodeOf := func(id string) *storage.Node {
		node, err := engine.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		return node
	}
	// One graph holding stored nodes and INTEGER ids, so a node value, an id
	// string and an INTEGER each name a node of it.
	_, err := run("MATCH (a:P) WHERE id(a) = 'a' MATCH (b:P) WHERE id(b) = 'b' "+
		"UNWIND [[a, b], [b, 1], [1, 2], [a, 2]] AS e "+
		"RETURN gds.graph.project('kinds', e[0], e[1], {sourceNodeProperties: {w: 1.0}, targetNodeProperties: {w: 1.0}}, {undirectedRelationshipTypes: ['*']})", nil)
	require.NoError(t, err)

	values := map[gdsNodeKind]interface{}{
		gdsLocalNode:  nodeOf("a"),
		gdsRemoteNode: storage.RemoteEntityValue{"elementId": "4:remote:a", "labels": []interface{}{"P"}, "properties": map[string]interface{}{}},
		gdsIDText:     "a",
		gdsIntegerID:  int64(1),
	}
	remoteRelationship := storage.RemoteEntityValue{"elementId": "5:remote:r", "type": "R", "properties": map[string]interface{}{}}
	rows := []interface{}{map[string]interface{}{"source": "a", "thing": "x"}, map[string]interface{}{"source": "b", "thing": "x"}}

	linkPrediction := func(name string) map[string]gdsNodeInputCase {
		return map[string]gdsNodeInputCase{
			"sourceNode":  {query: "CALL gds.linkPrediction." + name + ".stream('kinds', {sourceNode: $v}) YIELD node2 RETURN count(*)"},
			"targetNodes": {query: "CALL gds.linkPrediction." + name + ".stream('kinds', {sourceNode: 'b', targetNodes: [$v]}) YIELD node2 RETURN count(*)"},
		}
	}
	alpha := func(name string) map[string]gdsNodeInputCase {
		return map[string]gdsNodeInputCase{
			"node1": {query: "MATCH (b:P) WHERE id(b) = 'b' RETURN gds.alpha.linkprediction." + name + "($v, b)"},
			"node2": {query: "MATCH (b:P) WHERE id(b) = 'b' RETURN gds.alpha.linkprediction." + name + "(b, $v)"},
		}
	}
	procedures := map[string]map[string]gdsNodeInputCase{
		"gds.nodeSimilarity.filtered.stream": {
			"sourceNodeFilter": {query: "CALL gds.nodeSimilarity.filtered.stream('kinds', {sourceNodeFilter: $v}) YIELD node1 RETURN count(*)"},
			"targetNodeFilter": {query: "CALL gds.nodeSimilarity.filtered.stream('kinds', {targetNodeFilter: $v}) YIELD node1 RETURN count(*)"},
		},
		"nornicdb.similarity.stream": {
			"sourceNode":  {query: "CALL nornicdb.similarity.stream($rows, {sourceNode: $v}) YIELD node2 RETURN count(*)"},
			"rows.source": {query: "CALL nornicdb.similarity.stream([{source: $v, thing: 'x'}, {source: 'b', thing: 'x'}], {sourceNode: 'b'}) YIELD node2 RETURN count(*)"},
			"rows.thing":  {query: "CALL nornicdb.similarity.stream([{source: 'a', thing: $v}, {source: 'b', thing: $v}], {sourceNode: 'b'}) YIELD node2 RETURN count(*)"},
		},
	}
	for _, name := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
		procedures["gds.linkPrediction."+name+".stream"] = linkPrediction(name)
	}
	functions := map[string]map[string]gdsNodeInputCase{
		"gds.graph.project": {
			"sourceNode": {query: "RETURN gds.graph.project($name, $v, null).graphName"},
			"targetNode": {query: "RETURN gds.graph.project($name, 7, $v).graphName"},
		},
		"gds.util.asNode":       {"nodeId": {query: "RETURN gds.util.asNode($v) IS NOT NULL", nullWhenRefused: true}},
		"gds.util.asNodes":      {"nodeIds": {query: "RETURN size(gds.util.asNodes([$v])) = 1", nullWhenRefused: true}},
		"gds.util.nodeProperty": {"nodeId": {query: "RETURN gds.util.nodeProperty('kinds', $v, 'w')"}},
	}
	for _, name := range []string{"adamicAdar", "commonNeighbors", "preferentialAttachment", "resourceAllocation", "totalNeighbors", "sameCommunity"} {
		functions["gds.alpha.linkprediction."+name] = alpha(name)
	}

	check := func(owner string, inputs map[string]gdsNodeKind, cases map[string]gdsNodeInputCase) {
		require.Len(t, cases, len(inputs), "%s: a case per node-valued input", owner)
		for input, accepted := range inputs {
			c, ok := cases[input]
			require.True(t, ok, "%s.%s has no case", owner, input)
			kinds := []gdsNodeKind{gdsLocalNode, gdsRemoteNode, gdsIDText, gdsIntegerID}
			for _, kind := range kinds {
				graph := fmt.Sprintf("kinds_%s_%d", strings.ReplaceAll(input, ".", "_"), kind)
				params := map[string]interface{}{"v": values[kind], "rows": rows, "name": graph}
				result, err := run(c.query, params)
				label := fmt.Sprintf("%s.%s kind %d", owner, input, kind)
				if accepted&kind != 0 {
					require.NoError(t, err, label)
					if c.nullWhenRefused {
						require.Equal(t, true, result.Rows[0][0], label)
					}
				} else if c.nullWhenRefused {
					require.NoError(t, err, label)
					require.NotEqual(t, true, result.Rows[0][0], label)
				} else {
					require.Error(t, err, label)
				}
			}
			// A remote relationship is no node anywhere.
			params := map[string]interface{}{"v": remoteRelationship, "rows": rows, "name": "kinds_relationship"}
			result, err := run(c.query, params)
			if c.nullWhenRefused {
				require.NoError(t, err)
				require.NotEqual(t, true, result.Rows[0][0], "%s.%s remote relationship", owner, input)
			} else {
				require.Error(t, err, "%s.%s remote relationship", owner, input)
			}
		}
	}
	for name, entry := range gdsNodeProcedures {
		check(name, entry.inputs, procedures[name])
	}
	for name, inputs := range gdsNodeFunctions {
		check(name, inputs, functions[name])
	}
}

// TestGDSNodeValueNamesOnlyItsKind: wherever an input accepts both a stored
// node and a remote node, a stored node absent from the input's nodes never
// names a remote node with the same id, while an id string does (the
// review finding on #1386, for every such input).
func TestGDSNodeValueNamesOnlyItsKind(t *testing.T) {
	both := 0
	for name, entry := range gdsNodeProcedures {
		for input, kinds := range entry.inputs {
			if kinds&gdsLocalNode == 0 || kinds&gdsRemoteNode == 0 {
				continue
			}
			both++
			require.Equal(t, "nornicdb.similarity.stream", name, "a new input taking both kinds needs a case here: %s.%s", name, input)
		}
	}
	require.Equal(t, 3, both)
	remote := storage.RemoteEntityValue{"elementId": "a", "labels": []interface{}{"Doc"}, "properties": map[string]interface{}{}}
	local := &storage.Node{ID: "a"}
	scores := func(rows []interface{}, sourceNode interface{}) [][]interface{} {
		t.Helper()
		result, err := callNornicSimilarityStream([]interface{}{rows, map[string]interface{}{"sourceNode": sourceNode}})
		require.NoError(t, err)
		return result.Rows
	}
	// sourceNode.
	remoteRows := []interface{}{map[string]interface{}{"source": remote, "thing": "x"}, map[string]interface{}{"source": "b", "thing": "x"}}
	require.Empty(t, scores(remoteRows, local))
	require.Empty(t, scores(remoteRows, []interface{}{local}))
	require.Len(t, scores(remoteRows, "a"), 1)
	// rows.source and rows.thing: a stored node and a remote node with one
	// id are two sources, and two things.
	mixed := []interface{}{
		map[string]interface{}{"source": local, "thing": "x"}, map[string]interface{}{"source": remote, "thing": "y"},
		map[string]interface{}{"source": "b", "thing": local}, map[string]interface{}{"source": "c", "thing": remote},
	}
	require.Empty(t, scores(mixed, "b"), "b holds the stored node, c the remote one")
	rowsOf := scores(mixed, local)
	require.Empty(t, rowsOf, "the stored node shares nothing with the remote one")
}

// TestGDSNodeOutputKinds: every stream that returns node ids, over a native
// projection, an aggregation projection of stored nodes and INTEGER ids in
// both row orders, and rows of local and remote nodes, returns each id as
// the kind it was given as: a stored id as a STRING, an INTEGER as an
// INTEGER, a remote node as its elementId; and the row order changes
// nothing.
func TestGDSNodeOutputKinds(t *testing.T) {
	gdsFollowUpCatalog(t)
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "outputs")
	for _, id := range []string{"a", "b", "c", "d"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"P"}})
		require.NoError(t, err)
	}
	for _, edge := range [][2]string{{"a", "b"}, {"b", "c"}, {"c", "a"}, {"c", "d"}} {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(edge[0] + edge[1]), StartNode: storage.NodeID(edge[0]), EndNode: storage.NodeID(edge[1]), Type: "R"}))
	}
	exec := NewStorageExecutor(engine)
	ctx := context.Background()
	run := func(query string) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	run("CALL gds.graph.project('native', 'P', {R: {orientation: 'UNDIRECTED'}})")
	pairs := map[string]string{
		"mixedNodeFirst":    "[[a, b], [b, 1], [1, 2], [2, a], [b, 2]]",
		"mixedIntegerFirst": "[[2, a], [1, 2], [b, 1], [a, b], [b, 2]]",
	}
	for name, list := range pairs {
		run("MATCH (a:P) WHERE id(a) = 'a' MATCH (b:P) WHERE id(b) = 'b' UNWIND " + list + " AS e " +
			"RETURN gds.graph.project('" + name + "', e[0], e[1], {}, {undirectedRelationshipTypes: ['*']})")
	}
	queries := map[string]func(graph, source string) string{
		"gds.fastRP.stream": func(g, _ string) string {
			return "CALL gds.fastRP.stream('" + g + "', {embeddingDimension: 4, randomSeed: 1}) YIELD nodeId RETURN nodeId"
		},
		"gds.louvain.stream": func(g, _ string) string { return "CALL gds.louvain.stream('" + g + "') YIELD nodeId RETURN nodeId" },
		"gds.nodeSimilarity.stream": func(g, _ string) string {
			return "CALL gds.nodeSimilarity.stream('" + g + "') YIELD node1, node2, shared RETURN node1, node2, shared"
		},
		"gds.nodeSimilarity.filtered.stream": func(g, s string) string {
			return "CALL gds.nodeSimilarity.filtered.stream('" + g + "', {sourceNodeFilter: [" + s + "]}) YIELD node1, node2, shared RETURN node1, node2, shared"
		},
	}
	for _, name := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
		name := name
		queries["gds.linkPrediction."+name+".stream"] = func(g, s string) string {
			return "CALL gds.linkPrediction." + name + ".stream('" + g + "', {sourceNode: " + s + ", topK: 10}) YIELD node1, node2 RETURN node1, node2"
		}
	}
	for name, entry := range gdsNodeProcedures {
		if entry.rendering == gdsRenderProjection {
			require.Contains(t, queries, name, "%s returns node ids and has no case here", name)
		}
	}
	// The kind each id shows as: native ids are stored STRING ids; in the
	// mixed graphs a and b were given as nodes, 1 and 2 as INTEGERs.
	want := func(value interface{}, native bool) {
		t.Helper()
		switch id := value.(type) {
		case string:
			if native {
				require.Contains(t, []string{"a", "b", "c", "d"}, id)
			} else {
				require.Contains(t, []string{"a", "b"}, id)
			}
		case int64:
			require.False(t, native, "a native projection's id %d", id)
			require.Contains(t, []int64{1, 2}, id)
		default:
			t.Fatalf("an id of type %T: %v", value, value)
		}
	}
	for name, query := range queries {
		ordered := map[string][]string{}
		for _, graph := range []string{"native", "mixedNodeFirst", "mixedIntegerFirst"} {
			native := graph == "native"
			source := "'a'"
			if !native {
				source = "1"
			}
			rows := run(query(graph, source))
			require.NotEmpty(t, rows, "%s over %s", name, graph)
			var texts []string
			for _, row := range rows {
				for _, value := range row {
					if list, isList := value.([]interface{}); isList {
						for _, entry := range list {
							want(entry.(map[string]interface{})["nodeId"], native)
						}
						continue
					}
					if _, isID := gdsNodeIDColumnValue(value); isID {
						want(value, native)
					}
				}
				texts = append(texts, fmt.Sprintf("%#v", row))
			}
			sort.Strings(texts)
			ordered[graph] = texts
		}
		if name != "gds.fastRP.stream" {
			require.Equal(t, ordered["mixedNodeFirst"], ordered["mixedIntegerFirst"], "%s: row order changes the ids", name)
		}
	}

	// Rows: a local id shows as itself, a remote node as its elementId, and
	// a node1 list shows each.
	remote := storage.RemoteEntityValue{"elementId": "4:remote:r", "labels": []interface{}{"P"}, "properties": map[string]interface{}{}}
	result, err := callNornicSimilarityStream([]interface{}{[]interface{}{
		map[string]interface{}{"source": "a", "thing": remote}, map[string]interface{}{"source": remote, "thing": remote},
		map[string]interface{}{"source": "b", "thing": remote},
	}, map[string]interface{}{"sourceNode": []interface{}{"a", "4:remote:r"}}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"4:remote:r", "a"}, "b", 1.0, []interface{}{map[string]interface{}{"nodeId": "4:remote:r", "weight": 1.0}}}}, result.Rows)
}

// gdsNodeIDColumnValue reports whether a stream cell holds a node id rather
// than a score or embedding.
func gdsNodeIDColumnValue(value interface{}) (interface{}, bool) {
	switch value.(type) {
	case string, int64:
		return value, true
	}
	return nil, false
}
