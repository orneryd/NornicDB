package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestListExpressionsAreNotListLiterals pins expressions that start with '['
// and end with ']' without being one list literal ([1,0,0] + [],
// ['a','b'][0..1], [[0,1,0],[1,0,0]][1], ['it\'s', 'b'][1]) through every
// reader that decodes list literal text: the cosine query vector, the vector
// index query, UNWIND, list quantifiers, SET +=, the expression evaluator,
// IN lists, pattern comprehensions and entity lists. The expected rows are
// Neo4j 5.26's.
func TestListExpressionsAreNotListLiterals(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"cosine concatenation", "MATCH (n:V) WITH n, vector.similarity.cosine(n.emb, [1,0,0] + []) AS score RETURN n.id AS id, score ORDER BY score DESC LIMIT 1", "[[a 1]]"},
		{"cosine slice", "MATCH (n:V) WITH n, vector.similarity.cosine(n.emb, [1,0,0,9][0..3]) AS score RETURN n.id AS id, score ORDER BY score DESC LIMIT 1", "[[a 1]]"},
		{"cosine subscript", "MATCH (n:V) WITH n, vector.similarity.cosine(n.emb, [[0,1,0],[1,0,0]][1]) AS score RETURN n.id AS id, score ORDER BY score DESC LIMIT 1", "[[a 1]]"},
		{"unwind concatenation of strings", "UNWIND ['x'] + ['y'] AS v RETURN collect(v)", "[[[x y]]]"},
		{"unwind concatenation of numbers", "UNWIND [1, 2] + [3] AS v RETURN collect(v)", "[[[1 2 3]]]"},
		{"unwind subscript", "UNWIND [[1,2],[3]][1] AS v RETURN collect(v)", "[[[3]]]"},
		{"unwind slice", "UNWIND ['a','b'][0..1] AS v RETURN collect(v)", "[[[a]]]"},
		{"any over concatenation", "RETURN any(x IN ['a'] + ['b'] WHERE x = 'b')", "[[true]]"},
		{"none over concatenation", "RETURN none(x IN ['a'] + ['b'] WHERE x = 'b')", "[[false]]"},
		{"all over slice", "RETURN all(x IN [1, 2][0..1] WHERE x = 1)", "[[true]]"},
		{"set map subscript", "MATCH (n:R {id:'b0'}) SET n += {tag: ['x','y'][1]} RETURN n.tag", "[[y]]"},
		{"set map concatenation", "MATCH (n:R {id:'b0'}) SET n += {tags: ['x'] + ['y']} RETURN n.tags", "[[[x y]]]"},
		{"set map slice", "MATCH (n:R {id:'b0'}) SET n += {tags: [1,2,3][0..2]} RETURN n.tags", "[[[1 2]]]"},
		{"escaped quote subscript", `RETURN ['it\'s', 'b'][1]`, "[[b]]"},
		{"escaped quote subscript over a row", `MATCH (n:R {id:'b0'}) RETURN ['it\'s', n.id][1]`, "[[b0]]"},
		{"escaped quote concatenation", `RETURN ['a\'b'] + ['c']`, "[[[a'b c]]]"},
		{"cartesian in concatenation", "MATCH (a:R), (b:X) WHERE a.id IN ['b1'] + ['b21'] RETURN count(*)", "[[4]]"},
		{"in concatenation", "MATCH (n:R) WHERE n.id IN ['b1'] + ['b21'] RETURN count(n)", "[[2]]"},
		{"in slice", "MATCH (n:R) WHERE n.id IN ['b0','b1','b21'][1..3] RETURN count(n)", "[[2]]"},
		{"row in concatenation", "UNWIND ['b0','b1','b21'] AS id WITH id WHERE id IN ['b1'] + ['b21'] RETURN count(*)", "[[2]]"},
		{"in concatenation with escaped quote", `MATCH (n:R) WHERE n.id IN ['b1', 'it\'s'] + ['b21'] RETURN count(n)`, "[[2]]"},
		{"pattern comprehension concatenation", "MATCH (n:R {id:'b1'}) RETURN [(n)-->(x:X) | 1] + [2]", "[[[1 2]]]"},
		{"pattern comprehension subscript", "MATCH (n:R {id:'b1'}) RETURN [(n)-->(x:X) | 1][0]", "[[1]]"},
		{"entity list subscript", "MATCH (a:R {id:'b1'})-[r]->(b) WITH [a, b][1] AS x RETURN labels(x)", "[[[X]]]"},
	}
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			name := "scan"
			if indexed {
				name = "index"
			}
			if explicit {
				name += "/explicit transaction"
			} else {
				name += "/autocommit"
			}
			t.Run(name, func(t *testing.T) {
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						exec := newListLiteralReaderExecutor(t, indexed)
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := exec.Execute(ctx, tc.query, nil)
						require.NoError(t, err)
						require.Equal(t, tc.want, fmt.Sprint(result.Rows))
						if explicit {
							_, err := exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, err)
						}
					})
				}
			})
		}
	}
	// The vector index query reads its query vector itself.
	for _, query := range []string{
		"CALL db.index.vector.queryNodes('v_emb', 1, [1,0,0] + []) YIELD node, score RETURN node.id, score",
		"CALL db.index.vector.queryNodes('v_emb', 1, [[0,1,0],[1,0,0]][1]) YIELD node, score RETURN node.id, score",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := newListLiteralReaderExecutor(t, true).Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, "[[a 1]]", fmt.Sprint(result.Rows))
		})
	}
}

// TestSpatialListExpressionsAreNotListLiterals pins polygon() and
// lineString() over a list expression: every point of the evaluated list
// counts. They are NornicDB functions; Neo4j has no equivalent.
func TestSpatialListExpressionsAreNotListLiterals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "spatial_list_expressions"))
	for query, points := range map[string]int{
		"RETURN polygon([point({x: 0, y: 0}), point({x: 1, y: 0})] + [point({x: 0, y: 1})])": 3,
		"RETURN lineString([point({x: 0, y: 0})] + [point({x: 1, y: 0})])":                    2,
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		shape, ok := result.Rows[0][0].(map[string]interface{})
		require.True(t, ok, "%s: %#v", query, result.Rows[0][0])
		require.Len(t, shape["points"], points, query)
	}
}

func newListLiteralReaderExecutor(t *testing.T, indexed bool) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_literal_readers"))
	ctx := context.Background()
	if indexed {
		for _, query := range []string{
			"CREATE INDEX r_id FOR (n:R) ON (n.id)",
			"CREATE VECTOR INDEX v_emb FOR (n:V) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
		}
	}
	_, err := exec.Execute(ctx, "CREATE (:R {id:'b0'}), (:R {id:'b1'})-[:T {w: 1}]->(:X), (:R {id:'b21'})-[:T {w: 21}]->(:X), (:V {id: 'a', emb: [1.0, 0.0, 0.0]}), (:V {id: 'b', emb: [0.0, 1.0, 0.0]})", nil)
	require.NoError(t, err)
	return exec
}

// ownListLiteralCheck matches a function body that decides "list literal"
// itself: starts with '[' and ends with ']' (in either spelling), or strips
// one '[' and one ']'.
var ownListLiteralCheck = []*regexp.Regexp{
	regexp.MustCompile(`HasPrefix\([^)]*"\["\).*HasSuffix\([^)]*"\]"\)`),
	regexp.MustCompile(`HasSuffix\([^)]*"\]"\).*HasPrefix\([^)]*"\["\)`),
	regexp.MustCompile(`\[0\]\s*[!=]=\s*'\['.*\[len\([^)]*\)\s*-\s*1\]\s*[!=]=\s*'\]'`),
	regexp.MustCompile(`Trim\([^)]*"\[\]"\)`),
	regexp.MustCompile(`(?s)TrimPrefix\([^)]*"\["\).*TrimSuffix\([^)]*"\]"\)`),
}

func hasOwnListLiteralCheck(body string) bool {
	for _, pattern := range ownListLiteralCheck {
		if pattern.MatchString(body) {
			return true
		}
	}
	return false
}

// TestListLiteralReadersUseIsListLiteral fails when a function of this
// package checks for list literal text on its own instead of asking
// isListLiteral, whose '[' must close at the end: such a check took
// ['a', 'b'][i] or [1] + [2] for one list.
func TestListLiteralReadersUseIsListLiteral(t *testing.T) {
	for _, shape := range []string{
		`if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {`,
		`if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {`,
		`if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {`,
		"s = strings.TrimPrefix(s, \"[\")\n\ts = strings.TrimSuffix(s, \"]\")",
		`s = strings.Trim(s, "[]")`,
	} {
		require.True(t, hasOwnListLiteralCheck(shape), shape)
	}
	require.False(t, hasOwnListLiteralCheck(`if isListLiteral(s) {`))

	// Brackets that are no list literal.
	allowed := map[string]string{
		"traversal.go parseRelationshipPattern": "a relationship bracket [r:TYPE {props}]",
		"node_helpers.go applyArraySuffix":      "a subscript or slice suffix after an aggregate",
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fileSet := token.NewFileSet()
	seen := map[string]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		parsed, err := parser.ParseFile(fileSet, file, source, 0)
		require.NoError(t, err)
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			body := string(source[fileSet.Position(function.Body.Pos()).Offset:fileSet.Position(function.Body.End()).Offset])
			if !hasOwnListLiteralCheck(body) {
				continue
			}
			name := file + " " + function.Name.Name
			if _, ok := allowed[name]; ok {
				seen[name] = true
				continue
			}
			t.Errorf("%s checks for a list literal itself; use isListLiteral", name)
		}
	}
	for name := range allowed {
		require.True(t, seen[name], "%s no longer has a bracket check: drop it from the allowed list", name)
	}
}
