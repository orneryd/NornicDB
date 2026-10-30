package cypher

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

// Author: #1640 lane (Codex). These are the existing execution-option
// predecessors, not a new set of admitted forms. The grammar-owner census
// below makes a new eligible predecessor require a public contract case.
var boundOptionPredecessors = []struct {
	kind, update string
}{
	{"pipelineClauseCreate", "CREATE (n:BoundOption {content:'hello'})"},
	{"pipelineClauseMerge", "MERGE (n:BoundOption {content:'hello'})"},
	{"pipelineClauseSet", "CREATE (n:BoundOption) SET n.content='hello'"},
	{"pipelineClauseRemove", "CREATE (n:BoundOption {content:'hello', scratch:1}) REMOVE n.scratch"},
	{"pipelineClauseDelete", "CREATE (n:BoundOption {content:'hello'}) DELETE n"},
	{"pipelineClauseForeach", "FOREACH (i IN [1] | CREATE (:BoundOption {content:'hello'}))"},
}

func TestCanonicalBoundExecutionOptionPopulation(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "query_body_structure.go", nil, 0)
	require.NoError(t, err)
	var admitted []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "embeddingCandidate" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			clause, ok := node.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, item := range clause.List {
				if name, ok := item.(*ast.Ident); ok && strings.HasPrefix(name.Name, "pipelineClause") {
					admitted = append(admitted, name.Name)
				}
			}
			return true
		})
	}
	var exercised []string
	for _, predecessor := range boundOptionPredecessors {
		exercised = append(exercised, predecessor.kind)
	}
	require.NotEmpty(t, admitted)
	require.ElementsMatch(t, admitted, exercised, "every syntax-owner predecessor needs the public role contract")
}

// A written WITH can be either a projection or an execution option. The
// constructor decides that once, before type enrichment, dependencies, headers
// or effects. Empty input must not change that admission or its horizons.
func TestCanonicalBoundExecutionOptionRoles(t *testing.T) {
	for _, predecessor := range boundOptionPredecessors {
		for _, declared := range []bool{false, true} {
			for _, input := range []struct {
				name, prefix string
				count        int64
			}{{"ordinary", "", 1}, {"empty", "UNWIND [] AS __role_input ", 0}, {"singleton", "UNWIND [0] AS __role_input ", 1}} {
				for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
					name := fmt.Sprintf("%s/declared=%v/%s/%s", predecessor.kind, declared, input.name, strings.TrimSpace(mode))
					t.Run(name, func(t *testing.T) {
						executor, ctx := newUnitExecutor(t)
						executor.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "bound-option", dims: 3})
						prefix, result := "", "count(*)"
						if declared {
							prefix, result = "WITH 7 AS EMBEDDING ", "EMBEDDING"
						}
						query := mode + input.prefix + prefix + predecessor.update + " WITH EMBEDDING RETURN " + result + " AS c"
						seen := 0
						executor.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
							for _, clause := range statement.Clauses {
								if !strings.EqualFold(strings.TrimSpace(query[clause.Start:clause.End]), "WITH EMBEDDING") {
									continue
								}
								seen++
								require.NotNil(t, clause.Entry)
								require.NotNil(t, clause.Exit)
								if declared {
									require.Equal(t, resultmetadata.ClauseWith, clause.Kind)
									require.NotNil(t, clause.Projection)
									clause.After.Visit(func(names []string) { require.Equal(t, []string{"EMBEDDING"}, names) }, func() { t.Fatal("unknown projected scope") })
								} else {
									require.Equal(t, resultmetadata.ClauseWrite, clause.Kind)
									require.Nil(t, clause.Projection)
									clause.Before.Visit(func(names []string) {
										clause.After.Visit(func(after []string) { require.Equal(t, names, after) }, func() { t.Fatal("unknown option output") })
										for _, name := range names {
											before, known := clause.Before.Binding(name)
											after, retained := clause.After.Binding(name)
											require.True(t, known && retained)
											require.Equal(t, before, after, "option must preserve admitted identity %s", name)
										}
									}, func() { t.Fatal("unknown option input") })
								}
							}
						}, func() { t.Fatal("declined graph statement") }, func(reason resultmetadata.Unresolved) { t.Fatalf("construction failed: %v", reason.Err) })
						require.Equal(t, 1, seen)
						actual, err := executor.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						require.Equal(t, []string{"c"}, actual.Columns)
						if mode == "EXPLAIN " || declared && input.count == 0 {
							require.Empty(t, actual.Rows)
						} else if declared {
							require.Equal(t, [][]interface{}{{int64(7)}}, actual.Rows)
						} else {
							require.Equal(t, [][]interface{}{{input.count}}, actual.Rows)
						}
					})
				}
			}
		}
	}
}
