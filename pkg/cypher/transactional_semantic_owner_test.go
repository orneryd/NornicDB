package cypher_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/transactionalcalltest"
	"github.com/stretchr/testify/require"
)

// Guard the causal invariant, not a list of successful query spellings: every
// expression reaches the shared walk, and the planner cannot grow clause paths.
func TestTransactionalSemanticOwnerCensus(t *testing.T) {
	owners := map[string]int{"planPipelineCallChain": 0, "pipelineSyntaxEffects": 0,
		"pipelineEffectVisitor": 0, "classifyTransactionalBatchExpression": 0,
		"describeScalarExpression": 0, "scalarExpressionEvaluationError": 0,
		"bindTransactionalCallChain": 0, "pipelineApplyCallSubqueryWithMetadata": 0}
	effectContexts := map[string]bool{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(name), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, tracked := owners[fn.Name.Name]; tracked {
				owners[fn.Name.Name]++
			}
			if fn.Name.Name == "pipelineEffectVisitor" {
				effectContexts = semanticEffectContexts(fn.Body)
			}
			if fn.Name.Name == "planPipelineCallChain" {
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node.(type) {
					case *ast.SwitchStmt, *ast.TypeSwitchStmt:
						t.Error("planner bypasses the shared effect contract")
					}
					return true
				})
			}
			if fn.Name.Name == "walkCypherSyntax" {
				t.Error("duplicate legacy statement traversal remains")
			}
		}
	}
	for owner, count := range owners {
		require.Equal(t, 1, count, owner)
	}
	canonical, err := parser.ParseFile(token.NewFileSet(), "antlr/statement_syntax.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	walkers := 0
	for _, declaration := range canonical.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "walkSyntax" {
			continue
		}
		walkers++
		children, recursion := 0, 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if _, returns := node.(*ast.ReturnStmt); returns {
				t.Error("canonical traversal prunes children")
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok {
					if name.Name == "children" {
						children++
					}
					if name.Name == "walkSyntax" {
						recursion++
					}
				}
			}
			return true
		})
		require.Equal(t, 1, children)
		require.Equal(t, 1, recursion)
	}
	require.Equal(t, 1, walkers, "one generic traversal owns all statement consumers")
	grammar, err := parser.ParseFile(token.NewFileSet(), "antlr/cypher_parser.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	constructors := transactionalcalltest.SyntaxConstructors()
	checked := 0
	for _, declaration := range grammar.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, specification := range group.Specs {
			spec, ok := specification.(*ast.TypeSpec)
			if !ok {
				continue
			}
			expected, tracked := constructors[spec.Name.Name]
			if !tracked {
				continue
			}
			checked++
			actual := map[string]bool{}
			for _, method := range spec.Type.(*ast.InterfaceType).Methods.List {
				fn, ok := method.Type.(*ast.FuncType)
				if !ok || fn.Results == nil {
					continue
				}
				for _, result := range fn.Results.List {
					typeOf := result.Type
					if list, ok := typeOf.(*ast.ArrayType); ok {
						typeOf = list.Elt
					}
					if context, ok := typeOf.(*ast.Ident); ok && strings.HasPrefix(context.Name, "I") && strings.HasSuffix(context.Name, "Context") {
						actual[context.Name] = true
					}
				}
			}
			require.Len(t, actual, len(expected), spec.Name.Name)
			for context, contract := range expected {
				require.True(t, actual[context], context)
				if contract == "effect" {
					require.True(t, effectContexts[strings.TrimPrefix(context, "I")], context)
				}
			}
		}
	}
	require.Equal(t, len(constructors), checked)
}

// Clause alternatives are part of the same census as expression alternatives.
// Otherwise deleting CREATE's effect case leaves the old guard green.
func TestTransactionalReviewClauseCensusMembership(t *testing.T) {
	constructors := transactionalcalltest.SyntaxConstructors()
	require.Contains(t, constructors, "IReadingStatementContext")
	require.Contains(t, constructors, "IUpdatingStatementContext")
	require.Contains(t, constructors, "ISinglePartQContext")
	require.Contains(t, constructors, "IMultiPartQContext")
	require.Contains(t, constructors, "IProjectionBodyContext")
	require.Equal(t, "effect", constructors["IUpdatingStatementContext"]["ICreateStContext"])
}

func semanticEffectContexts(body *ast.BlockStmt) map[string]bool {
	contexts := map[string]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		if branch, ok := node.(*ast.CaseClause); ok {
			for _, expression := range branch.List {
				if context, ok := expression.(*ast.SelectorExpr); ok && strings.HasPrefix(context.Sel.Name, "CypherParserRULE_") {
					name := strings.TrimPrefix(context.Sel.Name, "CypherParserRULE_")
					contexts[strings.ToUpper(name[:1])+name[1:]+"Context"] = true
				}
			}
		}
		return true
	})
	return contexts
}

func TestTransactionalReviewCensusDetectsMissingCreate(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "pipeline_clause_effects.go", nil, 0)
	require.NoError(t, err)
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "pipelineEffectVisitor" {
			continue
		}
		require.True(t, semanticEffectContexts(fn.Body)["CreateStContext"])
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if branch, ok := node.(*ast.CaseClause); ok {
				for i, expression := range branch.List {
					selector, ok := expression.(*ast.SelectorExpr)
					if ok && selector.Sel.Name == "CypherParserRULE_createSt" {
						branch.List = append(branch.List[:i], branch.List[i+1:]...)
						break
					}
				}
			}
			return true
		})
		require.False(t, semanticEffectContexts(fn.Body)["CreateStContext"])
		require.Equal(t, "effect", transactionalcalltest.SyntaxConstructors()["IUpdatingStatementContext"]["ICreateStContext"], "the census must require the removed alternative")
		return
	}
	t.Fatal("effect owner not found")
}
