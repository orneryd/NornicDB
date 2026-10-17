package cypher

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A comprehension owns what its elements built: a map's list counts in the
// comprehension, whose builder gave its own reservation back. A value the
// elements share, or one the input holds, counts once (#1343).
func TestComprehensionsCountWhatTheirElementsHold(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	big := make([]interface{}, 100000)
	for i := range big {
		big[i] = int64(i)
	}
	list := make([]interface{}, 2000)
	for i := range list {
		list[i] = []interface{}{int64(i), "element"}
	}
	for _, compiled := range []bool{true, false} {
		name := "text"
		if compiled {
			name = "compiled"
		}
		t.Run(name, func(t *testing.T) {
			rowValueProgramsDisabled.Store(!compiled)
			t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
			for _, query := range []string{
				"RETURN size([i IN range(1, 2000) | {xs: range(i, i + 9999)}]) AS n",
				// Fewer elements than the old check interval of 1,024.
				"RETURN size([i IN range(1, 1000) | {xs: range(i, i + 9999)}]) AS n",
				"RETURN size([i IN range(1, 1000) | [range(i, i + 9999)]]) AS n",
			} {
				_, err := executor.Execute(ctx, query, nil)
				requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
				requirePoolsEmpty(t, "nornic")
			}

			// Shared and input values count once: these fit 4 MiB.
			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 4 << 20})
			for query, want := range map[string]int64{
				"RETURN size([i IN range(1, 2000) | {xs: $big}]) AS n": 2000,
				"RETURN size([x IN $list | x]) AS n":                    2000,
				"RETURN size([x IN $list | {pair: x}]) AS n":            2000,
			} {
				result, err := executor.Execute(ctx, query, map[string]interface{}{"big": big, "list": list})
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
				requirePoolsEmpty(t, "nornic")
			}
		})
	}
}

// A MATCH's candidate nodes count while the match holds them, from an index
// seek or a label scan, with one cached list per property value, also when
// the rows built from them don't stay (count(*)); they are given back when
// the statement completes, fails or is cancelled (#1343).
func TestNodeMatchCandidatesCountWhileHeld(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	for _, setup := range []string{
		"CREATE INDEX item_p FOR (n:Item) ON (n.p)",
		"UNWIND range(1, 50000) AS i CREATE (:Item {p: 1})",
		"UNWIND range(1, 50000) AS i CREATE (:Scan {p: 1})",
		"UNWIND range(1, 30000) AS i CREATE (:Multi {p: 1 + i % 2})",
	} {
		_, err := executor.Execute(ctx, setup, nil)
		require.NoError(t, err, setup)
	}

	for _, test := range []struct {
		name, query string
		count       int64
	}{
		{"index seek", "UNWIND [1] AS i MATCH (n:Item {p: i}) RETURN count(*) AS n", 50000},
		{"label scan", "UNWIND [1] AS i MATCH (n:Scan {p: i}) RETURN count(*) AS n", 50000},
	} {
		t.Run(test.name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.count}}, result.Rows)

			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
			_, err = executor.Execute(ctx, test.query, nil)
			requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
			requirePoolsEmpty(t, "nornic")

			// Within the limit, what the candidates took goes back when the
			// statement completes.
			ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20})
			result, err = executor.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.count}}, result.Rows)
			requirePoolsEmpty(t, "nornic")
		})
	}

	t.Run("cached property values", func(t *testing.T) {
		// 15,000 candidates per value: one list fits 1.5 MiB, two don't.
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 3 << 19})
		result, err := executor.Execute(ctx, "UNWIND [1] AS i MATCH (n:Multi {p: i}) RETURN count(*) AS n", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(15000)}}, result.Rows)
		requirePoolsEmpty(t, "nornic")
		_, err = executor.Execute(ctx, "UNWIND [1, 2] AS i MATCH (n:Multi {p: i}) RETURN count(*) AS n", nil)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
	})

	t.Run("cancelled", func(t *testing.T) {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20})
		cancelled, cancel := context.WithCancel(ctx)
		defer cancel()
		var once sync.Once
		sizingObserver = func() { once.Do(cancel) }
		t.Cleanup(func() { sizingObserver = nil })
		_, err := executor.Execute(cancelled, "UNWIND [1] AS i MATCH (n:Item {p: i}) RETURN count(*) AS n", nil)
		sizingObserver = nil
		require.Error(t, err)
		requirePoolsEmpty(t, "nornic")
	})
}

// candidateCollectors take an owner (their second argument) and return the
// candidates reserved in it.
var candidateCollectors = map[string]bool{
	"collectPipelineInitialNodeCandidates": true,
	"tryCollectNodesFromStartPropertyScan": true,
}

// Every caller of a candidate collector passes an owner that it releases
// (or its own parameter, which its caller releases), so no operator holds
// MATCH candidates the statement's limit doesn't see; only the collector
// collects them unreserved (#1343).
func TestNodeCandidateCollectorsReleaseTheirOwners(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	files := token.NewFileSet()
	calls := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		file, err := parser.ParseFile(files, name, source, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			function, isFunction := declaration.(*ast.FuncDecl)
			if !isFunction || function.Body == nil {
				continue
			}
			parameters := map[string]bool{}
			for _, field := range function.Type.Params.List {
				for _, parameter := range field.Names {
					parameters[parameter.Name] = true
				}
			}
			released := map[string]bool{}
			aliases := map[string][]string{}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					if selector, isSelector := n.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "release" {
						if receiver, isIdent := selector.X.(*ast.Ident); isIdent {
							released[receiver.Name] = true
						}
					}
				case *ast.AssignStmt:
					for index, left := range n.Lhs {
						target, isIdent := left.(*ast.Ident)
						if !isIdent || index >= len(n.Rhs) {
							continue
						}
						if source, isIdent := n.Rhs[index].(*ast.Ident); isIdent {
							aliases[target.Name] = append(aliases[target.Name], source.Name)
						}
					}
				}
				return true
			})
			releasedOwner := func(name string) bool {
				if parameters[name] || released[name] {
					return true
				}
				sources := aliases[name]
				for _, source := range sources {
					if !released[source] && !parameters[source] {
						return false
					}
				}
				return len(sources) > 0
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				selector, isSelector := call.Fun.(*ast.SelectorExpr)
				if !isSelector {
					return true
				}
				position := files.Position(call.Pos())
				if selector.Sel.Name == "collectPipelineInitialNodeCandidatesUnreserved" {
					require.Equal(t, "collectPipelineInitialNodeCandidates", function.Name.Name, "%s collects candidates unreserved", position)
					return true
				}
				if !candidateCollectors[selector.Sel.Name] {
					return true
				}
				calls++
				require.GreaterOrEqual(t, len(call.Args), 2, "%s", position)
				owner, isIdent := call.Args[1].(*ast.Ident)
				require.True(t, isIdent && owner.Name != "nil", "%s: %s needs an owner", position, selector.Sel.Name)
				require.True(t, releasedOwner(owner.Name), "%s: %s's owner %s is never released", position, selector.Sel.Name, owner.Name)
				return true
			})
		}
	}
	require.GreaterOrEqual(t, calls, 8, "the collectors' callers were not found")
}

// The other builders that keep what their elements hold count it too: an
// UNWIND over a list the expression built, a reduce map accumulator, and
// collect() of values that share a list (#1343).
func TestBuildersCountWhatTheirElementsHold(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	ctx := context.Background()
	big := make([]interface{}, 100000)
	for i := range big {
		big[i] = int64(i)
	}

	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20})
	for _, query := range []string{
		// Each range fits the limit alone; the list literal's three don't.
		"UNWIND [{xs: range(1, 20000)}, {xs: range(2, 20001)}, {xs: range(3, 20002)}] AS m RETURN count(*) AS n",
		"RETURN size(keys(reduce(m = {}, i IN range(1, 20) | apoc.map.setKey(m, toString(i), range(i, i + 9999))))) AS n",
	} {
		_, err := executor.Execute(ctx, query, nil)
		requireMemoryError(t, err, "Neo.TransientError.General.TransactionMemoryLimit", "db.memory.transaction.max")
		requirePoolsEmpty(t, "nornic")
	}

	// A list the row or a parameter holds streams as its slots, and values
	// that share a list count it once.
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 32 << 20})
	for query, want := range map[string]int64{
		"UNWIND $big AS x RETURN count(*) AS n":                                        100000,
		"WITH $big AS xs UNWIND xs AS x RETURN count(*) AS n":                          100000,
		"UNWIND range(1, 1000) AS i WITH collect({xs: $big}) AS ms RETURN size(ms) AS n": 1000,
	} {
		result, err := executor.Execute(ctx, query, map[string]interface{}{"big": big})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		requirePoolsEmpty(t, "nornic")
	}
}
