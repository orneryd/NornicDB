package cypher

import (
	"context"
	"go/ast"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cancellationRegistryCase is one statement that keeps a stage or evaluator
// busy well past a short deadline. covers names the pipeline stages
// (pipelineApply*) and compiled row-expression nodes it drives; every one of
// them must be covered by some case or listed in cancellationRegistryExempt.
type cancellationRegistryCase struct {
	name   string
	query  string
	small  map[string]interface{} // warm-up parameters: the same text, little work
	large  map[string]interface{} // the run that must stop at its deadline
	covers []string
	write  bool // the statement writes; nothing may be committed
}

var cancellationRegistryExempt = map[string]string{
	"rowConstNode":                            "constant-time leaf",
	"rowLiteralNode":                          "constant-time leaf",
	"rowEmptyListNode":                        "constant-time leaf",
	"rowUnresolvedNode":                       "constant-time leaf",
	"pipelineApplyProcedureCall":              "no unbounded built-in procedure; handlers take the statement context (static rule B)",
	"pipelineApplyBoundRelationshipListMatch": "reads one bound relationship list per row; bounded by the list the statement already built",
	"pipelineApplyRowDynamicMerge":            "per-row MERGE with dynamic labels; its loop is pipelineApplyMerge's (covered)",
	"pipelineApplyReturnSource":               "streaming RETURN; driven by the RETURN cases (pipelineApplyReturn)",
}

const cancellationRegistryDeadline = 50 * time.Millisecond

func cancellationRegistryCases() []cancellationRegistryCase {
	n := func(small, large int) (map[string]interface{}, map[string]interface{}) {
		return map[string]interface{}{"n": int64(small)}, map[string]interface{}{"n": int64(large)}
	}
	big := 100_000_000
	list := 5_000_000
	var cases []cancellationRegistryCase
	add := func(name, query string, small, large int, write bool, covers ...string) {
		s, l := n(small, large)
		cases = append(cases, cancellationRegistryCase{name: name, query: query, small: s, large: l, covers: covers, write: write})
	}
	add("unwind", "UNWIND range(1, $n) AS x RETURN count(x) AS c", 3, big, false,
		"pipelineApplyUnwind", "pipelineApplyUnwindPrefix", "pipelineApplyReturn", "pipelineApplyReturnPlan")
	add("with", "UNWIND range(1, $n) AS x WITH x + 1 AS y RETURN sum(y) AS s", 3, big, false,
		"pipelineApplyWith", "pipelineApplyWithSource", "rowArithmeticNode", "rowBoundNode")
	add("distinct", "UNWIND range(1, $n) AS x WITH DISTINCT x % 1000003 AS k RETURN count(k) AS c", 3, big, false)
	add("order by", "UNWIND range(1, $n) AS x WITH x ORDER BY -x RETURN count(x) AS c", 3, list, false)
	add("collect", "UNWIND range(1, $n) AS x RETURN size(collect(x)) AS c", 3, big, false)
	add("grouping", "UNWIND range(1, $n) AS x RETURN x % 100 AS k, count(*) AS c", 3, big, false)
	add("comprehension", "WITH range(1, $n) AS xs RETURN size([x IN xs WHERE x % 2 = 0 | x * 2]) AS c", 3, list, false,
		"rowComprehensionNode", "rowComparisonNode")
	add("nested comprehension", "WITH range(1, $n) AS xs RETURN size([x IN xs | size([y IN xs | x + y])]) AS c", 3, 20_000, false,
		"rowMemoNode", "rowUnaryFunctionNode")
	add("reduce", "WITH range(1, $n) AS xs RETURN reduce(s = 0, x IN xs | s + x) AS s", 3, list, false, "rowReduceNode")
	add("quantifier", "WITH range(1, $n) AS xs RETURN all(x IN xs WHERE x > 0) AS ok", 3, list, false, "rowQuantifierNode")
	add("case", "UNWIND range(1, $n) AS x RETURN sum(CASE WHEN x % 2 = 0 THEN 1 ELSE 0 END) AS s", 3, big, false, "rowCaseNode")
	add("coalesce and null tests", "UNWIND range(1, $n) AS x RETURN count(coalesce(null, x)) AS c, count(CASE WHEN x IS NULL THEN 1 END) AS d", 3, big, false,
		"rowCoalesceNode", "rowNullTestNode")
	add("string predicates and membership", "UNWIND range(1, $n) AS x WITH x, toString(x) AS s WHERE s STARTS WITH '9' OR x IN [1, 2, 3] RETURN count(*) AS c", 3, big, false,
		"rowStringPredicateNode", "rowMembershipNode", "rowLogicalNode")
	add("list functions", "WITH range(1, $n) AS xs RETURN size(reverse(xs)) AS a, size([1, 2, 3] + xs) AS b", 3, list*4, false, "rowListNode")
	add("apoc map", "WITH range(1, $n) AS xs RETURN size(keys(apoc.map.fromPairs([x IN xs | [toString(x), x]]))) AS c", 3, list, false, "rowFallbackNode")
	add("apoc join", "WITH [x IN range(1, $n) | 'w'] AS ws RETURN size(apoc.text.join(ws, ',')) AS c", 3, list, false, "rowTextNode")
	add("string padding", "RETURN size(lpad('', $n, 'x')) AS c", 3, 1_000_000_000, false)
	add("match product", "MATCH (a:N), (b:N) RETURN count(*) AS c", 0, 0, false,
		"pipelineApplyMatch", "pipelineApplyMatchProduct", "pipelineApplyInitialNodeMatch", "pipelineApplyNodeJoinProduct")
	add("chained match", "MATCH (a:N) MATCH (b:N) WHERE b.v > a.v RETURN count(*) AS c", 0, 0, false,
		"pipelineApplyChainedMatch", "pipelineApplyMatchWithHint", "rowPropertyNode", "rowPropertyChainNode")
	add("variable-length traversal", "MATCH (a:N)-[:R*1..60]->(b:N) RETURN count(*) AS c", 0, 0, false, "pipelineApplyInitialTraversalMatch")
	add("relationship endpoints", "MATCH (a:N)-[r:R]->(b:N) MATCH (c:N) RETURN count(startNode(r)) AS c", 0, 0, false, "rowEndpointNode")
	add("optional match", "MATCH (a:N) OPTIONAL MATCH (a)-[:R*1..60]->(b) RETURN count(b) AS c", 0, 0, false, "pipelineApplyOptionalMatch")
	add("shortest path", "MATCH (a:N), (b:N) WHERE a.id < 200 AND b.id > 2800 MATCH p = shortestPath((a)-[:R*]-(b)) RETURN count(p) AS c", 0, 0, false, "pipelineApplyShortestPathMatch")
	add("selected path", "MATCH p = ALL SHORTEST (a:N)-[:R]-+(b:N) RETURN count(p) AS c", 0, 0, false, "pipelineApplySelectedPathMatch")
	add("quantified path", "MATCH (a:N) ((x)-[:R]->(y)){1,60} (b) RETURN count(*) AS c", 0, 0, false, "pipelineApplyQuantifiedPathMatch")
	add("call subquery", "UNWIND range(1, $n) AS x CALL (x) { UNWIND range(1, 100) AS y RETURN sum(y) AS s } RETURN count(s) AS c", 3, list, false,
		"pipelineApplyCallSubquery", "pipelineApplyCallSubqueryWithMetadata")
	add("union", "UNWIND range(1, $n) AS x RETURN x AS v UNION UNWIND range(1, $n) AS x RETURN x AS v", 3, big, false)
	add("count subquery", "MATCH (a:N) WHERE COUNT { MATCH (a)-[:R*1..60]->(b) } > 0 RETURN count(a) AS c", 0, 0, false,
		"rowSubqueryNode", "rowDeferredOperandNode")
	add("pattern comprehension", "MATCH (a:N) RETURN sum(size([(a)-[:R*1..60]->(b) | b.id])) AS s", 0, 0, false)
	add("create", "UNWIND range(1, $n) AS x CREATE (:W {x: x})", 3, big, true, "pipelineApplyCreateClauses")
	add("merge", "UNWIND range(1, $n) AS x MERGE (:W {x: x % 1000003})", 3, big, true, "pipelineApplyMerge")
	add("set", "UNWIND range(1, $n) AS x MATCH (a:N {id: 1 + x % 3000}) SET a.tmp = x", 3, big, true, "pipelineApplySet")
	add("remove", "UNWIND range(1, $n) AS x MATCH (a:N {id: 1 + x % 3000}) REMOVE a.tmp", 3, big, true, "pipelineApplyRemove")
	add("foreach", "FOREACH (x IN range(1, $n) | CREATE (:W {x: x}))", 3, big, true, "pipelineApplyForeach")
	add("delete", "UNWIND range(1, $n) AS x OPTIONAL MATCH (w:Nope) DELETE w", 3, big, true, "pipelineApplyDelete")
	return cases
}

func newCancellationRegistryExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	executor := NewStorageExecutorWithQueryCachePolicy(newTransactionMemoryEngine(t), 0, 0)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE INDEX registry_n_id IF NOT EXISTS FOR (n:N) ON (n.id)",
		"UNWIND range(1, 3000) AS i CREATE (:N {id: i, v: i % 97})",
		"UNWIND range(1, 2999) AS i MATCH (a:N {id: i}), (b:N {id: i + 1}) CREATE (a)-[:R]->(b)",
		"UNWIND range(1, 2990) AS i MATCH (a:N {id: i}), (b:N {id: i + 7}) CREATE (a)-[:R]->(b)",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	return executor
}

// TestCancellationOwnerRegistry runs every registered stage and evaluator
// past a 50 ms deadline, cold (expression caches cleared) and warm (compiled
// on a small input first): each returns the deadline promptly, and a write
// commits nothing.
func TestCancellationOwnerRegistry(t *testing.T) {
	executor := newCancellationRegistryExecutor(t)
	slack := 250 * time.Millisecond
	if cypherRaceEnabled {
		slack = time.Second
	}
	for _, test := range cancellationRegistryCases() {
		for _, warm := range []bool{false, true} {
			mode := "cold"
			if warm {
				mode = "warm"
			}
			t.Run(test.name+"/"+mode, func(t *testing.T) {
				clearExpressionCaches()
				if warm {
					// The first rows compile the statement's programs and plans;
					// a fixture-sized statement may stop at the warm-up deadline.
					warmCtx, warmCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					_, err := executor.Execute(warmCtx, test.query, test.small)
					warmCancel()
					if test.write {
						_, _ = executor.Execute(context.Background(), "MATCH (w:W) DETACH DELETE w", nil)
						_, _ = executor.Execute(context.Background(), "MATCH (a:N) REMOVE a.tmp", nil)
					}
					if err != nil {
						require.ErrorIs(t, err, context.DeadlineExceeded, "warm-up")
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), cancellationRegistryDeadline)
				defer cancel()
				deadline, _ := ctx.Deadline()
				_, err := executor.Execute(ctx, test.query, test.large)
				overdue := time.Since(deadline)
				require.ErrorIs(t, err, context.DeadlineExceeded, "the statement must still be working at its deadline")
				require.Less(t, overdue, slack, "returned %s after its deadline", overdue)
				if test.write {
					result, err := executor.Execute(context.Background(), "MATCH (w:W) RETURN count(w) AS c", nil)
					require.NoError(t, err)
					require.Equal(t, int64(0), result.Rows[0][0], "a statement stopped at its deadline commits nothing")
					result, err = executor.Execute(context.Background(), "MATCH (a:N) WHERE a.tmp IS NOT NULL RETURN count(a) AS c", nil)
					require.NoError(t, err)
					require.Equal(t, int64(0), result.Rows[0][0], "a statement stopped at its deadline commits nothing")
				}
				t.Logf("overdue %s", overdue.Round(time.Millisecond))
			})
		}
	}
}

// TestCancellationOwnerRegistryCoversEveryEntry cross-checks the registry
// against the package source: every pipeline stage (pipelineApply*) and every
// compiled row-expression node with an eval method is covered by a case or
// exempt with a reason, and nothing is listed that no longer exists.
func TestCancellationOwnerRegistryCoversEveryEntry(t *testing.T) {
	_, files := loadCancellationSources(t)
	entries := map[string]bool{}
	for _, f := range files {
		for _, decl := range f.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			key := funcKey(fn)
			if strings.HasPrefix(fn.Name.Name, "pipelineApply") && fn.Recv != nil && strings.HasPrefix(key, "StorageExecutor.") {
				if ctxName, _ := cancellationScope(fn); ctxName != "" {
					entries[fn.Name.Name] = true
				}
			}
			if fn.Name.Name == "eval" && fn.Recv != nil && (f.name == "row_value_program.go" || f.name == "row_predicate_program.go") {
				entries[strings.TrimSuffix(key, ".eval")] = true
			}
		}
	}
	covered := map[string]bool{}
	for _, test := range cancellationRegistryCases() {
		for _, name := range test.covers {
			covered[name] = true
			require.True(t, entries[name], "case %q covers %s, which is not an entry", test.name, name)
		}
	}
	for name := range cancellationRegistryExempt {
		require.True(t, entries[name], "exempt entry %s does not exist", name)
	}
	var missing []string
	for name := range entries {
		if !covered[name] && cancellationRegistryExempt[name] == "" {
			missing = append(missing, name)
		}
	}
	require.Empty(t, missing, "entries no registry case covers")
}
