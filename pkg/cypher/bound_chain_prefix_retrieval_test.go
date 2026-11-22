package cypher

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reuse the retrieval audit's storage observer: these small contracts attribute
// suffix reads without duplicating the ledger or imposing timing assertions.
func boundChainPrefixFixture(t *testing.T) (*StorageExecutor, *nor406ReadLedger, map[string]*storage.Node) {
	t.Helper()
	ledger := newNOR406ReadLedger(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(ledger, "bound_prefix"))
	_, err := exec.Execute(t.Context(), `
 CREATE (n:PrefixStart {id:'a'})-[:R {kind:'a'}]->(m:PrefixMid {id:'ma',kind:'a',keep:true})-[:S]->(:PrefixEnd {id:'za'}),
        (m)-[:S]->(:PrefixEnd {id:'za2'}),
        (n)-[:R {kind:'b'}]->(:PrefixMid {id:'mb',kind:'b',keep:false})-[:S]->(:PrefixEnd {id:'zb'}),
        (:PrefixStart {id:'other'})`, nil)
	require.NoError(t, err)
	result, err := exec.Execute(t.Context(), `MATCH (n) RETURN n`, nil)
	require.NoError(t, err)
	nodes := make(map[string]*storage.Node, len(result.Rows))
	for _, row := range result.Rows {
		node, ok := row[0].(*storage.Node)
		require.True(t, ok)
		nodes[node.Properties["id"].(string)] = node
	}
	require.Len(t, nodes, 7)
	ledger.reset()
	return exec, ledger, nodes
}

func TestBoundChainPrefixPublicExistsAndCountStopRejectedSuffix(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ledger, _ := boundChainPrefixFixture(t)
		for _, test := range []struct {
			kind string
			want any
		}{{"EXISTS", true}, {"COUNT", int64(2)}} {
			t.Run(test.kind, func(t *testing.T) {
				ledger.reset()
				result, err := exec.Execute(t.Context(), `MATCH (n:PrefixStart {id:'a'}) RETURN `+test.kind+` { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep } AS admitted`, nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{test.want}}, result.Rows)
				counts := ledger.snapshot()
				require.Equal(t, int64(1), counts.AdjacencyByType["R"], "the bound start expands once")
				require.Equal(t, int64(1), counts.AdjacencyByType["S"], "the false m.keep prefix must never read its suffix; the surviving prefix retains both complete paths")
			})
		}
	})
}

func TestBoundChainPrefixPlanKeepsParametersAndEntityBindingsFresh(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _, nodes := boundChainPrefixFixture(t)
		planned := withPipelineClausePlans(t.Context())
		// Both property-map positions must rebind values. Their prefix metadata is
		// structural only; the final WHERE also observes each call's parameters.
		for _, body := range []string{
			`(n)-[:R]->(m {kind:$kind})-[:S]->(z) WHERE m.keep AND m.kind = $kind`,
			`(n)-[:R {kind:$kind}]->(m)-[:S]->(z) WHERE m.keep AND m.kind = $kind`,
			`(n)-[:R]->(m)-[:S]->(z) WHERE m.keep AND m.kind = $kind`,
		} {
			for _, test := range []struct {
				kind string
				want int64
			}{{"a", 2}, {"b", 0}, {"a", 2}} {
				ctx := withParams(planned, map[string]any{"kind": test.kind})
				values := map[string]any{"n": nodes["a"]}
				bindParameterRow(ctx, values)
				got, handled := exec.boundChainCount(ctx, body, values, false)
				require.True(t, handled, body)
				require.Equal(t, test.want, got, body+" kind="+test.kind)
			}
		}
		body := `(n)-[:R]->(m)-[:S]->(z) WHERE m.keep`
		for _, test := range []struct {
			start, mid, end string
			want            int64
		}{
			{"a", "ma", "za", 1}, {"a", "ma", "za2", 1}, {"a", "mb", "zb", 0},
			{"other", "ma", "za", 0}, {"a", "ma", "zb", 0}, {"a", "ma", "za", 1},
		} {
			values := map[string]any{"n": nodes[test.start], "m": nodes[test.mid], "z": nodes[test.end]}
			got, handled := exec.boundChainCount(planned, body, values, false)
			require.True(t, handled)
			require.Equal(t, test.want, got, "row-bound entities: %+v", test)
		}
	})
}

func TestBoundChainPrefixPlanConcurrentEvaluations(t *testing.T) {
	for _, body := range []string{
		`(n)-[:R]->(m)-[:S]->(z) WHERE m.keep`,
		`(n)-[:R]->(m)-[:S]->(z) WHERE m.keep = true`,
	} {
		t.Run(body, func(t *testing.T) {
			exec, _, nodes := boundChainPrefixFixture(t)
			ctx := withPipelineClausePlans(t.Context())
			var builds atomic.Int64
			hook := func(kind pipelineClauseAnalysis, text string) {
				if kind == clauseAnalysisBoundChain && text == body {
					builds.Add(1)
				}
			}
			old := pipelineClausePlanBuildHook.Swap(&hook)
			t.Cleanup(func() { pipelineClausePlanBuildHook.Store(old) })
			type outcome struct {
				count   int64
				handled bool
			}
			completed := make(chan outcome, 16)
			start := make(chan struct{})
			for range 16 {
				go func() {
					<-start
					// Several evaluations exercise lazy prepared predicate selection as well
					// as the first structural-plan publication under the race detector.
					for range 8 {
						count, handled := exec.boundChainCount(ctx, body, map[string]any{"n": nodes["a"]}, false)
						if !handled || count != 2 {
							completed <- outcome{count, handled}
							return
						}
					}
					completed <- outcome{2, true}
				}()
			}
			close(start)
			for range 16 {
				got := <-completed
				require.True(t, got.handled)
				require.Equal(t, int64(2), got.count)
			}
			require.Equal(t, int64(1), builds.Load(), "one shared immutable chain plan")
		})
	}
}

func TestBoundChainPrefixDefersOuterAndVolatilePredicates(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _, _ := boundChainPrefixFixture(t)
		for _, test := range []struct {
			expression string
			want       any
		}{
			{`COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep AND allowance = 1 }`, int64(2)},
			{`COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep AND allowance = 0 }`, int64(0)},
			{`EXISTS { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep AND allowance = 1 }`, true},
			{`COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep AND EXISTS { (z)<-[:S]-(m) } }`, int64(2)},
		} {
			result, err := exec.Execute(t.Context(), `WITH 1 AS allowance MATCH (n:PrefixStart {id:'a'}) RETURN `+test.expression+` AS admitted`, nil)
			require.NoError(t, err, test.expression)
			require.Equal(t, [][]any{{test.want}}, result.Rows, test.expression)
		}
		var calls int64
		withPluginFunctions(t, map[string]any{"inspection.prefixValue": func() int64 { calls++; return calls }})
		for _, test := range []struct {
			kind  string
			want  any
			calls int64
		}{{"COUNT", int64(3), 3}, {"EXISTS", true, 1}} {
			calls = 0
			result, err := exec.Execute(t.Context(), `MATCH (n:PrefixStart {id:'a'}) RETURN `+test.kind+` { (n)-[:R]->(m)-[:S]->(z) WHERE m.id <> toString(inspection.prefixValue()) } AS admitted`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{test.want}}, result.Rows)
			require.Equal(t, test.calls, calls, "volatile call occurs only on complete paths visited by final "+test.kind+" evaluation")
		}
	})
}

func TestBoundChainPrefixPreservesZeroHopBindings(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _, _ := boundChainPrefixFixture(t)
		for _, test := range []struct {
			body string
			want int64
		}{
			{`(n)-[:R*0..0]->(m)-[:R]->(z) WHERE n.id = m.id`, 2},
			{`(n)-[:R]->(m)-[:S*0..0]->(m) WHERE m.keep`, 1},
		} {
			result, err := exec.Execute(t.Context(), `MATCH (n:PrefixStart {id:'a'}) RETURN COUNT { `+test.body+` } AS admitted`, nil)
			require.NoError(t, err, test.body)
			require.Equal(t, [][]any{{test.want}}, result.Rows, test.body)
		}
	})
}

// Prefix checks are speculative: an expression error cannot become a statement
// error until a complete path reaches the caller's final WHERE. This also pins
// the existing MATCH consumer of the shared prefix-prune owner.
func TestBoundChainPrefixDefersErrorsUntilCompletePath(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, fixture := range []struct {
			name, seed string
			complete   bool
		}{
			{"missing_suffix", `CREATE (:PrefixStart)-[:R]->(:PrefixMid {value:1})`, false},
			{"complete_suffix", `CREATE (:PrefixStart)-[:R]->(:PrefixMid {value:1})-[:S]->(:PrefixEnd)`, true},
		} {
			t.Run(fixture.name, func(t *testing.T) {
				exec, _ := newTestExecutor(t, "prefix_error")
				_, err := exec.Execute(t.Context(), fixture.seed, nil)
				require.NoError(t, err)
				for _, test := range []struct {
					name, query string
					want        [][]any
				}{
					{"COUNT", `MATCH (n:PrefixStart) RETURN COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.value / 0 > 0 } AS admitted`, [][]any{{int64(0)}}},
					{"EXISTS", `MATCH (n:PrefixStart) RETURN EXISTS { (n)-[:R]->(m)-[:S]->(z) WHERE m.value / 0 > 0 } AS admitted`, [][]any{{false}}},
					{"MATCH", `MATCH (n:PrefixStart)-[:R]->(m)-[:S]->(z) WHERE m.value / 0 > 0 RETURN z`, nil},
				} {
					t.Run(test.name, func(t *testing.T) {
						if fixture.complete {
							requireStatus(t, exec, test.query, nil, "Neo.ClientError.Statement.ArithmeticError", "/ by zero")
							return
						}
						result, err := exec.Execute(t.Context(), test.query, nil)
						require.NoError(t, err, "a rejected incomplete prefix has no final predicate evaluation")
						if test.name == "MATCH" {
							require.Empty(t, result.Rows)
						} else {
							require.Equal(t, test.want, result.Rows)
						}
					})
				}
			})
		}
	})
}

// Complete prefix plans retain syntax only; each execution still owns its
// parameters, PROFILE collection and speculative error lifetime.
func TestBoundChainPrefixCompletePlanModes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, mode := range []string{"normal", "programs_disabled", "profile"} {
			t.Run(mode, func(t *testing.T) {
				if mode == "programs_disabled" {
					oldPredicate := rowPredicateProgramsDisabled.Swap(true)
					oldValue := rowValueProgramsDisabled.Swap(true)
					t.Cleanup(func() { rowPredicateProgramsDisabled.Store(oldPredicate); rowValueProgramsDisabled.Store(oldValue) })
				}
				exec, _, _ := boundChainPrefixFixture(t)
				match := exec.parseTraversalPattern(t.Context(), `(n)-[:R]->(m)-[:S]->(z)`)
				require.NotNil(t, match)
				match.pruneWhere = "m.keep = true"
				prune := exec.newChainPrefixPrune(t.Context(), match)
				require.NotNil(t, prune)
				require.NotNil(t, prune.levels[0].where.plan)
				require.True(t, prune.levels[0].where.plan.complete)
				query := `MATCH (n:PrefixStart {id:'a'}) RETURN COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.keep = true } AS admitted`
				if mode == "profile" {
					query = "PROFILE " + query
				}
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{int64(2)}}, result.Rows)
				match.pruneWhere = "m.value / 0 > 0"
				errorPrune := exec.newChainPrefixPrune(t.Context(), match)
				require.NotNil(t, errorPrune)
				require.NotNil(t, errorPrune.levels[0].where.plan, "error regression must reach the complete-plan route")
				require.True(t, errorPrune.levels[0].where.plan.complete)
				for _, complete := range []bool{false, true} {
					failing, _ := newTestExecutor(t, "complete_prefix_error")
					seed := `CREATE (:PrefixStart)-[:R]->(:PrefixMid {value:1})`
					if complete {
						seed += `-[:S]->(:PrefixEnd)`
					}
					_, err := failing.Execute(t.Context(), seed, nil)
					require.NoError(t, err)
					query := `MATCH (n:PrefixStart) RETURN COUNT { (n)-[:R]->(m)-[:S]->(z) WHERE m.value / 0 > 0 } AS admitted`
					if mode == "profile" {
						query = "PROFILE " + query
					}
					if complete {
						requireStatus(t, failing, query, nil, "Neo.ClientError.Statement.ArithmeticError", "/ by zero")
					} else {
						result, err := failing.Execute(t.Context(), query, nil)
						require.NoError(t, err)
						require.Equal(t, [][]any{{int64(0)}}, result.Rows)
					}
				}
			})
		}
	})
}
