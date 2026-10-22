package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// checkpointContext cancels a real context at successive work checkpoints.
// Tests snapshot products, levels, reads and bindings at cancellation and
// assert that the search performs no further observable work.
type checkpointContext struct {
	context.Context
	cancel  context.CancelFunc
	left    int
	observe func()
}

func newCheckpointContext(limit int, observe func()) *checkpointContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &checkpointContext{Context: ctx, cancel: cancel, left: limit, observe: observe}
}
func (ctx *checkpointContext) Err() error {
	if ctx.Context.Err() == nil {
		ctx.left--
		if ctx.left == 0 {
			ctx.cancel()
			if ctx.observe != nil {
				ctx.observe()
			}
		}
	}
	return ctx.Context.Err()
}
func TestSelectedPathCompilationCancellationCheckpoints(t *testing.T) {
	e, _ := newShortestSelectorExecutor(t)
	pattern := "(a)((x)-[r:T]->(y) WHERE x.id <= y.id){2,1000000}(()-[:T]->()){0,3}(b)"
	for budget := 1; budget <= 256; budget++ {
		ctx := newCheckpointContext(budget, nil)
		plan, _, ok := buildSelectedPathPlanContext(ctx, pattern, "p", "", nil)
		if ctx.Context.Err() != nil {
			require.False(t, ok)
			require.Nil(t, plan)
		} else {
			require.True(t, ok)
		}
		ctx.cancel()
		ctx = newCheckpointContext(budget, nil)
		_, _, ok = compileSelectedPathPlanContext(ctx, pattern, fmt.Sprintf("p%d", budget), "", nil)
		if ctx.Context.Err() != nil {
			require.False(t, ok)
		}
		ctx.cancel()
		ctx = newCheckpointContext(budget, nil)
		_, _, err := e.parseSelectedPathMatchContext(ctx, pattern, "p", nil, &pathSelector{}, "", nil)
		if ctx.Context.Err() != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
		ctx.cancel()
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, ok := parsePathPatternItemsContext(canceled, pattern, false)
	require.False(t, ok)
	_, _, ok = buildSelectedPathPlanContext(canceled, pattern, "p", "", nil)
	require.False(t, ok)
	a := &pathSearchAutomaton{ctx: context.Background(), slots: []pathSearchSlot{{empty: []int{0}}}}
	require.Len(t, a.chainsFrom(0, nil), 1)
}
func TestSelectedPathCancellationWorkCheckpoints(t *testing.T) {
	e, base := newShortestSelectorExecutor(t)
	result, err := e.Execute(base, "MATCH (a:S {id:1}), (b:S {id:4}) RETURN a,b", nil)
	require.NoError(t, err)
	source, target := result.Rows[0][0].(*storage.Node), result.Rows[0][1].(*storage.Node)
	patterns := []string{"(a)-[:T*]-(b)", "(a)-[:T*]->(b)", "(a)-[:T]->(x)-[:T]->(b)", "(a)((x)-[r:T]->(y) WHERE x.id <= y.id){1,3}(b)", "(a)(()-[:T]->()){2,3}(()-[:T]->()){0,2}(b)"}
	for _, pattern := range patterns {
		plan, _, ok := compileSelectedPathPlan(pattern, "p", "", nil)
		require.True(t, ok)
		for _, mode := range []string{"pair", "one", "run", "forward", "backward", "trace"} {
			for budget := 1; budget <= 200; budget++ {
				var search *pathSearch
				products, levels, bound, atBound := 0, 0, 0, 0
				ctx := newCheckpointContext(budget, func() {
					if search != nil {
						products = len(search.products)
						levels = len(search.levels)
					}
					atBound = bound
				})
				fixtureAdmission1685Context, fixtureAdmission1685 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
				_ = fixtureAdmission1685Context
				search = e.relationshipExecutionFor(fixtureAdmission1685, fixtureAdmission1685Context, pattern, "").newPathSearch(ctx, plan.forward, pipelineRow{}, 6, source)
				bind := func(path pathSearchPath) (pipelineRow, bool) {
					bound++
					fixtureAdmission1686Context, fixtureAdmission1686 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
					_ = fixtureAdmission1686Context
					return e.bindSelectedPath(fixtureAdmission1686, fixtureAdmission1686Context, plan, plan.forward, false, pipelineRow{}, path)
				}
				switch mode {
				case "one":
					fixtureAdmission1687Context, fixtureAdmission1687 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1687Context
					search.pair(fixtureAdmission1687, source, target, 1, false, true, bind)
				case "pair":
					fixtureAdmission1688Context, fixtureAdmission1688 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1688Context
					search.pair(fixtureAdmission1688, source, target, 1, true, true, bind)
				case "run":
					fixtureAdmission1689Context, fixtureAdmission1689 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1689Context
					search.run(fixtureAdmission1689, source, 2, true, bind)
				case "forward":
					fixtureAdmission1690Context, fixtureAdmission1690 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1690Context
					search.begin(fixtureAdmission1690, source)
					w := &pathSearchWalks{forward: map[int32]int{}, preds: map[int32][]pathSearchHop{}, backward: map[pathSearchKey]int{}}
					if len(search.levels) > 0 {
						for _, entry := range search.levels[0].entries {
							w.forward[entry.product] = 0
							w.frontier = append(w.frontier, entry.product)
						}
					}
					fixtureAdmission1691Context, fixtureAdmission1691 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1691Context
					search.expandForward(fixtureAdmission1691, w)
				case "backward":
					fixtureAdmission1692Context, fixtureAdmission1692 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1692Context
					search.begin(fixtureAdmission1692, source)
					key := pathSearchKey{node: target.ID, slot: int32(plan.forward.final)}
					w := &pathSearchWalks{backward: map[pathSearchKey]int{key: 0}, nodes: map[pathSearchKey]*storage.Node{key: target}, succs: map[pathSearchKey][]pathSearchBack{}, back: []pathSearchKey{key}, forward: map[int32]int{}}
					fixtureAdmission1693Context, fixtureAdmission1693 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1693Context
					search.expandBackward(fixtureAdmission1693, w)
				case "trace":
					fixtureAdmission1694Context, fixtureAdmission1694 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1694Context
					search.begin(fixtureAdmission1694, source)
					fixtureAdmission1695Context, fixtureAdmission1695 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1695Context
					for depth := 0; depth < 3 && search.ctx.Err() == nil && search.nextLevel(fixtureAdmission1695); depth++ {
					}
					if len(search.levels) > 1 {
						search.trace(len(search.levels)-1, 0, 1, func(path pathSearchPath) bool { _, ok := bind(path); return ok })
					}
				}
				if ctx.Context.Err() != nil {
					require.Equal(t, atBound, bound, "%s %s budget=%d", pattern, mode, budget)
					require.Equal(t, products, len(search.products), "%s %s budget=%d", pattern, mode, budget)
					require.Equal(t, levels, len(search.levels), "%s %s budget=%d", pattern, mode, budget)
				}
				ctx.cancel()
			}
		}
	}
}
func TestSelectedPathBindingCancellationCheckpoints(t *testing.T) {
	e, base := newShortestSelectorExecutor(t)
	r, err := e.Execute(base, "MATCH (a:S {id:1}), (b:S {id:4}) RETURN a,b", nil)
	require.NoError(t, err)
	source, target := r.Rows[0][0].(*storage.Node), r.Rows[0][1].(*storage.Node)
	for _, pattern := range []string{"(a)-[r:T*]->(b)", "(a)((x)-[r:T]->(y) WHERE x.id <= y.id)+(b)"} {
		plan, _, ok := compileSelectedPathPlan(pattern, "p", "", nil)
		require.True(t, ok)
		var paths []pathSearchPath
		fixtureAdmission1696Context, fixtureAdmission1696 := statementTestContext(base, ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1696Context
		s := e.relationshipExecutionFor(fixtureAdmission1696, fixtureAdmission1696Context, pattern, "").newPathSearch(base, plan.forward, pipelineRow{}, 4, source)
		fixtureAdmission1697Context, fixtureAdmission1697 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1697Context
		s.pair(fixtureAdmission1697, source, target, 1, true, false, func(path pathSearchPath) (pipelineRow, bool) { paths = append(paths, path); return pipelineRow{}, true })
		require.NotEmpty(t, paths)
		for _, path := range paths {
			for _, reversed := range []bool{false, true} {
				for budget := 1; budget <= 100; budget++ {
					ctx := newCheckpointContext(budget, nil)
					fixtureAdmission1698Context, fixtureAdmission1698 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
					_ = fixtureAdmission1698Context
					_, ok := e.bindSelectedPath(fixtureAdmission1698, fixtureAdmission1698Context, plan, plan.forward, reversed, pipelineRow{}, path)
					if ctx.Context.Err() != nil {
						require.False(t, ok)
					} else {
						require.True(t, ok)
					}
					ctx.cancel()
				}
			}
			for _, value := range []interface{}{nil, []interface{}{}, []interface{}{&storage.Edge{ID: "other"}, &storage.Edge{ID: "other2"}}} {
				fixtureAdmission1699Context, fixtureAdmission1699 := statementTestContext(base, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
				_ = fixtureAdmission1699Context
				_, ok := e.bindSelectedPath(fixtureAdmission1699, fixtureAdmission1699Context, plan, plan.forward, false, pipelineRow{"r": value}, path)
				require.False(t, ok)
			}
		}
	}
	for budget := 1; budget <= 20; budget++ {
		ctx := newCheckpointContext(budget, nil)
		value := e.pathToMapContext(ctx, PathResult{Nodes: []*storage.Node{source, target}, Relationships: []*storage.Edge{{ID: "e"}}})
		if ctx.Context.Err() != nil {
			require.Nil(t, value)
		} else {
			require.NotNil(t, value)
		}
		ctx.cancel()
	}
}
func TestSelectedPathUniformEligibilityAndTrailFallback(t *testing.T) {
	for _, pattern := range []string{"(a)(()-[:T]->()-[:U]->())+(b)", "(a)(()-[:T|U]->()-[:T]->())+(b)"} {
		plan, _, ok := compileSelectedPathPlan(pattern, "p", "", nil)
		require.True(t, ok)
		require.Nil(t, plan.forward.uniform)
	}
	require.False(t, equalStrings([]string{"T"}, []string{"U"}))
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "trailfallback"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:S {id:1})-[:T]->(:S {id:2})", nil)
	require.NoError(t, err)
	r, err := e.Execute(ctx, "MATCH (a:S {id:1}) RETURN a", nil)
	require.NoError(t, err)
	source := r.Rows[0][0].(*storage.Node)
	plan, _, ok := compileSelectedPathPlan("(a)-[:T]-(x)-[:T]-(b)", "p", "", nil)
	require.True(t, ok)
	for _, repeat := range []bool{false, true} {
		searchCtx := ctx
		if repeat {
			searchCtx = withRepeatableElements(ctx)
		}
		fixtureAdmission1700Context, fixtureAdmission1700 := statementTestContext(searchCtx, ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1700Context
		s := e.relationshipExecutionFor(fixtureAdmission1700, fixtureAdmission1700Context, "(a)-[:T]-(x)-[:T]-(b)", "").newPathSearch(searchCtx, plan.forward, pipelineRow{}, 2, source)
		fixtureAdmission1701Context, fixtureAdmission1701 := statementTestContext(context.Background(), ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1701Context
		paths := s.pair(fixtureAdmission1701, source, source, 1, true, true, func(path pathSearchPath) (pipelineRow, bool) {
			fixtureAdmission1702Context, fixtureAdmission1702 := statementTestContext(searchCtx, ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("MATCH "+("(a)-[:T]-(x)-[:T]-(b)")))
			_ = fixtureAdmission1702Context
			return e.bindSelectedPath(fixtureAdmission1702, fixtureAdmission1702Context, plan, plan.forward, false, pipelineRow{}, path)
		})
		if repeat {
			require.Len(t, paths, 1)
		} else {
			require.Empty(t, paths)
		}
	}
	_, err = e.Execute(ctx, "MATCH p = ALL SHORTEST (a:S {id:1})((x)-[r:T]->(y) WHERE true)+(b:S {id:2}) RETURN length(p)", nil)
	require.NoError(t, err)
}

func TestSelectedPathZeroCounterGuard(t *testing.T) {
	e, ctx := newShortestSelectorExecutor(t)
	r, err := e.Execute(ctx, "MATCH (a:S {id:1}) RETURN a", nil)
	require.NoError(t, err)
	source := r.Rows[0][0].(*storage.Node)
	plan, _, ok := compileSelectedPathPlan("(a)(()-[:T]->()){0,1}(b)", "p", "", nil)
	require.True(t, ok)
	// A zero budget also holds when a counted body remains in the automaton.
	a := *plan.forward
	a.counters = append([]pathSearchCounter(nil), a.counters...)
	a.counters[0].max = 0
	zeroContext, zeroStatement := traversalTestContext(ctx, "(a)(()-[:T]->()){0,1}(b)", "", nil)
	s := e.relationshipExecutionFor(zeroStatement, zeroContext, "(a)(()-[:T]->()){0,1}(b)", "").newPathSearch(ctx, &a, pipelineRow{}, 0, source)
	s.begin(zeroStatement, source)
	require.Len(t, s.levels[0].entries, 1)
	require.Equal(t, a.final, s.products[s.levels[0].entries[0].product].slot)
	path, ok := a.walkPath(source, nil)
	require.True(t, ok)
	require.Empty(t, path.steps)
}
