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
				fixtureAdmission1703Context, fixtureAdmission1703 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
				_ = fixtureAdmission1703Context
				search = e.relationshipExecutionFor(fixtureAdmission1703, fixtureAdmission1703Context, pattern, "").newPathSearch(ctx, plan.forward, pipelineRow{}, 6, source)
				bind := func(path pathSearchPath) (pipelineRow, bool) {
					bound++
					fixtureAdmission1704Context, fixtureAdmission1704 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
					_ = fixtureAdmission1704Context
					return e.bindSelectedPath(fixtureAdmission1704, fixtureAdmission1704Context, plan, plan.forward, false, pipelineRow{}, path)
				}
				switch mode {
				case "one":
					fixtureAdmission1705Context, fixtureAdmission1705 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1705Context
					search.pair(fixtureAdmission1705, source, target, 1, false, true, bind)
				case "pair":
					fixtureAdmission1706Context, fixtureAdmission1706 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1706Context
					search.pair(fixtureAdmission1706, source, target, 1, true, true, bind)
				case "run":
					fixtureAdmission1707Context, fixtureAdmission1707 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1707Context
					search.run(fixtureAdmission1707, source, 2, true, bind)
				case "forward":
					fixtureAdmission1708Context, fixtureAdmission1708 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1708Context
					search.begin(fixtureAdmission1708, source)
					w := &pathSearchWalks{forward: map[int32]int{}, preds: map[int32][]pathSearchHop{}, backward: map[pathSearchKey]int{}}
					if len(search.levels) > 0 {
						for _, entry := range search.levels[0].entries {
							w.forward[entry.product] = 0
							w.frontier = append(w.frontier, entry.product)
						}
					}
					fixtureAdmission1709Context, fixtureAdmission1709 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1709Context
					search.expandForward(fixtureAdmission1709, w)
				case "backward":
					fixtureAdmission1710Context, fixtureAdmission1710 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1710Context
					search.begin(fixtureAdmission1710, source)
					key := pathSearchKey{node: target.ID, slot: int32(plan.forward.final)}
					w := &pathSearchWalks{backward: map[pathSearchKey]int{key: 0}, nodes: map[pathSearchKey]*storage.Node{key: target}, succs: map[pathSearchKey][]pathSearchBack{}, back: []pathSearchKey{key}, forward: map[int32]int{}}
					fixtureAdmission1711Context, fixtureAdmission1711 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1711Context
					search.expandBackward(fixtureAdmission1711, w)
				case "trace":
					fixtureAdmission1712Context, fixtureAdmission1712 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1712Context
					search.begin(fixtureAdmission1712, source)
					fixtureAdmission1713Context, fixtureAdmission1713 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
					_ = fixtureAdmission1713Context
					for depth := 0; depth < 3 && search.ctx.Err() == nil && search.nextLevel(fixtureAdmission1713); depth++ {
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
		fixtureAdmission1714Context, fixtureAdmission1714 := statementTestContext(base, ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1714Context
		s := e.relationshipExecutionFor(fixtureAdmission1714, fixtureAdmission1714Context, pattern, "").newPathSearch(base, plan.forward, pipelineRow{}, 4, source)
		fixtureAdmission1715Context, fixtureAdmission1715 := statementTestContext(context.Background(), ("MATCH "+(pattern))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1715Context
		s.pair(fixtureAdmission1715, source, target, 1, true, false, func(path pathSearchPath) (pipelineRow, bool) { paths = append(paths, path); return pipelineRow{}, true })
		require.NotEmpty(t, paths)
		for _, path := range paths {
			for _, reversed := range []bool{false, true} {
				for budget := 1; budget <= 100; budget++ {
					ctx := newCheckpointContext(budget, nil)
					fixtureAdmission1716Context, fixtureAdmission1716 := statementTestContext(ctx, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
					_ = fixtureAdmission1716Context
					_, ok := e.bindSelectedPath(fixtureAdmission1716, fixtureAdmission1716Context, plan, plan.forward, reversed, pipelineRow{}, path)
					if ctx.Context.Err() != nil {
						require.False(t, ok)
					} else {
						require.True(t, ok)
					}
					ctx.cancel()
				}
			}
			for _, value := range []interface{}{nil, []interface{}{}, []interface{}{&storage.Edge{ID: "other"}, &storage.Edge{ID: "other2"}}} {
				fixtureAdmission1717Context, fixtureAdmission1717 := statementTestContext(base, ("MATCH "+(pattern))+"\n"+("MATCH "+(pattern)))
				_ = fixtureAdmission1717Context
				_, ok := e.bindSelectedPath(fixtureAdmission1717, fixtureAdmission1717Context, plan, plan.forward, false, pipelineRow{"r": value}, path)
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
		fixtureAdmission1718Context, fixtureAdmission1718 := statementTestContext(searchCtx, ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1718Context
		s := e.relationshipExecutionFor(fixtureAdmission1718, fixtureAdmission1718Context, "(a)-[:T]-(x)-[:T]-(b)", "").newPathSearch(searchCtx, plan.forward, pipelineRow{}, 2, source)
		fixtureAdmission1719Context, fixtureAdmission1719 := statementTestContext(context.Background(), ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("RETURN "+("")))
		_ = fixtureAdmission1719Context
		paths := s.pair(fixtureAdmission1719, source, source, 1, true, true, func(path pathSearchPath) (pipelineRow, bool) {
			fixtureAdmission1720Context, fixtureAdmission1720 := statementTestContext(searchCtx, ("MATCH "+("(a)-[:T]-(x)-[:T]-(b)"))+"\n"+("MATCH "+("(a)-[:T]-(x)-[:T]-(b)")))
			_ = fixtureAdmission1720Context
			return e.bindSelectedPath(fixtureAdmission1720, fixtureAdmission1720Context, plan, plan.forward, false, pipelineRow{}, path)
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
