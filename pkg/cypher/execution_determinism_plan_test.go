package cypher

import (
    "context"
    "testing"
    "sync"

    "github.com/stretchr/testify/require"
)

// Admission is on every result-cache hit. Unchanged syntax must not allocate
// another permission map, argument slice or literal request map on that path.
func TestExecutionDeterminismCachedAdmissionAllocations(t *testing.T) {
    exec := solR2CacheExecutor(t)
    ctx := withQueryParams(context.Background(), map[string]any{
        "request": map[string]any{"query": "transcript", "limit": int64(2)},
    })
    for _, query := range []string{
        "CALL db.rretrieve($request)",
        "CALL db.rretrieve({query: 'transcript', limit: 2})",
    } {
        t.Run(query, func(t *testing.T) {
            info := exec.analyzer.Analyze(query)
            require.True(t, exec.canUseResultCache(ctx, info, query))
            allocations := testing.AllocsPerRun(1000, func() {
                if !exec.canUseResultCache(ctx, info, query) {
                    t.Fatal("ordinary one-shot request must remain cacheable")
                }
            })
            require.Zero(t, allocations, "a cached analysis needs only binding-dependent admission")
        })
    }
}

// The same cached QueryInfo must inspect each execution's current request,
// including text reached through a parameter path and nested overrides.
func TestExecutionDeterminismCachedBindings(t *testing.T) {
    exec := solR2CacheExecutor(t)
    for _, query := range []string{
        "CALL db.rretrieve($envelope.request)",
        "CALL db.rretrieve($envelope['request'])",
        "CALL apoc.cypher.run('CALL db.rretrieve($request)', {request:$envelope.request}) YIELD value RETURN value",
    } {
        info := exec.analyzer.Analyze(query)
        for _, test := range []struct{request map[string]any; allowed bool}{
            {map[string]any{"query":"transcript"}, true},
            {map[string]any{"query":"transcript", "where":"timestamp() < $deadline"}, false},
            {map[string]any{"query":"transcript", "mode":"id", "n":1}, false},
            {map[string]any{"query":"transcript", "where":"node.kind = $kind"}, true},
            {map[string]any{"query":"transcript"}, true},
        } {
            ctx := withQueryParams(context.Background(), map[string]any{
                "envelope":map[string]any{"request":test.request},
                "request":map[string]any{"query":"transcript", "where":"rand() > 0.5"},
            })
            require.Equal(t, test.allowed, exec.canUseResultCache(ctx, info, query), query)
        }
    }
}

func TestExecutionDeterminismCachedRegistryAndPluginDependencies(t *testing.T) {
    exec := solR2CacheExecutor(t)
    ctx := context.Background()
    const query = "CALL db.labels() YIELD label RETURN label"
    info := exec.analyzer.Analyze(query)
    require.True(t, exec.canUseResultCache(ctx, info, query))
    original, found := globalProcedureRegistry.Get("db.labels")
    require.True(t, found)
    t.Cleanup(func(){require.NoError(t, globalProcedureRegistry.RegisterBuiltIn(original.Spec, original.Handler))})
    changed := original.Spec
    changed.ResultCache.Deterministic = false
    require.NoError(t, globalProcedureRegistry.RegisterBuiltIn(changed, original.Handler))
    require.False(t, exec.canUseResultCache(ctx, info, query), "cached policy must follow a replacement declaration")
    require.NoError(t, globalProcedureRegistry.RegisterBuiltIn(original.Spec, original.Handler))
    require.True(t, exec.canUseResultCache(ctx, info, query))

    // Literal inner text was folded into the outer verdict, but its function
    // dependency must still observe plugin loading after that compilation.
    const nested = "CALL apoc.cypher.run('RETURN toString(1) AS value', {}) YIELD value RETURN value"
    nestedInfo := exec.analyzer.Analyze(nested)
    require.True(t, exec.canUseResultCache(ctx, nestedInfo, nested))
    lookup := PluginFunctionLookup
    t.Cleanup(func(){PluginFunctionLookup=lookup})
    loaded := true
    PluginFunctionLookup = func(name string)(any,bool){return nil, loaded && name=="toString"}
    require.False(t, exec.canUseResultCache(ctx, nestedInfo, nested))
    loaded=false
    require.True(t, exec.canUseResultCache(ctx, nestedInfo, nested))
}

func TestExecutionDeterminismCachedConcurrentBindings(t *testing.T) {
    exec := solR2CacheExecutor(t)
    const query = "CALL db.rretrieve($request)"
    info := exec.analyzer.Analyze(query)
    var workers sync.WaitGroup
    for worker:=0; worker<8; worker++ {
        workers.Add(1)
        go func(worker int){
            defer workers.Done()
            request:=map[string]any{"query":"transcript"}
            allowed:=worker%2==0
            if !allowed {request["where"]="timestamp() < $deadline"}
            ctx:=withQueryParams(context.Background(),map[string]any{"request":request})
            for i:=0;i<100;i++ {
                if actual:=exec.canUseResultCache(ctx,info,query); actual!=allowed {t.Errorf("worker%d: admission=%t want%t",worker,actual,allowed);return}
            }
        }(worker)
    }
    workers.Wait()
}
