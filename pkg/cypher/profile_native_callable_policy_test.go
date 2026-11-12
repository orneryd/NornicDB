package cypher

// Author: Codex NOR-184 lane. Public proof: an admitted callable's own read
// observation decides whether a profile may certify its complete access count,
// in both parsers, for bundled and custom implementations.

import (
	"math"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// initializeBundledAPOC publishes the bundled APOC function table these
// controls read, exactly as server start does. Initialization is process-wide,
// so the first caller wins and every later call is a no-op.
func initializeBundledAPOC(t *testing.T) {
	t.Helper()
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
}

// bundledMathCeil is the reviewed bundled pure implementation the bundled and
// custom controls below publish through the same plugin lookup the server uses.
func bundledMathCeil(t *testing.T) *registry.Callable {
	t.Helper()
	initializeBundledAPOC(t)
	info, ok := apoc.GetInfo("apoc.math.ceil")
	require.True(t, ok, "apoc.math.ceil is a bundled function")
	require.Equal(t, registry.CanonicalReads, info.Handler.ReadObservation(),
		"a reviewed pure bundled implementation reports canonical reads")
	return info.Handler
}

// An admitted pure implementation reads no database state, so a profile of its
// invocation confirms the measured zero instead of reporting an unknown
// callback. Both parsers must reach the same plan facts.
func TestProfileBundledCanonicalCallableReadsAreObservable(t *testing.T) {
	handler := bundledMathCeil(t)
	forEachParser(t, func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{"apoc.math.ceil": handler})
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(t.Context(), "PROFILE RETURN apoc.math.ceil(2.5) AS value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{float64(3)}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.True(t, plan.TotalDBHitsObserved, "an admitted canonical callable reports its own complete reads")
		require.Zero(t, plan.TotalDBHits)
		require.Empty(t, plan.Unobserved)
		require.Contains(t, formatPlan(plan), "Total DB Hits: 0")
	})
}

// The same name answered by a custom implementation is not the reviewed
// bundled one: a profile must report the invocation as an opaque callback,
// even though the result value is identical.
func TestProfileCustomCallableReplacingBundledNameStaysOpaque(t *testing.T) {
	initializeBundledAPOC(t)
	forEachParser(t, func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{
			"apoc.math.ceil": func(value float64) float64 { return math.Ceil(value) },
		})
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(t.Context(), "PROFILE RETURN apoc.math.ceil(2.5) AS value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{float64(3)}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved, "a custom callback's complete access count is unknown")
		require.NotEmpty(t, plan.Unobserved)
		require.Contains(t, formatPlan(plan), "Total DB Hits: unobserved")
	})
}

// A handler the query cannot call is never entered: the built-in function
// answers instead, so no opaque callback may be reported for it.
func TestProfileRejectedCallableSignatureReportsNoOpaqueRead(t *testing.T) {
	initializeBundledAPOC(t)
	forEachParser(t, func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{
			"apoc.text.join": func(chan int) int8 { return 0 },
		})
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(t.Context(), "PROFILE RETURN apoc.text.join(['a', 'b'], '-') AS value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a-b"}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.True(t, plan.TotalDBHitsObserved, "a rejected callback was not invoked")
		require.Empty(t, plan.Unobserved)
		require.Zero(t, plan.TotalDBHits)
	})
}

func TestProfileOpaqueCallableReturningAnotherCallsRejectionIsStillOpaque(t *testing.T) {
	rejector, err := registry.Compile(func() {}, registry.OpaqueReads)
	require.NoError(t, err)
	_, rejected := rejector.CallWithPolicy([]interface{}{1}, nil, registry.GoArguments)
	require.ErrorIs(t, rejected, registry.ErrUnsupportedSignature)
	forEachParser(t, func(t *testing.T) {
		invoked := false
		withPluginFunctions(t, map[string]interface{}{
			"apoc.text.join": func(interface{}, string) (string, error) { invoked = true; return "", rejected },
		})
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		result, err := exec.Execute(t.Context(), "PROFILE RETURN apoc.text.join(['a','b'], '-') AS value", nil)
		require.NoError(t, err)
		require.True(t, invoked, "the opaque implementation ran before the supported fallback")
		require.Equal(t, [][]interface{}{{"a-b"}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved, "a callback's returned error cannot deny that it ran")
		require.NotEmpty(t, plan.Unobserved)
	})
}

func TestProfilePanickingOpaqueCallableStillReportsItsInvocation(t *testing.T) {
	handler, err := registry.Compile(func() { panic("callback entered") }, registry.OpaqueReads)
	require.NoError(t, err)
	collector := newProfileRuntimeCollector()
	_, err = callPluginHandlerContext(withProfileRuntimeCollector(t.Context(), collector), handler, nil)
	require.ErrorContains(t, err, "callback entered")
	accesses := collector.accessObservation()
	require.Len(t, accesses, 1, "the implementation was entered before its panic")
	require.True(t, accesses[0].Unknown)
}

func TestProfileBundledCallbackConsumerDoesNotCertifyItsCallersReads(t *testing.T) {
	initializeBundledAPOC(t)
	info, found := apoc.GetInfo("apoc.json.map")
	require.True(t, found)
	forEachParser(t, func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{"apoc.json.map": info.Handler})
		reader, _ := newProfileRuntimeTestExecutor(t, "CREATE (:Witness)")
		calls := 0
		transform := func(value interface{}) interface{} {
			result, err := reader.Execute(t.Context(), "MATCH (n:Witness) RETURN count(n) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			calls++
			return value
		}
		exec, _ := newProfileRuntimeTestExecutor(t)
		result, err := exec.Execute(t.Context(), "PROFILE RETURN apoc.json.map('[1]', $transform) AS value", map[string]interface{}{"transform": transform})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"[1]"}}, result.Rows)
		require.Equal(t, 1, calls, "the caller-supplied implementation performed its graph read")
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved, "a bundled adapter cannot certify an arbitrary callback's reads")
		require.NotEmpty(t, plan.Unobserved)
	})
}
