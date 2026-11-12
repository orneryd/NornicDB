// Backend-binding evidence for the APOC storage migration.
//
// Initialize publishes one function registry whose helpers are bound to the
// supplied storage backend. SetStorage publishes a complete replacement registry
// for future lookups; a handler that was already admitted keeps the backend it
// was built for, and each bound instance owns its own warmup cache. Every
// assertion below goes through the registered handler the query adapter uses,
// with a real cancellation or a real expired deadline where a lifetime matters.
package apoc

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/apoc/registry"
	apocrel "github.com/orneryd/nornicdb/apoc/rel"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// apocTestProcessContext is the initialization lifetime of this package's
// static test backend. That backend is built while the package initializes,
// before TestMain and before any test exists, so no statement and no *testing.T
// can supply a lifetime at that boundary: it belongs to this test process.
var apocTestProcessContext = context.Background()

// apocInitializedBackend is the backend the process's first Initialize supplies.
// It carries the pre-change reproduction's graph: one Person named Alice.
var apocInitializedBackend = func() apocstorage.Storage {
	backend := apocstorage.NewInMemoryStorage()
	if _, err := backend.CreateNode(apocTestProcessContext, []string{"Person"}, map[string]interface{}{"name": "Alice"}); err != nil {
		panic(err)
	}
	return backend
}()

// TestMain makes the initialized registry of this package bound to a backend
// that is not the module default, exactly as an application initializes APOC.
func TestMain(m *testing.M) {
	if err := Initialize(apocInitializedBackend, nil); err != nil {
		fmt.Fprintln(os.Stderr, "apoc initialize:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// apocBindingBackend returns an independent backend holding one Person per name.
func apocBindingBackend(t *testing.T, names ...string) apocstorage.Storage {
	t.Helper()
	backend := apocstorage.NewInMemoryStorage()
	for _, name := range names {
		_, err := backend.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{"name": name})
		require.NoError(t, err)
	}
	return backend
}

// apocRegisteredHandler returns the admitted handler behind a registered name.
func apocRegisteredHandler(t *testing.T, name string) *registry.Callable {
	t.Helper()
	info, found := GetInfo(name)
	require.True(t, found, "%s must be registered", name)
	require.NotNil(t, info.Handler, name)
	return info.Handler
}

// apocCallStatementHandler invokes a registered handler the way the Cypher
// adapter does: the caller's context is bound as the handler's first argument.
func apocCallStatementHandler(t *testing.T, name string, ctx context.Context, args ...interface{}) (interface{}, error) {
	t.Helper()
	return apocRegisteredHandler(t, name).CallWithPolicy(
		args,
		[]reflect.Value{reflect.ValueOf(ctx)},
		registry.CypherArguments,
	)
}

// apocSearchPrefixNodes runs the registered prefix search and returns its nodes.
func apocSearchPrefixNodes(t *testing.T, ctx context.Context, prefix string) []*apocstorage.Node {
	t.Helper()
	value, err := apocCallStatementHandler(t, "apoc.search.prefix", ctx, "Person", "name", prefix)
	require.NoError(t, err, "apoc.search.prefix")
	nodes, ok := value.([]*apocstorage.Node)
	require.True(t, ok, "apoc.search.prefix returns nodes, got %T", value)
	return nodes
}

// TestRegisteredSearchHandlerUsesInitializedBackend preserves the pre-change
// reproduction: Initialize supplies a backend, and the registered
// apoc.search.prefix handler answers from it. Before the migration the same call
// returned zero nodes and no error because the handler read a package default.
func TestRegisteredSearchHandlerUsesInitializedBackend(t *testing.T) {
	require.Equal(t, apocInitializedBackend, GetStorage(),
		"GetStorage returns the default registry's own backend")

	nodes := apocSearchPrefixNodes(t, context.Background(), "Ali")
	require.Len(t, nodes, 1, "the initialized backend supplies the registered handler's result")
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	require.Empty(t, apocSearchPrefixNodes(t, context.Background(), "Nobody"))
}

// TestReconfigurationReplacesRegistryForFutureLookups proves a second backend
// becomes what future lookups see: the replacement registry is bound to it and
// nothing from the previous backend leaks through.
func TestReconfigurationReplacesRegistryForFutureLookups(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, SetStorage(apocInitializedBackend)) })

	second := apocBindingBackend(t, "Bob", "Carol")
	require.NoError(t, SetStorage(second))
	require.Equal(t, second, GetStorage())

	nodes := apocSearchPrefixNodes(t, context.Background(), "Bo")
	require.Len(t, nodes, 1)
	require.Equal(t, "Bob", nodes[0].Properties["name"])

	require.Empty(t, apocSearchPrefixNodes(t, context.Background(), "Ali"),
		"the previous backend's node must not answer from the replacement registry")
}

// TestRetainedHandlerKeepsItsOriginalBackend proves the lifetime half of the
// contract: an admitted handler keeps the backend it was built for, so a caller
// holding it is never silently rebound to another backend.
func TestRetainedHandlerKeepsItsOriginalBackend(t *testing.T) {
	retained := apocRegisteredHandler(t, "apoc.search.prefix")

	t.Cleanup(func() { require.NoError(t, SetStorage(apocInitializedBackend)) })
	second := apocBindingBackend(t, "Bob")
	require.NoError(t, SetStorage(second))

	value, err := retained.CallWithPolicy(
		[]interface{}{"Person", "name", "Ali"},
		[]reflect.Value{reflect.ValueOf(context.Background())},
		registry.CypherArguments,
	)
	require.NoError(t, err)
	nodes, ok := value.([]*apocstorage.Node)
	require.True(t, ok, "%T", value)
	require.Len(t, nodes, 1, "the retained handler still reads its original backend")
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	fresh := apocSearchPrefixNodes(t, context.Background(), "Bo")
	require.Len(t, fresh, 1, "a lookup made after the swap reads the replacement backend")
	require.Equal(t, "Bob", fresh[0].Properties["name"])
}

// TestInstancesKeepSeparateWarmupCaches proves the warmup cache belongs to the
// bound instance: a backend swap cannot read another backend's cached graph,
// and the instance that was warmed keeps its own cache.
func TestInstancesKeepSeparateWarmupCaches(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, SetStorage(apocInitializedBackend)) })

	require.NoError(t, SetStorage(apocBindingBackend(t, "Alice")))
	warmed := apocRegisteredHandler(t, "apoc.warmup.stats")

	value, err := apocCallStatementHandler(t, "apoc.warmup.run", context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, value.(map[string]interface{})["nodesLoaded"])

	firstStats := apocWarmupStats(t, "apoc.warmup.stats")
	require.Equal(t, 1, firstStats["nodesCached"])

	second := apocstorage.NewInMemoryStorage()
	_, err = second.CreateNode(t.Context(), []string{"Company"}, map[string]interface{}{"name": "Acme"})
	require.NoError(t, err)
	require.NoError(t, SetStorage(second))

	emptyStats := apocWarmupStats(t, "apoc.warmup.stats")
	require.Equal(t, 0, emptyStats["nodesCached"],
		"a new backend must not inherit another backend's warmup cache")

	value, err = apocCallStatementHandler(t, "apoc.warmup.run", context.Background())
	require.NoError(t, err)
	secondStats := apocWarmupStats(t, "apoc.warmup.stats")
	require.Equal(t, 1, secondStats["nodesCached"])

	// The retained handler still answers from the instance warmed first.
	value, err = warmed.Call(nil)
	require.NoError(t, err)
	require.Equal(t, 1, value.(map[string]interface{})["nodesCached"],
		"the retained warmup handler keeps its own instance and cache")
}

// apocWarmupStats invokes one warmup statistics handler. The warmup statistics
// helper reads only its instance cache, so it takes no context.
func apocWarmupStats(t *testing.T, name string) map[string]interface{} {
	t.Helper()
	value, err := apocRegisteredHandler(t, name).Call(nil)
	require.NoError(t, err, name)
	stats, ok := value.(map[string]interface{})
	require.True(t, ok, "%s returns a map, got %T", name, value)
	return stats
}

// TestRegisteredBoundHandlersReceiveTheCallersLifetime drives bound handlers of
// a separately constructed instance with a real cancellation and a real expired
// deadline: the whole-graph read sees that exact lifetime and the helper keeps
// its documented empty result.
func TestRegisteredBoundHandlersReceiveTheCallersLifetime(t *testing.T) {
	bound := apocBindingBackend(t, "Alice")

	for _, interrupted := range []struct {
		name string
		open func() (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "cancel",
			open: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			want: context.Canceled,
		},
		{
			name: "deadline",
			open: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
				<-ctx.Done()
				return ctx, cancel
			},
			want: context.DeadlineExceeded,
		},
	} {
		t.Run(interrupted.name, func(t *testing.T) {
			t.Cleanup(func() { require.NoError(t, SetStorage(apocInitializedBackend)) })
			require.NoError(t, SetStorage(bound))

			ctx, cancel := interrupted.open()
			defer cancel()
			require.True(t, ctx.Err() != nil, "the lifetime must already be stopped")
			require.ErrorIs(t, ctx.Err(), interrupted.want)

			value, err := apocCallStatementHandler(t, "apoc.search.prefix", ctx, "Person", "name", "Ali")
			require.ErrorIs(t, err, interrupted.want)
			require.Nil(t, value)

			value, err = apocCallStatementHandler(t, "apoc.warmup.run", ctx)
			require.ErrorIs(t, err, interrupted.want)
			require.Nil(t, value)
		})
	}
}

// TestRegisteredLoadHandlerKeepsItsIOCapabilityBoundary pins the unchanged I/O
// security contract through a registered handler: the migration changes
// receivers only, so APOC I/O still refuses a context with no bound capability
// and performs no read.
func TestRegisteredLoadHandlerKeepsItsIOCapabilityBoundary(t *testing.T) {
	_, err := apocCallStatementHandler(t, "apoc.load.json", context.Background(), "payload.json")
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)
}

// TestMetadataPropertyKeysIsOneOwnerForBothCypherNames pins the retired Go
// duplicate: apoc.meta.propertyKeys and apoc.schema.properties register the one
// backend-bound metadata PropertyKeys owner, with no forwarding alias.
func TestMetadataPropertyKeysIsOneOwnerForBothCypherNames(t *testing.T) {
	metaHandler := apocRegisteredHandler(t, "apoc.meta.propertyKeys")
	schemaHandler := apocRegisteredHandler(t, "apoc.schema.properties")
	require.Equal(t, metaHandler.Symbol(), schemaHandler.Symbol(),
		"both Cypher names must register the one metadata PropertyKeys owner")
	require.Contains(t, metaHandler.Symbol(), "/apoc/meta.(*BoundFunctions).PropertyKeys",
		"the single owner is the metadata instance method, not a schema duplicate")

	metaValue, err := apocCallStatementHandler(t, "apoc.meta.propertyKeys", context.Background())
	require.NoError(t, err)
	schemaValue, err := apocCallStatementHandler(t, "apoc.schema.properties", context.Background())
	require.NoError(t, err)
	require.Equal(t, metaValue, schemaValue, "both names answer with the same keys")
	require.ElementsMatch(t, []string{"name"}, metaValue.([]string))
}

// TestBoundHandlerAddsNoPerRowAllocations measures the guarded invocation of
// the same helper on two independently built instances: binding to a separate
// backend instance adds no allocation on the per-row path. A free helper of the
// same shape is measured beside it as a reference.
func TestBoundHandlerAddsNoPerRowAllocations(t *testing.T) {
	first := apocrel.New(apocstorage.NewInMemoryStorage()).StartNode
	second := apocrel.New(apocstorage.NewInMemoryStorage()).StartNode
	firstHandler, err := registry.Compile(first, registry.OpaqueReads)
	require.NoError(t, err)
	secondHandler, err := registry.Compile(second, registry.OpaqueReads)
	require.NoError(t, err)
	legacyHandler, err := registry.Compile(apocrel.ID, registry.CanonicalReads)
	require.NoError(t, err)
	freeHandler, err := registry.Compile(func(context.Context, *apocstorage.Relationship) (*apocstorage.Node, error) { return nil, nil }, registry.OpaqueReads)
	require.NoError(t, err)

	relationship := &apocstorage.Relationship{ID: 1, StartNode: 1, EndNode: 2, Type: "KNOWS"}
	values := []reflect.Value{reflect.ValueOf(t.Context()), reflect.ValueOf(relationship)}
	legacyValues := []reflect.Value{reflect.ValueOf(relationship)}
	freeValues := values

	measure := func(handler *registry.Callable, bound []reflect.Value) float64 {
		return testing.AllocsPerRun(100, func() {
			if _, callErr := handler.InvokeValues(bound); callErr != nil {
				t.Fatal(callErr)
			}
		})
	}

	firstAllocs := measure(firstHandler, values)
	secondAllocs := measure(secondHandler, values)
	freeAllocs := measure(freeHandler, freeValues)
	legacyAllocs := measure(legacyHandler, legacyValues)
	t.Logf("one-result legacy reflection reference: %v allocs; two-result reference: %v", legacyAllocs, freeAllocs)
	t.Logf("per-row allocations: instance-bound %v, independent instance %v, free helper %v",
		firstAllocs, secondAllocs, freeAllocs)

	require.Equal(t, firstAllocs, secondAllocs,
		"an independently built instance must not add allocations per call")
	require.LessOrEqual(t, secondAllocs, freeAllocs,
		"binding must not cost more per call than a free helper of the same shape")
}

// Callback arguments cross an implementation boundary: a pure adapter cannot
// certify graph reads performed by the caller's closure. Inspect every admitted
// bundled signature, including future additions, rather than trusting names.
func TestBundledCallbackConsumersKeepOpaqueReadPolicy(t *testing.T) {
	callbacks := 0
	for _, name := range List() {
		function, found := GetInfo(name)
		require.True(t, found, name)
		signature := function.Handler.Signature()
		for i := 0; i < signature.NumIn(); i++ {
			if signature.In(i).Kind() == reflect.Func {
				callbacks++
				require.Equal(t, registry.OpaqueReads, function.Handler.ReadObservation(), name)
				break
			}
		}
	}
	require.Positive(t, callbacks, "the fixture must exercise callback consumers")
}
