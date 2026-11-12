package fn

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// graphTestContext is a function context on a composite database whose
// graphs are graphs; composite false is any other database. Eval returns
// the value vars holds for an argument expression.
func graphTestContext(graphs []string, composite bool, vars map[string]interface{}) Context {
	return Context{
		Eval: func(expr string) (interface{}, error) {
			if expr == "boom" {
				return nil, errors.New("eval failed")
			}
			return vars[expr], nil
		},
		Graphs: testGraphCatalog{graphs: graphs, composite: composite},
	}
}

// testGraphCatalog is a fixed GraphCatalog.
type testGraphCatalog struct {
	graphs    []string
	composite bool
}

func (c testGraphCatalog) CompositeGraphs() ([]string, bool) { return c.graphs, c.composite }

func TestGraphNamesListsCompositeGraphs(t *testing.T) {
	ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, nil)
	value, found, err := EvaluateFunction(context.Background(), "graph.names", nil, ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []interface{}{"cmp.a", "cmp.b"}, value)

	// A composite database with no graphs lists none.
	value, found, err = EvaluateFunction(context.Background(), "GRAPH.NAMES", nil, graphTestContext(nil, true, nil))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []interface{}{}, value)
}

func TestGraphFunctionsAreUnknownOutsideComposite(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  Context
	}{
		{"no graphs callback", Context{Eval: func(string) (interface{}, error) { return nil, nil }}},
		{"not a composite", graphTestContext(nil, false, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, function := range []string{"graph.names", "graph.propertiesByName"} {
				value, found, err := EvaluateFunction(context.Background(), function, []string{"'cmp.a'"}, tc.ctx)
				require.True(t, found)
				require.Nil(t, value)
				var unknown *UnknownFunctionError
				require.ErrorAs(t, err, &unknown)
				require.Equal(t, function, unknown.Function)
				require.EqualError(t, err, "Unknown function '"+function+"'")
			}
		})
	}
}

func TestGraphPropertiesByName(t *testing.T) {
	vars := map[string]interface{}{"a": "cmp.a", "upper": "CMP.B", "missing": "cmp.zz", "number": int64(1)}
	ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, vars)

	t.Run("a graph of the composite is an empty map", func(t *testing.T) {
		for _, arg := range []string{"a", "upper"} {
			value, found, err := EvaluateFunction(context.Background(), "graph.propertiesByName", []string{arg}, ctx)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, map[string]interface{}{}, value)
		}
	})

	t.Run("a name that is no graph of the composite is Graph not found", func(t *testing.T) {
		value, found, err := EvaluateFunction(context.Background(), "graph.propertiesByName", []string{"missing"}, ctx)
		require.True(t, found)
		require.Nil(t, value)
		var notFound *GraphNotFoundError
		require.ErrorAs(t, err, &notFound)
		require.Equal(t, "cmp.zz", notFound.Name)
		require.EqualError(t, err, "Graph not found: cmp.zz")
	})

	t.Run("a non-string name is an argument type error", func(t *testing.T) {
		_, _, err := EvaluateFunction(context.Background(), "graph.propertiesByName", []string{"number"}, ctx)
		var argErr *ArgumentTypeError
		require.ErrorAs(t, err, &argErr)
		require.Equal(t, "graph.propertiesByName", argErr.Function)
		require.Equal(t, int64(1), argErr.Value)
	})

	t.Run("wrong argument count", func(t *testing.T) {
		_, _, err := EvaluateFunction(context.Background(), "graph.propertiesByName", nil, ctx)
		require.EqualError(t, err, "Insufficient parameters for function 'graph.propertiesByName'")
		_, _, err = EvaluateFunction(context.Background(), "graph.propertiesByName", []string{"a", "a"}, ctx)
		require.EqualError(t, err, "Too many parameters for function 'graph.propertiesByName'")
		// An empty argument list may arrive as one empty argument.
		names, _, err := EvaluateFunction(context.Background(), "graph.names", []string{" "}, ctx)
		require.NoError(t, err)
		require.NotNil(t, names)
		_, _, err = EvaluateFunction(context.Background(), "graph.names", []string{"1"}, ctx)
		require.EqualError(t, err, "Too many parameters for function 'graph.names'")
		var count *ParameterCountError
		require.ErrorAs(t, err, &count)
		require.Equal(t, "graph.names", count.Function)
		require.True(t, count.TooMany)
	})

	t.Run("an argument that fails to evaluate fails the call", func(t *testing.T) {
		_, _, err := EvaluateFunction(context.Background(), "graph.propertiesByName", []string{"boom"}, ctx)
		require.EqualError(t, err, "eval failed")
	})
}

// catalogRead is one access a graph function reported, in the profile ledger's
// unit: the logical catalog operation it performed and the population that
// operation returned.
type catalogRead struct {
	kind, detail string
	rows, hits   int64
}

// catalogAccess is one access a graph function reported and the counter the
// function added that access's rows and hits to.
type catalogAccess struct {
	kind, detail string
	counter      *queryprofile.ReadCounter
}

// catalogObserver owns the access counters of the canonical reads and the
// opaque callbacks one function call reported.
type catalogObserver struct {
	accessed   []catalogAccess
	unobserved []string
}

func (o *catalogObserver) UnobservedRead(_ context.Context, category, name string) {
	o.unobserved = append(o.unobserved, category+": "+name)
}

func (o *catalogObserver) Access(_ context.Context, kind, detail string) *queryprofile.ReadCounter {
	for _, access := range o.accessed {
		if access.kind == kind && access.detail == detail {
			return access.counter
		}
	}
	counter := &queryprofile.ReadCounter{}
	o.accessed = append(o.accessed, catalogAccess{kind: kind, detail: detail, counter: counter})
	return counter
}

// reads reports the accesses this observer received in the profile ledger's
// unit, reading what each access counted from its counter.
func (o *catalogObserver) reads() []catalogRead {
	out := make([]catalogRead, 0, len(o.accessed))
	for _, access := range o.accessed {
		rows, hits := access.counter.Values()
		out = append(out, catalogRead{kind: access.kind, detail: access.detail, rows: rows, hits: hits})
	}
	return out
}

// evaluateObservedGraphFunction evaluates name under a fresh request-owned
// observer, the way a PROFILE statement's execution context carries one.
func evaluateObservedGraphFunction(t *testing.T, name string, args []string, ctx Context) (interface{}, *catalogObserver, error) {
	t.Helper()
	observer := &catalogObserver{}
	value, found, err := EvaluateFunction(queryprofile.WithObserver(context.Background(), observer), name, args, ctx)
	require.True(t, found, name)
	return value, observer, err
}

// hiddenCatalogVisibility admits no database's metadata.
func hiddenCatalogVisibility() auth.CatalogVisibility {
	return auth.NewCatalogVisibility(nil, nil, func(string, string) bool { return false })
}

// TestGraphCatalogFunctionsRecordTheCatalogAcquisition: graph.names() and
// graph.propertiesByName() report the one composite catalog acquisition they
// make - the graphs the catalog listed, before the function applies the
// statement's visibility - and add no opaque callback of their own.
func TestGraphCatalogFunctionsRecordTheCatalogAcquisition(t *testing.T) {
	acquired := func(rows int64) []catalogRead {
		return []catalogRead{{kind: profileKindCompositeGraphCatalog, rows: rows, hits: 1}}
	}

	t.Run("graph.names lists what the catalog returned", func(t *testing.T) {
		value, observer, err := evaluateObservedGraphFunction(t, "graph.names", nil, graphTestContext([]string{"cmp.a", "cmp.b"}, true, nil))
		require.NoError(t, err)
		require.Equal(t, []interface{}{"cmp.a", "cmp.b"}, value)
		require.Empty(t, observer.unobserved, "a canonical registration reports no opaque callback")
		require.Equal(t, acquired(2), observer.reads())
	})

	t.Run("a hidden graph stays in the acquisition it was filtered out of", func(t *testing.T) {
		ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, nil)
		ctx.CatalogVisibility = hiddenCatalogVisibility()
		value, observer, err := evaluateObservedGraphFunction(t, "graph.names", nil, ctx)
		require.NoError(t, err)
		require.Equal(t, []interface{}{}, value)
		require.Equal(t, acquired(2), observer.reads(), "the record keeps the catalog's population, not the empty row")
	})

	t.Run("an empty catalog is one acquisition of nothing", func(t *testing.T) {
		_, observer, err := evaluateObservedGraphFunction(t, "graph.names", nil, graphTestContext(nil, true, nil))
		require.NoError(t, err)
		require.Equal(t, acquired(0), observer.reads())
	})

	t.Run("graph.propertiesByName acquires the same catalog once", func(t *testing.T) {
		value, observer, err := evaluateObservedGraphFunction(t, "graph.propertiesByName", []string{"a"},
			graphTestContext([]string{"cmp.a", "cmp.b"}, true, map[string]interface{}{"a": "cmp.a"}))
		require.NoError(t, err)
		require.Equal(t, map[string]interface{}{}, value)
		require.Empty(t, observer.unobserved)
		require.Equal(t, acquired(2), observer.reads())
	})

	t.Run("a graph the statement cannot see is still acquired", func(t *testing.T) {
		ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, map[string]interface{}{"a": "cmp.a"})
		ctx.CatalogVisibility = hiddenCatalogVisibility()
		_, observer, err := evaluateObservedGraphFunction(t, "graph.propertiesByName", []string{"a"}, ctx)
		var notFound *GraphNotFoundError
		require.ErrorAs(t, err, &notFound)
		require.Equal(t, acquired(2), observer.reads())
	})

	t.Run("a context without a catalog makes no request", func(t *testing.T) {
		ctx := Context{Eval: func(string) (interface{}, error) { return nil, nil }}
		_, observer, err := evaluateObservedGraphFunction(t, "graph.names", nil, ctx)
		var unknown *UnknownFunctionError
		require.ErrorAs(t, err, &unknown)
		require.Empty(t, observer.reads())
	})
}

// TestGraphFunctionNameCarriesNoRegistrationTrust: replacing a graph function's
// registration with a custom callback is reported as an opaque callback, though
// the built-in under that name records the whole access it performs.
func TestGraphFunctionNameCarriesNoRegistrationTrust(t *testing.T) {
	for _, name := range []string{"graph.names", "graph.propertiesByName"} {
		t.Run(name, func(t *testing.T) {
			restoreRegistration(t, name)
			Register(name, func(_ context.Context, _ Context, _ []string) (interface{}, error) {
				return nil, nil
			}, OpaqueReads)
			_, observer, err := evaluateObservedGraphFunction(t, name, []string{"a"}, graphTestContext([]string{"cmp.a"}, true, nil))
			require.NoError(t, err)
			require.Empty(t, observer.reads(), "the replacement acquires no catalog")
			require.Equal(t, []string{"registered function callback: " + strings.ToLower(name)}, observer.unobserved,
				"a custom callback under a built-in name stays opaque")
		})
	}
}

// This component ledger has no clause scope to freeze.
func (o *catalogObserver) WithLifetime(lifetime context.Context, _ context.Context) context.Context {
	return queryprofile.WithObserver(lifetime, o)
}
