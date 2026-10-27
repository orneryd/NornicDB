package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPluginCanonicalProcedureABI(t *testing.T) {
	cypher.ClearUserProcedures()
	t.Cleanup(cypher.ClearUserProcedures)
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	e := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "plugin_abi"))
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "actual context")
	handler := cypher.ProcedureHandler(func(actual context.Context, exec *cypher.StorageExecutor, query string, args []interface{}) (*cypher.ExecuteResult, error) {
		require.Same(t, e, exec)
		require.Equal(t, "actual context", actual.Value(key{}))
		require.Contains(t, query, "custom.actualContext")
		return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{args[0]}}}, nil
	})
	registerProcedureWithCypher(PluginProcedure{Name: "custom.actualContext", Handler: handler, Signature: "custom.actualContext(value :: ANY) :: (value :: ANY)", Mode: "READ", MinArgs: 1, MaxArgs: 1})
	r, err := e.Execute(ctx, "CALL custom.actualContext(7) YIELD value RETURN value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
	_, err = e.Execute(ctx, "CALL custom.actualContext()", nil)
	require.Error(t, err)
	_, err = e.Execute(ctx, "CALL custom.actualcontext(7)", nil)
	require.Error(t, err)
}

type invalidProcedureFixture struct{}

func (invalidProcedureFixture) Name() string                                        { return "invalid_proc_fixture" }
func (invalidProcedureFixture) Version() string                                     { return "1.0" }
func (invalidProcedureFixture) Functions() map[string]struct{ Handler interface{} } { return nil }
func (invalidProcedureFixture) Procedures() map[string]struct {
	Handler interface{}
	Mode    string
} {
	return map[string]struct {
		Handler interface{}
		Mode    string
	}{"good": {Handler: func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error) {
		return &cypher.ExecuteResult{}, nil
	}, Mode: "READ"}, "bad": {Handler: 3, Mode: "WRITE"}}
}
func TestPluginInvalidProcedureFailsWholeLoad(t *testing.T) {
	cypher.ClearUserProcedures()
	t.Cleanup(cypher.ClearUserProcedures)
	_, err := loadFunctionPlugin(invalidProcedureFixture{}, "fixture.so")
	require.Error(t, err)
	require.Empty(t, cypher.RegisteredProceduresForCalls(t.Context(), "CALL invalid_proc_fixture.good()", cypher.RawSource))
}

func TestPluginProcedureDeclarationChecks(t *testing.T) {
	t.Run("declaration validation and atomic load", func(t *testing.T) {
		cypher.ClearUserProcedures()
		t.Cleanup(cypher.ClearUserProcedures)
		raw := func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
			return &cypher.ExecuteResult{}, nil
		}
		for _, mode := range []string{"", "READ", "WRITE", "SCHEMA", "ADMIN", "DBMS"} {
			d, err := pluginProcedureForCypher(PluginProcedure{Name: "checked.mode", Mode: mode, Handler: raw})
			require.NoError(t, err)
			require.NotNil(t, d.handler)
		}
		for _, proc := range []PluginProcedure{
			{Name: "bad", Handler: raw, Mode: "TYPO"}, {Name: "bad", Handler: cypher.ProcedureHandler(nil)},
			{Name: "bad", Handler: (func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error))(nil)},
			{Name: "bad", Handler: 3}, {Name: "", Handler: raw}, {Name: "bad", Handler: raw, MinArgs: 2, MaxArgs: 1},
		} {
			require.Error(t, registerProcedureWithCypher(proc))
		}
		require.Error(t, registerPluginProcedures([]PluginProcedure{{Name: "atomic.good", Handler: raw}, {Name: "atomic.bad", Handler: 3}}))
		require.Empty(t, cypher.RegisteredProceduresForCalls(t.Context(), "CALL atomic.good()", cypher.RawSource))

	})
	t.Run("invocation failures and cancellation", func(t *testing.T) {
		sentinel := errors.New("fixture error")
		for _, tc := range []struct {
			handler cypher.ProcedureHandler
			want    string
		}{
			{func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
				panic("fixture")
			}, "panicked"},
			{func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
				return nil, nil
			}, "nil result"},
			{func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
				return nil, sentinel
			}, "fixture error"},
		} {
			d, err := pluginProcedureForCypher(PluginProcedure{Name: "checked.invoke", Handler: tc.handler})
			require.NoError(t, err)
			_, err = d.handler(context.Background(), nil, "", nil)
			require.ErrorContains(t, err, tc.want)
		}
		d, err := pluginProcedureForCypher(PluginProcedure{Name: "checked.cancel", Handler: cypher.ProcedureHandler(func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
			panic("not invoked")
		})})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = d.handler(ctx, nil, "", nil)
		require.ErrorIs(t, err, context.Canceled)

	})
}
