package fabric

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type applyLookupAdmissionRows struct{ nextCalls int }

func (r *applyLookupAdmissionRows) Next() bool       { r.nextCalls++; return false }
func (*applyLookupAdmissionRows) Row() []interface{} { return nil }
func (*applyLookupAdmissionRows) Err() error         { return nil }
func (*applyLookupAdmissionRows) Close() error       { return nil }

type applyLookupAdmissionColumns struct {
	useOwnerLookupColumns
	projectionCalls int
}

type applyLookupBatchProbe struct {
	mockCypherExecutor
	run func(context.Context, string, string, map[string]interface{}) ([]string, [][]interface{}, error)
}

func (p *applyLookupBatchProbe) ExecuteQueryWithRecord(ctx context.Context, database string, _ storage.Engine, query string, params map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	return p.run(ctx, database, query, params)
}

func runLookupContractShape(e *FabricExecutor, ctx context.Context, shape string, input *ResultStream, inner *FragmentExec, params map[string]interface{}) (*ResultStream, bool, error) {
	switch shape {
	case "rows":
		return e.tryExecuteApplyBatchedLookupRows(ctx, nil, input, inner, params, "")
	case "stream":
		return e.tryExecuteApplyBatchedLookup(ctx, nil, input.Columns, NewResultRowIterator(input), inner, params, "", nil)
	case "count":
		return e.tryExecuteApplyBatchedCountLookup(ctx, nil, input, inner, params, "")
	case "collect":
		return e.tryExecuteApplyBatchedLookup(ctx, nil, input.Columns, nil, inner, params, "", input)
	default:
		panic("invalid test shape")
	}
}

func TestApplyLookupProductsPreserveOuterRowsAndParams(t *testing.T) {
	for _, shape := range []string{"rows", "stream", "count", "collect"} {
		t.Run(shape, func(t *testing.T) {
			projection, column := "n.v AS v", "v"
			batchRows := [][]interface{}{{"a", "a1"}, {"a", "a2"}, {"b", "b1"}}
			expected := [][]interface{}{{"b", "B", "b1"}, {"a", "A1", "a1"}, {"a", "A1", "a2"}, {"a", "A2", "a1"}, {"a", "A2", "a2"}}
			if shape == "count" {
				projection, column = "count(*) AS c", "c"
				batchRows = [][]interface{}{{"a", int64(2)}, {"b", int64(1)}}
				expected = [][]interface{}{{"b", "B", int64(1)}, {"a", "A1", int64(2)}, {"a", "A2", int64(2)}, {nil, "none", int64(0)}, {nil, nil, int64(0)}}
			} else if shape == "collect" {
				projection = "collect(n.v) AS v"
				batchRows = [][]interface{}{{"a", []interface{}{"a1", "a2"}}, {"b", []interface{}{"b1"}}}
				expected = [][]interface{}{{"b", "B", []interface{}{"b1"}}, {"a", "A1", []interface{}{"a1", "a2"}}, {"a", "A2", []interface{}{"a1", "a2"}}, {nil, "none", []interface{}{}}, {nil, nil, []interface{}{}}}
			}
			batchQuery := "MATCH (n) WHERE n.id IN $__fabric_apply_keys_1 RETURN n.id AS __fabric_apply_key, " + projection
			input := &ResultStream{Columns: []string{"k", "tag"}, Rows: [][]interface{}{{"b", "B"}, {"a", "A1"}, {"a", "A2"}, {nil, "none"}, {}}}
			inner := &FragmentExec{Input: &FragmentInit{Columns: []string{"k"}, ImportColumns: []string{"k"}}, Query: "WITH k MATCH (n) WHERE n.id = k RETURN " + projection, GraphName: "db1", Columns: []string{column}}
			originalInner := *inner
			params := map[string]interface{}{"sentinel": "keep", "__fabric_apply_keys": "reserved"}
			type contextKey struct{}
			ctx := context.WithValue(t.Context(), contextKey{}, "kept")
			var calls atomic.Int32
			probe := &applyLookupBatchProbe{run: func(ctx context.Context, database, query string, local map[string]interface{}) ([]string, [][]interface{}, error) {
				calls.Add(1)
				if database != "db1" || query != batchQuery || ctx.Value(contextKey{}) != "kept" || local["sentinel"] != "keep" || local["__fabric_apply_keys"] != "reserved" || !reflect.DeepEqual(local["__fabric_apply_keys_1"], []interface{}{"b", "a"}) {
					return nil, nil, fmt.Errorf("batch route, query, context or private parameters changed")
				}
				local["backend-only"] = true
				return []string{"__fabric_apply_key", column}, batchRows, nil
			}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			local := NewLocalFragmentExecutor(probe, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
			executor := NewFabricExecutor(catalog, local, nil)
			executor.SetResultColumnProvider(useOwnerLookupColumns{inner.Query: inner.Columns})
			result, handled, err := runLookupContractShape(executor, ctx, shape, input, inner, params)
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, []string{"k", "tag", column}, result.Columns)
			require.Equal(t, expected, result.Rows)
			require.EqualValues(t, 1, calls.Load())
			require.Equal(t, map[string]interface{}{"sentinel": "keep", "__fabric_apply_keys": "reserved"}, params)
			require.Equal(t, originalInner, *inner)
		})
	}
}

func TestApplyLookupActiveCancellationReturnsOutcome(t *testing.T) {
	for _, shape := range []string{"rows", "stream", "count", "collect"} {
		t.Run(shape, func(t *testing.T) {
			projection, column := "n.v AS v", "v"
			if shape == "count" {
				projection, column = "count(*) AS c", "c"
			} else if shape == "collect" {
				projection = "collect(n.v) AS v"
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{}, 1)
			probe := &applyLookupBatchProbe{run: func(ctx context.Context, _, _ string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, nil, ctx.Err()
			}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			local := NewLocalFragmentExecutor(probe, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
			executor := NewFabricExecutor(catalog, local, nil)
			inner := &FragmentExec{Query: "WITH k MATCH (n) WHERE n.id = k RETURN " + projection, GraphName: "db1", Columns: []string{column}}
			executor.SetResultColumnProvider(useOwnerLookupColumns{inner.Query: inner.Columns})
			outcome := make(chan error, 1)
			go func() {
				result, handled, err := runLookupContractShape(executor, ctx, shape, &ResultStream{Columns: []string{"k"}, Rows: [][]interface{}{{"a"}}}, inner, nil)
				if !handled || result != nil {
					outcome <- fmt.Errorf("accepted cancellation lost result contract: handled=%v result=%v", handled, result)
					return
				}
				outcome <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("lookup did not reach the real local dispatch")
			}
			cancel()
			select {
			case err := <-outcome:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("lookup did not join its canceled dispatch")
			}
		})
	}
}

func TestApplyLookupMaterializedMultipleBatches(t *testing.T) {
	for _, shape := range []string{"rows", "count", "collect"} {
		t.Run(shape, func(t *testing.T) {
			projection, column := "n.v AS v", "v"
			if shape == "count" {
				projection, column = "count(*) AS c", "c"
			} else if shape == "collect" {
				projection = "collect(n.v) AS v"
			}
			ctx, cancel := context.WithCancel(t.Context())
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			var releaseOnce sync.Once
			var calls atomic.Int32
			probe := &applyLookupBatchProbe{run: func(ctx context.Context, _, _ string, params map[string]interface{}) ([]string, [][]interface{}, error) {
				calls.Add(1)
				keys, ok := params["__fabric_apply_keys"].([]interface{})
				if !ok || len(keys) != 512 || params["sentinel"] != "keep" {
					return nil, nil, fmt.Errorf("private key batch or caller parameter changed")
				}
				params["local-only"] = true
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
				rows := make([][]interface{}, 0, len(keys))
				for i := len(keys) - 1; i >= 0; i-- {
					value := interface{}("v-" + keys[i].(string))
					if shape == "count" {
						value = int64(1)
					} else if shape == "collect" {
						value = []interface{}{"v-" + keys[i].(string)}
					}
					rows = append(rows, []interface{}{keys[i], value})
				}
				return []string{"__fabric_apply_key", column}, rows, nil
			}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			local := NewLocalFragmentExecutor(probe, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
			executor := NewFabricExecutor(catalog, local, nil)
			inner := &FragmentExec{Query: "WITH k MATCH (n) WHERE n.id = k RETURN " + projection, GraphName: "db1", Columns: []string{column}}
			executor.SetResultColumnProvider(useOwnerLookupColumns{inner.Query: inner.Columns})
			input := &ResultStream{Columns: []string{"k"}}
			expected := make([][]interface{}, 0, 1024)
			for i := range 1024 {
				key := fmt.Sprintf("key-%04d", i)
				input.Rows = append(input.Rows, []interface{}{key})
				value := interface{}("v-" + key)
				if shape == "count" {
					value = int64(1)
				} else if shape == "collect" {
					value = []interface{}{"v-" + key}
				}
				expected = append(expected, []interface{}{key, value})
			}
			params := map[string]interface{}{"sentinel": "keep"}
			type lookupOutcome struct {
				result  *ResultStream
				handled bool
				err     error
			}
			outcome := make(chan lookupOutcome, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				result, handled, err := runLookupContractShape(executor, ctx, shape, input, inner, params)
				outcome <- lookupOutcome{result, handled, err}
			}()
			t.Cleanup(func() {
				cancel()
				releaseOnce.Do(func() { close(release) })
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("batch dispatch did not finish during cleanup")
				}
			})
			for range 2 {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("the two materialized batches did not dispatch concurrently")
				}
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case result := <-outcome:
				require.NoError(t, result.err)
				require.True(t, result.handled)
				require.Equal(t, []string{"k", column}, result.result.Columns)
				require.Equal(t, expected, result.result.Rows)
				require.EqualValues(t, 2, calls.Load())
				require.Equal(t, map[string]interface{}{"sentinel": "keep"}, params)
			case <-time.After(5 * time.Second):
				t.Fatal("materialized batches did not return their joined product")
			}

		})
	}
}

func (p *applyLookupAdmissionColumns) ResultProjection(ctx context.Context, query string, incoming []string, stage SourceStage) resultmetadata.Projection {
	p.projectionCalls++
	return p.useOwnerLookupColumns.ResultProjection(ctx, query, incoming, stage)
}

func TestApplyLookupStreamModifiersDeclineBeforeInput(t *testing.T) {
	for _, modifiers := range []string{"ORDER BY v", "SKIP 1", "LIMIT 1"} {
		t.Run(modifiers, func(t *testing.T) {
			inner := &FragmentExec{Query: "WITH k MATCH (n) WHERE n.id = k RETURN n.v AS v " + modifiers, Columns: []string{"v"}}
			columns := &applyLookupAdmissionColumns{useOwnerLookupColumns: useOwnerLookupColumns{inner.Query: inner.Columns}}
			executor := NewFabricExecutor(NewCatalog(), nil, nil)
			executor.SetResultColumnProvider(columns)
			input := &applyLookupAdmissionRows{}
			result, handled, err := executor.tryExecuteApplyBatchedLookup(t.Context(), nil, []string{"k"}, input, inner, nil, "", nil)
			require.NoError(t, err)
			assert.False(t, handled)
			assert.Nil(t, result)
			assert.Zero(t, input.nextCalls, "declined source must leave the input iterator untouched")
			assert.Zero(t, columns.projectionCalls, "declined source must not request projection facts")
		})
	}
}
