package fabric

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestApplyLookupProjectionFactContract(t *testing.T) {
	require.Equal(t, 2*unsafe.Sizeof(""), unsafe.Sizeof(fabricCountReturnItem{}))
	require.Equal(t, 3*unsafe.Sizeof(""), unsafe.Sizeof(fabricReturnItem{}))
	for _, test := range []struct {
		name     string
		shape    applyLookupShape
		source   string
		matchVar string
		accepted bool
		facts    applyLookupProjectionFacts
	}{
		{"property fields and aliases", applyLookupShapeRows, "n.name AS name, n.age AS age", "n", true, applyLookupProjectionFacts{returnItems: []fabricReturnItem{{expr: "n.name", prop: "name", alias: "name"}, {expr: "n.age", prop: "age", alias: "age"}}, aliases: []string{"name", "age"}}},
		{"property variable folding", applyLookupShapeRows, "N.value AS value", "n", true, applyLookupProjectionFacts{returnItems: []fabricReturnItem{{expr: "N.value", prop: "value", alias: "value"}}, aliases: []string{"value"}}},
		{"property wrong variable", applyLookupShapeRows, "m.name AS name", "n", false, applyLookupProjectionFacts{}},
		{"property valid prefix then rejected item", applyLookupShapeRows, "n.name AS name, broken AS other", "n", false, applyLookupProjectionFacts{}},
		{"property empty", applyLookupShapeRows, "", "n", false, applyLookupProjectionFacts{}},
		{"count typed products", applyLookupShapeCount, "count(*) AS c, count(tt) AS n, count(tt.id) AS ids", "", true, applyLookupProjectionFacts{countItems: []fabricCountReturnItem{{expr: "count(*)", alias: "c"}, {expr: "count(tt)", alias: "n"}, {expr: "count(tt.id)", alias: "ids"}}, aliases: []string{"c", "n", "ids"}}},
		{"count duplicate aliases retained", applyLookupShapeCount, "count(*) AS c, count(n.id) AS c", "", true, applyLookupProjectionFacts{countItems: []fabricCountReturnItem{{expr: "count(*)", alias: "c"}, {expr: "count(n.id)", alias: "c"}}, aliases: []string{"c", "c"}}},
		{"count wrong aggregate", applyLookupShapeCount, "sum(tt.x) AS s", "", false, applyLookupProjectionFacts{}},
		{"count valid prefix then rejected item", applyLookupShapeCount, "count(*) AS c, sum(n.v) AS total", "", false, applyLookupProjectionFacts{}},
		{"collect expression and alias", applyLookupShapeCollect, "collect(n.v) AS values", "", true, applyLookupProjectionFacts{collectExpression: "n.v", collectAlias: "values"}},
		{"collect opaque inner comma", applyLookupShapeCollect, " CoLlEcT( {a:n.v,b:[1,2]} ) AS values ", "", true, applyLookupProjectionFacts{collectExpression: "{a:n.v,b:[1,2]}", collectAlias: "values"}},
		{"collect explicit alias required", applyLookupShapeCollect, "collect(n.v)", "", false, applyLookupProjectionFacts{}},
		{"collect empty argument", applyLookupShapeCollect, "collect() AS v", "", false, applyLookupProjectionFacts{}},
		{"collect one projection only", applyLookupShapeCollect, "collect(n.v) AS a, collect(n.w) AS b", "", false, applyLookupProjectionFacts{}},
		{"collect wrong aggregate", applyLookupShapeCollect, "count(*) AS v", "", false, applyLookupProjectionFacts{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts, ok := readApplyLookupProjection(test.shape, test.source, test.matchVar)
			require.Equal(t, test.accepted, ok)
			require.Equal(t, test.facts, facts)
		})
	}
}

func TestApplyLookupCollectBindingContract(t *testing.T) {
	for _, test := range []struct {
		name      string
		columns   []string
		rows      [][]interface{}
		wantValue interface{}
		wantError string
	}{
		{"missing key alias", []string{"v"}, nil, nil, "batched APPLY lookup produced unexpected columns: [v]"},
		{"missing value alias", []string{"__fabric_apply_key"}, nil, nil, "batched APPLY lookup produced unexpected columns: [__fabric_apply_key]"},
		{"short row is skipped", []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a"}}, []interface{}{}, ""},
		{"last product wins", []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a", []interface{}{"first"}}, {"a", []interface{}{"last"}}}, []interface{}{"last"}, ""},
		{"last nil product wins", []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a", []interface{}{"first"}}, {"a", nil}}, []interface{}{}, ""},
		{"nil product is empty", []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a", nil}}, []interface{}{}, ""},
		{"typed nil product remains typed nil", []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a", []interface{}(nil)}}, []interface{}(nil), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &applyLookupBatchProbe{run: func(_ context.Context, _, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
				if query != "MATCH (n) WHERE n.id IN $__fabric_apply_keys RETURN n.id AS __fabric_apply_key, collect(n.v) AS v" || !reflect.DeepEqual(params["__fabric_apply_keys"], []interface{}{"a", "unmatched"}) {
					return nil, nil, fmt.Errorf("collect batch query or keys changed")
				}
				return test.columns, test.rows, nil
			}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			executor := NewFabricExecutor(catalog, NewLocalFragmentExecutor(probe, func(string) (storage.Engine, error) { return &mockEngine{}, nil }), nil)
			inner := &FragmentExec{Query: "WITH k MATCH (n) WHERE n.id = k RETURN collect(n.v) AS v", GraphName: "db1", Columns: []string{"v"}}
			executor.SetResultColumnProvider(useOwnerLookupColumns{inner.Query: inner.Columns})
			input := &ResultStream{Columns: []string{"k", "tag"}, Rows: [][]interface{}{{"a", "first"}, {"a", "repeat"}, {"unmatched", "none"}}}
			result, handled, err := runLookupContractShape(executor, t.Context(), "collect", input, inner, nil)
			require.True(t, handled)
			if test.wantError != "" {
				require.EqualError(t, err, test.wantError)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{"k", "tag", "v"}, result.Columns)
			require.Equal(t, [][]interface{}{{"a", "first", test.wantValue}, {"a", "repeat", test.wantValue}, {"unmatched", "none", []interface{}{}}}, result.Rows)
		})
	}
}

type applyLookupCollectMetadata struct {
	useOwnerLookupColumns
	incoming [][]string
}

func (p *applyLookupCollectMetadata) ResultProjection(ctx context.Context, query string, incoming []string, stage SourceStage) resultmetadata.Projection {
	p.incoming = append(p.incoming, append([]string(nil), incoming...))
	return p.useOwnerLookupColumns.ResultProjection(ctx, query, incoming, stage)
}

func TestApplyLookupCollectColumnAndAdmissionContract(t *testing.T) {
	for _, test := range []struct {
		name         string
		query        string
		inputColumns []string
		rowColumns   []string
		wantHandled  bool
		wantCalls    int
	}{
		{"separate key and metadata columns", "WITH k MATCH (n) WHERE n.id = k RETURN collect(n.v) AS v", []string{"k", "tag"}, []string{"tag", "k"}, true, 1},
		{"metadata precedes missing import key", "WITH k MATCH (n) WHERE n.id = k RETURN collect(n.v) AS v", []string{"different", "tag"}, []string{"k", "tag"}, false, 1},
		{"modifier declines before metadata", "WITH k MATCH (n) WHERE n.id = k RETURN collect(n.v) AS v LIMIT 1", []string{"k", "tag"}, []string{"k", "tag"}, false, 0},
		{"multiple collect items decline before metadata", "WITH k MATCH (n) WHERE n.id = k RETURN collect(n.v) AS v, collect(n.w) AS w", []string{"k", "tag"}, []string{"k", "tag"}, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			var transports int
			probe := &applyLookupBatchProbe{run: func(_ context.Context, _, _ string, params map[string]interface{}) ([]string, [][]interface{}, error) {
				transports++
				if !reflect.DeepEqual(params["__fabric_apply_keys"], []interface{}{"a"}) {
					return nil, nil, fmt.Errorf("keys did not use input column positions")
				}
				return []string{"__fabric_apply_key", "v"}, [][]interface{}{{"a", []interface{}{"ok"}}}, nil
			}}
			catalog := NewCatalog()
			catalog.Register("db1", &LocationLocal{DBName: "db1"})
			executor := NewFabricExecutor(catalog, NewLocalFragmentExecutor(probe, func(string) (storage.Engine, error) { return &mockEngine{}, nil }), nil)
			inner := &FragmentExec{Query: test.query, GraphName: "db1", Columns: []string{"v"}}
			metadata := &applyLookupCollectMetadata{useOwnerLookupColumns: useOwnerLookupColumns{inner.Query: inner.Columns}}
			executor.SetResultColumnProvider(metadata)
			input := &ResultStream{Columns: test.rowColumns, Rows: [][]interface{}{{"a", "first"}}}
			result, handled, err := executor.tryExecuteApplyBatchedLookup(t.Context(), nil, test.inputColumns, nil, inner, nil, "", input)
			require.NoError(t, err)
			require.Equal(t, test.wantHandled, handled)
			require.Len(t, metadata.incoming, test.wantCalls)
			if test.wantCalls != 0 {
				require.Equal(t, test.rowColumns, metadata.incoming[0])
			}
			if !handled {
				require.Nil(t, result)
				require.Zero(t, transports)
				return
			}
			require.Equal(t, 1, transports)
			require.Equal(t, []string{"tag", "k", "v"}, result.Columns)
			require.Equal(t, [][]interface{}{{"first", "a", []interface{}{"ok"}}}, result.Rows)
		})
	}
}
