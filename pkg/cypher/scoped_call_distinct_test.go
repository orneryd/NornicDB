package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type scopedDistinctControl struct {
	name, query string
	params      map[string]interface{}
}

// The archive preserves the twelve original statements and parameter files.
func scopedDistinctControls(t testing.TB) []scopedDistinctControl {
	t.Helper()
	archive, err := zip.OpenReader("testdata/scoped_call_distinct.zip")
	require.NoError(t, err)
	defer archive.Close()
	files := map[string][]byte{}
	for _, file := range archive.File {
		reader, err := file.Open()
		require.NoError(t, err)
		files[file.Name], err = io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
	}
	var controls []scopedDistinctControl
	for _, kind := range []string{"scoped-distinct", "keys-only-distinct", "scoped-aggregate"} {
		for _, size := range []int{0, 128, 1024, 4188} {
			name := fmt.Sprintf("%s-%d", kind, size)
			var params map[string]interface{}
			require.NoError(t, json.Unmarshal(files[name+".parameters.json"], &params))
			controls = append(controls, scopedDistinctControl{name, string(files[name+".cypher"]), params})
		}
	}
	return controls
}

func TestScopedCallDistinctControls(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scoped_distinct"))
	for _, control := range scopedDistinctControls(t) {
		t.Run(control.name, func(t *testing.T) {
			started := time.Now()
			result, err := exec.Execute(context.Background(), control.query, control.params)
			require.NoError(t, err)
			require.Equal(t, []string{"things"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(996)}}, result.Rows)
			t.Logf("996 things; elapsed=%s", time.Since(started))
		})
	}
}

func TestScopedCallDistinctScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scoped_distinct_scope"))
	for _, query := range []string{
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,3] AS y WITH y RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT x, y RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH y, count(*) AS c ORDER BY x RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y RETURN DISTINCT x*y AS a ORDER BY x } RETURN a ORDER BY a",
		"WITH {factor:11} AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x.factor RETURN x.factor*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (*) { UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { CALL () { UNWIND [22,33] AS a RETURN a } RETURN a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y WHERE x = 11 WITH y ORDER BY x RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { WITH 1 AS ignored CALL (x) { UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS b } RETURN b AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL (x) { UNWIND [2,2] AS y WITH DISTINCT y RETURN x*y AS a UNION UNWIND [3,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a ORDER BY a",
		"WITH 11 AS x CALL { WITH x UNWIND [2,2,3] AS y WITH DISTINCT x, y RETURN x*y AS a } RETURN a ORDER BY a",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(22)}, {int64(33)}}, result.Rows)
		})
	}
	for _, query := range []string{
		"CALL () { WITH 11 AS x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a",
		"WITH 11 AS x CALL { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y RETURN x*y AS a } RETURN a",
		"WITH 11 AS imported CALL (imported) { WITH 7 AS local UNWIND [2,2,3] AS y WITH DISTINCT y RETURN local*y AS a } RETURN a",
		"CALL () { WITH 11 AS x UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x RETURN y AS a } RETURN a",
		"CALL () { WITH 11 AS x UNWIND [2,2,3] AS y RETURN DISTINCT y AS a ORDER BY x } RETURN a",
		"WITH 11 AS x CALL { WITH x UNWIND [2,2,3] AS y WITH DISTINCT y ORDER BY x RETURN y AS a } RETURN a",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			variable := "x"
			if strings.Contains(query, "local*y") {
				variable = "local"
			}
			require.EqualError(t, err, "Variable `"+variable+"` not defined")
		})
	}
}

func TestDistinctTupleBoundaries(t *testing.T) {
	// Joining value keys with a separator loses the tuple boundaries when a
	// string contains that separator and the following value's type prefix.
	rows := [][]interface{}{{"a\x1fSb", "c"}, {"a", "b\x1fSc"}, {"a\x1fSb", "c"}}
	require.Len(t, deduplicatePipelineResultRows(rows), 2)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "distinct_values"))
	for _, query := range []string{
		"WITH $values AS values CALL (values) { UNWIND values AS each WITH DISTINCT each AS thing RETURN count(*) AS things } RETURN things",
		"UNWIND $values AS each RETURN count(DISTINCT each) AS things",
	} {
		result, err := exec.Execute(context.Background(), query, map[string]interface{}{"values": []interface{}{
			map[string]interface{}{"a": int64(1), "b": nil}, map[string]interface{}{"b": nil, "a": 1.0},
			[]interface{}{int64(1), nil}, []interface{}{1.0, nil}, int64(1), 1.0, nil, nil,
		}})
		require.NoError(t, err)
		want := int64(4)
		if strings.Contains(query, "count(DISTINCT") {
			want = 3
		}
		require.Equal(t, [][]interface{}{{want}}, result.Rows)
	}
	result, err := exec.Execute(context.Background(), "WITH {active: true} AS eff_view, [{thing: 1}, {thing: 1.0}, {thing: 2}] AS incidence CALL (eff_view, incidence) { UNWIND incidence AS each WITH DISTINCT eff_view, each.thing AS thing RETURN count(*) AS things } RETURN things", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	result, err = exec.Execute(context.Background(), "WITH 0/0.0 AS payload, [1,1,2] AS keys CALL (payload, keys) { UNWIND keys AS each WITH DISTINCT each AS thing RETURN count(*) AS things } RETURN things", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, "an invariant NaN import is not a DISTINCT key")
	result, err = exec.Execute(context.Background(), "UNWIND $rows AS row WITH row[0] AS a, row[1] AS b RETURN a, b, count(*) AS c", map[string]interface{}{"rows": rows})
	require.NoError(t, err)
	require.Len(t, result.Rows, 2, "grouping preserves tuple boundaries too")
	projected := []pipelineRow{
		{"eff_view": map[string]interface{}{"active": true}, "thing": int64(1)},
		{"eff_view": map[string]interface{}{"active": false}, "thing": int64(1)},
	}
	require.Len(t, deduplicatePipelineRows(nil, projected, []string{"eff_view", "thing"}), 2, "a projected import belongs in the key")
	require.Len(t, deduplicatePipelineRows(nil, projected, []string{"thing"}), 1)
}

func BenchmarkScopedCallDistinct(b *testing.B) {
	store := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { require.NoError(b, store.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "scoped_distinct_bench"))
	for _, control := range scopedDistinctControls(b) {
		var nonce int64
		b.Run(control.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				nonce++ // Stay unique across benchmark calibration rounds too.
				control.params["nonce"] = nonce
				result, err := exec.Execute(context.Background(), control.query, control.params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != int64(996) {
					b.Fatal(result.Rows)
				}
			}
		})
	}
}
