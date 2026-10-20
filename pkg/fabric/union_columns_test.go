package fabric

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnionColumnContract_AllExecutionEntries(t *testing.T) {
	for _, entry := range []string{"stream_read", "stream_write", "sequential", "parallel"} {
		for _, distinct := range []bool{false, true} {
			t.Run(entry+"/"+map[bool]string{false: "all", true: "distinct"}[distinct], func(t *testing.T) {
				catalog := NewCatalog()
				catalog.Register("db", &LocationLocal{DBName: "db1"})
				mock := &mockCypherExecutor{results: map[string]*ResultStream{
					"left":  {Columns: []string{"first", "second"}, Rows: [][]interface{}{{"a", "b"}}},
					"right": {Columns: []string{"second", "first"}, Rows: [][]interface{}{{"b", "a"}}},
				}}
				exec := newTestFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
				union := &FragmentUnion{
					LHS:      &FragmentExec{GraphName: "db", Query: "left", IsWrite: entry == "stream_write"},
					RHS:      &FragmentExec{GraphName: "db", Query: "right"},
					Distinct: distinct,
					Columns:  []string{"first", "second"},
				}
				var result *ResultStream
				var err error
				switch entry {
				case "sequential":
					result, err = exec.executeUnionSequential(context.Background(), nil, union, nil, "")
				case "parallel":
					result, err = exec.executeUnionParallel(context.Background(), nil, union, nil, "")
				default:
					result, err = exec.Execute(context.Background(), nil, union, nil, "")
				}
				require.NoError(t, err)
				want := [][]interface{}{{"a", "b"}, {"a", "b"}}
				if distinct {
					want = want[:1]
				}
				require.Equal(t, []string{"first", "second"}, result.Columns)
				require.Equal(t, want, result.Rows)
			})
		}
	}
}

func TestUnionColumnContract_PlannedEmptyMetadata(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "reordered", true: "mismatched"}[mismatch], func(t *testing.T) {
			rightColumns := []string{"second", "first"}
			if mismatch {
				rightColumns = []string{"other", "first"}
			}
			union := &FragmentUnion{
				LHS: &FragmentExec{Columns: []string{"first", "second"}},
				RHS: &FragmentExec{Columns: rightColumns},
			}
			exec := newTestFabricExecutor(NewCatalog(), nil, nil)
			columns, rows, err := exec.mergeUnionRows(union, nil, NewResultRowIterator(nil), rightColumns, NewResultRowIterator(&ResultStream{Rows: [][]interface{}{{"b", "a"}}}))
			if mismatch {
				var columnError *UnionColumnsError
				require.ErrorAs(t, err, &columnError)
				return
			}
			require.NoError(t, err)
			defer rows.Close()
			require.Equal(t, []string{"first", "second"}, columns)
			require.True(t, rows.Next())
			require.Equal(t, []interface{}{"a", "b"}, rows.Row())
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
