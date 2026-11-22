package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNOR335IndexExplainSharesSyntaxWithoutBindingOptions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"CREATE INDEX nor335_range FOR (n:NOR335) ON (n.p)",
			"CREATE RANGE INDEX nor335_range FOR (n:NOR335) ON (n.p)",
			"CREATE RANGE INDEX nor335_range FOR ()-[r:NOR335]-() ON (r.p)",
			"CREATE VECTOR INDEX nor335_vector FOR (n:NOR335) ON (n.embedding)",
			"CREATE VECTOR INDEX nor335_vector FOR ()-[r:NOR335]-() ON (r.embedding)",
			"CREATE INDEX nor335_options FOR (n:NOR335) ON (n.p) OPTIONS $opts",
		} {
			t.Run(query, func(t *testing.T) {
				exec := nor335Executor(t)
				before := showConstraintsSchemaSnapshot(exec.storage.GetSchema())
				// Reuse the same statement with changed caller values. Metadata admission
				// must neither require/bind options nor retain them in a cached plan.
				for _, params := range []map[string]interface{}{nil, {"opts": int64(7)}, {"opts": map[string]interface{}{}}} {
					result, err := exec.Execute(t.Context(), "EXPLAIN "+query, params)
					require.NoError(t, err)
					require.Empty(t, result.Columns)
					require.Empty(t, result.Rows)
					require.NotNil(t, result.Metadata["plan"])
					require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()))
				}
				columns, err := exec.StatementResultColumnsContext(t.Context(), query, RawSource)
				require.NoError(t, err)
				require.Empty(t, columns)
				require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()))
			})
		}
		exec := nor335Executor(t)
		const query = "CREATE INDEX nor335_options FOR (n:NOR335) ON (n.p) OPTIONS $opts"
		_, err := exec.Execute(t.Context(), query, map[string]interface{}{"opts": int64(7)})
		require.Error(t, err, "actual execution still binds and validates options")
		require.NotContains(t, nor335IndexNames(t, exec, t.Context()), "nor335_options")
		_, err = exec.Execute(t.Context(), query, map[string]interface{}{"opts": map[string]interface{}{}})
		require.NoError(t, err)
		require.Contains(t, nor335IndexNames(t, exec, t.Context()), "nor335_options", "actual execution publishes the index")
	})
}

// Target admission shares its diagnostic. OPTIONS 7 is rejected earlier by
// grammar parsing, whose positions include the mode prefix. It must reject and
// remain effect-free, without requiring identical parser offsets.
func TestNOR335IndexModesRejectTheSameInvalidTarget(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			query          string
			sameDiagnostic bool
		}{
			{"CREATE INDEX bad FOR (n:NOR335) ON ()", true},
			{"CREATE RANGE INDEX bad FOR (n:NOR335) ON ()", true},
			{"CREATE INDEX bad FOR (n:NOR335) ON", true},
			{"CREATE VECTOR INDEX bad FOR (n:NOR335) ON (n.a,n.b)", true},
			{"CREATE VECTOR INDEX bad FOR (n:NOR335) ON ()", true},
			{"CREATE INDEX bad FOR (n:NOR335) ON (n.p) OPTIONS 7", false},
		} {
			query := tc.query
			t.Run(query, func(t *testing.T) {
				exec := nor335Executor(t)
				before := showConstraintsSchemaSnapshot(exec.storage.GetSchema())
				_, ordinaryErr := exec.Execute(t.Context(), query, nil)
				require.Error(t, ordinaryErr)
				for _, prefix := range []string{"EXPLAIN ", "PROFILE "} {
					_, err := exec.Execute(t.Context(), prefix+query, nil)
					require.Error(t, err)
					if tc.sameDiagnostic {
						require.EqualError(t, err, ordinaryErr.Error())
					}
					require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()))
				}
				_, err := exec.StatementResultColumnsContext(t.Context(), query, RawSource)
				require.Error(t, err)
				if tc.sameDiagnostic {
					require.EqualError(t, err, ordinaryErr.Error())
				}
				require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()))
			})
		}
	})
}
