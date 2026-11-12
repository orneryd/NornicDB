package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestProfileStaticProceduresReportKnownZeroGraphAccess(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t, `CREATE (:StaticProfile {x: 1})`)
		for _, query := range []string{
			`CALL db.ping() YIELD success RETURN success`,
			`CALL dbms.clientConfig() YIELD name, value RETURN name, value ORDER BY name`,
			`CALL db.index.fulltext.listAvailableAnalyzers() YIELD analyzer RETURN analyzer ORDER BY analyzer`,
		} {
			t.Run(query, func(t *testing.T) {
				ordinary, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.NotEmpty(t, ordinary.Rows)
				profiled, err := exec.Execute(ctx, "PROFILE "+query, nil)
				require.NoError(t, err)
				require.Equal(t, ordinary.Columns, profiled.Columns)
				require.Equal(t, ordinary.Rows, profiled.Rows)
				plan := executionPlanFromResult(t, profiled)
				require.Empty(t, plan.Unobserved)
				require.True(t, plan.TotalDBHitsObserved)
				require.Zero(t, plan.TotalDBHits, "these implementations return constants or registered descriptions without invoking plugins or reading graph state")
			})
		}
	})
}
