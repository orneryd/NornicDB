package testfixture

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ZeroRowCorrelatedCallSeedCypher is the graph every zero-row correlated CALL
// aggregation case runs against. It stays transport-independent: each public
// boundary executes it with its own context, connection or executor.
const ZeroRowCorrelatedCallSeedCypher = "CREATE (:RowAggregateNode:RowAggregateItem {id:'n0',origin:'automatic',group_id:'group0',source_key:'s0',name:'item',cite_start:[],cite_end:[],identifier_values:[],identifier_schemes:[],identifier_issuers:[],identifier_jurisdictions:[],identifier_periods:[],identifier_categories:[],alias_names:[],alias_types:[]}), (:RowAggregateSource {id:'s0',text:''})"

// ZeroRowCorrelatedCallCase is one transport-independent population of the
// correlated CALL query fixtures: the statement parameters plus the number of
// subjects, subject versions and carried ids the ungrouped
// aggregation must return.
type ZeroRowCorrelatedCallCase struct {
	Params map[string]any
	Want   int
}

// ZeroRowCorrelatedCallCaseFor builds the shared parameters for an empty or
// populated $active/$items stream and the cardinality the aggregation must
// return for it.
func ZeroRowCorrelatedCallCaseFor(populated bool) ZeroRowCorrelatedCallCase {
	testCase := ZeroRowCorrelatedCallCase{
		Params: map[string]any{
			"active":     []any{},
			"items":      []any{},
			"current":    true,
			"group_id":   "group0",
			"source_key": "s0",
		},
	}
	if !populated {
		return testCase
	}
	testCase.Params["active"] = []any{map[string]any{"targets": []any{"n0"}}}
	testCase.Params["items"] = []any{map[string]any{"continues": "n0", "own": "fallback"}}
	testCase.Want = 1
	return testCase
}

// AssertZeroRowCorrelatedCallRows asserts the exact rows an ungrouped
// aggregation over the correlated CALL fixtures must produce: one row holding
// subjects, given versions and carry, with subject, party/role and ids/carried
// cardinality matching the case. A populated stream must resolve subject id n0
// and carry it exactly once.
func AssertZeroRowCorrelatedCallRows(t *testing.T, rows [][]any, testCase ZeroRowCorrelatedCallCase) {
	t.Helper()
	require.Len(t, rows, 1, "an ungrouped aggregation produces one row on empty input")
	require.Len(t, rows[0], 3)
	subjects := rows[0][0].([]any)
	require.Len(t, subjects, testCase.Want)
	given := rows[0][1].(map[string]any)
	require.Len(t, given["subject"], testCase.Want)
	require.Empty(t, given["party"])
	require.Empty(t, given["role"])
	carry := rows[0][2].(map[string]any)
	require.Len(t, carry["ids"], testCase.Want)
	require.Len(t, carry["carried"], testCase.Want)
	if testCase.Want == 0 {
		return
	}
	require.Equal(t, "n0", subjects[0].(map[string]any)["id"])
	require.Equal(t, []any{"n0"}, carry["ids"])
	require.Equal(t, []any{"n0"}, carry["carried"])
}
