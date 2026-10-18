package cypher

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPreparationCallFramingCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_framing_budget"))
	for _, query := range []string{
		"CALL {RETURN '" + strings.Repeat("x", 129) + "' AS value}",
		"CALL { WITH 1 AS x FINISH UNION WITH 2 AS x FINISH } RETURN 1",
		"CALL { RETURN {value:'}'} AS m } IN TRANSACTIONS OF +" + strings.Repeat("0", 129) + "1 ROWS RETURN 1",
		"CALL { WITH 1 AS x /*" + strings.Repeat("x", 129) + "*/ FINISH }",
		"CALL { WITH 1 AS x //" + strings.Repeat("x", 129) + "\n FINISH }",
	} {
		body, tail, transaction, batch := executor.parseCallSubquery(query)
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			actualBody, actualTail, actualTx, actualBatch := executor.parseCallSubqueryWithCancellation(query, probe)
			if probe.err == nil {
				require.Equal(t, body, actualBody)
				require.Equal(t, tail, actualTail)
				require.Equal(t, transaction, actualTx)
				require.Equal(t, batch, actualBatch)
			}
		})
	}
	for _, query := range []string{
		"WITH 1 AS x FINISH UNION WITH 2 AS x FINISH",
		"RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x",
		"WITH '" + strings.Repeat("x", 129) + "' AS x FINISH",
		"WITH 1 AS x /*" + strings.Repeat("x", 129) + "*/ FINISH",
		"WITH 1 AS x STARTS /*" + strings.Repeat("x", 129) + "*/ WITH 'x' FINISH",
		"FOR " + strings.Repeat(" ", 129) + "x IN [1] FINISH",
		"WITH 1 AS x //" + strings.Repeat("x", 129) + "\n FINISH",
	} {
		want, wantStripped := stripUnionBranchFinishes(query)
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			actual, stripped := stripUnionBranchFinishesWithCancellation(query, probe)
			if probe.err == nil {
				require.Equal(t, want, actual)
				require.Equal(t, wantStripped, stripped)
			}
		})
		branches, all, mixed, ok := parseTopLevelUnionBranches(query)
		runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
			actual, a, m, o := parseTopLevelUnionBranchesWithCancellation(query, probe)
			if probe.err == nil {
				require.Equal(t, branches, actual)
				require.Equal(t, all, a)
				require.Equal(t, mixed, m)
				require.Equal(t, ok, o)
			}
		})
	}
}

func TestPreparationCallBatchIntegerParity(t *testing.T) {
	for _, text := range []string{"", "0", "1", "+1", "-1", "-0", " 1", "1 ", "a", strings.Repeat("0", 10000) + "1", strings.Repeat("9", 10000), "9223372036854775807", "9223372036854775808"} {
		value, err := strconv.Atoi(text)
		ctx, cancel := context.WithCancel(context.Background())
		probe := &pipelineCancellation{ctx: ctx}
		actual, ok := callSubqueryBatchSizeWithCancellation(text, probe)
		require.Equal(t, err == nil && value > 0, ok)
		if ok {
			require.Equal(t, value, actual)
		}
		cancel()
	}
	runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
		callSubqueryBatchSizeWithCancellation(strings.Repeat("0", 129)+"1", probe)
	})
}
