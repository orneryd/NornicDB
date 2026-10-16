package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type innerAggregateOracleCase struct {
	ID         string
	Expression string
	Query      string
	Rows       json.RawMessage
}

func innerAggregateOracleCases(t testing.TB) []innerAggregateOracleCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/inner_aggregate_subquery_oracle.json")
	require.NoError(t, err)
	var cases []innerAggregateOracleCase
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.Len(t, cases, 16)
	return cases
}

// A scalar brace subquery owns aggregates in its body. Both the span scanner
// and the projection plan must keep those aggregates inside that subquery.
func TestInnerAggregateSubqueryBoundaryAndPlan(t *testing.T) {
	for _, tc := range innerAggregateOracleCases(t) {
		t.Run(tc.ID, func(t *testing.T) {
			spans := findAggregateSpans(tc.Expression)
			texts := make([]string, 0, len(spans))
			for _, span := range spans {
				texts = append(texts, tc.Expression[span.start:span.end])
			}
			keyword, open, scalar := subqueryExpressionKeywordAt(tc.Expression, 0)
			end := subqueryExpressionEnd(tc.Expression, 0)
			plan := &returnProjectionPlan{}
			plan.addProjection(tc.Expression, "v")
			record, err := json.Marshal(map[string]interface{}{
				"id": tc.ID, "keyword": keyword, "open": open, "boundary": end,
				"scalar": scalar, "spans": texts, "planAggregate": plan.hasAggregate,
				"projectionAggregate": plan.projections[0].isAggr,
			})
			require.NoError(t, err)
			t.Log("INNER_AGG_PLAN " + string(record))
			outer := tc.ID == "outer_count"
			assert.Equal(t, outer, plan.hasAggregate)
			assert.Equal(t, outer, plan.projections[0].isAggr)
			if outer {
				assert.Equal(t, []string{"count(*)"}, texts)
			} else {
				assert.Empty(t, texts)
			}
			if tc.ID == "quoted_literal" || outer {
				assert.False(t, scalar)
				assert.Equal(t, -1, end)
			} else {
				assert.True(t, scalar)
				assert.Equal(t, len(tc.Expression), end)
			}
		})
	}
	for _, expr := range []string{
		"COUNT /*gap*/ { RETURN count(*) AS c } + count(*)",
		"count(*) + COLLECT //gap\n { RETURN count(*) AS c }",
	} {
		spans := findAggregateSpans(expr)
		require.Len(t, spans, 1, expr)
		assert.Equal(t, "count(*)", expr[spans[0].start:spans[0].end])
	}
}

// Expected rows were measured on Neo4j5.26.30, including the exact original
// range-UNWIND statement, rather than inferred from the projection plan.
func TestInnerAggregateSubqueryRows(t *testing.T) {
	normalization := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() {
		config.SetCypherQueryNormalizationEnabled(normalization)
	})
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, tc := range innerAggregateOracleCases(t) {
				name := fmt.Sprintf("normalize=%t/mode=native-default/explicit=%t/%s", normalize, explicit, tc.ID)
				t.Run(name, func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					exec, _ := newTestExecutor(t)
					if explicit {
						base, err := storage.NewBadgerEngineInMemory()
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, base.Close()) })
						tx, err := base.BeginTransaction()
						require.NoError(t, err)
						t.Cleanup(func() { _ = tx.Rollback() })
						exec = NewStorageExecutor(&transactionStorageWrapper{
							tx: tx, underlying: base, separator: ":",
							mutatedNodeIDs: make(map[string]struct{}),
						})
					}
					result, queryErr := exec.Execute(context.Background(), tc.Query, nil)
					rows := make([][]interface{}, 0)
					if result != nil && result.Rows != nil {
						rows = result.Rows
					}
					code, message := "", ""
					if queryErr != nil {
						code, message = nornicerrors.Neo4jStatus(queryErr)
					}
					record, err := json.Marshal(map[string]interface{}{
						"id": tc.ID, "query": tc.Query, "route": "embedded",
						"normalize": normalize, "mode": "native-default", "explicit": explicit,
						"rows": rows, "code": code, "message": message,
					})
					require.NoError(t, err)
					t.Log("INNER_AGG_RESULT " + string(record))
					if assert.NoError(t, queryErr) {
						got, err := json.Marshal(rows)
						require.NoError(t, err)
						assert.JSONEq(t, string(tc.Rows), string(got))
					}
				})
			}
		}
	}
}
