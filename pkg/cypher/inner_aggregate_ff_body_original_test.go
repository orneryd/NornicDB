package cypher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInnerAggregateFFBodyOriginal preserves the three exact COUNT inputs
// measured on Neo4j 5.26.30. \f in the two controls is one U+000C byte.
func TestInnerAggregateFFBodyOriginal(t *testing.T) {
	cases := []struct {
		id, expression, query, querySHA256 string
	}{
		{
			id:          "ascii_body_control",
			expression:  "COUNT /*gap*/ { RETURN count(*) AS c }",
			query:       "UNWIND range(1,2) AS n RETURN COUNT /*gap*/ { RETURN count(*) AS c } AS v",
			querySHA256: "956af83f243a758ce54b89fa3a6f6e3d5031d92c0225483ec09a80de083e6ac5",
		},
		{
			id:          "ff_body_plain_gap",
			expression:  "COUNT {\fRETURN count(*) AS c }",
			query:       "UNWIND range(1,2) AS n RETURN COUNT {\fRETURN count(*) AS c } AS v",
			querySHA256: "5d9d93d51a73cfec33b30a5126d985c50de4a1dca4652bc427cd7453c00608c7",
		},
		{
			id:          "ff_body_commented_gap",
			expression:  "COUNT /*gap*/ {\fRETURN count(*) AS c }",
			query:       "UNWIND range(1,2) AS n RETURN COUNT /*gap*/ {\fRETURN count(*) AS c } AS v",
			querySHA256: "b4b592892518d01507e04e9ee0b8bde9da928a8237be7db5f9a9dbbfe4b835a5",
		},
	}

	priorNormalization := config.IsCypherQueryNormalizationEnabled()
	config.SetCypherQueryNormalizationEnabled(false)
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(priorNormalization) })

	for _, tc := range cases {
		t.Run("boundary_span_plan/"+tc.id, func(t *testing.T) {
			require.Equal(t, tc.querySHA256, fmt.Sprintf("%x", sha256.Sum256([]byte(tc.query))))
			keyword, open, scalar := subqueryExpressionKeywordAt(tc.expression, 0)
			boundary := subqueryExpressionEnd(tc.expression, 0)
			spans := findAggregateSpans(tc.expression)
			spanTexts := make([]string, 0, len(spans))
			for _, span := range spans {
				spanTexts = append(spanTexts, tc.expression[span.start:span.end])
			}
			plan := &returnProjectionPlan{}
			plan.addProjection(tc.expression, "v")
			record, err := json.Marshal(map[string]interface{}{
				"id": tc.id, "expression": tc.expression, "query": tc.query,
				"keyword": keyword, "open": open, "scalar": scalar,
				"boundary": boundary, "spans": spanTexts,
				"planAggregate":       plan.hasAggregate,
				"projectionAggregate": plan.projections[0].isAggr,
			})
			require.NoError(t, err)
			t.Log("INNER_AGG_FF_BOUNDARY " + string(record))

			assert.Equal(t, "COUNT", keyword)
			assert.Equal(t, strings.IndexByte(tc.expression, '{'), open)
			assert.Equal(t, len(tc.expression), boundary)
			assert.True(t, scalar)
			assert.Empty(t, spanTexts)
			assert.False(t, plan.hasAggregate)
			assert.False(t, plan.projections[0].isAggr)
		})

		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("embedded/explicit=%t/%s", explicit, tc.id), func(t *testing.T) {
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

				result, queryErr := exec.Execute(context.Background(), tc.query, nil)
				rows := make([][]interface{}, 0)
				if result != nil && result.Rows != nil {
					rows = result.Rows
				}
				code, message := "", ""
				if queryErr != nil {
					code, message = nornicerrors.Neo4jStatus(queryErr)
				}
				record, err := json.Marshal(map[string]interface{}{
					"id": tc.id, "query": tc.query, "querySHA256": tc.querySHA256,
					"route": "embedded", "normalize": false, "explicit": explicit,
					"rows": rows, "code": code, "message": message,
				})
				require.NoError(t, err)
				t.Log("INNER_AGG_FF_RESULT " + string(record))
				if assert.NoError(t, queryErr) {
					actualRows, err := json.Marshal(rows)
					require.NoError(t, err)
					assert.JSONEq(t, `[[1],[1]]`, string(actualRows))
				}
			})
		}
	}
}
