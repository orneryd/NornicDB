package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalBindingOwner(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	driver := fabricCallRowsReference(t)
	ctx := context.Background()
	for _, literal := range []string{"true", "false", "null"} {
		var value interface{}
		if literal != "null" {
			value = literal == "true"
		}
		for _, tc := range []struct {
			body               string
			want               [][]interface{}
			batched, extension bool
		}{
			{"WITH " + literal + " AS unused CALL { WITH " + literal + " AS value RETURN value } RETURN value", [][]interface{}{{value}}, false, false},
			{"WITH {" + literal + ": 1} AS unused CALL { WITH " + literal + " AS value RETURN value } RETURN value", [][]interface{}{{value}}, false, false},
			{"WITH 17 AS value CALL { WITH " + literal + " AS value RETURN value AS innerValue } RETURN value, innerValue", [][]interface{}{{int64(17), value}}, false, false},
			{"WITH " + literal + " AS unused CALL { WITH " + literal + " AS value CALL (value) { RETURN value AS innerValue } RETURN innerValue } RETURN innerValue", [][]interface{}{{value}}, false, false},
			{"WITH " + literal + " AS unused OPTIONAL CALL { WITH " + literal + " AS value WHERE false RETURN value } RETURN value", [][]interface{}{{nil}}, false, false},
			{"WITH " + literal + " AS unused CALL { WITH " + literal + " AS value RETURN value } IN TRANSACTIONS OF 1 ROWS RETURN value", [][]interface{}{{value}}, true, false},
			// Scoped-name rebinding is a documented NornicDB extension.
			{"WITH 17 AS value CALL (value) { WITH " + literal + " AS value RETURN value AS innerValue } RETURN value, innerValue", [][]interface{}{{int64(17), value}}, false, true},
			{"WITH 17 AS value CALL (value) { WITH " + literal + " AS value RETURN value AS innerValue UNION WITH " + literal + " AS value RETURN value AS innerValue } RETURN value, innerValue", [][]interface{}{{int64(17), value}}, false, true},
		} {
			if driver != nil && !tc.extension {
				session := driver.NewSession(ctx, neo4j.SessionConfig{})
				reference, err := session.Run(ctx, tc.body, nil)
				require.NoError(t, err, tc.body)
				records, err := reference.Collect(ctx)
				require.NoError(t, err, tc.body)
				require.NoError(t, session.Close(ctx))
				var rows [][]interface{}
				for _, record := range records {
					rows = append(rows, record.Values)
				}
				require.Equal(t, tc.want, rows, tc.body)
			}
			for _, normalize := range []bool{false, true} {
				config.SetCypherQueryNormalizationEnabled(normalize)
				for _, composite := range []bool{false, true} {
					for _, explicit := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/normalize_%v/composite_%v/explicit_%v/%s", literal, normalize, composite, explicit, tc.body), func(t *testing.T) {
							exec := newFabricCallRowsExecutor(t)
							query := tc.body
							if composite {
								exec = fabricCallRowsCompositeExecutor(t, exec)
								query = "USE comp " + strings.ReplaceAll(query, "{ WITH", "{ USE comp.one WITH")
							}
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer exec.Execute(ctx, "ROLLBACK", nil)
								if composite {
									_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
									require.True(t, ok)
								}
							}
							for repeat := 0; repeat < 2; repeat++ {
								result, err := exec.Execute(ctx, query, nil)
								if tc.batched && composite {
									// Constituent batching is an existing unsupported route;
									// semantic admission must reach its routing rejection.
									require.ErrorContains(t, err, "CALL IN TRANSACTIONS across composite constituents is not supported")
									code, _ := nornicerrors.Neo4jStatus(err)
									require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
									break
								}
								if tc.batched && explicit {
									require.Error(t, err)
									code, _ := nornicerrors.Neo4jStatus(err)
									require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", code)
									break
								}
								require.NoError(t, err, query)
								require.Equal(t, tc.want, result.Rows, query)
							}
						})
					}
				}
			}
		}
	}
}

// Labels and native vector syntax name schema/type operands, not input variables.
// Whole-statement admission must preserve that distinction before USE lowering.
func TestFabricLogicalExpressionRoles(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, route := range []string{"direct", "use", "nested-use"} {
				for _, sample := range []struct{ query, rows string }{
					{"RETURN head([null]):A AS v", "[[null]]"},
					{"RETURN (null):A AS v", "[[null]]"},
					// Native vector calls are a supported extension on the 5.26 baseline.
					{"RETURN vector_norm(vector([3,4],2,FLOAT),EUCLIDEAN) AS v", "[[5]]"},
				} {
					t.Run(fmt.Sprintf("normalize_%v/explicit_%v/%s/%s", normalize, explicit, route, sample.query), func(t *testing.T) {
						config.SetCypherQueryNormalizationEnabled(normalize)
						exec := newFabricCallRowsExecutor(t)
						query := sample.query
						if route != "direct" {
							exec = fabricCallRowsCompositeExecutor(t, exec)
							query = "USE comp.one " + query
							if route == "nested-use" {
								query = "CALL { " + query + " } RETURN v"
							}
						}
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
							if route != "direct" {
								_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
								require.True(t, ok)
							}
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := exec.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							fabricDispatchRows(t, result.Rows, sample.rows)
						}
					})
				}
			}
		}
	}
}

func TestNativeExpressionBindingFacts(t *testing.T) {
	for _, sample := range []struct {
		expression string
		names      []string
	}{
		{"head([null]):A", nil},
		{"(null):A", nil},
		{"vector_norm(vector([3,4],2,FLOAT),EUCLIDEAN)", nil},
		{"vector_norm(vector([x],dimension,FLOAT),EUCLIDEAN)", []string{"x", "dimension"}},
		{"normalize(value,NFC)", []string{"value"}},
		{"n:A AND n.other IS NOT NULL", []string{"n", "n"}},
		{"{A: n.value, FLOAT: $parameter}", []string{"n"}},
		{"[x IN values WHERE x > minimum | x + offset]", []string{"values", "minimum", "offset"}},
		{"[x IN values]", []string{"values"}},
		{"[x IN values, true]", []string{"x", "values"}},
		{"filter /* native scope */ (x IN values WHERE x > minimum)", []string{"values", "minimum"}},
		{"reduce(total = initial, x IN values | total + x + offset)", []string{"initial", "values", "offset"}},
		{"[(a)-->(b) | a.value + outside]", []string{"outside"}},
		{"coalesce(n:A, missing)", []string{"n", "missing"}},
	} {
		t.Run(sample.expression, func(t *testing.T) {
			require.Equal(t, sample.names, expressionFreeVariables(sample.expression))
		})
	}
}

func TestNativeExpressionReferenceInputShapes(t *testing.T) {
	// The property projection consumer reads complete clauses, not just operands.
	// Losing that input shape silently strips properties before evaluating rows.
	require.Equal(t, []string{"id"}, pipelineNodeReadProperties("n", physicalClauseReadReferences(pipelineClause{kind: pipelineClauseReturn, text: "RETURN n.id AS v"}), nil))
	require.Nil(t, pipelineNodeReadProperties("n", physicalClauseReadReferences(pipelineClause{kind: pipelineClauseWith, text: "WITH n, n.num + n.num2 AS sum"}), nil))
	for index, expression := range []string{"n.id", "m.name"} {
		statement := "RETURN " + expression
		var operand, clause []fabric.ExpressionReference
		if index == 0 {
			operand = fabric.ExpressionReferences(expression)
			clause = fabric.ExpressionReferences(statement)
		} else {
			clause = fabric.ExpressionReferences(statement)
			operand = fabric.ExpressionReferences(expression)
		}
		require.Len(t, operand, 1)
		require.Len(t, clause, 1)
		require.Equal(t, operand[0].Name, clause[0].Name)
		require.Equal(t, operand[0].Property, clause[0].Property)
		require.Zero(t, operand[0].Start)
		require.Equal(t, len("RETURN "), clause[0].Start)
	}
}
