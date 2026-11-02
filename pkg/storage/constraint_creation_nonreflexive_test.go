package storage

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstraintCreationBoundsNonreflexiveCandidates(t *testing.T) {
	for name, value := range map[string]interface{}{"scalar": math.NaN(), "nested list": []interface{}{[]interface{}{math.NaN()}}} {
		t.Run(name, func(t *testing.T) {
			require.False(t, compareValues(value, value))
			for _, other := range []interface{}{nil, true, "NaN", int64(1), math.Inf(1), math.NaN(), []interface{}{math.NaN()}, []interface{}{[]interface{}{math.NaN()}}, []float64{math.NaN()}, map[string]interface{}{"v": math.NaN()}} {
				require.False(t, compareValues(value, other))
				require.False(t, compareValues(other, value))
			}
			seen := make(constraintCreationValues)
			for i := 0; i < 128; i++ {
				_, duplicate, err := seen.add(t.Context(), fmt.Sprint(i), []interface{}{"same", value})
				require.NoError(t, err)
				require.False(t, duplicate)
			}
			retained := 0
			for _, candidates := range seen {
				retained += len(candidates)
			}
			assert.LessOrEqual(t, retained, 1, "nonreflexive collisions must not grow the candidate population")
		})
	}
	// Self-equality is not an excuse to bypass true duplicate validation.
	for name, value := range map[string]interface{}{"null": nil, "typed list": []int64{1, 2}, "nested list": []interface{}{[]interface{}{int64(1)}}, "map": map[string]interface{}{"x": int64(1)}} {
		t.Run(name, func(t *testing.T) {
			require.True(t, compareValues(value, value))
			seen := make(constraintCreationValues)
			_, duplicate, err := seen.add(t.Context(), "first", []interface{}{value})
			require.NoError(t, err)
			require.False(t, duplicate)
			id, duplicate, err := seen.add(t.Context(), "second", []interface{}{value})
			require.NoError(t, err)
			require.True(t, duplicate)
			require.Equal(t, "first", id)
		})
	}
}

func TestConstraintCreationNonreflexivePublicValidation(t *testing.T) {
	for _, entity := range []ConstraintEntityType{ConstraintEntityNode, ConstraintEntityRelationship} {
		for _, nested := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nested=%v", entity, nested), func(t *testing.T) {
				e := newTestEngine(t)
				if entity == ConstraintEntityRelationship {
					for _, id := range []NodeID{"test:a", "test:b"} {
						_, err := e.CreateNode(t.Context(), &Node{ID: id})
						require.NoError(t, err)
					}
				}
				for i := 0; i < 16; i++ {
					var value interface{} = math.NaN()
					if nested {
						value = []interface{}{[]interface{}{value}}
					}
					props := map[string]interface{}{"v": value}
					if entity == ConstraintEntityNode {
						_, err := e.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Value"}, Properties: props})
						require.NoError(t, err)
					} else {
						require.NoError(t, e.CreateEdge(t.Context(), &Edge{ID: EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "Value", Properties: props}))
					}
				}
				require.NoError(t, ValidateConstraintOnCreationForEngine(context.Background(), e, Constraint{Name: "nan_values", Type: ConstraintUnique, EntityType: entity, Label: "Value", Properties: []string{"v"}}))
			})
		}
	}
}

func BenchmarkConstraintCreationCandidateRetention(b *testing.B) {
	for _, shape := range []string{"ordinary", "NaN", "nested NaN", "list partitions"} {
		for _, count := range []int{128, 1024} {
			b.Run(fmt.Sprintf("%s/%d", shape, count), func(b *testing.B) {
				values := make([][]interface{}, count)
				for i := range values {
					var value interface{} = int64(i)
					switch shape {
					case "NaN":
						value = math.NaN()
					case "nested NaN":
						value = []interface{}{[]interface{}{math.NaN()}}
					case "list partitions":
						// Distinct flat lists that raw fmt prints identically; the existing
						// canonical bucket writer preserves their actual element boundaries.
						list := []interface{}{}
						part := "x"
						for bit := 0; bit < 10; bit++ {
							if i&(1<<bit) != 0 {
								list = append(list, part)
								part = "x"
							} else {
								part += " x"
							}
						}
						list = append(list, part)
						value = list
					}
					values[i] = []interface{}{value}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					seen := make(constraintCreationValues)
					for j, value := range values {
						if _, duplicate, err := seen.add(b.Context(), fmt.Sprint(j), value); err != nil || duplicate {
							b.Fatalf("unexpected duplicate=%v error=%v", duplicate, err)
						}
					}
				}
			})
		}
	}
}
