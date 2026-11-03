package storage

import (
	"fmt"
	"math"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNativeBulkConstraintEqualityContract(t *testing.T) {
	cases := []struct {
		name             string
		first, second    interface{}
		duplicate, tuple bool
		composite        bool
	}{
		{name: "ordinary distinct", first: int64(1), second: int64(2)},
		{name: "ordinary duplicate", first: int64(1), second: int64(1), duplicate: true},
		{name: "list text collision", first: []interface{}{"a b"}, second: []interface{}{"a", "b"}},
		{name: "nested NaN", first: []interface{}{[]interface{}{math.NaN()}}, second: []interface{}{[]interface{}{math.NaN()}}},
		{name: "numeric list representations", first: []int64{1}, second: []interface{}{float64(1)}, duplicate: true},
		{name: "tuple duplicate", first: int64(1), second: int64(1), duplicate: true, tuple: true},
		{name: "tuple distinct", first: int64(1), second: int64(2), tuple: true},
		{name: "tuple NaN", first: math.NaN(), second: math.NaN(), tuple: true},
		{name: "composite UNIQUE duplicate", first: int64(1), second: int64(1), duplicate: true, composite: true},
		{name: "composite UNIQUE distinct", first: int64(1), second: int64(2), composite: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, bulk := range []bool{false, true} {
				t.Run(fmt.Sprintf("bulk=%v", bulk), func(t *testing.T) {
					e := newTestEngine(t)
					c := Constraint{Name: "batch_value", Type: ConstraintUnique, Label: "Value", Properties: []string{"v"}}
					if tc.composite {
						c.Properties = []string{"tenant", "v"}
					}
					if tc.tuple {
						c.Type = ConstraintNodeKey
						c.Properties = []string{"tenant", "v"}
					}
					require.NoError(t, e.GetSchemaForNamespace("test").AddConstraint(c))
					nodes := []*Node{{ID: "test:a", Labels: []string{"Value"}, Properties: map[string]interface{}{"tenant": "same", "v": tc.first}}, {ID: "test:b", Labels: []string{"Value"}, Properties: map[string]interface{}{"tenant": "same", "v": tc.second}}}
					var err error
					want := int64(2)
					if bulk {
						err = e.BulkCreateNodes(t.Context(), nodes)
						if tc.duplicate {
							want = 0
						}
					} else {
						_, firstErr := e.CreateNode(t.Context(), nodes[0])
						assert.NoError(t, firstErr)
						_, err = e.CreateNode(t.Context(), nodes[1])
						if tc.duplicate {
							want = 1
						}
					}
					if tc.duplicate {
						var violation *ConstraintViolationError
						assert.ErrorAs(t, err, &violation)
					} else {
						assert.NoError(t, err)
					}
					count, countErr := e.NodeCount(t.Context())
					assert.NoError(t, countErr)
					assert.Equal(t, want, count)
					if bulk && tc.duplicate {
						for _, node := range nodes {
							_, readErr := e.GetStoredNode(t.Context(), node.ID)
							assert.ErrorIs(t, readErr, ErrNotFound, "duplicate bulk must publish none")
						}
					}
				})
			}
		})
	}
}

func BenchmarkNativeBulkConstraintPreflight(b *testing.B) {
	for _, count := range []int{1, 128, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = e.Close() })
			if err = e.GetSchemaForNamespace("test").AddUniqueConstraint("batch_value", "Value", "v"); err != nil {
				b.Fatal(err)
			}
			nodes := make([]*Node, count)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(i)}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err = e.validateBulkNodeConstraints(b.Context(), nodes); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestNativeBulkConstraintScopesAndErrorOwnership(t *testing.T) {
	e := newTestEngine(t)
	for _, namespace := range []string{"alpha", "beta"} {
		require.NoError(t, e.GetSchemaForNamespace(namespace).AddUniqueConstraint("same:name", "Value", "v"))
	}
	// BulkCreateNodes itself is single-namespace; exercise the preflight's
	// complete grouping contract directly without changing that public rule.
	require.NoError(t, e.validateBulkNodeConstraints(t.Context(), []*Node{{ID: "alpha:a", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(1)}}, {ID: "beta:b", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(1)}}}))
	require.NoError(t, e.GetSchemaForNamespace("alpha").AddUniqueConstraint("other:name", "Other", "v"))
	require.NoError(t, e.BulkCreateNodes(t.Context(), []*Node{{ID: "alpha:a", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(1)}}, {ID: "alpha:b", Labels: []string{"Other"}, Properties: map[string]interface{}{"v": int64(1)}}}))
	nodes := []*Node{{ID: "alpha:c", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(2)}}, {ID: "alpha:d", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": int64(2)}}}
	err := e.validateBulkNodeConstraints(t.Context(), nodes)
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	violation.Properties[0] = "changed"
	constraints, err := e.GetSchemaForNamespace("alpha").GetConstraintsForLabels(t.Context(), []string{"Value"})
	require.NoError(t, err)
	require.Len(t, constraints, 1)
	require.Equal(t, []string{"v"}, constraints[0].Properties, "error property slice must remain independently owned")
}
