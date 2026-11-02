package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NaN is a supported float property and is unequal to itself under the
// existing constraint comparator. Native batching must not give that value a
// different equality meaning from the canonical individual write path.
func TestNativeBulkUniqueNaNMatchesIndividualWrites(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := "individual"
		if bulk {
			name = "bulk"
		}
		t.Run(name, func(t *testing.T) {
			engine := newTestEngine(t)
			require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("nan_unique", "Value", "v"))
			nodes := []*Node{
				{ID: "test:nan_a", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": math.NaN()}},
				{ID: "test:nan_b", Labels: []string{"Value"}, Properties: map[string]interface{}{"v": math.NaN()}},
			}
			if bulk {
				assert.NoError(t, engine.BulkCreateNodes(t.Context(), nodes))
			} else {
				for _, node := range nodes {
					_, err := engine.CreateNode(t.Context(), node)
					assert.NoError(t, err)
				}
			}
			count, err := engine.NodeCount(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, int64(2), count, "compare actual persisted population, not attempted writes")
			for _, node := range nodes {
				stored, err := engine.GetStoredNode(t.Context(), node.ID)
				if assert.NoError(t, err) && assert.NotNil(t, stored) {
					value, ok := stored.Properties["v"].(float64)
					assert.True(t, ok)
					assert.True(t, math.IsNaN(value))
				}
			}
		})
	}
}
