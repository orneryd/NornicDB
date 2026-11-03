package apocexport

import (
	"testing"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/stretchr/testify/require"
)

func TestNumericWireGraphJSON(t *testing.T) {
	text, err := Json([]*Node{{ID: 1, Properties: map[string]interface{}{"v": float64(2), "nested": []interface{}{float64(3)}}}}, nil)
	require.NoError(t, err)
	var graph struct {
		Nodes []Node `json:"nodes"`
	}
	require.NoError(t, valueconvert.PropertyJSON.Unmarshal([]byte(text), &graph))
	require.Equal(t, float64(2), graph.Nodes[0].Properties["v"])
	require.Equal(t, []interface{}{float64(3)}, graph.Nodes[0].Properties["nested"])
}
