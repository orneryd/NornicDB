package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeSimilarityComponentValueMessages(t *testing.T) {
	exec := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.nodeSimilarity.filtered.stream"} {
		t.Run(procedure, func(t *testing.T) {
			for _, tc := range []struct{ value, want string }{
				{"[1,2]", "Invalid component spec: cannot parse type ArrayList with value [1, 2]"},
				{"{z:1,a:2,k:[1,2],b:'text'}", "Invalid component spec: cannot parse type HashMap with value {a=2, b=text, z=1, k=[1, 2]}"},
				{"[{i:1,b:2,a:3,k:4},{q:1,a:2,z:3},{a:1,q:2,z:3}]", "Invalid component spec: cannot parse type ArrayList with value [{a=3, i=1, b=2, k=4}, {q=1, a=2, z=3}, {a=1, q=2, z=3}]"},
				{"''", "Invalid component spec: expected a valid node property"},
				{"' '", "Invalid component spec: expected a valid node property"},
				{"5", "Invalid component spec: cannot parse type Long with value 5"},
			} {
				t.Run(tc.value, func(t *testing.T) {
					_, err := exec.Execute(t.Context(), "CALL "+procedure+"('rev',{useComponents:"+tc.value+"})", nil)
					require.EqualError(t, err, "Failed to invoke procedure `"+procedure+"`: Caused by: "+tc.want)
				})
			}
		})
	}
}

func TestNodeSimilarityComponentParameterValueMessages(t *testing.T) {
	exec := reviewGraph(t, make([]map[string]interface{}, 3), []reviewEdge{{"0", "2", "R", nil}, {"1", "2", "R", nil}}, "'N'", "'R'")
	for _, value := range []interface{}{[]interface{}{int64(1), int64(2)}, []int64{1, 2}, [2]int{1, 2}} {
		for _, procedure := range []string{"gds.nodeSimilarity.stream", "gds.nodeSimilarity.filtered.stream"} {
			_, err := exec.Execute(t.Context(), "CALL "+procedure+"('rev',{useComponents:$value})", map[string]interface{}{"value": value})
			require.EqualError(t, err, "Failed to invoke procedure `"+procedure+"`: Caused by: Invalid component spec: cannot parse type ArrayList with value [1, 2]")
		}
	}
}

func TestGDSJavaValueText(t *testing.T) {
	for _, tc := range []struct {
		value    interface{}
		property bool
		want     string
	}{
		{nil, false, "null"},
		{"text", false, "text"},
		{"text", true, `String("text")`},
		{true, false, "true"},
		{false, true, "Boolean('false')"},
		{int32(7), false, "7"},
		{float32(1.5), false, "1.5"},
		{2.0, false, "2.0"},
		{[]int64{1, 2}, false, "[1, 2]"},
		{[]int64{1, 2}, true, "LongArray[1, 2]"},
		{[]float64{1.0, 2.5}, true, "DoubleArray[1.0, 2.5]"},
		{[]float64{}, true, "DoubleArray[]"},
		{[]string{"a", "b"}, true, "StringArray[a, b]"},
		{[]interface{}{true, nil, "text", []int64{1, 2}}, false, "[true, null, text, [1, 2]]"},
		{map[string]interface{}{}, false, "{}"},
		{map[string]interface{}{"z": int64(1), "a": int64(2), "k": []int64{1, 2}, "b": "text"}, false, "{a=2, b=text, z=1, k=[1, 2]}"},
	} {
		for i := 0; i < 10; i++ {
			require.Equal(t, tc.want, gdsJavaValueText(tc.value, tc.property))
		}
	}
}
