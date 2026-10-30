package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGDSImportNumericListDefault(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewdefault", gdsNode("a", "N", map[string]interface{}{"v": []int64{1, 2}}), gdsNode("b", "N", nil))
	result, err := run("CALL gds.graph.project('defaults',{N:{properties:{v:{defaultValue:[0,0]}}}},'*')")
	t.Logf("result=%+v error=%v", result, err)
	require.NoError(t, err)
	projection, err := lookupGraphProjection("reviewdefault", "defaults")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, projection.NodeProperties["a"]["v"])
	require.Equal(t, []int64{0, 0}, projection.NodeProperties["b"]["v"])
}

func TestGDSNativeImportLongArrayValidation(t *testing.T) {
	gdsFollowUpCatalog(t)
	_, run := newGDSEngineGraph(t, "reviewnativelossy", gdsNode("a", "N", map[string]interface{}{"v": []float64{1.5}}), gdsNode("b", "N", map[string]interface{}{"v": []int64{9007199254740993}}))
	result, err := run("CALL gds.graph.project('nativelossy',{N:{properties:'v'}},'*')")
	t.Logf("result=%+v error=%v", result, err)
	if err == nil {
		p, _ := lookupGraphProjection("reviewnativelossy", "nativelossy")
		t.Logf("stored=%+v", p.NodeProperties)
	}
	require.Error(t, err)
}

func TestGDSImportLongToDoubleBounds(t *testing.T) {
	for _, value := range []int64{-9223372036854775808, -9007199254740993, 9007199254740992, 9007199254740993, 9007199254740994, 9223372036854775807} {
		for _, target := range []string{"Float", "Relationship", "List of Float", ""} {
			for _, widen := range []bool{false, true} {
				var input interface{} = value
				if target == "List of Float" {
					input = []int64{value}
				}
				if target == "" {
					input = []interface{}{1.5, value}
				}
				_, _, _, err := gdsConvertPropertyValue(input, target, widen)
				// GDS converts a mixed numeric list, and a native
				// relationship's stored Long, before reading: both round.
				unbounded := target == "" || (target == "Relationship" && widen)
				if value > 1<<53 && !unbounded {
					require.ErrorContains(t, err, "into an double value")
				} else {
					require.NoError(t, err)
				}
			}
		}
	}
}
