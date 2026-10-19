package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApocMapCleanTemporalEquivalence(t *testing.T) {
	const early = "datetime('2025-11-02T01:45:12.000000019-02:30[America/St_Johns]')"
	const late = "datetime('2025-11-02T01:45:12.000000019-03:30[America/St_Johns]')"
	for _, tc := range []struct {
		name, a, b string
		kept       int64
	}{
		{"scalar-overlap", early, late, 1},
		{"list-overlap", "[" + early + "]", "[" + late + "]", 1},
		{"map-overlap", "{v:[" + early + "]}", "{v:[" + late + "]}", 1},
		{"deep-overlap", "[{v:[" + early + ",null]}]", "[{v:[" + late + ",null]}]", 1},
		{"same-occurrence", early, early, 0},
		{"same-nested-occurrence", "[{v:[" + early + ",null]}]", "[{v:[" + early + ",null]}]", 0},
		{"integer-float", "1", "1.0", 1},
		{"float-integer", "1.0", "1", 1},
		{"nested-numeric-kinds", "{v:[1]}", "{v:[1.0]}", 1},
		{"same-numeric-kind", "{v:[1.0]}", "{v:[1.0]}", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			ctx := context.Background()
			raw, err := exec.Execute(ctx, "RETURN "+tc.a+" AS a,"+tc.b+" AS b", nil)
			require.NoError(t, err)
			for _, programs := range []bool{true, false} {
				t.Run(fmt.Sprintf("programs=%v", programs), func(t *testing.T) {
					rowValueProgramsDisabled.Store(!programs)
					defer rowValueProgramsDisabled.Store(false)
					for _, form := range []string{"literal", "with", "parameter", "pointer"} {
						t.Run(form, func(t *testing.T) {
							a, b, prefix, params := tc.a, tc.b, "", map[string]interface{}(nil)
							if form == "parameter" || form == "pointer" {
								a, b = "$a", "$b"
								params = map[string]interface{}{"a": raw.Rows[0][0], "b": raw.Rows[0][1]}
							}
							if form == "pointer" {
								params["a"] = temporalPointerCarrier(params["a"])
								params["b"] = temporalPointerCarrier(params["b"])
							}
							if form == "with" {
								prefix = "WITH " + a + " AS a," + b + " AS b "
								a, b = "a", "b"
							}
							query := prefix + "RETURN size(keys(apoc.map.clean({keep:" + a + "},[],[" + b + "]))) AS kept"
							result, err := exec.Execute(ctx, query, params)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{tc.kept}}, result.Rows)
						})
					}
				})
			}
		})
	}
}

func TestTemporalProjectionSourceOffset(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	raw, err := exec.Execute(ctx, "RETURN datetime('2025-07-17T12:13:14.000000017[America/St_Johns]') AS source", nil)
	require.NoError(t, err)
	source := raw.Rows[0][0]
	for _, carrier := range []interface{}{source, temporalPointerCarrier(source)} {
		result, err := exec.Execute(ctx, "RETURN time($source)=time('12:13:14.000000017-02:30') AS equal", map[string]interface{}{"source": carrier})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true}}, result.Rows)
	}
}
