package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalBindingTemporalOffsetMutationRoutes(t *testing.T) {
	t.Run("opaque map binding", func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "opaque"))
		point := CypherPoint{SRID: 7203, X: 1.25, Y: 2.5}
		r, err := exec.Execute(context.Background(), "MERGE (n:Opaque) SET n += $m RETURN n.v", map[string]any{"m": map[string]any{"v": point}})
		require.NoError(t, err)
		require.Equal(t, [][]any{{point}}, r.Rows)
	})
	london, ok := loadPinnedTemporalLocation("Europe/London")
	require.True(t, ok)
	// Neo4j 5.26.30's toString of the instant, on every route below.
	texts := map[int]string{
		0:         "2026-10-25T01:30:00+01:00[Europe/London]",
		123456789: "2026-10-25T01:30:00.123456789+01:00[Europe/London]",
	}
	for _, nanos := range []int{0, 123456789} {
		instant := CypherDateTime{Time: time.Unix(1792888200, int64(nanos)).In(london), ZoneID: "Europe/London"}
		text := texts[nanos]
		for _, explicit := range []bool{false, true} {
			for _, relationship := range []bool{false, true} {
				for _, source := range []string{"$m", "{t: $t, ts: [$t]}", "{t: datetime('" + text + "'), ts: [datetime('" + text + "')]}"} {
					for _, route := range []string{"create", "property", "replace", "create merge map", "merge", "with merge", "on create", "on match"} {
						t.Run(fmt.Sprintf("nanos=%d/tx=%t/rel=%t/%s/%s", nanos, explicit, relationship, source, route), func(t *testing.T) {
							exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_binding"))
							ctx := context.Background()
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							pattern := "(n:TemporalBinding {id: 'v'})"
							read := "MATCH (n:TemporalBinding {id: 'v'})"
							prefix := ""
							if relationship {
								_, err := exec.Execute(ctx, "CREATE (:TemporalBindingA), (:TemporalBindingB)", nil)
								require.NoError(t, err)
								prefix = "MATCH (a:TemporalBindingA), (b:TemporalBindingB) "
								pattern = "(a)-[n:TemporalBinding {id: 'v'}]->(b)"
								read = "MATCH (:TemporalBindingA)-[n:TemporalBinding {id: 'v'}]->(:TemporalBindingB)"
							}
							params := map[string]interface{}{"t": instant, "m": map[string]interface{}{"id": "v", "t": instant, "ts": []interface{}{instant}}}
							query := ""
							switch route {
							case "create": // CREATE writes the same typed value directly.
								if relationship {
									query = "CREATE (a)-[n:TemporalBinding {id:'v', t:$t, ts:[$t]}]->(b)"
								} else {
									query = "CREATE (n:TemporalBinding {id:'v', t:$t, ts:[$t]})"
								}
							case "property":
								query = "CREATE " + pattern + " SET n.t = $t, n.ts = [$t]"
							case "replace":
								query = "CREATE " + pattern + " SET n = " + source + ", n.id = 'v'"
							case "create merge map":
								query = "CREATE " + pattern + " SET n += " + source
							case "merge":
								query = "MERGE " + pattern + " SET n += " + source
							case "with merge":
								query = "WITH 1 AS x WHERE $m.t IS NOT NULL MERGE " + pattern + " SET n += " + source
							case "on create":
								query = "MERGE " + pattern + " ON CREATE SET n += " + source
							case "on match":
								_, err := exec.Execute(ctx, prefix+"CREATE "+pattern, nil)
								require.NoError(t, err)
								query = "MERGE " + pattern + " ON MATCH SET n += " + source
							}
							if relationship && route == "with merge" {
								query = "WITH 1 AS x WHERE $m.t IS NOT NULL " + prefix + "MERGE " + pattern + " SET n += " + source
								prefix = ""
							}
							projection := " RETURN n.t.epochSeconds, toString(n.t), n.ts[0].epochSeconds, toString(n.ts[0])"
							want := []interface{}{int64(1792888200), text, int64(1792888200), text}
							result, err := exec.Execute(ctx, prefix+query+projection, params)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{want}, result.Rows)
							if explicit {
								_, err := exec.Execute(ctx, "COMMIT", nil)
								require.NoError(t, err)
							}
							result, err = exec.Execute(ctx, read+projection, nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{want}, result.Rows)
						})
					}
				}
			}
		}
	}
}

func TestTemporalBindingParameterSubstitutionKeepsTypedValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_binding_params"))
	london, ok := loadPinnedTemporalLocation("Europe/London")
	require.True(t, ok)
	instant := CypherDateTime{Time: time.Unix(1792888200, 123456789).In(london), ZoneID: "Europe/London"}
	values := []interface{}{[]byte{0, 127, 128, 255}, []bool{true, false}, instant, &instant, []interface{}{instant}, map[string]interface{}{"t": instant}, map[interface{}]interface{}{"ts": []interface{}{instant}}, []string{"x"}, []int{1}, []int64{1}, []float32{1}, []float64{1}, instant.Time, &instant.Time}
	for _, value := range values {
		require.Equal(t, "RETURN $v", exec.substituteParams("RETURN $v", map[string]interface{}{"v": value}), "%T", value)
	}
}

func TestTemporalBindingNamedZonePreferredOffset(t *testing.T) {
	for _, text := range []string{"2026-10-25T01:30:00.123456789+01:00[Europe/London]", "2026-10-25T01:30:00Z[Europe/London]", "2026-11-01T01:30:00-04:00[America/New_York]", "2026-11-01T01:30:00-05:00[America/New_York]"} {
		parsed, ok := parseTemporalText("datetime", text)
		require.True(t, ok)
		require.Equal(t, text, formatTemporalISO(parsed))
	}

}

func TestTemporalBindingTypedBindingPreservesNullTemporals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_binding_null"))
	for _, value := range []interface{}{(*CypherDateTime)(nil), (*CypherDate)(nil), (*CypherLocalDateTime)(nil), (*CypherTime)(nil), (*CypherLocalTime)(nil), (*CypherDuration)(nil), (*CypherPoint)(nil)} {
		result, err := exec.Execute(context.Background(), "RETURN $v IS NULL AS v", map[string]interface{}{"v": value})
		require.NoError(t, err, "%T", value)
		require.Equal(t, [][]interface{}{{true}}, result.Rows, "%T", value)
		result, err = exec.Execute(context.Background(), "MERGE (n:TemporalBindingNull {id:'v'}) SET n.t=1 SET n += $m RETURN n.t IS NULL AS v", map[string]interface{}{"m": map[string]interface{}{"t": value}})
		require.NoError(t, err, "%T", value)
		require.Equal(t, [][]interface{}{{true}}, result.Rows, "%T", value)
	}
}
