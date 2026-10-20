package cypher

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// Broaden the admitted-list and local-frame population, retaining a real
// reference and an actual-error assertion before inspecting the status.
func TestReviewer13e4ListFrameOracle(t *testing.T) {
	ref := fabricCallRowsReference(t)
	require.NotNil(t, ref)
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	cases := []struct {
		name, body string
		value      interface{}
	}{
		{"null_then_boolean", "RETURN [z IN [null,x] | abs(z)] AS v", true},
		{"boolean_then_null", "RETURN [z IN [x,null] | abs(z)] AS v", true},
		{"nested_list", "RETURN [zs IN [[x]] | [z IN zs | abs(z)]] AS v", true},
		{"reduce_parameter", "RETURN reduce(a=0,z IN [1] | a + abs($p)) AS v", true},
		{"local_shadow", "RETURN [x IN [1] | abs(x)] AS v", true},
		{"null_local", "RETURN [z IN [null,x] | abs(z)] AS v", nil},
		{"integer_local", "RETURN [z IN [null,x] | abs(z)] AS v", int64(2)},
		{"import_list", "CALL (x) { RETURN [z IN [x] | abs(z)] AS v } RETURN v", true},
	}
	for _, c := range cases {
		for _, empty := range []bool{false, true} {
			q := "WITH $p AS x "
			if empty {
				q += "UNWIND $empty AS stop "
			}
			q += c.body
			p := map[string]interface{}{"p": c.value, "empty": []interface{}{}}
			rr, re := neo4j.ExecuteQuery(context.Background(), ref, q, p, neo4j.EagerResultTransformer)
			for _, norm := range []bool{false, true} {
				config.SetCypherQueryNormalizationEnabled(norm)
				for _, route := range []string{"native", "use"} {
					t.Run(fmt.Sprintf("%s/empty_%v/norm_%v/%s", c.name, empty, norm, route), func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						text := q
						if route == "use" {
							e = fabricCallRowsCompositeExecutor(t, e)
							text = "USE comp.one " + q
						}
						r, err := e.Execute(context.Background(), text, p)
						t.Logf("query=%s reference=%v actual=%v result=%v", q, re, err, r)
						if re != nil {
							require.Error(t, err)
							ne, ok := re.(*neo4j.Neo4jError)
							require.True(t, ok)
							code, _ := nornicerrors.Neo4jStatus(err)
							require.Equal(t, ne.Code, code)
						} else {
							require.NoError(t, err)
							require.Len(t, r.Rows, len(rr.Records))
						}
					})
				}
			}
		}
	}
}

func TestReviewer13e4DirectAliasOracle(t *testing.T) {
	ref := fabricCallRowsReference(t)
	require.NotNil(t, ref)
	for _, c := range []struct {
		q        string
		p        map[string]interface{}
		identity string
	}{
		{"RETURN toUpper($s) AS v", map[string]interface{}{"s": int64(1)}, "s"},
		{"RETURN tail($l) AS v", map[string]interface{}{"l": int64(1)}, "l"},
		{"WITH $s AS x RETURN toUpper(x) AS v", map[string]interface{}{"s": int64(1)}, ""},
		{"WITH $l AS x RETURN tail(x) AS v", map[string]interface{}{"l": int64(1)}, ""},
	} {
		t.Run(c.q, func(t *testing.T) {
			_, re := neo4j.ExecuteQuery(context.Background(), ref, c.q, c.p, neo4j.EagerResultTransformer)
			require.Error(t, re)
			ne, ok := re.(*neo4j.Neo4jError)
			require.True(t, ok)
			e := newFabricCallRowsExecutor(t)
			_, err := e.Execute(context.Background(), c.q, c.p)
			require.Error(t, err)
			code, msg := nornicerrors.Neo4jStatus(err)
			require.Equal(t, ne.Code, code)
			require.Contains(t, msg, strings.SplitN(ne.Msg, " (line ", 2)[0])
			require.Equal(t, c.identity != "", strings.Contains(msg, "for parameter"))
			t.Logf("ORACLE query=%s reference=%s actual=%s", c.q, ne.Msg, msg)
		})
	}
}
