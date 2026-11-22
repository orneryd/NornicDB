package cypher

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// These probes intentionally log actual observations instead of asserting the
// review's conclusion, so the same small suite can compare head, base and tip.
func TestPR1826VerifyCypherObservations(t *testing.T) {
	ctx := t.Context()
	queries := []struct{ name, setup, query string }{
		{"r1_f1_subquery_write", "CREATE CONSTRAINT verify_doc IF NOT EXISTS FOR (n:VDoc) REQUIRE n.key IS UNIQUE; CREATE (a:VDoc {key:'one', path:'guide', status:'current', extra:7, list:[1,2,3], body:'body one'}), (b:VDoc {key:'two', path:'second', status:'old', extra:8, list:[4], body:'body two'})",
			"CALL () { MATCH (start:VDoc {key:'one'}), (end:VDoc {key:'two'}) UNWIND [start, end] AS x RETURN x } SET x.flag = 1"},
		{"r1_f2_labeled_exists", "CREATE CONSTRAINT verify_file IF NOT EXISTS FOR (n:VFile) REQUIRE n.id IS UNIQUE; CREATE (:VFile {id:1, restricted:true, owner:'ann', body:'restricted text'})",
			"MATCH (f:VFile {id:1}) WHERE NOT EXISTS { (f:VFile {restricted:true}) } RETURN f.body"},
		{"r1_f3_single_node_path", "CREATE CONSTRAINT verify_path IF NOT EXISTS FOR (n:VPath) REQUIRE n.id IS UNIQUE; CREATE (:VPath {id:1, name:'alice', secret:'s3cret'})",
			"MATCH p=(n:VPath {id:1}) RETURN nodes(p)[0].name AS name, size(keys(nodes(p)[0])) AS props"},
		{"r1_f4_keyword_variable", "CREATE CONSTRAINT verify_end IF NOT EXISTS FOR (n:VEnd) REQUIRE n.key IS UNIQUE; CREATE (:VEnd {key:'one', extra:7, body:'kept'})",
			"MATCH (end:VEnd {key:'one'}) MATCH (end:VEnd {extra:7}) RETURN properties(end) AS props"},
		{"r1_f11_relationship_endpoint", "CREATE CONSTRAINT verify_endpoint IF NOT EXISTS FOR (n:VEndPoint) REQUIRE n.key IS UNIQUE; CREATE (:VEndPoint {key:'one'})-[:VREL]->(:VEndPoint {key:'two', extra:8, body:'kept'})",
			"MATCH (start:VEndPoint {key:'one'})-[:VREL]->(end) RETURN properties(end) AS props, end.extra AS extra"},
	}
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			exec := setupTestExecutor(t)
			for _, stmt := range strings.Split(tc.setup, "; ") {
				if _, err := exec.Execute(ctx, stmt, nil); err != nil { t.Logf("SETUP_ERROR %v statement=%q", err, stmt); return }
			}
			if tc.name == "r1_f1_subquery_write" {
				res, e := exec.Execute(ctx, tc.query, nil)
				t.Logf("OBS query=%q result=%#v err=%v", tc.query, res, e)
				whole, e := exec.Execute(ctx, "MATCH (n:VDoc {key:'two'}) RETURN properties(n) AS props", nil)
				t.Logf("OBS stored_two=%#v err=%v", whole, e)
				return
			}
			res, e := exec.Execute(ctx, tc.query, nil)
			t.Logf("OBS query=%q result=%#v err=%v", tc.query, res, e)
		})
	}
}

func TestPR1826VerifyReplacementDemandCache(t *testing.T) {
	exec := setupTestExecutor(t)
	if _, err := exec.Execute(t.Context(), "CREATE (:VReg {key:'one', secret:'value'})", nil); err != nil { t.Fatalf("seed: %v", err) }
	query := "MATCH (n:VReg {key:'one'}) RETURN size(tail([n,n])) AS v"
	first, err := exec.Execute(t.Context(), query, nil)
	if err != nil { t.Fatalf("built-in tail: %v", err) }
	// The replacement inspects properties through the public function registry
	// and returns the first argument node's property count. This test runs alone.
	fn.Register("tail", func(workCtx context.Context, ctx fn.Context, args []string) (interface{}, error) {
		value, err := ctx.Eval(args[0])
		if err != nil { return nil, err }
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Slice && v.Len() > 0 { v = reflect.ValueOf(v.Index(0).Interface()) }
		if v.Kind() == reflect.Ptr { v = v.Elem() }
		field := v.FieldByName("Properties")
		if field.IsValid() && field.Kind() == reflect.Map { return int64(field.Len()), nil }
		return int64(0), nil
	}, fn.OpaqueReads, fn.ArgumentInputImage)
	stale, staleErr := exec.Execute(t.Context(), query, nil)
	fresh, freshErr := exec.Execute(t.Context(), "MATCH (m:VReg {key:'one'}) RETURN size(tail([m,m])) AS v", nil)
	t.Logf("OBS r2_f2 built_in=%#v repeated=%#v err=%v fresh_variable=%#v err=%v", first.Rows, stale.Rows, staleErr, fresh.Rows, freshErr)
}

