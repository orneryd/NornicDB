package server

// v1.5.2 verification (scratch, test-only): a narrowed node that leaves a CALL
// subquery is written back, and a wrong row drives a delete (R1 #11's owner).

import (
	"strings"
	"testing"
)

func TestV152P26TrimmedWrites(t *testing.T) {
	const source, target = `{"body":"bs","key":"s","name":"S"}`, `{"body":"bt","key":"t","name":"T"}`
	flagged := func(props string) string { return strings.Replace(props, `,"key"`, `,"flag":1,"key"`, 1) }
	for _, c := range []struct{ name, q, want string }{
		{"scan-keyword", "CALL () { MATCH (end:W {key:'s'}) RETURN coalesce(end, null) AS x } SET x.flag = 1", "[" + flagged(source) + "] [" + target + "]"},
		{"scan-label-again", "CALL () { MATCH (n:W {key:'s'}) MATCH (n:W)-[:WREL*0..1]->(m) WITH m WHERE m.key = 's' RETURN m AS x } SET x.flag = 1", "[" + flagged(source) + "] [" + target + "]"},
		{"endpoint-keyword-start", "CALL () { MATCH (end)-[:WREL]->(m:W {key:'t'}) RETURN coalesce(end, null) AS x } SET x.flag = 1", "[" + flagged(source) + "] [" + target + "]"},
		{"endpoint-keyword-start-plain", "CALL () { MATCH (end)-[:WREL]->(m) RETURN coalesce(end, null) AS x } SET x.flag = 1", "[" + flagged(source) + "] [" + target + "]"},
		{"endpoint-keyword-end", "CALL () { MATCH (start:W {key:'s'})-[:WREL]->(end) RETURN coalesce(end, null) AS x } SET x.flag = 1", "[" + source + "] [" + flagged(target) + "]"},
		{"control-ordinary-name", "CALL () { MATCH (n:W {key:'s'}) RETURN coalesce(n, null) AS x } SET x.flag = 1", "[" + flagged(source) + "] [" + target + "]"},
		{"wrong-row-deletes", "CALL () { MATCH (n:W) WHERE NOT EXISTS { (n:W {name:'S'}) } RETURN n.key AS k } MATCH (x:W {key:k}) DETACH DELETE x", "[" + source + "]"},
		{"control-wrong-row", "CALL () { MATCH (n:W) WHERE NOT n.name = 'S' RETURN n.key AS k } MATCH (x:W {key:k}) DETACH DELETE x", "[" + source + "]"},
	} {
		for _, transport := range []string{"http-auto", "bolt-auto", "bolt-explicit"} {
			t.Run(c.name+"/"+transport, func(t *testing.T) {
				e := p26Serve(t, nil)
				e.must("CREATE (:W {key:'s', name:'S', body:'bs'})-[:WREL]->(:W {key:'t', name:'T', body:'bt'})", nil)
				out := ""
				switch transport {
				case "http-auto":
					out = e.httpAuto().run(c.q, nil)
				case "bolt-auto":
					out = e.boltAuto().run(c.q, nil)
				default:
					out = e.boltCommit(c.q)
				}
				got := e.boltAuto().run("MATCH (n:W) RETURN properties(n)", nil)
				verdict := "ok"
				if got != c.want {
					verdict = "WRONG"
					t.Errorf("P26 R11W %s %s: %s (%s)\n  expected stored: %s\n  actual stored:   %s", c.name, transport, c.q, out, c.want, got)
				}
				t.Logf("P26ROW R11W %s %s %s %q (%s) -> %s", c.name, transport, verdict, c.q, out, got)
			})
		}
	}
}
