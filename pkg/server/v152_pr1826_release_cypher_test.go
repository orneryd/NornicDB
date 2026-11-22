package server

// v1.5.2 verification (scratch, test-only): defects found while reviewing
// PR #1826 that the release has too, over HTTP and Bolt. A test fails
// (t.Errorf "P26 ...") where the defect is present; harness problems are fatal.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ---------------------------------------------------------------------------
// F1: policy visibility decided on the narrowed node.

func p26DecayConfig(c *nornicdb.Config) { c.Memory.DecayEnabled = true }

// p26PolicyFixture makes two nodes of label that their profile hides after two
// seconds and a policy that keeps the one for which rule holds. Every property
// has its own long half-life, so only the node's own visibility is at stake.
func p26PolicyFixture(t *testing.T, e *p26Env, label, rule, kept, gone string) string {
	t.Helper()
	statements := []string{
		"CREATE DECAY PROFILE " + label + "_age OPTIONS { halfLifeSeconds: 1, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }",
		"CREATE DECAY PROFILE " + label + "_bind FOR (n:" + label + ") APPLY {\n  DECAY PROFILE '" + label + "_age'\n  n.key DECAY HALF LIFE 3153600000\n  n.text DECAY HALF LIFE 3153600000\n  n.keep DECAY HALF LIFE 3153600000\n  n.importance DECAY HALF LIFE 3153600000\n  n.archived DECAY HALF LIFE 3153600000\n}",
		"CREATE PROMOTION PROFILE " + label + "_keep OPTIONS { multiplier: 1.25, scoreFloor: 0.9, scoreCap: 0.95, scope: 'NODE' }",
		"CREATE PROMOTION POLICY " + label + "_policy FOR (n:" + label + ") APPLY { WHEN " + rule + " APPLY PROFILE '" + label + "_keep' }",
	}
	for _, s := range statements {
		if out := e.boltAuto().run(s, nil); strings.HasPrefix(out, "ERR") {
			return s + ": " + out
		}
	}
	e.must("CREATE (:"+label+" {key:'kept', text:'t-kept'"+kept+"}), (:"+label+" {key:'gone', text:'t-gone'"+gone+"})", nil)
	return ""
}

func TestV152P26PolicyVisibility(t *testing.T) {
	e := p26Serve(t, p26DecayConfig)
	rules := []struct{ label, rule, kept, gone string }{
		{"FactKeep", "n.keep = true", ", keep:true", ", keep:false"},
		{"FactNum", "n.importance >= 3", ", importance:5", ", importance:1"},
		{"FactCoal", "coalesce(n.archived, 0) = 0", "", ", archived:1"},
	}
	for _, r := range rules {
		if refused := p26PolicyFixture(t, e, r.label, r.rule, r.kept, r.gone); refused != "" {
			t.Fatalf("setup refused: %s", refused)
		}
	}
	time.Sleep(4 * time.Second)
	for _, indexed := range []bool{false, true} {
		for _, r := range rules {
			where := fmt.Sprintf("rule=%q indexed=%t", r.rule, indexed)
			if indexed {
				e.must("CREATE INDEX "+r.label+"_key FOR (n:"+r.label+") ON (n.key)", nil)
				e.boltAuto().run("CALL db.awaitIndexes(30)", nil)
			}
			m := "MATCH (n:" + r.label
			// Controls: reads of the whole node.
			e.check("F1-CONTROL", where, m+" {key:'kept'}) RETURN n.text, properties(n).key", `["t-kept","kept"]`)
			e.check("F1-CONTROL", where, m+") WITH n AS x, properties(n) AS p RETURN x.key, size(keys(p)) > 1", `["kept",true]`)
			e.check("F1-CONTROL", where, m+" {key:'gone'}) RETURN properties(n)", "")
			// Property-only reads.
			e.check("F1", where, m+" {key:'kept'}) RETURN n.text", `["t-kept"]`)
			e.check("F1", where, m+" {key:'gone'}) RETURN n.text", "")
			e.check("F1", where, m+") WHERE n.key = 'gone' RETURN n.key", "")
			e.check("F1", where, m+") WHERE n.key STARTS WITH 'g' RETURN n.text", "")
			e.check("F1", where, m+") RETURN n.key", `["kept"]`)
			e.check("F1", where, m+") RETURN count(n)", `[1]`)
		}
	}
}

// Rules the language has no words for are stored and never hold.
func TestV152P26PolicyRuleAccepted(t *testing.T) {
	e := p26Serve(t, p26DecayConfig)
	rules := []struct{ label, rule, kept, gone, holds string }{
		{"RuleEq", "n.keep = true", ", keep:true", ", keep:false", "kept"}, // control
		{"RuleIsNull", "n.archived IS NULL", "", ", archived:1", "kept"},
		{"RuleIsNotNull", "n.keep IS NOT NULL", ", keep:1", "", "kept"},
		{"RuleNot", "NOT n.archived", ", archived:false", ", archived:true", "kept"},
		{"RuleIn", "n.importance IN [3, 5]", ", importance:5", ", importance:1", "kept"},
		{"RuleStarts", "n.text STARTS WITH 't-k'", "", "", "kept"},
		{"RuleGarbage", "n.keep = true )( ???", ", keep:true", ", keep:false", "refused"},
		{"RuleWord", "n.keep = true unless archived", ", keep:true", ", keep:false", "refused"},
	}
	refused := map[string]string{}
	for _, r := range rules {
		refused[r.label] = p26PolicyFixture(t, e, r.label, r.rule, r.kept, r.gone)
	}
	time.Sleep(4 * time.Second)
	for _, r := range rules {
		if refused[r.label] != "" {
			t.Logf("P26ROW F1b %s rule %q REFUSED: %s", r.label, r.rule, refused[r.label])
			if r.label == "RuleEq" {
				t.Errorf("control rule refused")
			}
			continue
		}
		got := e.boltAuto().run("MATCH (n:"+r.label+") WITH n AS x, properties(n) AS p RETURN x.key, size(keys(p)) > 1", nil)
		t.Logf("P26ROW F1b %s rule %q ACCEPTED, visible: %s", r.label, r.rule, got)
		if r.holds == "refused" || got != `["kept",true]` {
			t.Errorf("P26 F1b rule %q is accepted and stored without an error, but never holds: visible %q, expected %s", r.rule, got, map[bool]string{true: "a refusal at CREATE", false: `["kept",true] or a refusal at CREATE`}[r.holds == "refused"])
		}
	}
	t.Logf("P26ROW F1b SHOW %.1500s", e.boltAuto().run("SHOW PROMOTION POLICIES", nil))
}

// ---------------------------------------------------------------------------
// F2: named single-node paths.

func (e *p26Env) pathData(label, schema string) {
	switch schema {
	case "range":
		e.must("CREATE INDEX "+label+"_i FOR (n:"+label+") ON (n.key)", nil)
	case "unique":
		e.must("CREATE CONSTRAINT "+label+"_u FOR (n:"+label+") REQUIRE n.key IS UNIQUE", nil)
	}
	e.must("CREATE (:"+label+" {key:'a', name:'Alpha', rank:1, body:'ba'}), (:"+label+" {key:'b', name:'Beta', rank:2, body:'bb'})", nil)
}

func TestV152P26NamedNodePaths(t *testing.T) {
	e := p26Serve(t, nil)
	for _, schema := range []string{"none", "range", "unique"} {
		label := "Path" + strings.ToUpper(schema[:1]) + schema[1:]
		e.pathData(label, schema)
		where := "schema=" + schema
		for _, c := range []struct{ q, want string }{
			{"MATCH p = (n:L {key:'b'}) RETURN nodes(p)[0].name", `["Beta"]`},
			{"MATCH p = (n:L {key:'b'}) RETURN properties(nodes(p)[0])", `[{"body":"bb","key":"b","name":"Beta","rank":2}]`},
			{"MATCH p = (n:L {key:'b'}) RETURN size(keys(nodes(p)[0]))", `[4]`},
			{"MATCH p = (n:L {key:'b'}) RETURN [x IN nodes(p) | x.body]", `[["bb"]]`},
			{"MATCH p = (n:L {key:'b'}) RETURN n.key, nodes(p)[0].name", `["b","Beta"]`},
			{"MATCH p = (n:L) WHERE n.rank > 1 RETURN [x IN nodes(p) | x.name]", `[["Beta"]]`},
			{"MATCH p = (n:L {key:'b'}) WHERE length(p) = 0 AND n.name = 'Beta' RETURN n.key", `["b"]`},
			{"MATCH p = (n:L) WHERE length(p) = 0 AND n.name = 'Beta' RETURN n.key", `["b"]`},
			{"MATCH p = (n:L) WHERE any(x IN nodes(p) WHERE x.name = 'Beta') RETURN n.key", `["b"]`},
			{"MATCH p = (n:L {key:'b'}) WITH p RETURN nodes(p)[0].rank", `[2]`},
			{"CALL () { MATCH p = (n:L {key:'b'}) RETURN nodes(p)[0] AS x } RETURN x.name", `["Beta"]`},
			{"MATCH p = (n:L {key:'b'}) RETURN properties(n), nodes(p)[0].name", `[{"body":"bb","key":"b","name":"Beta","rank":2},"Beta"]`}, // control: the node itself is read whole
		} {
			e.check("F2", where, strings.ReplaceAll(c.q, ":L", ":"+label), c.want)
		}
		// The narrowed node written back: the stored node loses properties.
		for i, write := range []string{
			"CALL () { MATCH p=(n:L {key:'K'}) RETURN nodes(p)[0] AS x } SET x.flag = 1",
			"CALL () { MATCH p=(n:L) WHERE n.key = 'K' RETURN p } UNWIND nodes(p) AS x SET x.flag = 1",
			"MATCH p=(n:L {key:'K'}) FOREACH (x IN nodes(p) | SET x.flag = 1)", // control
		} {
			for j, s := range []p26Session{e.httpAuto(), e.boltAuto()} {
				key := fmt.Sprintf("w%d%d", i, j)
				e.must("CREATE (:"+label+" {key:'"+key+"', name:'Gamma', rank:3, body:'bg'})", nil)
				q := strings.ReplaceAll(strings.ReplaceAll(write, ":L", ":"+label), "'K'", "'"+key+"'")
				out := s.run(q, nil)
				got := s.run("MATCH (n:"+label+") WHERE n.flag = 1 OR n.key = '"+key+"' RETURN properties(n)", nil)
				want := `[{"body":"bg","flag":1,"key":"` + key + `","name":"Gamma","rank":3}]`
				verdict := "ok"
				if got != want {
					verdict = "WRONG"
					t.Errorf("P26 F2-WRITE %s %s: %s\n  statement: %s\n  expected stored: %s\n  actual stored:   %s", where, s.name, out, q, want, got)
				}
				t.Logf("P26ROW F2-WRITE %s %s %s %q (%s) -> %s", where, s.name, verdict, q, out, got)
				e.must("MATCH (n:"+label+") WHERE n.flag = 1 OR n.key = '"+key+"' DETACH DELETE n", nil)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// F3: an older explicit transaction's index lookups.

func TestV152P26OlderTransaction(t *testing.T) {
	for _, schema := range []string{"none", "range", "unique"} {
		for _, transport := range []string{"bolt", "http"} {
			t.Run(schema+"/"+transport, func(t *testing.T) {
				e := p26Serve(t, nil)
				switch schema {
				case "range":
					e.must("CREATE INDEX d_key FOR (n:Doc) ON (n.key)", nil)
					e.must("CREATE INDEX d_rank FOR (n:Doc) ON (n.rank)", nil)
					e.must("CREATE INDEX d_cr FOR (n:Doc) ON (n.category, n.rank)", nil)
				case "unique":
					e.must("CREATE CONSTRAINT d_key FOR (n:Doc) REQUIRE n.key IS UNIQUE", nil)
				}
				e.must("UNWIND range(0,9) AS i CREATE (:Doc {key:'k'+toString(i), rank:i, category:'x', body:'b'+toString(i)})", nil)
				statements := []struct{ q, want string }{
					{"MATCH (n:Doc {key:'k1'}) RETURN n.body", `["b1"]`},                       // deleted by the other session
					{"MATCH (n:Doc) WHERE n.key IN ['k1'] RETURN n.body", `["b1"]`},            // deleted
					{"MATCH (n:Doc {rank:2}) RETURN n.body", `["b2"]`},                         // rank changed
					{"MATCH (n:Doc {category:'x', rank:2}) RETURN n.body", `["b2"]`},           // rank changed
					{"MATCH (n:Doc) WHERE n.rank >= 2 AND n.rank < 3 RETURN n.body", `["b2"]`}, // rank changed
					{"MATCH (n:Doc {key:'k3'}) RETURN n.body", `["b3"]`},                       // label removed
					{"MATCH (n:Doc) WHERE toString(n.key) = 'k1' RETURN n.body", `["b1"]`},     // control: no index can serve it
					{"MATCH (n:Doc) WHERE n.rank + 0 = 2 RETURN n.body", `["b2"]`},             // control
					{"MATCH (n:Doc) RETURN count(n)", `[10]`},                                  // control
				}
				var old p26Session
				if transport == "bolt" {
					old = e.boltTx()
				} else {
					old = e.httpTx()
				}
				defer old.end()
				for _, s := range statements {
					if got := old.run(s.q, nil); got != s.want {
						t.Fatalf("before the other session's writes: %s -> %s", s.q, got)
					}
				}
				e.must("MATCH (n:Doc {key:'k1'}) DETACH DELETE n", nil)
				e.must("MATCH (n:Doc {key:'k2'}) SET n.rank = 77", nil)
				e.must("MATCH (n:Doc {key:'k3'}) REMOVE n:Doc SET n:Gone", nil)
				for _, s := range statements {
					got := old.run(s.q, nil)
					verdict := "ok"
					if got != s.want {
						verdict = "WRONG"
						t.Errorf("P26 F3 schema=%s %s: %s\n  before the other session's commit, same transaction: %s\n  after it: %q", schema, old.name, s.q, s.want, got)
					}
					t.Logf("P26ROW F3 schema=%s %s %s %q -> %s", schema, old.name, verdict, s.q, got)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// F4: snapshot of an explicit transaction with asynchronous writes.

func TestV152P26AsyncSnapshot(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, transport := range []string{"bolt", "http"} {
			t.Run(fmt.Sprintf("async=%t/%s", async, transport), func(t *testing.T) {
				e := p26Serve(t, func(c *nornicdb.Config) {
					c.Database.AsyncWritesEnabled = async
					if async {
						c.Database.StrictDurability = false
					}
				})
				e.must("UNWIND range(0,9) AS i CREATE (:Doc {key:'k'+toString(i), rank:i})", nil)
				var old p26Session
				if transport == "bolt" {
					old = e.boltTx()
				} else {
					old = e.httpTx()
				}
				defer old.end()
				statements := []struct{ q, want string }{
					{"MATCH (n:Doc) RETURN count(n)", `[10]`},
					{"MATCH (n:Doc) WHERE n.key STARTS WITH 'k' RETURN count(n.rank)", `[10]`},
					{"MATCH (n:Doc) WHERE n.key = 'new' RETURN n.rank", ""},
					{"MATCH (n:Doc) WHERE n.key = 'k1' RETURN n.rank", `[1]`},
					{"MATCH (n:Doc) WHERE n.key = 'k2' RETURN n.rank", `[2]`},
				}
				for _, s := range statements {
					if got := old.run(s.q, nil); got != s.want {
						t.Fatalf("before: %s -> %s", s.q, got)
					}
				}
				e.must("CREATE (:Doc {key:'new', rank:100})", nil)
				e.must("MATCH (n:Doc) WHERE n.key = 'k1' SET n.rank = 55", nil)
				e.must("MATCH (n:Doc) WHERE n.key = 'k2' DETACH DELETE n", nil)
				for round, wait := range []time.Duration{0, time.Second} {
					time.Sleep(wait)
					for _, s := range statements {
						got := old.run(s.q, nil)
						verdict := "ok"
						if got != s.want {
							verdict = "WRONG"
							t.Errorf("P26 F4 async=%t %s round %d: %s\n  first read in the transaction: %q\n  after the later writes: %q", async, old.name, round, s.q, s.want, got)
						}
						t.Logf("P26ROW F4 async=%t %s round=%d %s %q -> %s", async, old.name, round, verdict, s.q, got)
					}
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// R1 #11: relationship endpoints, and the label scans the same scan narrows.

func TestV152P26TrimmedNodes(t *testing.T) {
	e := p26Serve(t, nil)
	e.must("CREATE (a:Doc {key:'one', path:'guide', status:'current', extra:7, body:'body one'})-[:REL {w:1}]->(b:Doc {key:'two', path:'second', status:'old', extra:8, body:'body two'}), (b)-[:BACK]->(a)", nil)
	e.must("CREATE (:File {id:1, restricted:true, owner:'ann', body:'restricted text'}), (:File {id:2, restricted:false, owner:'bob', body:'public text'})", nil)
	const one = `{"body":"body one","extra":7,"key":"one","path":"guide","status":"current"}`
	const two = `{"body":"body two","extra":8,"key":"two","path":"second","status":"old"}`
	for _, indexed := range []bool{false, true} {
		if indexed {
			e.must("CREATE CONSTRAINT p26_key FOR (n:Doc) REQUIRE n.key IS UNIQUE", nil)
			e.must("CREATE CONSTRAINT p26_id FOR (n:File) REQUIRE n.id IS UNIQUE", nil)
		}
		where := fmt.Sprintf("indexed=%t", indexed)
		for _, c := range []struct{ kind, q, want string }{
			{"ENDPOINT-KEYWORD", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN properties(end) AS p", "[" + two + "]"},
			{"ENDPOINT-KEYWORD", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN properties(end) AS p, end.extra AS x", "[" + two + ",8]"},
			{"ENDPOINT-KEYWORD", "MATCH (start:Doc)-[:REL]->(end) RETURN properties(end) AS p, properties(start) AS q", "[" + two + "," + one + "]"},
			{"ENDPOINT-KEYWORD", "MATCH (start:Doc)-[:REL]->(end) RETURN size(keys(end)) AS n", "[5]"},
			{"ENDPOINT-KEYWORD", "MATCH (start:Doc)-[:REL]->(end) RETURN coalesce(end, null).body AS b", `["body two"]`},
			{"ENDPOINT-CONTROL", "MATCH (a:Doc {key:'one'})-[:REL]->(m) RETURN properties(m) AS p", "[" + two + "]"},
			{"ENDPOINT-LABEL", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", `["two"]`},
			{"ENDPOINT-LABEL", "MATCH (a:Doc {key:'one'})-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key", ""},
			{"ENDPOINT-LABEL", "MATCH (a)-[:REL]->(m) MATCH p=(m:Doc)-[:BACK]->(x) RETURN [y IN nodes(p) | y.extra] AS l", `[[8,7]]`},
			{"ENDPOINT-CONTROL", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m {extra:8}) RETURN m.key", `["two"]`},
			{"ENDPOINT-CONTROL", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m:Doc) WHERE m.extra = 8 RETURN m.key", `["two"]`},
			{"SCAN-KEYWORD", "MATCH (end:Doc) WHERE end.status = 'old' RETURN properties(end) AS p", "[" + two + "]"},
			{"SCAN-KEYWORD", "MATCH (end:Doc) WHERE end.status = 'old' RETURN [end.key, size(keys(end))] AS p", `[["two",5]]`},
			{"SCAN-LABEL", "MATCH (f:File) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", `["public text"]`},
			{"SCAN-LABEL", "MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", ""},
			{"SCAN-LABEL", "MATCH (f:File {id:1}) WHERE EXISTS { (f:File {owner:'ann'}) } RETURN f.body", `["restricted text"]`},
			{"SCAN-LABEL", "MATCH (n:Doc) WHERE n.status = 'current' MATCH (n:Doc {extra:7}) RETURN n.key", `["one"]`},
			{"SCAN-CONTROL", "MATCH (f:File) WHERE NOT EXISTS { MATCH (f) WHERE f.restricted = true } RETURN f.body", `["public text"]`},
		} {
			e.check("R11-"+c.kind, where, c.q, c.want)
		}
		// Written back through a subquery: the endpoint's stored properties.
		for j, s := range []p26Session{e.httpAuto(), e.boltAuto()} {
			k := fmt.Sprintf("%t%d", indexed, j)
			e.must("CREATE (:W {key:'s"+k+"', name:'S', body:'bs'})-[:WREL]->(:W {key:'t"+k+"', name:'T', body:'bt'})", nil)
			q := "CALL () { MATCH (end:W {key:'s" + k + "'})-[:WREL]->(m) RETURN coalesce(end, null) AS x } SET x.flag = 1"
			out := s.run(q, nil)
			got := s.run("MATCH (n:W)-[:WREL]->(m:W {key:'t"+k+"'}) RETURN properties(n)", nil)
			want := `[{"body":"bs","flag":1,"key":"s` + k + `","name":"S"}]`
			verdict := "ok"
			if got != want {
				verdict = "WRONG"
				t.Errorf("P26 R11-WRITE %s %s: %s (%s)\n  expected stored: %s\n  actual stored:   %s", where, s.name, q, out, want, got)
			}
			t.Logf("P26ROW R11-WRITE %s %s %s %q (%s) -> %s", where, s.name, verdict, q, out, got)
		}
	}
}

// ---------------------------------------------------------------------------
// R1 #9: has_embedding of a node whose chunk vectors carry no metadata, in
// every read form; and the vector-similarity reads in and out of a transaction.

func TestV152P26HasEmbedding(t *testing.T) {
	e := p26Serve(t, nil)
	store, err := e.server.dbManager.GetStorage(e.database)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []*storage.Node{
		{ID: "he-chunks", Labels: []string{"He"}, Properties: map[string]any{"key": "chunks", "emb": []float64{1, 0, 0}}, ChunkEmbeddings: [][]float32{{1, 2, 3}}},
		{ID: "he-meta", Labels: []string{"He"}, Properties: map[string]any{"key": "meta", "owner_id": "u1", "emb": []float64{0, 1, 0}}, ChunkEmbeddings: [][]float32{{1, 2, 3}}, EmbedMeta: map[string]any{"has_embedding": true, "chunk_count": 1}},
		{ID: "he-none", Labels: []string{"He"}, Properties: map[string]any{"key": "none", "emb": []float64{0, 0, 1}}},
	} {
		if _, err := store.CreateNode(t.Context(), n); err != nil {
			t.Fatal(err)
		}
	}
	for _, indexed := range []bool{false, true} {
		if indexed {
			e.must("CREATE CONSTRAINT he_key FOR (n:He) REQUIRE n.key IS UNIQUE", nil)
			e.must("CREATE VECTOR INDEX he_vec FOR (n:He) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
			time.Sleep(3 * time.Second)
		}
		where := fmt.Sprintf("indexed=%t", indexed)
		for _, round := range []string{"first", "again"} {
			w := where + " " + round
			e.check("R9", w, "MATCH (n:He) RETURN n.key, n.has_embedding", `["chunks",true] ["meta",true] ["none",false]`)
			e.check("R9", w, "MATCH (n:He {key:'chunks'}) RETURN n.has_embedding", `[true]`)
			e.check("R9", w, "MATCH (n:He) WHERE n.has_embedding = true RETURN n.key", `["chunks"] ["meta"]`)
			e.check("R9", w, "MATCH (n:He) WITH n AS m RETURN m.key, m.has_embedding", `["chunks",true] ["meta",true] ["none",false]`)
			if indexed {
				e.check("R9-VECTOR", w, "MATCH (n:He) RETURN n.key AS k, n.has_embedding AS h, vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 1", `["chunks",true,1]`)
				e.check("R6-VECTOR", w, "MATCH (n:He) RETURN {node:n}.node.key AS k, vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 1", `["chunks",1]`)
			}
		}
	}
	// The public writer of chunk vectors without metadata: anonymising a data subject.
	response := makeRequest(t, e.server, http.MethodPost, "/gdpr/delete", map[string]any{"user_id": "u1", "anonymize": true, "confirm": true}, e.token)
	t.Logf("P26ROW R9 anonymise status=%d %.200s", response.Code, response.Body.String())
	if stored, err := store.GetNode(t.Context(), "he-meta"); err != nil {
		t.Logf("P26ROW R9 anonymised node: %v", err)
	} else {
		t.Logf("P26ROW R9 anonymised node: props=%v chunks=%d meta=%v", stored.Properties["owner_id"] != "u1", len(stored.ChunkEmbeddings), stored.EmbedMeta)
	}
	e.check("R9", "after anonymisation", "MATCH (n:He) RETURN n.key, n.has_embedding", `["chunks",true] ["meta",true] ["none",false]`)
	e.check("R9", "after anonymisation", "MATCH (n:He {key:'meta'}) RETURN n.has_embedding", `[true]`)
	e.check("R9-VECTOR", "after anonymisation", "MATCH (n:He) RETURN n.key AS k, n.has_embedding AS h, vector.similarity.cosine(n.emb, [0.0,1.0,0.0]) AS score ORDER BY score DESC LIMIT 1", `["meta",true,1]`)
}
