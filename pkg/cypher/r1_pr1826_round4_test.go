package cypher

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826, round 4: what a narrowed node does once a clause
// outside the proof receives it. A subquery (or the read half of a batching
// procedure) ends in RETURN, so its own tail is read-only and its reads are
// narrowed; the clauses after the subquery write.

// r1Round4Name is the database name of r1Round4Store's store.
func r1Round4Name(kind string) string {
	if kind == "namespaced" {
		return "r1"
	}
	return "nornic"
}

func r1Round4Store(t *testing.T, kind string, indexed bool) storage.Engine {
	t.Helper()
	if kind == "namespaced" {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
		r1SeedRound2(t, store, indexed)
		return store
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	r1SeedRound2(t, store, indexed)
	return store
}

// A statement that reads a node and then sets one property of it must leave the
// node's other stored properties as they were.
func TestR1PR1826Round4StoredPropertiesSurviveSet(t *testing.T) {
	one := map[string]any{"key": "one", "path": "guide", "status": "current", "extra": int64(7), "list": []any{int64(1), int64(2), int64(3)}, "body": "body one"}
	two := map[string]any{"key": "two", "path": "second", "status": "old", "extra": int64(8), "list": []any{int64(4)}, "body": "body two"}
	stored := func(t *testing.T, store storage.Engine, kind, key string) string {
		rows := r1Rows(NewStorageExecutor(r1FullReads{store, r1Round4Name(kind)}, QueryCachePolicy{MaxEntries: 0}).Execute(t.Context(), "MATCH (n:Doc) WHERE n.key = $key RETURN properties(n) AS p", map[string]any{"key": key}))
		return strings.Join(rows, " ; ")
	}
	for _, kind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			for _, test := range []struct {
				name, query string
				flagged     []string
			}{
				{"control/plain", "MATCH (n:Doc {key:'one'}) SET n.flag = 1", []string{"one"}},
				{"control/path-in-one-statement", "MATCH p=(n:Doc {key:'one'}) UNWIND nodes(p) AS x SET x.flag = 1", []string{"one"}},
				{"control/path-foreach", "MATCH p=(n:Doc {key:'one'}) FOREACH (x IN nodes(p) | SET x.flag = 1)", []string{"one"}},
				{"control/subquery-returns-node", "CALL () { MATCH (n:Doc {key:'one'}) RETURN n AS x } SET x.flag = 1", []string{"one"}},
				{"path/subquery-node", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", []string{"one"}},
				{"path/subquery-node-old-syntax", "CALL { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } SET x.flag = 1", []string{"one"}},
				{"path/subquery-path", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN p } UNWIND nodes(p) AS x SET x.flag = 1", []string{"one"}},
				{"path/subquery-path-foreach", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN p } FOREACH (x IN nodes(p) | SET x.flag = 1)", []string{"one"}},
				{"path/subquery-map-add", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } SET x += {flag: 1}", []string{"one"}},
				{"path/subquery-label", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } SET x:Marked", nil},
				{"path/subquery-remove", "CALL () { MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x } REMOVE x.absent", nil},
				{"path/imported", "MATCH p=(n:Doc {key:'one'}) CALL (p) { RETURN 1 AS one } UNWIND nodes(p) AS x SET x.flag = one", []string{"one"}},
				{"path/with-then-procedure", "MATCH p=(n:Doc {key:'one'}) WITH p CALL db.labels() YIELD label WITH p, count(label) AS c UNWIND nodes(p) AS x SET x.flag = 1", []string{"one"}},
				{"path/periodic-iterate", "CALL apoc.periodic.iterate(\"MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x\", \"SET x.flag = 1\", {batchSize: 10})", []string{"one"}},
				{"path/cypher-run", "CALL apoc.cypher.run(\"MATCH p=(n:Doc {key:'one'}) RETURN nodes(p)[0] AS x\", {}) YIELD value WITH value.x AS x SET x.flag = 1", []string{"one"}},
				{"varlength/subquery-node", "CALL () { MATCH (n:Doc {key:'one'}) MATCH (n:Doc)-[:REL*0..0]->(m) RETURN m AS x } SET x.flag = 1", []string{"one"}},
				{"varlength/subquery-both", "CALL () { MATCH (n:Doc {key:'one'}) MATCH (n:Doc)-[:REL*0..1]->(m) RETURN m AS x } SET x.flag = 1", []string{"one", "two"}},
				{"keyword/subquery-coalesce", "CALL () { MATCH (end:Doc {key:'one'}) RETURN coalesce(end, null) AS x } SET x.flag = 1", []string{"one"}},
				{"keyword/subquery-case", "CALL () { MATCH (end:Doc {key:'one'}) RETURN CASE WHEN true THEN end END AS x } SET x.flag = 1", []string{"one"}},
				{"keyword/subquery-unwind", "CALL () { MATCH (start:Doc {key:'one'}), (end:Doc {key:'two'}) UNWIND [start, end] AS x RETURN x } SET x.flag = 1", []string{"one", "two"}},
				{"keyword/imported-list", "MATCH (start:Doc {key:'one'}), (end:Doc {key:'two'}) WITH [start, end] AS pair CALL (pair) { RETURN 1 AS one } UNWIND pair AS x SET x.flag = one", []string{"one", "two"}},
				{"keyword/periodic-iterate", "CALL apoc.periodic.iterate(\"MATCH (end:Doc {key:'one'}) RETURN coalesce(end, null) AS x\", \"SET x.flag = 1\", {batchSize: 10})", []string{"one"}},
			} {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", kind, indexed, test.name), func(t *testing.T) {
					store := r1Round4Store(t, kind, indexed)
					exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
					result, err := exec.Execute(t.Context(), test.query, nil)
					if err != nil {
						t.Logf("R1SET %s: statement answered an error: %v", test.name, err)
					} else {
						t.Logf("R1SET %s: rows %q", test.name, r1Rows(result, nil))
					}
					for key, properties := range map[string]map[string]any{"one": one, "two": two} {
						want := map[string]any{}
						for name, value := range properties {
							want[name] = value
						}
						got := stored(t, store, kind, key)
						plain := r1Map(want)
						for _, flagged := range test.flagged {
							if flagged == key {
								want["flag"] = int64(1)
							}
						}
						if got != r1Map(want) && (err == nil || got != plain) {
							t.Errorf("stored %s after %s\n  want %s\n  got  %s", key, test.query, r1Map(want), got)
						}
					}
				})
			}
		}
	}
}

// A row a subquery returns by mistake drives the write after it.
func TestR1PR1826Round4WrongRowDrivesWrite(t *testing.T) {
	for _, kind := range []string{"namespaced", "managed"} {
		for _, test := range []struct{ name, query string }{
			{"control/one-statement-delete", "MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true})-[:IN]->() } DETACH DELETE f"},
			{"control/with-then-delete", "MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true})-[:IN]->() } WITH f.id AS id MATCH (x:File {id:id}) DETACH DELETE x"},
			{"subquery-delete", "CALL () { MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true})-[:IN]->() } RETURN f.id AS id } MATCH (x:File {id:id}) DETACH DELETE x"},
			{"subquery-delete-node-only-pattern", "CALL () { MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true}) } RETURN f.id AS id } MATCH (x:File {id:id}) DETACH DELETE x"},
			{"subquery-overwrite", "CALL () { MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true})-[:IN]->() } RETURN f.id AS id } MATCH (x:File {id:id}) SET x.body = 'overwritten'"},
			{"periodic-iterate-delete", "CALL apoc.periodic.iterate(\"MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {keep:true})-[:IN]->() } RETURN f.id AS id\", \"MATCH (x:File {id:$id}) DETACH DELETE x\", {batchSize: 10})"},
		} {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				store := r1Round4Store(t, kind, true)
				exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
				for _, statement := range []string{
					"CREATE CONSTRAINT r1_file_id IF NOT EXISTS FOR (f:File) REQUIRE f.id IS UNIQUE",
					"CREATE (:File {id:1, keep:true, body:'keep me'})-[:IN]->(:Folder {name:'a'})",
				} {
					_, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(t.Context(), test.query, nil)
				if err != nil {
					t.Logf("R1DELETE %s: statement answered an error: %v", test.name, err)
				} else {
					t.Logf("R1DELETE %s: rows %q", test.name, r1Rows(result, nil))
				}
				got := r1Rows(exec.Execute(t.Context(), "MATCH (f:File) RETURN f.id, f.keep, f.body", nil))
				require.Equal(t, []string{"1 | true | 'keep me'"}, got, test.query)
			})
		}
	}
}

// Reads the proof serves for relationship endpoints and for patterns that read
// each other.
func r1Round4Cases() []r1Case {
	return []r1Case{
		{"endpoint/match-inline", "MATCH (a:Doc {key:$k})-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"endpoint/match-inline-labelled", "MATCH (a:Doc {key:$k})-[:REL]->(m:Doc) MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"endpoint/start-match-inline", "MATCH (n:Doc {key:$k})-[:REL]->(m) MATCH (n:Doc {extra:7}) RETURN m.key"},
		{"endpoint/not-exists", "MATCH (a:Doc {key:$k})-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key"},
		{"endpoint/scan-match-inline", "MATCH (a)-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key"},
		{"endpoint/scan-not-exists", "MATCH (a)-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key"},
		{"endpoint/scan-exists", "MATCH (a)-[:REL]->(m) WHERE EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key"},
		{"endpoint/scan-keyword-end", "MATCH (start)-[:REL]->(end) RETURN properties(end) AS p, properties(start) AS q"},
		{"endpoint/scan-keyword-end-map", "MATCH (start)-[:REL]->(end) RETURN {node: end} AS m"},
		{"endpoint/keyword-end", "MATCH (start:Doc {key:$k})-[:REL]->(end) RETURN properties(end) AS p, properties(start) AS q"},
		{"endpoint/scan-path-later", "MATCH (a)-[:REL]->(m) MATCH p=(m:Doc)-[:BACK]->(x) RETURN [y IN nodes(p) | y.extra] AS l"},
		{"endpoint/scan-varlength-later", "MATCH (a)-[:REL]->(m) MATCH (m:Doc)-[:BACK*0..0]->(x) RETURN properties(x) AS p"},
		{"endpoint/scan-comprehension", "MATCH (a)-[:REL]->(m) RETURN [(m:Doc {extra:8})-[:BACK]->(x) | x.key] AS l"},
		{"sibling/dependent-inline", "MATCH (n:Doc {key:$k}), (o:Other {ref:n.key}) RETURN o.key"},
		{"sibling/dependent-inline-other-property", "MATCH (n:Doc {key:$k}), (m:Doc {extra: n.extra + 1}) RETURN m.key"},
		{"sibling/dependent-inline-properties", "MATCH (n:Doc {key:$k}), (m:Doc {extra: properties(n).extra + 1}) RETURN m.key"},
		{"sibling/dependent-inline-keyword", "MATCH (end:Doc {key:$k}), (m:Doc {extra: properties(end).extra + 1}) RETURN m.key"},
		{"sibling/dependent-where", "MATCH (n:Doc {key:$k}), (m:Doc) WHERE m.extra = n.extra + 1 RETURN m.key"},
		{"sibling/traversal", "MATCH (n:Doc {key:$k}), (n)-[:REL]->(m {extra: n.extra + 1}) RETURN m.key"},
		{"sibling/traversal-labelled", "MATCH (n:Doc {key:$k}), (n:Doc {extra:7})-[:REL]->(m) RETURN m.key"},
		{"sibling/traversal-first", "MATCH (n:Doc)-[:REL]->(m), (n:Doc {extra:7}) RETURN m.key"},
		{"sibling/path", "MATCH (n:Doc {key:$k}), p=(n:Doc)-[:REL]->(m) RETURN [x IN nodes(p) | x.extra] AS l"},
		{"later/dependent-inline", "MATCH (n:Doc {key:$k}) MATCH (m:Doc {extra: n.extra + 1}) RETURN m.key"},
		{"later/dependent-properties", "MATCH (n:Doc {key:$k}) MATCH (m:Doc {extra: properties(n).extra + 1}) RETURN m.key"},
		{"later/dependent-keyword", "MATCH (end:Doc {key:$k}) MATCH (m:Doc {extra: properties(end).extra + 1}) RETURN m.key"},
		{"later/label-predicate", "MATCH (n:Doc {key:$k}) WHERE n:Doc RETURN n.path"},
		{"later/label-predicate-value", "MATCH (n:Doc {key:$k}) RETURN n:Doc AS d, n.path"},
		{"later/type-predicate", "MATCH (n:Doc {key:$k}) RETURN n IS :: NODE AS t, n.path"},
		{"later/map-key-same-name", "MATCH (n:Doc {key:$k}) RETURN {n: n.path} AS m"},
		{"later/map-value", "MATCH (n:Doc {key:$k}) RETURN {a: n, b: 1} AS m"},
		{"later/list-of-node-and-label", "MATCH (n:Doc {key:$k}) RETURN [n, n:Doc] AS l"},
		{"later/label-in-case", "MATCH (n:Doc {key:$k}) RETURN CASE WHEN n:Doc THEN n ELSE null END AS c"},
		{"later/cypher-let", "MATCH (n:Doc {key:$k}) LET e = n.extra RETURN e"},
		{"later/cypher-filter", "MATCH (n:Doc {key:$k}) FILTER n.extra = 7 RETURN n.key"},
	}
}

func TestR1PR1826Round4ProofAnswersWholeNodeReads(t *testing.T) {
	for _, kind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			for _, test := range r1Round4Cases() {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", kind, indexed, test.name), func(t *testing.T) {
					ctx := t.Context()
					control := NewStorageExecutor(r1FullReads{r1Round4Store(t, kind, indexed), r1Round4Name(kind)}, QueryCachePolicy{MaxEntries: 0})
					want := r1Rows(control.Execute(ctx, test.query, r1Params()))
					exec := NewStorageExecutor(r1Round4Store(t, kind, indexed), QueryCachePolicy{MaxEntries: 0})
					for _, stage := range []string{"cold", "repeated"} {
						got := r1Rows(exec.Execute(ctx, test.query, r1Params()))
						if !reflect.DeepEqual(want, got) {
							t.Errorf("%s: %s\n  whole-node answer: %q\n  answer:            %q", stage, test.query, want, got)
						}
					}
				})
			}
		}
	}
}
