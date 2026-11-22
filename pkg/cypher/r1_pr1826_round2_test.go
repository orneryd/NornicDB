package cypher

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826, round 2: the proof gaps through more clauses, and
// through the stores the server gives an executor (managed and composite).

func r1Round2Cases() []r1Case {
	return []r1Case{
		{"cycle/back-to-start", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[:REL]->(m)-[:BACK]->(x) RETURN x.extra AS e"},
		{"cycle/back-to-start-unlabelled", "MATCH (n:Doc {key:$k}) MATCH (n)-[:REL]->(m)-[:BACK]->(x) RETURN x.extra AS e"},
		{"cycle/varlength", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[:REL|BACK*2..2]->(x) RETURN x.extra AS e"},
		{"endpoint/start-node-value", "MATCH (n:Doc {key:$k}) MATCH (n:Doc)-[r:REL]->(m) RETURN startNode(r) AS s"},
		{"endpoint/end-node-value", "MATCH (n:Doc {key:'two'}) MATCH (m)-[r:REL]->(n:Doc) RETURN endNode(r) AS s"},
		{"call/import-match", "MATCH (n:Doc {key:$k}) CALL (n) { MATCH (n:Doc {extra:7}) RETURN n.path AS p } RETURN n.key, p"},
		{"call/inner-hole", "CALL () { MATCH (n:Doc {key:'one'}) MATCH (n:Doc {extra:7}) RETURN n.path AS p } RETURN p"},
		{"union/second-branch", "MATCH (n:Doc {key:$k}) RETURN n.path AS p UNION MATCH (n:Doc {key:'two'}) MATCH (n:Doc {extra:8}) RETURN n.path AS p"},
		{"exists/nested-path", "MATCH (n:Doc {key:$k}) RETURN n.key, EXISTS { MATCH p=(n:Doc)-[:REL]->() WHERE any(x IN nodes(p) WHERE x.extra=7) } AS e"},
		{"count/nested-path", "MATCH (n:Doc {key:$k}) RETURN n.key, COUNT { MATCH p=(n:Doc)-[:REL]->() WHERE all(x IN nodes(p) WHERE x.extra IS NOT NULL) } AS c"},
		{"chain/two-later-matches", "MATCH (n:Doc {key:$k}) MATCH (o:Other) MATCH (n:Doc {extra:7}) RETURN n.key, o.key"},
		{"chain/optional-then-match", "MATCH (n:Doc {key:$k}) OPTIONAL MATCH (o:Missing) MATCH (n:Doc {extra:7}) RETURN n.key"},
		{"aggregate/after-hole", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7}) RETURN count(*) AS c"},
		{"aggregate/count-n", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7}) RETURN count(n) AS c"},
		{"limit/after-hole", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7}) RETURN n.path AS p LIMIT 1"},
		{"order/after-hole", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:7}) RETURN n.path AS p ORDER BY n.status"},
		{"range/then-hole", "MATCH (n:Doc) WHERE n.extra > 6 MATCH (n:Doc {status:'current'}) RETURN n.key"},
		{"in/then-hole", "MATCH (n:Doc) WHERE n.key IN ['one','two'] MATCH (n:Doc {status:'current'}) RETURN n.key"},
		{"or/then-hole", "MATCH (n:Doc) WHERE n.key = 'one' OR n.key = 'two' MATCH (n:Doc {status:'current'}) RETURN n.key"},
		{"label/different-label", "MATCH (n:Doc {key:$k}) MATCH (n:Doc&!Other {extra:7}) RETURN n.key"},
		{"inline/list-value", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {list:[1,2,3]}) RETURN n.key"},
		{"inline/wrong-value", "MATCH (n:Doc {key:$k}) MATCH (n:Doc {extra:8}) RETURN n.key"},
		{"negated/not-pattern", "MATCH (n:Doc {key:$k}) WHERE NOT (n:Doc {extra:7})-[:REL]->() RETURN n.key"},
		{"path/nodes-of-named-node", "MATCH p=(n:Doc {key:$k}) RETURN [x IN nodes(p) | properties(x)] AS l"},
		{"path/json", "MATCH p=(n:Doc {key:$k}) RETURN length(p) AS l, size(keys(nodes(p)[0])) AS k"},
		{"path/range", "MATCH p=(n:Doc) WHERE n.extra > 6 RETURN [x IN nodes(p) | x.path] AS l"},
		{"path/order-limit", "MATCH p=(n:Doc) WHERE n.extra > 6 RETURN [x IN nodes(p) | x.path] AS l ORDER BY n.extra LIMIT 1"},
	}
}

func r1SeedRound2(t testing.TB, store storage.Engine, indexed bool) {
	t.Helper()
	seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	var statements []string
	if indexed {
		statements = append(statements,
			"CREATE CONSTRAINT r1_doc_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE",
			"CREATE INDEX r1_doc_extra IF NOT EXISTS FOR (n:Doc) ON (n.extra)")
	}
	statements = append(statements,
		"CREATE (a:Doc {key:'one', path:'guide', status:'current', extra:7, list:[1,2,3], body:'body one'})"+
			"-[:REL {w:1}]->(b:Doc {key:'two', path:'second', status:'old', extra:8, list:[4], body:'body two'}), (b)-[:BACK]->(a)",
		"CREATE (:Other {key:'x', ref:'one'})")
	for _, statement := range statements {
		_, err := seed.Execute(t.Context(), statement, nil)
		require.NoError(t, err, statement)
	}
}

func TestR1PR1826Round2ProofAnswersWholeNodeReads(t *testing.T) {
	for _, storeKind := range []string{"namespaced", "managed"} {
		for _, indexed := range []bool{true, false} {
			build := func(t *testing.T) (storage.Engine, string) {
				if storeKind == "namespaced" {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
					r1SeedRound2(t, store, indexed)
					return store, "r1"
				}
				manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				store, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				r1SeedRound2(t, store, indexed)
				return store, "nornic"
			}
			for _, test := range append(r1Round2Cases(), r1ProofCases()...) {
				t.Run(fmt.Sprintf("%s/indexed=%t/%s", storeKind, indexed, test.name), func(t *testing.T) {
					ctx := t.Context()
					controlStore, name := build(t)
					control := NewStorageExecutor(r1FullReads{controlStore, name}, QueryCachePolicy{MaxEntries: 0})
					want := r1Rows(control.Execute(ctx, test.query, r1Params()))
					store, _ := build(t)
					exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
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

// A statement that writes first and then reads through the gap.
func TestR1PR1826Round2ReadAfterWriteInOneStatement(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
	r1SeedRound2(t, store, true)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	query := "CREATE (:Log {id:1}) WITH 1 AS one MATCH (n:Doc {key:'one'}) MATCH (n:Doc {extra:7}) RETURN n.key"
	require.Equal(t, []string{"'one'"}, r1Rows(exec.Execute(t.Context(), query, nil)))
}

// The application-level consequence: a MERGE-like "create if absent" decided by
// a property-only read.
func TestR1PR1826Round2CreateIfAbsentDecidedByRead(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r1")
	r1SeedRound2(t, store, true)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	found := r1Rows(exec.Execute(t.Context(), "MATCH (n:Doc {key:'one'}) MATCH (n:Doc {status:'current'}) RETURN count(n) AS c", nil))
	require.Equal(t, []string{"1"}, found)
}
