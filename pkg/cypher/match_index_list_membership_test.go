package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// listMembershipSetup is the graph the list-membership seek tests read: an
// identity population with scalar keys and list keys, and the values that
// test Cypher's membership edge cases (a scalar in place of a list, numbers
// of both kinds, empty lists, other labels).
const listMembershipSetup = `
UNWIND range(0, 59) AS i
CREATE (:GraphNode:Identity {id: 'id' + toString(i), name_key: 'name' + toString(i % 20),
  alias_keys: ['alias' + toString(i), 'shared' + toString(i % 7)], current: i % 5 <> 0,
  identifier_value_keys: ['acct' + toString(i % 9), 'tax' + toString(i)]})
WITH count(*) AS created
CREATE (:GraphNode:Identity {id: 'scalar', name_key: 'scalar', alias_keys: 'alias3', current: true}),
       (:GraphNode:Identity {id: 'nums', alias_keys: [1, 2.5], current: true}),
       (:GraphNode:Identity {id: 'empty', alias_keys: [], current: true}),
       (:GraphNode:Identity {id: 'none', current: true}),
       (:Doc {id: 'doc', alias_keys: ['alias3'], name_key: 'name3'})`

var listMembershipIndexes = []string{
	"CREATE INDEX identity_alias_keys FOR (n:Identity) ON (n.alias_keys)",
	"CREATE INDEX identity_name_key FOR (n:Identity) ON (n.name_key)",
	"CREATE INDEX identity_identifier_value_keys FOR (n:Identity) ON (n.identifier_value_keys)",
	"CREATE INDEX identity_id FOR (n:Identity) ON (n.id)",
}

// listMembershipQueries are answered by membership seeks once the indexes
// exist, and must return what the label scan returns without them.
var listMembershipQueries = []string{
	"MATCH (n:Identity) WHERE $x IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE 'alias3' IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE 1 IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE 1.0 IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE 2.5 IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE $missing IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN $keys WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE ANY(k IN ['shared2', 'alias4'] WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN $empty WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN $nullList WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	// The mirrored membership: the property is the iterated list.
	"MATCH (n:Identity) WHERE any(k IN n.alias_keys WHERE k IN $keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN coalesce(n.alias_keys, []) WHERE k IN $keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN n.alias_keys WHERE k IN ['alias3', 'shared4']) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN n.alias_keys WHERE k IN $empty) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE any(k IN coalesce(n.alias_keys, []) WHERE k IN $nullList) RETURN n.id ORDER BY n.id",
	"WITH $keys AS scopes MATCH (n:Identity) WHERE any(id IN coalesce(n.alias_keys, []) WHERE id IN scopes) WITH n ORDER BY n.id DESC LIMIT 3 RETURN n.id",
	"MATCH (n:Identity) WHERE n.id IN $ids OR n.name_key IN $names OR any(a IN n.alias_keys WHERE a IN $names) RETURN n.id ORDER BY n.id",
	"WITH 'alias3' AS k MATCH (n:Identity) WHERE any(k IN n.alias_keys WHERE k IN [k]) RETURN n.id ORDER BY n.id",
	// The downstream mention-match shape over stored keys.
	"MATCH (i:GraphNode:Identity) WHERE coalesce(i.current, true) AND (i.id IN $ids OR i.name_key IN $names OR any(a IN i.alias_keys WHERE a IN $names) OR any(v IN i.identifier_value_keys WHERE v IN $values)) RETURN i.id ORDER BY i.id",
	"MATCH (i:Identity) WHERE any(v IN i.identifier_value_keys WHERE v IN $values) RETURN i.id ORDER BY i.id",
	// An OR term that is an indexable conjunct AND a residual.
	"WITH $values AS values MATCH (i:GraphNode:Identity) WHERE coalesce(i.current, true) AND (i.id IN $ids OR i.name_key IN $names OR (any(value IN i.identifier_value_keys WHERE value IN values) AND any(theirs IN i.identifier_value_keys WHERE theirs STARTS WITH 'tax'))) RETURN i.id ORDER BY i.id",
	"MATCH (n:Identity) WHERE $x IN n.alias_keys AND n.current RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE n.current AND any(k IN $keys WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE n.id IN $ids OR n.name_key IN $names OR any(k IN $keys WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE n.name_key = $name OR $x IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE (n.id IN $ids OR any(k IN $keys WHERE k IN n.alias_keys)) AND coalesce(n.current, true) RETURN n.id ORDER BY n.id",
	"MATCH (n:GraphNode:Identity) WHERE $x IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity {current: true}) WHERE $x IN n.alias_keys OR n.name_key = 'name3' RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE $x IN n.alias_keys RETURN count(n) AS c",
	"UNWIND $keys AS key MATCH (n:Identity) WHERE key IN n.alias_keys RETURN key, n.id ORDER BY key, n.id",
	"MATCH (n:Identity) WHERE NOT $x IN n.alias_keys AND n.name_key = 'name3' RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE $x IN n.alias_keys OR n.unindexed = 1 RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE n.alias_keys = ['alias3', 'shared3'] RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) WHERE n.alias_keys = 'alias3' RETURN n.id ORDER BY n.id",
	"MATCH (n:Identity) RETURN n.id ORDER BY n.alias_keys, n.id LIMIT 5",
	"MATCH (n) WHERE $x IN n.alias_keys RETURN n.id ORDER BY n.id",
	"MATCH (n:Doc) WHERE $x IN n.alias_keys RETURN n.id ORDER BY n.id",
	"OPTIONAL MATCH (n:Identity) WHERE 'nothing' IN n.alias_keys RETURN n.id",
}

func listMembershipParams() map[string]interface{} {
	return map[string]interface{}{
		"x":        "alias3",
		"missing":  "nothing",
		"keys":     []interface{}{"shared2", "alias4", "nothing", nil},
		"empty":    []interface{}{},
		"nullList": nil,
		"ids":      []interface{}{"id11", "id12"},
		"names":    []interface{}{"name7"},
		"name":     "name8",
		"values":   []interface{}{"acct3", "tax17", "nothing"},
	}
}

func newListMembershipExecutor(t *testing.T, namespace string, indexed bool) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
	ctx := context.Background()
	if indexed {
		for _, ddl := range listMembershipIndexes {
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err, ddl)
		}
	}
	_, err := exec.Execute(ctx, listMembershipSetup, nil)
	require.NoError(t, err)
	return exec
}

// An index on a list property answers membership tests from the index and
// returns exactly what the label scan returns, in auto-commit and explicit
// transactions, before and after the transaction's own writes.
func TestListMembershipSeekMatchesScan(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			indexed := newListMembershipExecutor(t, "member_indexed", true)
			scanned := newListMembershipExecutor(t, "member_scanned", false)
			params := listMembershipParams()
			compare := func(stage string) {
				t.Helper()
				for _, query := range listMembershipQueries {
					want, err := scanned.Execute(ctx, query, params)
					require.NoError(t, err, "%s scan: %s", stage, query)
					got, err := indexed.Execute(ctx, query, params)
					require.NoError(t, err, "%s seek: %s", stage, query)
					require.Equal(t, want.Rows, got.Rows, "%s: %s", stage, query)
				}
			}
			if explicit {
				for _, exec := range []*StorageExecutor{indexed, scanned} {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					exec := exec
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
			}
			compare("before writes")
			for _, write := range []string{
				"CREATE (:GraphNode:Identity {id: 'new', alias_keys: ['alias3', 'fresh'], current: true})",
				"MATCH (n:Identity {id: 'id4'}) SET n.alias_keys = ['moved']",
				"MATCH (n:Identity {id: 'id3'}) SET n.alias_keys = n.alias_keys + ['extra']",
				"MATCH (n:Identity {id: 'id11'}) DETACH DELETE n",
				"MATCH (n:Identity {id: 'id2'}) REMOVE n:Identity",
				"MATCH (n:Doc {id: 'doc'}) SET n:Identity",
			} {
				for _, exec := range []*StorageExecutor{indexed, scanned} {
					_, err := exec.Execute(ctx, write, nil)
					require.NoError(t, err, write)
				}
			}
			compare("after writes")
		})
	}
}

// The membership tests are answered by the index: a node hidden from the
// alias index is not found by them, while a statement the index can't answer
// still finds it.
func TestListMembershipSeekUsesIndex(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			exec := newListMembershipExecutor(t, "member_proof", true)
			_, err := exec.Execute(ctx, "MATCH (n:Identity {id: 'scalar'}) DETACH DELETE n", nil)
			require.NoError(t, err)
			schema := exec.storage.GetSchema()
			hidden := schema.PropertyIndexLookup("Identity", "id", "id3")
			require.Len(t, hidden, 1)
			// id3's aliases are ['alias3', 'shared3']: hide them from the alias index.
			require.NoError(t, schema.PropertyIndexDelete("Identity", "alias_keys", hidden[0], []interface{}{"alias3", "shared3"}))
			require.NoError(t, schema.PropertyIndexDelete("Identity", "identifier_value_keys", hidden[0], []interface{}{"acct3", "tax3"}))
			exec.ClearQueryCaches()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			params := map[string]interface{}{"x": "alias3", "keys": []interface{}{"alias3", "alias5"}, "ids": []interface{}{"id9"}, "values": []interface{}{"acct3"}}
			rows := func(query string) [][]interface{} {
				t.Helper()
				result, err := exec.Execute(ctx, query, params)
				require.NoError(t, err, query)
				return result.Rows
			}
			for _, test := range []struct {
				query string
				want  [][]interface{}
			}{
				{"MATCH (n:Identity) WHERE $x IN n.alias_keys RETURN n.id ORDER BY n.id", nil},
				{"MATCH (n:Identity) WHERE any(k IN $keys WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id", [][]interface{}{{"id5"}}},
				{"MATCH (n:Identity) WHERE any(k IN n.alias_keys WHERE k IN $keys) RETURN n.id ORDER BY n.id", [][]interface{}{{"id5"}}},
				{"MATCH (n:Identity) WHERE any(v IN n.identifier_value_keys WHERE v IN $values) RETURN n.id ORDER BY n.id", [][]interface{}{{"id12"}, {"id21"}, {"id30"}, {"id39"}, {"id48"}, {"id57"}}},
				// An indexable conjunct AND a residual answers its OR term.
				{"MATCH (n:Identity) WHERE n.id IN $ids OR (any(v IN n.identifier_value_keys WHERE v IN $values) AND n.current) RETURN n.id ORDER BY n.id", [][]interface{}{{"id12"}, {"id21"}, {"id39"}, {"id48"}, {"id57"}, {"id9"}}},
				{"WITH $keys AS scopes MATCH (n:Identity) WHERE any(k IN coalesce(n.alias_keys, []) WHERE k IN scopes) RETURN n.id ORDER BY n.id", [][]interface{}{{"id5"}}},
				{"MATCH (n:Identity) WHERE n.id IN $ids OR any(k IN $keys WHERE k IN n.alias_keys) RETURN n.id ORDER BY n.id", [][]interface{}{{"id5"}, {"id9"}}},
				{"MATCH (n:Identity) WHERE (n.id IN $ids OR $x IN n.alias_keys) AND n.current RETURN n.id ORDER BY n.id", [][]interface{}{{"id9"}}},
				// Not answered by the alias index: the hidden node is found.
				{"MATCH (n:Identity) WHERE $x IN n.alias_keys OR n.unindexed = 1 RETURN n.id ORDER BY n.id", [][]interface{}{{"id3"}}},
				{"MATCH (n:Identity) WHERE NOT 'zzz' IN n.alias_keys AND n.id = 'id3' RETURN n.id", [][]interface{}{{"id3"}}},
			} {
				got := rows(test.query)
				if len(test.want) == 0 {
					require.Empty(t, got, test.query)
					continue
				}
				require.Equal(t, test.want, got, test.query)
			}
		})
	}
}

// Two OR-ed equalities are answered from the indexes only when both
// properties are indexed: with one side unindexed the union of seeks could
// miss the other side's matches.
func TestDisjunctionSeekNeedsEveryTermIndexed(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "disjunction"))
			_, err := exec.Execute(ctx, "CREATE INDEX p_a FOR (n:P) ON (n.a)", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:P {a: 1, b: 0, id: 'x'}), (:P {a: 0, b: 2, id: 'y'}), (:P {a: 0, b: 0, id: 'z'})", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, query := range []string{
				"MATCH (n:P) WHERE n.a = 1 OR n.b = 2 RETURN n.id ORDER BY n.id",
				"MATCH (n:P) WHERE n.b = 2 OR n.a = 1 RETURN n.id ORDER BY n.id",
				"MATCH (n:P) WHERE n.a IN [1] OR n.b IN [2] RETURN n.id ORDER BY n.id",
			} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{"x"}, {"y"}}, result.Rows, query)
			}
		})
	}
}

// A term of the disjunction seek is answered by the index of any label of
// the pattern: here the index a uniqueness constraint owns on the other label
// of a two-label pattern serves i.id IN $ids. A node hidden from that index
// is not found by the seek, and rows equal the scan's.
func TestDisjunctionSeekUsesConstraintIndexOfPatternLabel(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			setup := func(namespace string, indexed bool) *StorageExecutor {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
				if indexed {
					for _, ddl := range []string{
						"CREATE CONSTRAINT graph_node_id FOR (n:GraphNode) REQUIRE n.id IS UNIQUE",
						"CREATE INDEX identity_name_key FOR (n:Identity) ON (n.name_key)",
					} {
						_, err := exec.Execute(ctx, ddl, nil)
						require.NoError(t, err, ddl)
					}
				}
				_, err := exec.Execute(ctx, "UNWIND range(0, 9) AS i CREATE (:GraphNode:Identity {id: 'x' + toString(i), name_key: 'n' + toString(i)}), (:GraphNode {id: 'g' + toString(i)})", nil)
				require.NoError(t, err)
				return exec
			}
			indexed, scanned := setup("pattern_label_indexed", true), setup("pattern_label_scanned", false)
			query := "MATCH (i:GraphNode:Identity) WHERE i.id IN $ids OR i.name_key IN $names RETURN i.id ORDER BY i.id"
			params := map[string]interface{}{"ids": []interface{}{"x2", "x3", "g4"}, "names": []interface{}{"n5"}}
			if explicit {
				for _, exec := range []*StorageExecutor{indexed, scanned} {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					exec := exec
					t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				}
			}
			want, err := scanned.Execute(ctx, query, params)
			require.NoError(t, err)
			got, err := indexed.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, want.Rows, got.Rows)
			require.Equal(t, [][]interface{}{{"x2"}, {"x3"}, {"x5"}}, got.Rows)

			schema := indexed.storage.GetSchema()
			hidden := schema.PropertyIndexLookup("GraphNode", "id", "x2")
			require.Len(t, hidden, 1)
			require.NoError(t, schema.PropertyIndexDelete("GraphNode", "id", hidden[0], "x2"))
			indexed.ClearQueryCaches()
			got, err = indexed.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"x3"}, {"x5"}}, got.Rows, "the id term is answered by the GraphNode constraint index")
		})
	}
}
