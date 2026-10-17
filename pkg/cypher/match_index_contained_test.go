package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// containedSetup is the identity population the contained-key seek tests
// read: name keys and alias key lists, with the edge cases of CONTAINS (an
// empty key, a non-string key, a scalar in place of a list, a missing
// property), identities that are not current, and another label.
const containedSetup = `
UNWIND range(0, 39) AS i
CREATE (:GraphNode:Identity {id: 'id' + toString(i), name_key: 'name' + toString(i),
  alias_keys: ['alias' + toString(i), 'corp' + toString(i % 6)], current: i % 7 <> 0})
WITH count(*) AS created
CREATE (:GraphNode:Identity {id: 'emptyname', name_key: '', alias_keys: ['zz'], current: true}),
       (:GraphNode:Identity {id: 'emptyalias', name_key: 'qq', alias_keys: ['', 'corp1'], current: true}),
       (:GraphNode:Identity {id: 'nonstring', name_key: 42, alias_keys: [7, 'alias30'], current: true}),
       (:GraphNode:Identity {id: 'scalaralias', name_key: 'ss', alias_keys: 'name3', current: true}),
       (:GraphNode:Identity {id: 'nokeys', current: true}),
       (:Doc {id: 'doc', name_key: 'name1', alias_keys: ['alias1']})`

const containedIndexDDL = "CREATE CONTAINED INDEX identity_keys FOR (n:Identity) ON EACH [n.name_key, n.alias_keys]"

// containedQueries are answered by contained-key seeks once the index
// exists, and must return what the label scan returns without it. The first
// is the downstream identity-candidate shape.
var containedQueries = []string{
	"WITH $text AS text MATCH (i:Identity) WHERE coalesce(i.current, true) AND (text CONTAINS i.name_key OR any(a IN i.alias_keys WHERE a <> '' AND text CONTAINS a)) WITH i ORDER BY i.id LIMIT $limit RETURN i.id",
	"WITH $text AS text MATCH (i:Identity) WHERE coalesce(i.current, true) AND ((i.name_key <> '' AND text CONTAINS i.name_key) OR any(a IN i.alias_keys WHERE a <> '' AND text CONTAINS a)) WITH i ORDER BY i.id LIMIT $limit RETURN i.id",
	"MATCH (i:Identity) WHERE $text CONTAINS i.name_key RETURN i.id ORDER BY i.id",
	"MATCH (i:Identity) WHERE any(a IN i.alias_keys WHERE $text CONTAINS a) RETURN i.id ORDER BY i.id",
	"MATCH (i:Identity) WHERE any(a IN coalesce(i.alias_keys, []) WHERE $text CONTAINS a) RETURN i.id ORDER BY i.id",
	"MATCH (i:GraphNode:Identity) WHERE $text CONTAINS i.name_key OR any(a IN i.alias_keys WHERE $text CONTAINS a) RETURN i.id ORDER BY i.id",
	"WITH toLower($text) AS text MATCH (i:Identity) WHERE text CONTAINS i.name_key RETURN count(i) AS c",
	"MATCH (i:Identity) WHERE $nullText CONTAINS i.name_key RETURN i.id",
	"MATCH (i:Identity) WHERE $number CONTAINS i.name_key RETURN i.id",
	"MATCH (i:Identity) WHERE NOT $text CONTAINS i.name_key AND i.id = 'id3' RETURN i.id",
	"UNWIND [$text, 'alias9 and corp2'] AS text MATCH (i:Identity) WHERE text CONTAINS i.name_key OR any(a IN i.alias_keys WHERE text CONTAINS a) RETURN text, i.id ORDER BY text, i.id",
	"MATCH (n:Doc) WHERE $text CONTAINS n.name_key RETURN n.id",
}

func containedParams() map[string]interface{} {
	return map[string]interface{}{
		"text":     "Letter from name3 to the alias12 office of corp4, ref alias30",
		"limit":    int64(25),
		"nullText": nil,
		"number":   int64(3),
	}
}

func newContainedExecutor(t *testing.T, namespace string, indexed bool) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
	ctx := context.Background()
	if indexed {
		_, err := exec.Execute(ctx, containedIndexDDL, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, containedSetup, nil)
	require.NoError(t, err)
	return exec
}

// A contained-key index answers $text CONTAINS n.key and its list form from
// the index and returns exactly what the label scan returns, in auto-commit
// and explicit transactions, before and after the transaction's own writes.
func TestContainedSeekMatchesScan(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			indexed := newContainedExecutor(t, "contained_indexed", true)
			scanned := newContainedExecutor(t, "contained_scanned", false)
			params := containedParams()
			compare := func(stage string) {
				t.Helper()
				for _, query := range containedQueries {
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
				"CREATE (:GraphNode:Identity {id: 'new', name_key: 'office', alias_keys: ['fresh'], current: true})",
				"MATCH (i:Identity {id: 'id4'}) SET i.alias_keys = ['moved']",
				"MATCH (i:Identity {id: 'id12'}) SET i.name_key = 'renamed'",
				"MATCH (i:Identity {id: 'id30'}) DETACH DELETE i",
				"MATCH (i:Identity {id: 'id3'}) REMOVE i:Identity",
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

// The contained terms are answered by the index: a node hidden from it is
// not found by them, while a statement the index can't answer still finds
// it.
func TestContainedSeekUsesIndex(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			ctx := context.Background()
			exec := newContainedExecutor(t, "contained_proof", true)
			idx, ok := exec.storage.GetSchema().GetContainedIndex("identity_keys")
			require.True(t, ok)
			var id3 storage.NodeID
			for _, match := range idx.Lookup("name3") {
				if match.Key == "name3" {
					id3 = match.NodeID
				}
			}
			require.NotEmpty(t, id3, "id3's name key is filed")
			idx.RefileNode(id3, func() (map[string]interface{}, error) { return nil, nil }, nil)
			exec.ClearQueryCaches()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			params := map[string]interface{}{"text": "about name3 and alias3", "limit": int64(10)}
			for _, test := range []struct {
				query string
				want  [][]interface{}
			}{
				// id3 (name_key 'name3', alias 'alias3') is hidden; the empty
				// name key is contained in every text.
				{"MATCH (i:Identity) WHERE $text CONTAINS i.name_key RETURN i.id ORDER BY i.id", [][]interface{}{{"emptyname"}}},
				{"WITH $text AS text MATCH (i:Identity) WHERE coalesce(i.current, true) AND (text CONTAINS i.name_key OR any(a IN i.alias_keys WHERE a <> '' AND text CONTAINS a)) WITH i ORDER BY i.id LIMIT $limit RETURN i.id", [][]interface{}{{"emptyname"}, {"scalaralias"}}},
				// Not answered by the contained index: the hidden node is found.
				{"MATCH (i:Identity) WHERE $text CONTAINS i.name_key OR i.unindexed = 1 RETURN i.id ORDER BY i.id", [][]interface{}{{"emptyname"}, {"id3"}}},
			} {
				result, err := exec.Execute(ctx, test.query, params)
				require.NoError(t, err, test.query)
				require.Equal(t, test.want, result.Rows, test.query)
			}
		})
	}
}
