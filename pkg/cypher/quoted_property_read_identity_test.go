package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Quoted property names denote one exact key, while unquoted dotted map access
// denotes a path. Verify projection and filtering through public node and
// relationship reads, including a competing ordinary key and an escaped name.
// These tests use both parsers in isolated processes; no reference server runs.
func TestQuotedDottedPropertyReadsAreExactWithoutIndex(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_property_read"))
		ctx := context.Background()

		for _, statement := range []string{
			"CREATE (:QPRNode {id: 1, `external.id`: 'flat', `external``.id`: 'escaped', external: 'neighbour', plain: 'kept'})",
			"CREATE (:QPRNode {id: 2, plain: 'kept'})",
			"CREATE (:QPRRelA {id: 1})-[r:QPRRel {`external.id`: 'rel-flat', `external``.id`: 'rel-escaped', external: 'rel-neighbour'}]->(:QPRRelB {id: 1})",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		schema := exec.storage.GetSchema()
		require.False(t, schema.MaintainsPropertyIndex("QPRNode", "external.id"),
			"the node reads run with no index on the quoted dotted property")
		require.False(t, schema.MaintainsPropertyIndex("QPRRel", "external.id"),
			"the relationship reads run with no index on the quoted dotted property")

		// The stored names the quoted references must read. properties() is the
		// name-holder view, so this pins the fixtures themselves: the dotted
		// names are the keys that were written, and storage is not what a read
		// may re-interpret.
		stored, err := exec.Execute(ctx, "MATCH (n:QPRNode {id: 1}) RETURN properties(n)['external.id'] AS dotted, properties(n)['external`.id'] AS escaped, properties(n)['external'] AS neighbour, properties(n)['plain'] AS plain", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"flat", "escaped", "neighbour", "kept"}}, stored.Rows,
			"the fixtures hold the exact property names")

		for _, tc := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			// Node projections: the quoted reference names the dotted key.
			{"node quoted dotted projection", "MATCH (n:QPRNode {id: 1}) RETURN n.`external.id` AS v", [][]interface{}{{"flat"}}},
			{"node escaped backtick and dot projection", "MATCH (n:QPRNode {id: 1}) RETURN n.`external``.id` AS v", [][]interface{}{{"escaped"}}},
			{"node ordinary property control", "MATCH (n:QPRNode {id: 1}) RETURN n.plain AS v", [][]interface{}{{"kept"}}},
			{"node neighbour is its own name", "MATCH (n:QPRNode {id: 1}) RETURN n.external AS v", [][]interface{}{{"neighbour"}}},
			{"node without the dotted property projects null", "MATCH (n:QPRNode {id: 2}) RETURN n.`external.id` AS v", [][]interface{}{{nil}}},

			// Node matching: the same name selects the same rows.
			{"node quoted dotted match", "MATCH (n:QPRNode) WHERE n.`external.id` = 'flat' RETURN n.id AS id", [][]interface{}{{int64(1)}}},
			{"node escaped backtick and dot match", "MATCH (n:QPRNode) WHERE n.`external``.id` = 'escaped' RETURN n.id AS id", [][]interface{}{{int64(1)}}},
			{"node quoted dotted name does not fall through to the neighbour", "MATCH (n:QPRNode) WHERE n.`external.id` = 'neighbour' RETURN count(*) AS c", [][]interface{}{{int64(0)}}},
			{"node ordinary property match control", "MATCH (n:QPRNode) WHERE n.plain = 'kept' RETURN count(*) AS c", [][]interface{}{{int64(2)}}},

			// The relationship owner reads the same names the same way.
			{"relationship quoted dotted projection", "MATCH ()-[r:QPRRel]->() RETURN r.`external.id` AS v", [][]interface{}{{"rel-flat"}}},
			{"relationship escaped backtick and dot projection", "MATCH ()-[r:QPRRel]->() RETURN r.`external``.id` AS v", [][]interface{}{{"rel-escaped"}}},
			{"relationship ordinary property control", "MATCH ()-[r:QPRRel]->() RETURN r.external AS v", [][]interface{}{{"rel-neighbour"}}},
			{"relationship quoted dotted match", "MATCH ()-[r:QPRRel]->() WHERE r.`external.id` = 'rel-flat' RETURN count(r) AS c", [][]interface{}{{int64(1)}}},
			{"relationship quoted dotted name does not fall through to the neighbour", "MATCH ()-[r:QPRRel]->() WHERE r.`external.id` = 'rel-neighbour' RETURN count(r) AS c", [][]interface{}{{int64(0)}}},

			// The competing nested reading: one key holding a dot, and a map
			// path reaching the same text through two keys, stay distinct.
			{"map key with a dot is one key, not a path", "WITH {`external.id`: 'map-flat', external: {id: 'map-nested'}} AS m RETURN m.`external.id` AS key, m.external.id AS path", [][]interface{}{{"map-flat", "map-nested"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				assert.Equal(t, tc.want, result.Rows, tc.query)
			})
		}
	})
}

// The same reads over an existing RANGE index on the quoted dotted property
// answer exactly what the no-index reads answer: adding an index is not
// allowed to change which property name the statement denotes. The fixture is
// the supported CREATE INDEX ... FOR (n:Label) ON (n.<property>) form; the
// index is asserted to hold the exact decoded dotted name, and the index's
// own equality lookup must return the node whose property carries the value,
// so the comparison is between two live routes over that name rather than a
// plan claim. Both parser selections run these steps in their own process.
func TestQuotedDottedPropertyReadsAgreeWithAndWithoutIndex(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		const create = "CREATE (:QPRIndexed {`external.id`: 'flat', external: 'neighbour'})"
		const projection = "MATCH (n:QPRIndexed) RETURN n.`external.id` AS v"
		const matching = "MATCH (n:QPRIndexed) WHERE n.`external.id` = 'flat' RETURN count(*) AS c"
		wantProjection := [][]interface{}{{"flat"}}
		wantMatching := [][]interface{}{{int64(1)}}

		scanExec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_property_read_scan"))
		scanCtx := context.Background()
		_, err := scanExec.Execute(scanCtx, create, nil)
		require.NoError(t, err, create)
		require.False(t, scanExec.storage.GetSchema().MaintainsPropertyIndex("QPRIndexed", "external.id"),
			"the scan side has no index on the quoted dotted property")

		indexedExec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted_property_read_indexed"))
		indexedCtx := context.Background()
		_, err = indexedExec.Execute(indexedCtx, create, nil)
		require.NoError(t, err, create)
		const createIndex = "CREATE INDEX qpr_indexed_dot FOR (n:QPRIndexed) ON (n.`external.id`)"
		_, err = indexedExec.Execute(indexedCtx, createIndex, nil)
		require.NoError(t, err, createIndex)
		indexedSchema := indexedExec.storage.GetSchema()
		require.True(t, indexedSchema.MaintainsPropertyIndex("QPRIndexed", "external.id"),
			"the index holds the exact quoted dotted name")
		indexedIDs, err := indexedSchema.PropertyIndexLookup(indexedCtx, "QPRIndexed", "external.id", "flat")
		require.NoError(t, err)
		require.Len(t, indexedIDs, 1, "the index answers the quoted dotted name with the stored node")

		scanProjection, err := scanExec.Execute(scanCtx, projection, nil)
		require.NoError(t, err, projection)
		scanMatching, err := scanExec.Execute(scanCtx, matching, nil)
		require.NoError(t, err, matching)
		indexedProjection, err := indexedExec.Execute(indexedCtx, projection, nil)
		require.NoError(t, err, projection)
		indexedMatching, err := indexedExec.Execute(indexedCtx, matching, nil)
		require.NoError(t, err, matching)

		assert.Equal(t, wantProjection, scanProjection.Rows, "no index: %s", projection)
		assert.Equal(t, wantMatching, scanMatching.Rows, "no index: %s", matching)
		assert.Equal(t, wantProjection, indexedProjection.Rows, "with index: %s", projection)
		assert.Equal(t, wantMatching, indexedMatching.Rows, "with index: %s", matching)
		assert.Equal(t, scanProjection.Rows, indexedProjection.Rows, "the projection reads the same name either way")
		assert.Equal(t, scanMatching.Rows, indexedMatching.Rows, "the filter matches the same name either way")
	})
}
