package nornicdb

import (
	"context"
	"fmt"
	"sort"
	"testing"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDB_CreateIndexKeepsStoredRowsReadable is the public product-path
// regression for G1-2 (PR1667 index maintenance preparation): DB.CreateIndex
// declares an equality index on a label/property whose nodes are already
// stored. Once it returns, an equality read on that label/property must still
// return every stored node that matches, and writes after the declaration must
// be visible to the same read.
//
// Only the public surface is used: DB.Open, DB.ExecuteCypher, DB.CreateIndex
// and DB.GetIndexes. Both storage engines (MemoryEngine for an empty data
// directory, BadgerEngine for a real one) and both Cypher parsers run the same
// scenario, so no mock, no direct SchemaManager seek and no private helper
// stands in for the product path.
//
// The second half is the control: the same population read through the Cypher
// DDL owner (CREATE INDEX), which fills a new index from the stored nodes
// before the statement returns. It shows the read path itself is sound and
// isolates the defect to the declaration that publishes an unfilled index.
//
// Every assertion uses its own statement text: the executor's result cache is
// keyed by statement text and parameters, so repeating one text across the
// declaration would serve the pre-declaration rows and hide the defect.
func TestDB_CreateIndexKeepsStoredRowsReadable(t *testing.T) {
	backends := []struct {
		name string
		open func(t *testing.T) *DB
	}{
		{
			name: "memory",
			open: func(t *testing.T) *DB {
				t.Helper()
				db, err := Open("", nil)
				require.NoError(t, err)
				return db
			},
		},
		{
			name: "badger",
			open: func(t *testing.T) *DB {
				t.Helper()
				db, err := Open(t.TempDir(), nil)
				require.NoError(t, err)
				return db
			},
		},
	}
	parsers := []string{featureflags.ParserTypeNornic, featureflags.ParserTypeANTLR}

	for _, backend := range backends {
		for _, parser := range parsers {
			t.Run(backend.name+"/"+parser, func(t *testing.T) {
				previous := featureflags.GetParserType()
				featureflags.SetParserType(parser)
				t.Cleanup(func() { featureflags.SetParserType(previous) })

				db := backend.open(t)
				t.Cleanup(func() { assert.NoError(t, db.Close()) })
				ctx := context.Background()

				// Stored nodes that exist before any index does.
				execCypher(t, db, ctx, "CREATE (n:IdxPerson {name: 'alice', age: 31})", nil)
				execCypher(t, db, ctx, "CREATE (n:IdxPerson {name: 'bob', age: 42})", nil)
				require.Equal(t, int64(2), labelCount(t, db, ctx, "IdxPerson", "seeded"))
				storedBefore := labelNodeIDs(t, db, ctx, "IdxPerson")
				require.Len(t, storedBefore, 2)
				require.Empty(t, userIndexSpecs(t, db, "IdxPerson"),
					"fixture must start without a user index on the label")

				// The exported product API that declares the index.
				require.NoError(t, db.CreateIndex(ctx, "IdxPerson", "name", "property"))

				t.Logf("index metadata after DB.CreateIndex: %v", userIndexSpecs(t, db, "IdxPerson"))
				t.Logf("label count after DB.CreateIndex: %d", labelCount(t, db, ctx, "IdxPerson", "after_declare"))

				// The declaration publishes one user index for the label and
				// leaves the stored nodes as they were.
				assert.Equal(t, []string{"IdxPerson/name/property"}, userIndexSpecs(t, db, "IdxPerson"))
				assert.Equal(t, storedBefore, labelNodeIDs(t, db, ctx, "IdxPerson"),
					"declaring the index changed the stored node identities")

				// The affected product reads: equality and IN on the declared
				// property must return the stored nodes that match.
				assert.Equal(t, []string{"alice"}, equalityNames(t, db, ctx, "IdxPerson", "declared_eq", "alice"),
					"equality MATCH lost a stored node after DB.CreateIndex")
				assert.Equal(t, []string{"bob"}, equalityNames(t, db, ctx, "IdxPerson", "declared_eq_second", "bob"),
					"equality MATCH lost a stored node after DB.CreateIndex")
				assert.Equal(t, []string{"alice", "bob"},
					equalityNames(t, db, ctx, "IdxPerson", "declared_in", "alice", "bob"),
					"IN-list MATCH lost stored nodes after DB.CreateIndex")
				assert.Equal(t, int64(2), labelCount(t, db, ctx, "IdxPerson", "after_reads"),
					"the label scan and the indexed read disagree about the stored population")

				// Write maintenance after the declaration, on the same read.
				execCypher(t, db, ctx, "CREATE (n:IdxPerson {name: 'carol', age: 27})", nil)
				assert.Equal(t, []string{"carol"}, equalityNames(t, db, ctx, "IdxPerson", "inserted", "carol"),
					"a node inserted after the declaration is not found")
				assert.Equal(t, int64(3), labelCount(t, db, ctx, "IdxPerson", "after_insert"))

				// The update predicate uses an unindexed property, so the
				// update itself does not depend on the declared index.
				execCypher(t, db, ctx, "MATCH (n:IdxPerson) WHERE n.age = 42 SET n.name = 'bob2'", nil)
				assert.Empty(t, equalityNames(t, db, ctx, "IdxPerson", "stale_value", "bob"),
					"an updated (stale) indexed value is still returned")
				assert.Equal(t, []string{"bob2"}, equalityNames(t, db, ctx, "IdxPerson", "updated_value", "bob2"),
					"an updated indexed value is not found")
				assert.Equal(t, int64(3), labelCount(t, db, ctx, "IdxPerson", "after_update"))

				execCypher(t, db, ctx, "MATCH (n:IdxPerson) WHERE n.age = 31 DELETE n", nil)
				assert.Empty(t, equalityNames(t, db, ctx, "IdxPerson", "removed_value", "alice"),
					"a removed node is still returned")
				assert.Equal(t, []string{"bob2", "carol"},
					equalityNames(t, db, ctx, "IdxPerson", "survivors", "bob2", "carol"))
				assert.Equal(t, int64(2), labelCount(t, db, ctx, "IdxPerson", "after_delete"))

				// Control: the Cypher DDL owner fills a new index from the
				// stored nodes before the statement returns, so the same
				// population read keeps every matching node.
				execCypher(t, db, ctx, "CREATE (n:IdxControl {name: 'dave', age: 50})", nil)
				execCypher(t, db, ctx, "CREATE (n:IdxControl {name: 'erin', age: 60})", nil)
				execCypher(t, db, ctx, "CREATE INDEX idx_IdxControl_name FOR (n:IdxControl) ON (n.name)", nil)
				require.Equal(t, []string{"IdxControl/name/property"}, userIndexSpecs(t, db, "IdxControl"),
					"the Cypher DDL owner did not declare the control index")
				assert.Equal(t, []string{"dave"}, equalityNames(t, db, ctx, "IdxControl", "ddl_eq", "dave"),
					"equality MATCH lost a stored node after the Cypher DDL declaration")
				assert.Equal(t, []string{"dave", "erin"},
					equalityNames(t, db, ctx, "IdxControl", "ddl_in", "dave", "erin"),
					"IN-list MATCH lost stored nodes after the Cypher DDL declaration")
				assert.Equal(t, int64(2), labelCount(t, db, ctx, "IdxControl", "control_count"))
			})
		}
	}
}

// execCypher runs a statement through the public DB surface and fails the test
// on error.
func execCypher(t *testing.T, db *DB, ctx context.Context, query string, params map[string]any) *CypherResult {
	t.Helper()
	result, err := db.ExecuteCypher(ctx, query, params)
	require.NoErrorf(t, err, "query failed: %s", query)
	return result
}

// equalityNames returns the sorted returned node names of
// MATCH (n:<label>) WHERE n.name = <value> for each wanted value, merged. alias
// keeps each statement text distinct so the result cache cannot serve another
// statement's rows.
func equalityNames(t *testing.T, db *DB, ctx context.Context, label, alias string, wanted ...string) []string {
	t.Helper()
	out := make([]string, 0, len(wanted))
	for index, value := range wanted {
		result := execCypher(t, db, ctx,
			fmt.Sprintf("MATCH (n:%s) WHERE n.name = $name RETURN n.name AS %s_%d", label, alias, index),
			map[string]any{"name": value})
		for _, row := range result.Rows {
			require.Len(t, row, 1)
			name, ok := row[0].(string)
			require.Truef(t, ok, "name column is not a string: %#v", row[0])
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// labelCount returns MATCH (n:<label>) RETURN count(n): the label scan, which
// does not use a declared index.
func labelCount(t *testing.T, db *DB, ctx context.Context, label, alias string) int64 {
	t.Helper()
	result := execCypher(t, db, ctx,
		fmt.Sprintf("MATCH (n:%s) RETURN count(n) AS count_%s", label, alias), nil)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)
	count, ok := result.Rows[0][0].(int64)
	require.Truef(t, ok, "count(n) is not an int64: %#v", result.Rows[0][0])
	return count
}

// labelNodeIDs returns the sorted stored node identities for the label.
func labelNodeIDs(t *testing.T, db *DB, ctx context.Context, label string) []string {
	t.Helper()
	result := execCypher(t, db, ctx, fmt.Sprintf("MATCH (n:%s) RETURN id(n) AS stored_id", label), nil)
	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 1)
		ids = append(ids, fmt.Sprint(row[0]))
	}
	sort.Strings(ids)
	return ids
}

// userIndexSpecs returns the label/property/type of every non-token index the
// public metadata surface reports for one label, sorted.
func userIndexSpecs(t *testing.T, db *DB, label string) []string {
	t.Helper()
	indexes, err := db.GetIndexes(context.Background())
	require.NoError(t, err)
	specs := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index == nil || index.Type == "lookup" || index.Label != label {
			continue
		}
		specs = append(specs, fmt.Sprintf("%s/%s/%s", index.Label, index.Property, index.Type))
	}
	sort.Strings(specs)
	return specs
}
