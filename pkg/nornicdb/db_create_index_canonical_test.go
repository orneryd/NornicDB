package nornicdb

// DB.CreateIndex canonical-declaration tests (PR1667 G1-2). DB.CreateIndex
// declares a property or btree index through the one Cypher DDL owner
// (CREATE INDEX ... IF NOT EXISTS), so its duplicate rule, its name quoting and
// its fill/publish belong to the schema transaction that owns ordinary schema
// DDL. Nothing here is a second declaration path.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forEachDBBackendAndParser runs body once per storage engine and Cypher
// parser, against a freshly opened database (the public construction surface).
func forEachDBBackendAndParser(t *testing.T, body func(t *testing.T, db *DB, ctx context.Context)) {
	t.Helper()
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
	for _, backend := range backends {
		for _, parser := range []string{featureflags.ParserTypeNornic, featureflags.ParserTypeANTLR} {
			t.Run(backend.name+"/"+parser, func(t *testing.T) {
				previous := featureflags.GetParserType()
				featureflags.SetParserType(parser)
				t.Cleanup(func() { featureflags.SetParserType(previous) })

				db := backend.open(t)
				t.Cleanup(func() { assert.NoError(t, db.Close()) })
				body(t, db, context.Background())
			})
		}
	}
}

// TestDB_CreateIndexCanonicalDuplicateRule : the declaration is canonical
// CREATE INDEX ... IF NOT EXISTS. Repeating it for the name it derives changes
// nothing, the btree spelling names the same declaration, and an equivalent
// index that already exists under another name is left alone while the stored
// population stays readable.
func TestDB_CreateIndexCanonicalDuplicateRule(t *testing.T) {
	forEachDBBackendAndParser(t, func(t *testing.T, db *DB, ctx context.Context) {
		const label, property = "IdemPerson", "name"
		execCypher(t, db, ctx, "CREATE (n:IdemPerson {name: 'alice'})", nil)
		execCypher(t, db, ctx, "CREATE (n:IdemPerson {name: 'alice'})", nil)
		execCypher(t, db, ctx, "CREATE (n:IdemPerson {name: 'bob'})", nil)

		require.NoError(t, db.CreateIndex(ctx, label, property, "property"))
		declared := userIndexSpecs(t, db, label)
		require.Equal(t, []string{"IdemPerson/name/property"}, declared)
		require.Equal(t, []string{"alice", "alice"}, equalityNames(t, db, ctx, label, "first_alice", "alice"))

		// Exact name: the canonical rule leaves the filled index as it is.
		require.NoError(t, db.CreateIndex(ctx, label, property, "property"))
		assert.Equal(t, declared, userIndexSpecs(t, db, label))
		assert.Equal(t, []string{"alice", "alice"}, equalityNames(t, db, ctx, label, "repeat_alice", "alice"))

		// btree is the same declaration, not a second index.
		require.NoError(t, db.CreateIndex(ctx, label, property, "btree"))
		assert.Equal(t, declared, userIndexSpecs(t, db, label))
		assert.Equal(t, []string{"bob"}, equalityNames(t, db, ctx, label, "repeat_bob", "bob"))

		// An equivalent index under another name is the existing equivalent:
		// the declared no-op leaves that index and the population untouched.
		const equivalent = "EquivalentPerson"
		execCypher(t, db, ctx, "CREATE (n:EquivalentPerson {name: 'carol'})", nil)
		execCypher(t, db, ctx, "CREATE INDEX idx_equivalent_person FOR (n:EquivalentPerson) ON (n.name)", nil)
		require.Equal(t, []string{"EquivalentPerson/name/property"}, userIndexSpecs(t, db, equivalent))
		require.NoError(t, db.CreateIndex(ctx, equivalent, "name", "property"))
		assert.Equal(t, []string{"EquivalentPerson/name/property"}, userIndexSpecs(t, db, equivalent))
		assert.Equal(t, []string{"carol"}, equalityNames(t, db, ctx, equivalent, "equivalent_carol", "carol"))
	})
}

// TestDB_CreateIndexKeepsQuotedIdentities declares indexes for labels and
// properties that are not plain identifiers. The stored declaration and the
// published index keep the identity the nodes hold, so the equality read
// written with the same quoted names returns every matching node.
func TestDB_CreateIndexKeepsQuotedIdentities(t *testing.T) {
	const unpaired = "mark\xed\xa0\x80"
	cases := []struct{ label, property string }{
		{"DbQuoted.Person", "given.name"},
		{"DbQuoted,Person", "given,name"},
		{"DbQuoted`Person", "given`name"},
		{"DbQuotedUnicodePerson", "名前"},
		{"DbQuotedEmojiPerson", "🚀"}, // one scalar above the BMP (a UTF-16 surrogate pair)
		{"DbQuotedSurrogatePerson", unpaired},
		{"DbQuoted" + unpaired, "name"},
	}
	forEachDBBackendAndParser(t, func(t *testing.T, db *DB, ctx context.Context) {
		for index, tc := range cases {
			label := cyphertext.PrintIdentifier(tc.label, nil, cyphertext.RawSource)
			property := cyphertext.PrintIdentifier(tc.property, nil, cyphertext.RawSource)
			value := fmt.Sprintf("db-value-%d", index)
			for range 2 {
				execCypher(t, db, ctx, "CREATE (n:"+label+" {"+property+": $value})", map[string]any{"value": value})
			}

			require.NoError(t, db.CreateIndex(ctx, tc.label, tc.property, "property"), tc.label)
			require.Equal(t, []string{tc.label + "/" + tc.property + "/property"}, userIndexSpecs(t, db, tc.label),
				"the declaration renamed %q/%q", tc.label, tc.property)

			// The published index holds exactly the stored nodes under that
			// identity: the declaration and the fill agree.
			ids, err := db.storage.GetSchema().PropertyIndexLookup(ctx, tc.label, tc.property, value)
			require.NoError(t, err)
			require.Len(t, ids, 2, "the filled index does not hold both stored %q nodes", tc.property)

			result := execCypher(t, db, ctx,
				"MATCH (n:"+label+") WHERE n."+property+" = $value RETURN n."+property+" AS v",
				map[string]any{"value": value})
			require.Len(t, result.Rows, 2, "the quoted equality read lost a stored node for %q", tc.property)
			for _, row := range result.Rows {
				require.Len(t, row, 1)
				require.Equal(t, value, row[0])
			}
		}
	})
}
