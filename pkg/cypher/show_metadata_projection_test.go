package cypher

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-300: a SHOW command declares its result schema where its listing builds
// the rows. EXPLAIN and PROFILE report the header the listing itself produces,
// for the tail the statement retains too: a YIELD projection, its aliases and
// the RETURN over them compose with the declaration as any query composes with
// its input, through the one projection owner. A column the command does not
// declare is rejected with the error execution reports for it, and a header is
// declared, never observed — EXPLAIN runs no listing.

// showMetadataCommands are the SHOW commands whose schema this owner declares,
// each with the column its listing names rows by.
var showMetadataCommands = []struct {
	listing string
	column  string
}{
	{listing: "SHOW CONSTRAINTS", column: "name"},
	{listing: "SHOW INDEXES", column: "name"},
	{listing: "SHOW CONSTRAINT CONTRACTS", column: "name"},
	{listing: "SHOW USERS", column: "user"},
}

// showMetadataFixture creates the schema the listings below report, so their
// rows (and so the tails that filter and page them) are not vacuous.
func showMetadataFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, ddl := range []string{
		"CREATE CONSTRAINT show_metadata_unique FOR (n:Person) REQUIRE n.email IS UNIQUE",
		"CREATE CONSTRAINT show_metadata_exists FOR (n:Book) REQUIRE n.title IS NOT NULL",
		"CREATE INDEX show_metadata_index FOR (n:Person) ON (n.name)",
	} {
		if _, err := exec.Execute(t.Context(), ddl, nil); err != nil {
			t.Fatalf("fixture %q: %v", ddl, err)
		}
	}
	return exec
}

// showMetadataTails are the tails a statement retains over a SHOW command: the
// forms Neo4j and this executor accept, each composed from the declaration.
func showMetadataTails(column string) []string {
	return []string{
		"",
		"YIELD " + column,
		"YIELD " + column + " AS named",
		"YIELD *",
		"YIELD " + column + " RETURN " + column,
		"YIELD " + column + " AS named RETURN named",
		"YIELD " + column + " RETURN " + column + " AS named",
		"YIELD * RETURN " + column,
		"YIELD " + column + " WHERE " + column + " <> 'zzz'",
		"YIELD * WHERE " + column + " <> 'zzz'",
		"YIELD " + column + " ORDER BY " + column + " DESC SKIP 0 LIMIT 100",
		"YIELD " + column + " RETURN DISTINCT " + column,
		"YIELD " + column + " RETURN count(*) AS counted",
		"WHERE " + column + " <> 'zzz'",
	}
}

// TestShowCommandModeHeaderMatchesListing: EXPLAIN's and PROFILE's header is the
// header the statement itself returns, for every declared command and every tail
// shape, on both parsers.
func TestShowCommandModeHeaderMatchesListing(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := showMetadataFixture(t)
		for _, command := range showMetadataCommands {
			for _, tail := range showMetadataTails(command.column) {
				statement := command.listing
				if tail != "" {
					statement += " " + tail
				}
				t.Run(statement, func(t *testing.T) {
					listed, err := exec.Execute(t.Context(), statement, nil)
					require.NoError(t, err, "the listing runs")
					for _, mode := range []struct {
						name, prefix string
						rows         bool
					}{
						{name: "EXPLAIN", prefix: "EXPLAIN "},
						{name: "PROFILE", prefix: "PROFILE ", rows: true},
					} {
						result, err := exec.Execute(t.Context(), mode.prefix+statement, nil)
						require.NoError(t, err, mode.name)
						require.Equal(t, listed.Columns, result.Columns, "%s header is the statement's own", mode.name)
						if !mode.rows {
							require.Empty(t, result.Rows, "%s runs no listing", mode.name)
							continue
						}
						require.Len(t, result.Rows, len(listed.Rows), "PROFILE runs the statement it profiles")
					}
				})
			}
		}
	})
}

// The listing's header is the declaration, not a second copy of the column
// order: YIELD * reaches exactly the columns the declaration holds.
func TestShowCommandListingHeaderIsTheDeclaration(t *testing.T) {
	exec := showMetadataFixture(t)
	declarations := map[string]showResult{
		"SHOW CONSTRAINTS":          showConstraintResult(false),
		"SHOW INDEXES":              showIndexesResult,
		"SHOW CONSTRAINT CONTRACTS": showConstraintContractsResult,
		"SHOW USERS":                showUsersResult,
	}
	for _, command := range showMetadataCommands {
		declared := declarations[command.listing]
		t.Run(command.listing, func(t *testing.T) {
			result, err := exec.Execute(t.Context(), command.listing+" YIELD *", nil)
			require.NoError(t, err)
			require.Equal(t, declared.columns, result.Columns)
			plain, err := exec.Execute(t.Context(), command.listing, nil)
			require.NoError(t, err)
			require.Equal(t, declared.defaultColumns(), plain.Columns)
			// The header view the rewriters and transaction adapters read is the
			// same declaration, for a tail as for the listing itself.
			projected, err := exec.StatementResultColumns(command.listing+" YIELD "+command.column+" RETURN "+command.column, AcceptedSource)
			require.NoError(t, err)
			require.Equal(t, []string{command.column}, projected)
		})
	}
}

// A column the command does not declare is the error execution reports for it,
// through the same owner, and a name the tail never produced is undefined: the
// mode answers with that error, not with a header.
func TestShowCommandTailRejectsUndeclaredColumns(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := showMetadataFixture(t)
		for _, command := range showMetadataCommands {
			undeclared := command.listing + " YIELD unlistedColumn"
			_, err := exec.Execute(t.Context(), undeclared, nil)
			require.Error(t, err, "execution rejects the column")
			for _, prefix := range []string{"EXPLAIN ", "PROFILE "} {
				_, modeErr := exec.Execute(t.Context(), prefix+undeclared, nil)
				require.Error(t, modeErr, prefix)
				require.Equal(t, err.Error(), modeErr.Error(), "%s reports the listing's error", prefix)
			}
			undefined := command.listing + " YIELD " + command.column + " RETURN neverYielded"
			for _, prefix := range []string{"EXPLAIN ", "PROFILE "} {
				_, modeErr := exec.Execute(t.Context(), prefix+undefined, nil)
				require.Error(t, modeErr, prefix)
				require.Contains(t, modeErr.Error(), "`neverYielded` not defined")
			}
		}
	})
}

// A header is declared, not observed: the same statement answers with the same
// columns whether the schema holds rows or nothing at all.
func TestShowCommandModeHeaderIsDeclaredOnEmptySchema(t *testing.T) {
	populated := showMetadataFixture(t)
	empty := NewStorageExecutor(newTestMemoryEngine(t))
	for _, command := range showMetadataCommands {
		for _, tail := range []string{"", " YIELD *", " YIELD " + command.column + " RETURN " + command.column} {
			statement := command.listing + tail
			t.Run(statement, func(t *testing.T) {
				declared, err := empty.Execute(t.Context(), "EXPLAIN "+statement, nil)
				require.NoError(t, err)
				listed, err := populated.Execute(t.Context(), "EXPLAIN "+statement, nil)
				require.NoError(t, err)
				require.Equal(t, listed.Columns, declared.Columns)
			})
		}
	}
}

// EXPLAIN and PROFILE read the declaration without changing the schema.
func TestShowCommandModesLeaveSchemaUnchanged(t *testing.T) {
	exec := showMetadataFixture(t)
	schema := exec.storage.GetSchema()
	before := showConstraintsSchemaSnapshot(schema)
	for _, command := range showMetadataCommands {
		for _, tail := range []string{"", " YIELD *", " YIELD " + command.column + " RETURN " + command.column} {
			for _, prefix := range []string{"EXPLAIN ", "PROFILE "} {
				if _, err := exec.Execute(t.Context(), prefix+command.listing+tail, nil); err != nil {
					t.Fatalf("%s%s%s: %v", prefix, command.listing, tail, err)
				}
			}
		}
	}
	if after := showConstraintsSchemaSnapshot(schema); !slices.Equal(before, after) {
		t.Fatalf("schema changed\nbefore: %v\nafter:  %v", before, after)
	}
}

// The declared commands keep their listing's rows and versions: a schema with
// constraints and an index reports them, and a Cypher 25 statement reports its
// own column set.
func TestShowCommandListingsRetainRowsAndVersions(t *testing.T) {
	exec := showMetadataFixture(t)
	constraints, err := exec.Execute(t.Context(), "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.Len(t, constraints.Rows, 2)
	indexes, err := exec.Execute(t.Context(), "SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotEmpty(t, indexes.Rows)
	ctx := context.WithValue(t.Context(), cypherVersionKey{}, "25")
	for _, listing := range []string{"SHOW CONSTRAINTS", "SHOW INDEXES"} {
		listed, err := exec.Execute(ctx, listing, nil)
		require.NoError(t, err)
		explained, err := exec.Execute(ctx, "EXPLAIN "+listing, nil)
		require.NoError(t, err)
		require.Equal(t, listed.Columns, explained.Columns, "%s in Cypher 25", listing)
	}
	if !slices.Contains(showConstraintResult(true).columns, "enforcedLabel") {
		t.Fatal("a Cypher 25 constraint declaration has no enforcedLabel")
	}
}

// The declaration is the statement version's own: a Cypher 25 statement declares
// the enforcedLabel and classification columns a Cypher 5 statement lacks, and a
// YIELD tail over them reports or rejects exactly that.
func TestShowCommandTailUsesTheStatementVersionDeclaration(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := showMetadataFixture(t)
		ctx25 := context.WithValue(t.Context(), cypherVersionKey{}, "25")
		for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
			result, err := exec.Execute(ctx25, prefix+"SHOW CONSTRAINTS YIELD enforcedLabel", nil)
			require.NoError(t, err, prefix)
			require.Equal(t, []string{"enforcedLabel"}, result.Columns, prefix)
			_, err = exec.Execute(t.Context(), prefix+"SHOW CONSTRAINTS YIELD enforcedLabel", nil)
			require.Error(t, err, "Cypher 5 declares no enforcedLabel: %s", prefix)
		}
	})
}

// The declared commands are the ones whose listing schema this owner holds: a
// command with a listing of its own keeps the contract it had.
func TestShowCommandModeDeclinesUndeclaredListings(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, statement := range []string{"SHOW PROCEDURES", "SHOW FUNCTIONS", "SHOW SETTINGS"} {
		_, err := exec.Execute(t.Context(), "EXPLAIN "+statement, nil)
		require.Error(t, err, statement)
		require.Equal(t, unresolvedStatementError().Error(), err.Error(), statement)
	}
}
