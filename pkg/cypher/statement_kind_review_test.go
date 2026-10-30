package cypher

// Public-path reproductions of every finding of the family review of
// 12fedf207/593c8b5b8 (r73/1695-1699-12fe-family-review.md), on both parsers.
// Each failed on those heads; each passes when one owner decides a statement's
// kind, effect and permission.

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// P1-1: every index kind needs the schema entitlement.
func TestReviewKindIndexKindsNeedSchemaPermission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, statement := range []string{
			"CREATE LOOKUP INDEX review_lookup FOR (n) ON EACH labels(n)",
			"CREATE TEXT INDEX review_text FOR (n:L) ON (n.p)",
			"CREATE POINT INDEX review_point FOR (n:L) ON (n.p)",
		} {
			got := QueryPermissionRequirements(t.Context(), statement, RawSource)
			require.True(t, got.Schema, statement)
			require.True(t, got.ChangesState, statement)
			require.Equal(t, "schema", MissingPermission(got, func(p string) bool { return p == "read" || p == "write" }, nil), statement)
		}
	})
}

// P1-2: a READ procedure cannot contain a schema or administration change,
// and Bolt/HTTP write handling reads the same effect.
func TestReviewKindReadProcedureRefusesEveryStateChange(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		for _, body := range []string{"DROP INDEX review_idx", "CREATE INDEX review_idx FOR (n:L) ON (n.p)", "DROP DATABASE review_db"} {
			_, err := exec.Execute(t.Context(), "CREATE PROCEDURE custom.reviewSchema() MODE READ AS "+body, nil)
			require.Error(t, err, body)
			require.Contains(t, err.Error(), localization.CypherProceduresReadContainsWrite().Fallback, body)
		}
		for _, statement := range []string{"CREATE DATABASE review_db", "DROP INDEX review_idx", "CREATE (:Review)"} {
			require.True(t, QueryPermissionRequirements(t.Context(), statement, RawSource).ChangesState, statement)
		}
		require.False(t, QueryPermissionRequirements(t.Context(), "SHOW DATABASES", RawSource).ChangesState)
	})
}

// P1-3: a SHOW command continuing with an updating clause changes state.
func TestReviewKindShowContinuationEffects(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		statement := "CYPHER 25 SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' CREATE (:ShowX) RETURN name"
		got := QueryPermissionRequirements(t.Context(), statement, RawSource)
		require.True(t, got.ChangesState, statement)
		require.True(t, got.Write, statement)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		_, err := exec.Execute(t.Context(), "CREATE PROCEDURE custom.reviewShow() MODE READ AS "+statement, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), localization.CypherProceduresReadContainsWrite().Fallback)
	})
}

// P1-4: metadata of composite and privilege commands reports their columns.
func TestReviewKindMetadataSelectsCommandsByIdentity(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		_, exec, manager := newDatabaseCommandFixture(t)
		exec.SetDatabaseManager(manager)
		columns, err := exec.StatementResultColumns("CREATE COMPOSITE DATABASE review_comp ALIAS seed FOR DATABASE nornic", RawSource)
		require.NoError(t, err)
		require.NotEmpty(t, columns)
		_, err = exec.StatementResultColumns("GRANT EXECUTE PROCEDURE * ON DBMS TO review_role", RawSource)
		require.NoError(t, err)
	})
}

// P1-5: a CYPHER preamble before a privilege command reaches its owner.
func TestReviewKindPreambleBeforeCommand(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		_, exec, manager := newDatabaseCommandFixture(t)
		exec.SetDatabaseManager(manager)
		for _, statement := range []string{"GRANT EXECUTE PROCEDURE * ON DBMS TO review_role", "CREATE DATABASE review_pre"} {
			_, bare := exec.Execute(t.Context(), statement, nil)
			_, prefixed := exec.Execute(t.Context(), "CYPHER 5 "+statement, nil)
			if prefixed != nil {
				require.NotContains(t, prefixed.Error(), "invalid CYPHER option", statement)
			}
			require.Equal(t, bare == nil, prefixed == nil || strings.Contains(prefixed.Error(), "already exists"), statement)
		}
	})
}

// P1-6: USE before an administration command is refused on every entry.
func TestReviewKindUseRefusalOnEveryEntry(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		_, exec, manager := newDatabaseCommandFixture(t)
		exec.SetDatabaseManager(manager)
		statement := "USE system CREATE DATABASE review_use"
		_, err := exec.Execute(t.Context(), statement, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), localization.CypherCommandRoutingUseAdministrationCommand().Fallback)
		_, _, _, _, err = readUseStatement(t.Context(), statement, false)
		require.EqualError(t, err, localization.CypherCommandRoutingUseAdministrationCommand().Fallback)
	})
}

// P2-1: a leading comment does not change the statement's kind, with query
// normalization off.
func TestReviewKindLeadingCommentKeepsKind(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		previous := config.IsCypherQueryNormalizationEnabled()
		config.SetCypherQueryNormalizationEnabled(false)
		t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		_, plain := exec.Execute(t.Context(), "UNWIND [1] AS x RETURN x 'a'", nil)
		_, commented := exec.Execute(t.Context(), "/* c */ UNWIND [1] AS x RETURN x 'a'", nil)
		require.Error(t, plain)
		require.Error(t, commented)
		require.Contains(t, plain.Error(), "an expression is followed by another expression without an operator")
		require.Contains(t, commented.Error(), "an expression is followed by another expression without an operator")
		require.True(t, classifyStatement("/* c */ MATCH (n) RETURN n").graphQuery)
	})
}

// Census findings on the corrected head: the statement start, a command's own
// syntax diagnosis and dangling-clause detection read the owner, not word lists.
func TestReviewKindStartAndSyntaxOwnershipFromOwner(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := &StorageExecutor{}
		for _, statement := range []string{"INVALID", "DENY WHATEVER", "SET n.x = 1"} {
			require.False(t, hasValidStartKeyword(classifyStatement(statement)), statement)
			require.ErrorContains(t, exec.validateStatementSyntax(t.Context(), statement, false), "query must start with a valid clause", statement)
		}
		for _, statement := range []string{"LOAD CSV FROM 'file:///x.csv' AS row RETURN row", "EXPLAIN MATCH (n) RETURN n", "/* c */ USE\u00a0nornic RETURN 1", "GRANT EXECUTE PROCEDURE * ON DBMS TO r"} {
			require.True(t, hasValidStartKeyword(classifyStatement(statement)), statement)
		}
		// Non-ASCII whitespace ends a word; a leading comment or byte order
		// mark is not part of the statement.
		require.Equal(t, commandShowIndexes, classifyStatement("SHOW\u00a0INDEXES").command)
		require.Equal(t, commandProcedurePrivilege, classifyStatement("/* c */ GRANT EXECUTE PROCEDURE * ON DBMS TO r").command)
		require.Equal(t, commandCreateDatabase, classifyStatement("\ufeff// c\nCREATE DATABASE d").command)
		// A command's body never routes it as a graph query.
		exec3 := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		_, err := exec3.Execute(t.Context(), "CREATE PROCEDURE custom.reviewWithBody() MODE READ AS MATCH (n) WITH n RETURN n", nil)
		require.NoError(t, err)
		// A CYPHER preamble ends before NornicDB's own statements outside the
		// grammar, as before any grammar statement.
		exec2 := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		result, err := exec2.Execute(t.Context(), "CYPHER 25 WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), result.Rows[0][0])
		require.True(t, classifyStatement("CYPHER 5 LOAD CSV FROM 'file:///x.csv' AS row RETURN row").command == commandNone)
		require.True(t, classifyStatement("SHOW RETENTION BINDINGS").commandOwned)
		require.True(t, classifyStatement("DROP RETENTION POLICY kind").commandOwned)
		require.True(t, classifyStatement("DROP WHATEVER").commandOwned)
		require.False(t, classifyStatement("CREATE RETENTION POLICY kind FOR (n:L) DURATION 1d").commandOwned)
		require.Equal(t, "RETURN", lastTopLevelClauseWord("SHOW FUNCTIONS YIELD name WITH name RETURN name"))
		require.Equal(t, "", lastTopLevelClauseWord("SHOW INDEXES"))
	})
}
