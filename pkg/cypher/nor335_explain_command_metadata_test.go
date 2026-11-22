package cypher

// NOR-335 reproduction owners: EXPLAIN of SHOW commands and schema DDL.
//
// The original report (Linear NOR-335, 2026-10-30, release f0c932b90) records
// `EXPLAIN SHOW PROCEDURES`, `SHOW PROCEDURES YIELD name RETURN name`,
// `SHOW FUNCTIONS YIELD name`, `SHOW INDEXES` and
// `CREATE INDEX i FOR (n:L) ON (n.p)` failing with "Cannot establish the
// statement scope and result columns", while `SHOW USERS` and a GRANT control
// already worked. The target contract is: EXPLAIN answers with the command's
// own declared columns, returns no rows, runs nothing, and rejects an invalid
// statement with the error its execution reports.
//
// These tests pin that contract, so each claim is its own subtest. On the
// measured head they are expected to fail for the forms the report lists and
// that later work has not corrected: EXPLAIN of SHOW PROCEDURES / SHOW FUNCTIONS
// (with or without a YIELD/RETURN tail) still declines the statement, and
// EXPLAIN of malformed schema DDL is accepted instead of rejected. The same
// tests pass today for SHOW INDEXES, SHOW USERS, the GRANT control and the
// reported CREATE INDEX form, so a correction cannot satisfy the failing cases
// by weakening those.
//
// Columns are asserted against the SHOW owner's own declaration (the listing's
// header) and, where the report states it, the literal header. Every case runs
// on both parser settings. Reference-Neo4j column sets, HTTP and Bolt routes are
// owned by the parent: this file only establishes the Execute-level contract.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor335Executor is a fresh executor over an isolated namespace: the schema
// starts with only the engine's lookup indexes.
func nor335Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor335_explain"))
}

// nor335IndexNames lists index names through the public listing rather than
// reading the schema manager, so "the index is absent" is a caller-visible fact.
func nor335IndexNames(t *testing.T, exec *StorageExecutor, ctx context.Context) []string {
	t.Helper()
	listed, err := exec.Execute(ctx, "SHOW INDEXES YIELD name", nil)
	require.NoError(t, err)
	names := make([]string, 0, len(listed.Rows))
	for _, row := range listed.Rows {
		require.Len(t, row, 1)
		name, ok := row[0].(string)
		require.True(t, ok, "an index name is a string: %v", row[0])
		names = append(names, name)
	}
	return names
}

// TestNOR335ExplainShowCommandsDeclareTheirOwnHeader is the SHOW half of the
// reported scope: EXPLAIN answers with the header the command itself declares,
// never by running the listing and never by declining the statement.
func TestNOR335ExplainShowCommandsDeclareTheirOwnHeader(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := nor335Executor(t)
		schema := exec.storage.GetSchema()
		before := showConstraintsSchemaSnapshot(schema)

		for _, tc := range []struct {
			name      string
			explain   string
			statement string
			// want is the header the report and the declared column set state.
			// nil means the header is compared with the statement's own only,
			// because a second copy of that list here could drift from it.
			want []string
		}{
			{
				name:      "procedures/default",
				explain:   "EXPLAIN SHOW PROCEDURES",
				statement: "SHOW PROCEDURES",
				want:      []string{"name", "description", "mode", "worksOnSystem"},
			},
			{
				name:      "procedures/yield",
				explain:   "EXPLAIN SHOW PROCEDURES YIELD name",
				statement: "SHOW PROCEDURES YIELD name",
				want:      []string{"name"},
			},
			{
				// The report's user journey: the declared name column survives
				// the RETURN projection.
				name:      "procedures/yield_return",
				explain:   "EXPLAIN SHOW PROCEDURES YIELD name RETURN name",
				statement: "SHOW PROCEDURES YIELD name RETURN name",
				want:      []string{"name"},
			},
			{
				name:      "procedures/yield_star",
				explain:   "EXPLAIN SHOW PROCEDURES YIELD *",
				statement: "SHOW PROCEDURES YIELD *",
			},
			{
				name:      "functions/default",
				explain:   "EXPLAIN SHOW FUNCTIONS",
				statement: "SHOW FUNCTIONS",
				want:      []string{"name", "category", "description"},
			},
			{
				name:      "functions/yield",
				explain:   "EXPLAIN SHOW FUNCTIONS YIELD name",
				statement: "SHOW FUNCTIONS YIELD name",
				want:      []string{"name"},
			},
			{
				name:      "functions/yield_return",
				explain:   "EXPLAIN SHOW FUNCTIONS YIELD name RETURN name",
				statement: "SHOW FUNCTIONS YIELD name RETURN name",
				want:      []string{"name"},
			},
			{
				name:      "indexes/default",
				explain:   "EXPLAIN SHOW INDEXES",
				statement: "SHOW INDEXES",
				want:      showIndexesResult.defaultColumns(),
			},
			{
				name:      "indexes/yield_return",
				explain:   "EXPLAIN SHOW INDEXES YIELD name RETURN name",
				statement: "SHOW INDEXES YIELD name RETURN name",
				want:      []string{"name"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				listed, err := exec.Execute(t.Context(), tc.statement, nil)
				require.NoError(t, err, "the statement itself stays executable")
				require.NotEmpty(t, listed.Columns, "the statement declares its header")
				if tc.want != nil {
					require.Equal(t, tc.want, listed.Columns, "the statement's own declaration")
				}

				explained, err := exec.Execute(t.Context(), tc.explain, nil)
				require.NoError(t, err, "EXPLAIN must answer with the command's declared columns")
				require.Equal(t, listed.Columns, explained.Columns, "EXPLAIN header is the statement's own declaration")
				if tc.want != nil {
					require.Equal(t, tc.want, explained.Columns)
				}
				require.Empty(t, explained.Rows, "EXPLAIN runs no listing")
				require.NotNil(t, explained.Stats)
				plan, ok := explained.Metadata["plan"].(*ExecutionPlan)
				require.True(t, ok, "EXPLAIN attaches a plan")
				require.Equal(t, ModeExplain, plan.Mode)
				require.NotNil(t, plan.Root)

				// The header owner EXPLAIN reads: the same declaration, with no
				// listing run to observe it.
				columns, err := exec.StatementResultColumnsContext(t.Context(), tc.statement, AcceptedSource)
				require.NoError(t, err, "the statement-column owner resolves a declared command")
				require.Equal(t, listed.Columns, columns)
			})
		}

		require.Equal(t, before, showConstraintsSchemaSnapshot(schema),
			"EXPLAIN of a SHOW command lists nothing and changes no schema")
	})
}

// TestNOR335ExplainSchemaDDLDeclaresNoColumnsAndCreatesNothing is the DDL half
// of the reported scope: the plan carries the empty result-column contract, and
// the index the statement names is still absent afterwards.
func TestNOR335ExplainSchemaDDLDeclaresNoColumnsAndCreatesNothing(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := nor335Executor(t)
		schema := exec.storage.GetSchema()
		before := showConstraintsSchemaSnapshot(schema)
		const name = "nor335_explain_index"

		explained, err := exec.Execute(t.Context(), "EXPLAIN CREATE INDEX "+name+" FOR (n:L) ON (n.p)", nil)
		require.NoError(t, err, "the reported DDL form plans")
		require.Empty(t, explained.Columns, "schema DDL declares no result columns")
		require.Empty(t, explained.Rows, "EXPLAIN runs no DDL")
		plan, ok := explained.Metadata["plan"].(*ExecutionPlan)
		require.True(t, ok, "EXPLAIN attaches a plan")
		require.Equal(t, ModeExplain, plan.Mode)

		require.Equal(t, before, showConstraintsSchemaSnapshot(schema), "EXPLAIN creates no index")
		require.NotContains(t, nor335IndexNames(t, exec, t.Context()), name)

		// Positive control: the name EXPLAIN did not use is still free, and the
		// executed statement keeps its own empty header.
		created, err := exec.Execute(t.Context(), "CREATE INDEX "+name+" FOR (n:L) ON (n.p)", nil)
		require.NoError(t, err)
		require.Empty(t, created.Columns)
		require.Empty(t, created.Rows)
		require.Contains(t, nor335IndexNames(t, exec, t.Context()), name)
	})
}

// TestNOR335ExplainControlsRetainExistingBehavior holds the report's controls
// fixed: SHOW USERS and a GRANT already worked, and ordinary non-EXPLAIN
// commands keep their rows and headers while the reported forms are corrected.
func TestNOR335ExplainControlsRetainExistingBehavior(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := nor335Executor(t)
		before := showConstraintsSchemaSnapshot(exec.storage.GetSchema())

		users, err := exec.Execute(t.Context(), "EXPLAIN SHOW USERS", nil)
		require.NoError(t, err, "the SHOW USERS control keeps working")
		require.Equal(t, showUsersResult.defaultColumns(), users.Columns)
		require.Empty(t, users.Rows)

		// GRANT is an administration command with no result columns; EXPLAIN
		// must plan it without performing it.
		grant, err := exec.Execute(t.Context(), "EXPLAIN GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer", nil)
		require.NoError(t, err, "the GRANT control parses and plans on both parsers")
		require.Empty(t, grant.Rows, "EXPLAIN performs no administration")

		require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()),
			"EXPLAIN of the controls changes no schema")

		// Ordinary results of the reported commands are untouched.
		for _, tc := range []struct {
			statement string
			want      []string
		}{
			{"SHOW PROCEDURES", []string{"name", "description", "mode", "worksOnSystem"}},
			{"SHOW FUNCTIONS", []string{"name", "category", "description"}},
		} {
			listed, err := exec.Execute(t.Context(), tc.statement, nil)
			require.NoError(t, err, tc.statement)
			require.Equal(t, tc.want, listed.Columns, tc.statement)
			require.NotEmpty(t, listed.Rows, tc.statement)
		}
		indexes, err := exec.Execute(t.Context(), "SHOW INDEXES", nil)
		require.NoError(t, err)
		require.Equal(t, showIndexesResult.defaultColumns(), indexes.Columns)
		require.NotEmpty(t, indexes.Rows, "the engine's lookup indexes are listed")
	})
}

// TestNOR335ExplainRejectsInvalidCommandsBeforeEffects covers the report's
// invalid-command row: an invalid YIELD, an undefined RETURN variable and
// malformed schema DDL are rejected with their own error and leave no effect.
func TestNOR335ExplainRejectsInvalidCommandsBeforeEffects(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			explain   string
			statement string
			// wantInError is the name the statement's own error reports; the
			// modes must answer with that error, not with unresolved metadata.
			wantInError string
		}{
			{
				name:        "undeclared_yield_column",
				explain:     "EXPLAIN SHOW FUNCTIONS YIELD unlistedColumn",
				statement:   "SHOW FUNCTIONS YIELD unlistedColumn",
				wantInError: "unlistedColumn",
			},
			{
				name:        "undefined_return_variable",
				explain:     "EXPLAIN SHOW PROCEDURES YIELD name RETURN neverYielded",
				statement:   "SHOW PROCEDURES YIELD name RETURN neverYielded",
				wantInError: "neverYielded",
			},
			{
				name:      "malformed_schema_ddl",
				explain:   "EXPLAIN CREATE INDEX nor335_bad FOR (n:L) ON",
				statement: "CREATE INDEX nor335_bad FOR (n:L) ON",
				// The executed statement reports the DDL syntax error through
				// the schema parser; the mode's message is not pinned here.
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := nor335Executor(t)
				before := showConstraintsSchemaSnapshot(exec.storage.GetSchema())

				_, statementErr := exec.Execute(t.Context(), tc.statement, nil)
				require.Error(t, statementErr, "the statement itself is invalid")

				_, explainErr := exec.Execute(t.Context(), tc.explain, nil)
				require.Error(t, explainErr, "EXPLAIN rejects the invalid statement")
				require.NotEqual(t, unresolvedStatementError().Error(), explainErr.Error(),
					"a declared command reports its own error, not unresolved columns")
				if tc.wantInError != "" {
					require.Contains(t, explainErr.Error(), tc.wantInError)
					require.Equal(t, statementErr.Error(), explainErr.Error(),
						"the mode reports the statement's own error")
				}

				require.Equal(t, before, showConstraintsSchemaSnapshot(exec.storage.GetSchema()),
					"a rejected statement performs no DDL")
				require.NotContains(t, nor335IndexNames(t, exec, t.Context()), "nor335_bad")
			})
		}
	})
}
