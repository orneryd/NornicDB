package cypher

// An explicit transaction sees its own staged schema, on both routes that can
// stage a drop.
//
// CALL db.index.fulltext.drop and CALL db.index.vector.drop edit the schema of
// the transaction the statement runs in. The native route's executor holds the
// transaction's storage adapter directly (StorageExecutor.storage is its
// transactionStorageWrapper); the composite route runs the statement on a
// Fabric participant whose executor holds the same adapter
// (compositeContext -> ctxKeyTxStorage, participantHandle.run). SHOW INDEXES
// reads the schema of the storage the statement runs on
// (executeShowIndexes -> e.storage.GetSchema), and that schema is the
// transaction's isolated view once ordinary schema DDL prepared one
// (prepareSchemaTransaction). So both routes must report the staged drop to
// their own transaction and nothing to a reader outside it until COMMIT.
//
// The contract pinned here, per public statements only:
//
//   - BEGIN; CALL ...drop(..); SHOW INDEXES in the same transaction no longer
//     lists the dropped index (each parser, each drop procedure, both routes);
//   - an independent executor still lists it before COMMIT;
//   - COMMIT publishes exactly that staged drop and the independent executor
//     lists what remains;
//   - ROLLBACK publishes nothing: the independent executor still lists both.
//
// The cancellation-only qualification originally left this visibility open.
// This file qualifies that contract. Fail-first evidence, before the correction
// this file is paired with: the native route passed, while the composite route
// failed for both parsers and both procedures with rows [ft_docs vec_docs] -
// the constituent's published schema, not the transaction's staged view.
//
// The root was Fabric admission, not schema visibility: shouldUseFabricPlanner
// read every USE it found with the subquery grammar, whose follower set excludes
// SHOW, so USE <composite>.<alias> SHOW INDEXES never reached the Fabric
// participant and ran on the constituent's published engine instead. A leading
// top-level USE is now read as the top-level clause it is.

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// indexDropShowInTransaction is the schema introspection every route's own
// transaction runs: only the fixture's two index names, ordered.
const indexDropShowInTransaction = "SHOW INDEXES YIELD name WHERE name IN ['" +
	indexDropFulltextName + "', '" + indexDropVectorName + "'] RETURN name ORDER BY name"

// indexDropProcedureCases is the closed procedure population whose staged drop
// must be visible to its own transaction.
var indexDropProcedureCases = []struct {
	name, statement, dropped string
	remaining                []string
}{
	{"fulltext", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", indexDropFulltextName, []string{indexDropVectorName}},
	{"vector", "CALL db.index.vector.drop('" + indexDropVectorName + "')", indexDropVectorName, []string{indexDropFulltextName}},
}

// TestIndexDropProcedureNativeExplicitTransactionSeesOwnStagedDrop is the
// native explicit transaction control: the drop staged in the transaction is
// the drop the transaction's own SHOW INDEXES reports, it stays private until
// COMMIT, and COMMIT publishes it.
func TestIndexDropProcedureNativeExplicitTransactionSeesOwnStagedDrop(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range indexDropProcedureCases {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFixture(t)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					fixture.indexDropCreateBothKinds(t, ctx)
					outside := fixture.indexDropOutside()

					_, err := fixture.exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					dropped, err := fixture.exec.Execute(ctx, procedure.statement, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{procedure.dropped, true}}, dropped.Rows)

					require.Equal(t, procedure.remaining, readSchemaNames(t, ctx, fixture.exec, indexDropNamesQuery),
						"the transaction must see its own staged drop")
					require.Equal(t, indexDropBothKinds(), readSchemaNames(t, ctx, outside, indexDropNamesQuery),
						"no reader outside the transaction may see the staged drop")

					_, err = fixture.exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
					require.Equal(t, procedure.remaining, readSchemaNames(t, ctx, outside, indexDropNamesQuery),
						"COMMIT must publish the staged drop")
					require.Equal(t, indexDropBothKinds(), readSchemaNames(t, ctx, fixture.other, indexDropNamesQuery))
				})
			}
		})
	}
}

// TestIndexDropProcedureFabricExplicitTransactionSeesOwnStagedDrop is the
// composite explicit transaction: BEGIN on the composite database, the drop
// through USE <composite>.<alias>, and the same transaction's SHOW INDEXES
// through the same supported constituent route. The constituent's own schema
// manager is the independent oracle for publication.
func TestIndexDropProcedureFabricExplicitTransactionSeesOwnStagedDrop(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, procedure := range indexDropProcedureCases {
				t.Run(procedure.name, func(t *testing.T) {
					fixture := newIndexDropFabricFixture(t)
					fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
					fixture.createBothKinds(t, fixture.root, indexDropFabricGraphB)
					session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
					graph := indexDropFabricGraphA

					_, err := session.Execute(fixture.ctx, "BEGIN", nil)
					require.NoError(t, err)
					dropped, err := session.Execute(fixture.ctx, "USE "+graph+" "+procedure.statement, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{procedure.dropped, true}}, dropped.Rows)

					shown, err := session.Execute(fixture.ctx, "USE "+graph+" "+indexDropShowInTransaction, nil)
					require.NoError(t, err)
					require.Equal(t, indexDropNamesOf(procedure.remaining), shown.Rows,
						"the transaction must see its own staged drop")

					require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA),
						"no reader outside the transaction may see the staged drop")
					require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))

					_, err = session.Execute(fixture.ctx, "COMMIT", nil)
					require.NoError(t, err)
					require.Equal(t, procedure.remaining, fixture.liveNames(t, indexDropFabricShardA),
						"COMMIT must publish the staged drop")
					require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardB))
				})
			}
		})
	}
}

// TestIndexDropProcedureExplicitTransactionRollbackKeepsStagedDrop is the
// rollback control for both routes: a staged drop that is rolled back is
// published nowhere, and the transaction's introspection agrees with the
// rollback afterwards.
func TestIndexDropProcedureExplicitTransactionRollbackKeepsStagedDrop(t *testing.T) {
	for _, route := range []struct {
		name      string
		begin     func(*testing.T) (execute func(context.Context, string) *ExecuteResult, published func(*testing.T) []string, cleanup func())
		statement string
		show      string
	}{
		{
			name: "native",
			begin: func(t *testing.T) (func(context.Context, string) *ExecuteResult, func(*testing.T) []string, func()) {
				fixture := newIndexDropFixture(t)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				fixture.indexDropCreateBothKinds(t, ctx)
				outside := fixture.indexDropOutside()
				return func(callCtx context.Context, cypher string) *ExecuteResult {
						result, err := fixture.exec.Execute(callCtx, cypher, nil)
						require.NoError(t, err, cypher)
						return result
					}, func(*testing.T) []string {
						return readSchemaNames(t, ctx, outside, indexDropNamesQuery)
					}, cancel
			},
			statement: "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')",
			show:      indexDropShowInTransaction,
		},
		{
			name: "composite",
			begin: func(t *testing.T) (func(context.Context, string) *ExecuteResult, func(*testing.T) []string, func()) {
				fixture := newIndexDropFabricFixture(t)
				fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
				session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
				return func(callCtx context.Context, cypher string) *ExecuteResult {
						result, err := session.Execute(callCtx, cypher, nil)
						require.NoError(t, err, cypher)
						return result
					}, func(*testing.T) []string {
						return fixture.liveNames(t, indexDropFabricShardA)
					}, func() {}
			},
			statement: "USE " + indexDropFabricGraphA + " CALL db.index.fulltext.drop('" + indexDropFulltextName + "')",
			show:      "USE " + indexDropFabricGraphA + " " + indexDropShowInTransaction,
		},
	} {
		t.Run(route.name, func(t *testing.T) {
			execute, published, cleanup := route.begin(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			require.Equal(t, indexDropBothKinds(), published(t))
			execute(ctx, "BEGIN")
			dropped := execute(ctx, route.statement)
			require.Equal(t, [][]interface{}{{indexDropFulltextName, true}}, dropped.Rows)
			require.Equal(t, indexDropBothKinds(), published(t),
				"the staged drop stays unpublished before ROLLBACK")

			rolledBack := execute(ctx, "ROLLBACK")
			require.Equal(t, [][]interface{}{{"Transaction rolled back"}}, rolledBack.Rows)
			require.Equal(t, indexDropBothKinds(), published(t),
				"ROLLBACK must publish nothing")
			require.Equal(t, indexDropNamesOf(indexDropBothKinds()), execute(ctx, route.show).Rows,
				"the transaction's introspection agrees with the rollback")
		})
	}
}

// indexDropNamesOf renders the fixture's expected name list as the rows the
// introspection statement returns.
func indexDropNamesOf(names []string) [][]interface{} {
	rows := make([][]interface{}, 0, len(names))
	for _, name := range names {
		rows = append(rows, []interface{}{name})
	}
	return rows
}

// indexDropAdmissionCases are the leading forms Fabric admission must read the
// same way: whitespace and comments before a statement's first token are
// trivia, so a USE that follows them is the top-level clause, while a USE after
// statement text starts a CALL { } subquery and keeps that grammar.
var indexDropAdmissionCases = []struct {
	name     string
	cypher   string
	admitted bool
}{
	{"no leading trivia", "USE " + indexDropFabricGraphA + " SHOW INDEXES", true},
	{"leading blank line", "\n\nUSE " + indexDropFabricGraphA + " SHOW INDEXES", true},
	{"leading line comment", "// staged schema\nUSE " + indexDropFabricGraphA + " SHOW INDEXES", true},
	{"leading block comment", "/* staged schema */ USE " + indexDropFabricGraphA + " SHOW INDEXES", true},
	{"nested query clause", "CALL { USE " + indexDropFabricGraphA + " MATCH (n) RETURN n } RETURN 1 AS ok", true},
	{"nested show", "CALL { USE " + indexDropFabricGraphA + " SHOW INDEXES } RETURN 1 AS ok", false},
}

// TestIndexDropProcedureFabricAdmissionIgnoresNormalization is the comment-prefix
// control for that classification. Routing must not depend on the query text
// normalization (config.EnvCypherQueryNormalization: "Permission and routing
// decisions don't depend on it"), and with the pass off a statement reaches
// admission with its comments as written.
//
// Fail-first evidence, before the GapEnd correction: both comment prefixes were
// read with the subquery grammar, whose follower set has no SHOW, so admission
// reported false and the statement left Fabric.
func TestIndexDropProcedureFabricAdmissionIgnoresNormalization(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newIndexDropFabricFixture(t)
			session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
			for _, tc := range indexDropAdmissionCases {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, tc.admitted, session.shouldUseFabricPlanner(fixture.ctx, tc.cypher),
						"normalization enabled")
					restore := config.WithCypherQueryNormalizationDisabled()
					disabled := session.shouldUseFabricPlanner(fixture.ctx, tc.cypher)
					restore()
					require.Equal(t, tc.admitted, disabled, "normalization disabled")
				})
			}
		})
	}
}

// TestIndexDropProcedureNestedUseShowGate is the nested control for the same
// admission: only a statement's first USE is a top-level clause. A USE inside a
// CALL { } subquery keeps the subquery grammar, which admits the query clauses
// and no administration command, so a nested SHOW INDEXES is never admitted to
// Fabric and stays the statement's own rejection.
func TestIndexDropProcedureNestedUseShowGate(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())

			// A nested query clause routes to its constituent and runs.
			fixture := newIndexDropFabricFixture(t)
			fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
			session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
			valid := "CALL { USE " + indexDropFabricGraphA + " RETURN 1 AS v } RETURN v AS nested"
			require.True(t, session.shouldUseFabricPlanner(fixture.ctx, valid))
			result, err := session.Execute(fixture.ctx, valid, nil)
			require.NoError(t, err, valid)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

			// A nested SHOW is not admitted, so no engine runs it. The composite
			// session's own route reports the untargeted statement as
			// Neo.ClientError.Statement.NotAllowed (it requires explicit graph
			// targeting), which is that route's decision, not Fabric admission.
			invalid := "CALL { USE " + indexDropFabricGraphA + " SHOW INDEXES } RETURN 1 AS nested"
			require.False(t, session.shouldUseFabricPlanner(fixture.ctx, invalid))
			result, err = session.Execute(fixture.ctx, invalid, nil)
			require.Error(t, err, invalid)
			require.Nil(t, result, invalid)

			// On a plain database the subquery grammar owns the rejection: SHOW
			// is not one of its followers, and the statement is a syntax error
			// before any index is read.
			plain := NewStorageExecutor(newTestMemoryEngine(t))
			nested := "CALL { USE plain_db SHOW INDEXES } RETURN 1 AS nested"
			require.False(t, plain.shouldUseFabricPlanner(fixture.ctx, nested))
			_, err = plain.Execute(fixture.ctx, nested, nil)
			require.Error(t, err, nested)
			require.ErrorContains(t, err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

// The public path must preserve transaction-local schema visibility even when
// source normalization is disabled and USE follows client-supplied trivia.
func TestIndexDropProcedureFabricCommentPrefixStagedVisibility(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			t.Cleanup(config.WithCypherQueryNormalizationDisabled())
			for _, tc := range []struct{ name, prefix string }{
				{"line_comment", "// staged schema\n"},
				{"block_comment", "/* staged schema */ "},
				{"bom_comment", "\xef\xbb\xbf/* staged schema */ "},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fixture := newIndexDropFabricFixture(t)
					fixture.createBothKinds(t, fixture.root, indexDropFabricGraphA)
					session := newCompositeTestExecutor(t, fixture.mgr, indexDropFabricComposite, fixture.root.dbManager)
					_, err := session.Execute(fixture.ctx, "BEGIN", nil)
					require.NoError(t, err)
					_, err = session.Execute(fixture.ctx, tc.prefix+"USE "+indexDropFabricGraphA+" "+indexDropFabricDropFT, nil)
					require.NoError(t, err)
					rows, err := session.Execute(fixture.ctx, tc.prefix+"USE "+indexDropFabricGraphA+" "+indexDropShowInTransaction, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{indexDropVectorName}}, rows.Rows)
					require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA), "staging must remain private")
					_, err = session.Execute(fixture.ctx, "ROLLBACK", nil)
					require.NoError(t, err)
					require.Equal(t, indexDropBothKinds(), fixture.liveNames(t, indexDropFabricShardA), "rollback must preserve published indexes")
				})
			}
		})
	}
}
