package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This list covers each registry route that executes supplied statements, not
// the broader expression-input inventory (whose 19-handler bound is unchanged).
func relationshipSuppliedStatementCases() []struct {
	name, query string
	lookups     int64
	rows        [][]interface{}
} {
	var cases []struct {
		name, query string
		lookups     int64
		rows        [][]interface{}
	}
	add := func(name, query string, lookups int64, rows [][]interface{}) {
		cases = append(cases, struct {
			name, query string
			lookups     int64
			rows        [][]interface{}
		}{name, query, lookups, rows})
	}
	one := [][]interface{}{{"x"}}
	add("direct", "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p AS p", 1, one)
	for _, name := range []string{"apoc.cypher.run", "apoc.cypher.doIt", "apoc.cypher.doitall"} {
		add(name, "CALL "+name+"($statement,{x:$x}) YIELD value RETURN value.p AS p", 1, one)
	}
	add("runMany", "CALL apoc.cypher.runMany($many,{x:$x},{statistics:false}) YIELD result RETURN result.p AS p", 2, [][]interface{}{{"x"}, {"x"}})
	for _, name := range []string{"apoc.periodic.iterate", "apoc.periodic.rock_n_roll"} {
		add(name, "CALL "+name+"($statement,$action,{batchSize:1}) YIELD total WHERE total=1 RETURN 'x' AS p", 2, one)
	}
	add("commit", "CALL apoc.periodic.commit($limited,{x:$x}) YIELD updates,executions WHERE updates=0 AND executions=1 RETURN 'x' AS p", 1, one)
	for _, format := range []string{"json", "csv"} {
		add("export_"+format, "CALL apoc.export."+format+".query($statement,'',{}) YIELD rows WHERE rows=1 RETURN 'x' AS p", 1, one)
	}
	for _, name := range []string{"custom.site_install", "custom.site_declare", "site.ddl"} {
		add(name, "CALL "+name+"($x) YIELD p RETURN p", 1, one)
	}
	add("computed_text", "CALL apoc.cypher.run($prefix+$suffix,{x:$x}) YIELD value RETURN value.p AS p", 1, one)
	add("nested_procedure", "CALL apoc.cypher.run($nested,{statement:$statement,x:$x}) YIELD value RETURN value.p AS p", 1, one)
	add("supplied_call_body", "CALL apoc.cypher.run($callBody,{x:$x}) YIELD value RETURN value.p AS p", 1, one)
	add("supplied_collect_body", "CALL apoc.cypher.run($collectBody,{x:$x}) YIELD value RETURN value.p AS p", 1, one)
	return cases
}

func TestRelationshipAccessProgramSuppliedStatements(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		for _, outer := range []bool{false, true} {
			for _, tc := range relationshipSuppliedStatementCases() {
				t.Run(fmt.Sprintf("%s/transaction_%t/outer_%t", tc.name, transaction, outer), func(t *testing.T) {
					ClearUserProcedures()
					t.Cleanup(ClearUserProcedures)
					e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "supplied_sites"))
					setupRelationshipSuppliedFixture(t, e)
					ctx := context.Background()
					if transaction {
						_, err := e.Execute(WithTransactionControl(ctx), "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, err := e.Execute(WithTransactionControl(ctx), "ROLLBACK", nil); require.NoError(t, err) })
					}
					query := tc.query
					if outer {
						query = "MATCH (a:Key)-[s:T]->() WITH a " + query
					}
					params := relationshipSuppliedParams()
					// Ordinary public execution and public PROFILE must agree on rows
					// and actual seeks. Reuse one observer only for ordinary execution;
					// PROFILE creates its own recorder and is checked through its plan.
					observed := withRelationshipSeekProfile(context.WithValue(ctx, profileExecutionKey{}, true))
					result, err := e.Execute(observed, query, params)
					require.NoError(t, err)
					require.Equal(t, tc.rows, result.Rows)
					profile := observed.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
					require.Empty(t, profile.unregistered, "every executing statement must own its source sites")
					seek := profile.seeks["r\x00rel_p"]
					require.NotNil(t, seek)
					require.Equal(t, tc.lookups, seek.lookups)
					result, err = e.Execute(ctx, "PROFILE "+query, params)
					require.NoError(t, err)
					require.Equal(t, tc.rows, result.Rows)
					assertRelationshipSuppliedPlan(t, result, tc.lookups)
				})
			}
		}
	}
}

func TestRelationshipAccessProgramAdmissionInvariant(t *testing.T) {
	requireRelationshipAdmissionBuildRejected(t, `func(e *StorageExecutor, ctx context.Context) { _ = e.relationshipAccessFor(ctx, "()-[r:T]->()", "r.p=$x") }`, "relationshipAccessFor")
	requireRelationshipAdmissionBuildRejected(t, `func(e *StorageExecutor, ctx context.Context) { _, _ = e.executeFragment(ctx, "RETURN 1", nil) }`, "executeFragment")
	// Fragment admission cannot recreate a template removed from its source.
	const query = "RETURN COLLECT { MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p }"
	source := &relationshipAccessProgram{query: query, byID: map[string]*relationshipAccessTemplate{}}
	ctx, statement := compileRelationshipStatement(context.WithValue(context.Background(), relationshipAccessProgramKey{}, source), query, nil)
	ctx, statement = compileRelationshipStatement(ctx, "WITH $x AS k MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p", statement.program)
	require.Same(t, source, statement.program)
	require.Empty(t, statement.program.byID)
	// New executable text in that same context owns a distinct complete source.
	_, statement = compileRelationshipStatement(ctx, "MATCH ()-[r:T]->() WHERE r.p=$x RETURN r.p", nil)
	require.NotSame(t, source, statement.program)
	require.Len(t, statement.program.sites, 1)
}
