package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A statement apoc.cypher.runMany runs that fails fails the call, as one
// apoc.cypher.run runs does, and with it the caller's transaction: in an
// explicit transaction COMMIT fails and keeps nothing, and in auto-commit
// nothing the earlier statements wrote is kept.
func TestRunManyFailedStatementFailsTheTransaction(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto-commit", true: "explicit transaction"}[explicit], func(t *testing.T) {
			ctx := context.Background()
			exec, stores := nestedUseRouterExecutor(t, "nornic")
			if explicit {
				_, err := exec.handleBegin()
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "CREATE (:BeforeRunMany)", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "CALL apoc.cypher.runMany('CREATE (:InRunMany); RETURN __nornic_interpolate(1) AS v;', {}) YIELD row RETURN row", nil)
			require.ErrorContains(t, err, "unknown function: __nornic_interpolate")
			if explicit {
				_, err = exec.handleCommit(context.Background())
				require.Error(t, err, "the refused statement failed the transaction")
			}
			for _, label := range []string{"BeforeRunMany", "InRunMany"} {
				nodes, err := stores["nornic"].GetNodesByLabel(label)
				require.NoError(t, err)
				require.Empty(t, nodes, label)
			}
		})
	}
}

// An administration command in a statement a procedure runs is refused the
// same way in auto-commit and in an explicit transaction: the procedure's
// statement belongs to the caller's data statement, so it is Neo4j's
// ForbiddenDueToTransactionType, and no grant is written.
func TestAdministrationCommandInProcedureStatementIsRefused(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto-commit", true: "explicit transaction"}[explicit], func(t *testing.T) {
			ctx := context.Background()
			exec, stores := nestedUseRouterExecutor(t, "nornic")
			if explicit {
				_, err := exec.handleBegin()
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "CALL apoc.cypher.run('GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer', {}) YIELD value RETURN value", nil)
			requireStatusCode(t, err, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType")
			if explicit {
				_, err = exec.handleCommit(context.Background())
				require.Error(t, err)
			}
			grants, err := auth.NewProcedurePrivilegeStore(stores["system"]).List(ctx)
			require.NoError(t, err)
			require.Empty(t, grants)
		})
	}
}

// GRANT checks and locks its roles through the one role-record owner
// (auth.WithRoleRecords): a ROLE_ prefixed name grants the role it names,
// and a missing role is Neo4j's ArgumentError naming it as written.
func TestGrantGoesThroughTheRoleRecordOwner(t *testing.T) {
	ctx := context.Background()
	exec, stores := nestedUseRouterExecutor(t, "system")
	require.NoError(t, auth.NewRoleStore(stores["system"]).CreateRole(ctx, "ops"))
	_, err := exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ROLE_ops", nil)
	require.NoError(t, err)
	allowed, err := auth.NewProcedurePrivilegeStore(stores["system"]).AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed)

	_, err = exec.Execute(ctx, "GRANT EXECUTE PROCEDURE db.ping ON DBMS TO nobody", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	require.ErrorContains(t, err, "Failed to grant execute privilege to role 'nobody': Role does not exist.")
}
