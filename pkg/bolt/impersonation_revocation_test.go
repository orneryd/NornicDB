package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestBoltImpersonationRevokesRemovedAccountTransaction(t *testing.T) {
	for _, account := range []string{"svc936", "writer936"} {
		for _, change := range []string{"disabled", "deleted", "recreated"} {
			t.Run(account+"/"+change, func(t *testing.T) {
				f := newImpersonationFixture(t)
				f.grant("svc", auth.ImpersonateAllUsers, false)
				require.Empty(t, f.begin(imp("writer936")))
				_, code := f.run("CREATE (:RevokedTransaction {before: true})", nil)
				require.Empty(t, code)
				if change == "disabled" {
					require.NoError(t, f.auth.DisableUser(account))
				} else {
					require.NoError(t, f.auth.DeleteUser(account))
					if change == "recreated" {
						role := auth.Role("svc")
						if account == "writer936" {
							role = auth.Role("writer")
						}
						_, err := f.auth.CreateUser(account, "password123", []auth.Role{role})
						require.NoError(t, err)
					}
				}
				// Use the public RUN handler directly: the test's convenience
				// run helper sends RESET on failure and could hide late cleanup.
				require.NoError(t, f.session.handleRun(buildRunMessageData("CREATE (:RevokedTransaction {after: true})", nil, nil)))
				code, _ = firstFailure(f.responses())
				require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
				require.False(t, f.session.inTransaction)
				require.Nil(t, f.session.impersonated)
				require.True(t, f.session.failedUntilReset)
				require.Zero(t, f.nodeCount(), "revocation rolls back earlier writes and executes no later write")
			})
		}
	}
}
