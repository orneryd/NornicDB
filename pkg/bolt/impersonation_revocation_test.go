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
				require.Zero(t, f.labelCount("RevokedTransaction"), "revocation rolls back earlier writes and executes no later write")
			})
		}
	}
}

// A transaction impersonating a user ends, rolled back, at the first
// statement after the signer may no longer impersonate that user: its
// IMPERSONATE privilege revoked, the role holding it removed from the
// signer, or (for an administrator) the admin entitlement lost. New
// impersonation is refused the same way.
func TestBoltImpersonationRevokesTransactionWhenImpersonationRevoked(t *testing.T) {
	for _, change := range []string{"privilege revoked", "role removed", "admin lost"} {
		t.Run(change, func(t *testing.T) {
			f := newImpersonationFixture(t)
			signer := "svc936"
			if change == "admin lost" {
				var err error
				f.session.authResult, err = f.adapter.Authenticate("basic", "root936", "password123")
				require.NoError(t, err)
				signer = "root936"
			} else {
				f.grant("svc", auth.ImpersonateAllUsers, false)
			}
			require.Empty(t, f.begin(imp("writer936")))
			_, code := f.run("CREATE (:RevokedImpersonation {before: true})", nil)
			require.Empty(t, code)
			switch change {
			case "privilege revoked":
				removed, err := f.privileges.Remove(f.ctx, "svc", auth.ImpersonateAllUsers, false, false)
				require.NoError(t, err)
				require.True(t, removed)
			default:
				require.NoError(t, f.auth.UpdateRoles(signer, []auth.Role{"reader"}))
			}
			require.NoError(t, f.session.handleRun(buildRunMessageData("CREATE (:RevokedImpersonation {after: true})", nil, nil)))
			code, message := firstFailure(f.responses())
			require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			require.Contains(t, message, "writer936")
			require.False(t, f.session.inTransaction)
			require.Nil(t, f.session.impersonated)
			require.Zero(t, f.labelCount("RevokedImpersonation"), "revocation rolls back earlier writes and executes no later write")
			f.reset()
			require.Equal(t, "Neo.ClientError.Security.Forbidden", f.begin(imp("writer936")), "a new impersonation is refused too")
		})
	}
}
