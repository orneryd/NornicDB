package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// An operation contract is valid only with known effects, at least one, and
// None alone; Has reports any of the given effects.
func TestOperationClassContract(t *testing.T) {
	for _, valid := range []OperationClass{OperationNone, OperationRead, OperationRead | OperationWrite, OperationAdmin | OperationDBMS | OperationSharedState, OperationFileIO | OperationRemote} {
		require.True(t, valid.Valid(), valid)
	}
	for _, invalid := range []OperationClass{0, OperationNone | OperationRead, OperationRemote << 1} {
		require.False(t, invalid.Valid(), invalid)
	}
	contract := OperationRead | OperationWrite
	require.True(t, contract.Has(OperationWrite))
	require.True(t, contract.Has(OperationSchema|OperationRead))
	require.False(t, contract.Has(OperationAdmin))
}

// The request carries the principal's name and roles for the refusals that
// name them; a request without them has neither.
func TestRequestPrincipalName(t *testing.T) {
	ctx := WithRequestPrincipalName(WithRequestPrincipalRoles(context.Background(), []string{"reader"}), "alice")
	require.Equal(t, "alice", RequestPrincipalNameFromContext(ctx))
	require.Equal(t, []string{"reader"}, RequestPrincipalRolesFromContext(ctx))
	require.Empty(t, RequestPrincipalNameFromContext(context.Background()))
	require.Nil(t, RequestPrincipalRolesFromContext(context.Background()))
}
