package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// Over Bolt, the old password stops working as soon as it is changed,
// though the adapter caches the password check.
func TestChangedPasswordStopsWorkingOverBolt(t *testing.T) {
	authenticator, adapter, _ := boltAccountFixture(t)
	_, err := authenticator.CreateUser("bob", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	for range 2 {
		_, err = adapter.Authenticate("basic", "bob", "password123")
		require.NoError(t, err)
	}
	require.NoError(t, authenticator.ChangePassword("bob", "password123", "newPassword456"))
	_, err = adapter.Authenticate("basic", "bob", "password123")
	require.Error(t, err)
	_, err = adapter.Authenticate("basic", "bob", "newPassword456")
	require.NoError(t, err)

	anonymous := NewAuthenticatorAdapterWithAnonymous(authenticator)
	_, err = anonymous.Authenticate("basic", "bob", "newPassword456")
	require.NoError(t, err)
	require.NoError(t, authenticator.ChangePassword("bob", "newPassword456", "thirdPassword789"))
	_, err = anonymous.Authenticate("basic", "bob", "newPassword456")
	require.Error(t, err)
}
