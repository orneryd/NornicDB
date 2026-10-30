package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// EntitlementsAllow holds a permission the entitlements name, in any case or
// spacing, and every permission for an entitlement set holding admin.
func TestEntitlementsAllow(t *testing.T) {
	reader := PermissionsForRoles([]string{"viewer"}, nil)
	require.True(t, EntitlementsAllow(reader, string(PermRead)))
	require.True(t, EntitlementsAllow(reader, " READ "))
	require.False(t, EntitlementsAllow(reader, string(PermWrite)))
	require.False(t, EntitlementsAllow(reader, string(PermAdmin)))

	admin := PermissionsForRoles([]string{"admin"}, nil)
	for _, permission := range []Permission{PermRead, PermWrite, PermSchema, PermAdmin, "user_manage"} {
		require.True(t, EntitlementsAllow(admin, string(permission)), permission)
	}
	require.False(t, EntitlementsAllow(nil, string(PermRead)))
}
