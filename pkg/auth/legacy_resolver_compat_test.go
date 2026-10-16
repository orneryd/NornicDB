package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyPrivilegeResolverSourceAndBehavior(t *testing.T) {
	store := NewPrivilegesStore(nil)
	store.matrix["viewer"] = map[string]DbPrivilege{"explicit": {Write: true}}
	var resolve func([]string, string) ResolvedAccess = store.Resolve
	require.Equal(t, ResolvedAccess{Write: true}, resolve([]string{"viewer"}, "explicit"), "legacy matrix grants remain unchanged")
	require.Equal(t, ResolvedAccess{Read: true}, resolve([]string{"viewer"}, "other"))
	require.Equal(t, ResolvedAccess{}, resolve([]string{"unknown"}, "other"))
	// The production resolver continues to apply the global entitlement gate.
	require.Equal(t, ResolvedAccess{}, ResolveDatabaseAccess([]string{"viewer"}, "explicit", store, nil))
}

func TestLegacyRequestResolverPreservesOtherEntitlements(t *testing.T) {
	var attach func(context.Context, func(string) ResolvedAccess) context.Context = WithRequestResolvedAccessResolver
	entitlements := RequestEntitlementResolver(func(database string, permission Permission) bool {
		return database == "db" && (permission == PermAdmin || permission == PermRead)
	})
	ctx := WithRequestEntitlementResolver(context.Background(), entitlements)
	ctx = attach(ctx, func(database string) ResolvedAccess { return ResolvedAccess{Write: database == "db"} })
	require.Equal(t, ResolvedAccess{Write: true}, RequestResolvedAccessResolverFromContext(ctx)("db"))
	allows := RequestEntitlementResolverFromContext(ctx)
	require.True(t, allows("db", PermAdmin))
	require.True(t, allows("db", PermRead))
	require.False(t, allows("db", PermSchema))
	require.False(t, allows("other", PermAdmin))
	cleared := attach(ctx, nil)
	require.Nil(t, RequestResolvedAccessResolverFromContext(cleared))
	require.True(t, RequestEntitlementResolverFromContext(cleared)("db", PermAdmin))
	// Without an explicit legacy adapter, the shared entitlement owner supplies data access.
	require.Equal(t, ResolvedAccess{Read: true}, RequestResolvedAccessResolverFromContext(WithRequestEntitlementResolver(context.Background(), entitlements))("db"))
}
