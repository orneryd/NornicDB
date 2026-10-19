package auth

import (
	"context"
	"testing"
)

func TestRequestDatabasePermissionUsesExistingResolvers(t *testing.T) {
	for _, test := range []struct {
		name string
		context context.Context
		database string
		permission Permission
		allowed bool
	}{
		{"nil request", nil, "nornic", PermRead, false},
		{"embedded no auth", context.Background(), "nornic", PermRead, true},
		{"auth disabled", WithRequestAuthenticationDisabled(context.Background()), "named", PermRead, true},
		{"selection denied", WithRequestDatabaseScope(context.Background(), NewRequestDatabaseScope("nornic", map[string]string{"nornic":"nornic"})), "named", PermRead, false},
		{"mode denied", WithRequestDatabaseAccessMode(context.Background(), DenyAllDatabaseAccessMode), "nornic", PermRead, false},
		{"legacy write only read", WithRequestResolvedAccessResolver(context.Background(), func(string) ResolvedAccess { return ResolvedAccess{Write:true} }), "nornic", PermRead, false},
		{"legacy write only write", WithRequestResolvedAccessResolver(context.Background(), func(string) ResolvedAccess { return ResolvedAccess{Write:true} }), "nornic", PermWrite, true},
		{"complete read", WithRequestEntitlementResolver(context.Background(), func(database string, permission Permission) bool {return database=="named" && permission==PermRead}), "named", PermRead, true},
		{"complete write refused", WithRequestEntitlementResolver(context.Background(), func(string, Permission) bool {return false}), "named", PermWrite, false},
		{"complete admin refused", WithRequestEntitlementResolver(context.Background(), func(string, Permission) bool {return false}), "named", PermAdmin, false},
	} {
		t.Run(test.name,func(t *testing.T){
			if allowed := RequestDatabasePermissionAllowed(test.context,test.database,test.permission); allowed!=test.allowed {t.Fatalf("allowed=%v, want %v",allowed,test.allowed)}
		})
	}
}
