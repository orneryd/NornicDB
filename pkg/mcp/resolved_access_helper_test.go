package mcp

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// withResolvedAccess attaches a principal whose read and write on each
// database are access(database), through the one permission resolver the
// server attaches (auth.WithRequestEntitlementResolver); it holds no other
// permission.
func withResolvedAccess(ctx context.Context, access func(database string) auth.ResolvedAccess) context.Context {
	return auth.WithRequestEntitlementResolver(ctx, func(database string, permission auth.Permission) bool {
		switch permission {
		case auth.Permission("access"):
			return true // selection is independently limited by the fixture scope
		case auth.PermRead:
			return access(database).Read
		case auth.PermWrite:
			return access(database).Write
		}
		return false
	})
}
