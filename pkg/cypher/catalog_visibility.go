package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// catalogDatabaseVisible checks selection visibility before catalog rows are
// projected or aggregated. It deliberately asks for access, not data read or
// administrator privileges: those remain the statement admission owner's job.
func catalogDatabaseVisible(ctx context.Context, database string) bool {
	if mode := auth.RequestDatabaseAccessModeFromContext(ctx); mode != nil && !mode.CanSeeDatabase(database) {
		return false
	}
	if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
		if _, allowed := scope.Resolve(database); !allowed {
			return false
		}
	}
	return authorizeDatabase(ctx, database, "access") == nil
}
