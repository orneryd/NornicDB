package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// catalogDatabaseVisible checks selection visibility before catalog rows are
// projected or aggregated. It deliberately asks for access, not data read or
// administrator privileges: those remain the statement admission owner's job.
func catalogDatabaseVisible(ctx context.Context, database string) bool {
	mode := auth.RequestDatabaseAccessModeFromContext(ctx)
	if mode != nil && !mode.CanSeeDatabase(database) {
		return false
	}
	if scope := auth.RequestDatabaseScopeFromContext(ctx); scope != nil {
		if _, allowed := scope.Resolve(database); !allowed {
			return false
		}
	}
	// An explicit metadata visibility grant is not a data-access grant.
	// Without that mode, the existing selection resolver owns visibility.
	return mode != nil || authorizeDatabase(ctx, database, "access") == nil
}
