package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// AuthorizeReadOnlyQuery admits a complete read-only query or script without
// granting execution permissions. It accepts registered READ procedures whose
// declared effects are fixed and nonadministrative; the caller must still pass
// its original context to the normal execution and authorization boundary.
// For example, a read-only diagnostic tool can call this before Execute.
func AuthorizeReadOnlyQuery(ctx context.Context, query string) error {
	syntax, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.RawSource)
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if err != nil {
		return syntaxAdmissionError(ctx, query, RawSource, err)
	}
	ensureBuiltInProceduresRegistered()
	allowed := syntax.Effects().ReadOnly(func(call cypherantlr.StatementCall) bool {
		procedure, known := globalProcedureRegistry.Get(call.Name())
		if !known {
			return false
		}
		spec := procedure.Spec
		return spec.Mode == ProcedureModeRead && !spec.Admin && !spec.ExecutesStatements
	})
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if allowed {
		allowed = syntax.Effects().AllowsExecutableQueries(func(source string) bool {
			requirements := QueryPermissionRequirements(ctx, source, RawSource)
			return requirements.ReadOnly()
		})
	}
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if !allowed {
		return &PermissionDeniedError{Permission: permissionReadOnlyConstituent}
	}
	return nil
}
