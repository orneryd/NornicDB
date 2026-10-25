package replication

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// admittedVectorCommand extends the existing local command envelope before its
// first queue or wait. A wire receiver admits against its own runtime schema;
// these private fields are deliberately absent from serialized commands.
func admittedVectorCommand(cmd *Command) *Command {
	if cmd == nil || cmd.vectorOriginBound {
		return cmd
	}
	bound := *cmd
	bound.vectorWorkOrigin, bound.vectorOriginBound = storage.CaptureVectorWorkOrigin(), true
	return &bound
}

// commandVectorContext transports the earlier of the context's and the
// command's admissions. With neither, the context carries no origin and the
// consumer reports the command as not admitted.
func commandVectorContext(ctx context.Context, cmd *Command) context.Context {
	origin, admitted := storage.VectorWorkOrigin(ctx)
	if cmd != nil && cmd.vectorOriginBound && (!admitted || cmd.vectorWorkOrigin < origin) {
		origin, admitted = cmd.vectorWorkOrigin, true
	}
	if !admitted {
		return ctx
	}
	return storage.WithVectorWorkOrigin(ctx, origin)
}
