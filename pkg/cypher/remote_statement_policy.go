package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// readRemoteStatementPolicy is the remote adapter of the existing syntax
// preparation owner. The exact emitted raw remote fragment is compiled once in
// the same bounded cache used by local admission; no transport parses source.
func readRemoteStatementPolicy(ctx context.Context, source string) (fabric.RemoteStatementPolicy, error) {
	policy := fabric.RemoteStatementPolicy{AccessMode: storage.RemoteAccessWrite, Uncertain: true}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.RawSource)
	if canceled := ctx.Err(); canceled != nil {
		return policy, canceled
	}
	if err != nil {
		return policy, nil
	} // The remote grammar remains authoritative.
	effects := syntax.Effects()
	policy.Uncertain = effects.CallCount() > 0
	if effects.RemoteReadOnly() {
		policy.AccessMode = storage.RemoteAccessRead
	}
	return policy, nil
}
