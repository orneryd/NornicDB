package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// statementNodeImagePolicy belongs to one admitted execution, not an executor
// or database-global flag. It is fixed before any rows or parallel fragments run.
type statementNodeImagePolicy struct {
	images   ProcedureNodeImages
	readOnly bool
	hasMatch bool
	resolved bool
}

func (e *StorageExecutor) resolveStatementNodeImages(ctx context.Context, info *QueryInfo, source string) error {
	statement, _ := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext)
	if statement == nil {
		return nil
	}
	policy := statementNodeImagePolicy{images: FullNodeImages, resolved: true, hasMatch: info.HasMatch}
	if effect := info.effect.Load(); effect != nil {
		policy.images, policy.readOnly = effect.nodeImages, effect.effect == statementEffectRead && info.readResult()
		if policy.images == MetadataNodeImages {
			var inputs nativeFunctionInputs
			loaded := false
			for index := 0; index < effect.functions.FunctionCount(); index++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				call := effect.functions.Function(index)
				name := call.ChildRule(cypherantlr.CypherParserRULE_invocationName).QualifiedName()
				handler, found := pluginFunctionHandler(name)
				if !found || !nativePluginFunction(handler) {
					continue
				}
				if !loaded {
					key := semanticCacheKey(source, cypherVersionFromContext(ctx) == "25")
					var known bool
					inputs, known = e.matchSemanticValidationCache.get(key)
					if !known || inputs.generation != globalProcedureRegistry.generation.Load() {
						// The outer semantic verdict may have shared a literal
						// canonical form. Argument identity is scoped to this
						// exact source, so obtain its canonical type walk.
						if err := e.validateMatchSemanticScopes(ctx, source, cypherVersionFromContext(ctx) == "25"); err != nil {
							return err
						}
						inputs, _ = e.matchSemanticValidationCache.get(key)
					}
					loaded = true
				}
				reaches, proven := inputs.sites[nativeInputSiteFor(call)]
				if !proven || reaches || inputs.uncertain || inputs.generation != globalProcedureRegistry.generation.Load() {
					policy.images = FullNodeImages
					break
				}
			}
		}
	}
	// Transparent routing and nested clauses must retain the full-image
	// requirement of a later native consumer in the enclosing statement.
	if outer, ok := statement.Context.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok &&
		outer.outcome == statement.outcome && outer.nodeImages.resolved && outer.nodeImages.images == FullNodeImages {
		policy.images = FullNodeImages
	}
	statement.nodeImages = policy
	return nil
}
