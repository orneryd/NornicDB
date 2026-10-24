package cypher

import (
	"context"
	"strings"
)

// patternSyntaxReader owns the shared node/relationship grammar. Property
// evaluation is supplied by its caller: admission uses structure only, while
// runtime readers capture an explicitly admitted statement. The grammar never
// chooses an execution program or recovers one from context.
type patternSyntaxReader struct {
	parsePropertyMap func(context.Context, string, bool) (map[string]interface{}, error)
}

func (reader patternSyntaxReader) parseProperties(ctx context.Context, props string) (map[string]interface{}, error) {
	return reader.parsePropertyMap(ctx, props, false)
}

func relationshipSyntaxReader() patternSyntaxReader {
	return patternSyntaxReader{parsePropertyMap: relationshipSyntaxProperties}
}

// Source admission strips property operands before structural parsing, and
// only retains a static traversal when the original pattern has no operands.
// Reject a violation of that pure boundary without evaluating its expression.
func relationshipSyntaxProperties(_ context.Context, props string, _ bool) (map[string]interface{}, error) {
	if strings.TrimSpace(props) != "" {
		return nil, unresolvedExpressionError(props)
	}
	return make(map[string]interface{}), nil
}

func (e *StorageExecutor) admittedPatternReader(statement admittedRelationshipStatement) patternSyntaxReader {
	return patternSyntaxReader{parsePropertyMap: func(ctx context.Context, props string, retainNull bool) (map[string]interface{}, error) {
		return e.parsePropertyMap(statement, ctx, props, retainNull)
	}}
}

func (e *StorageExecutor) parseNodePattern(statement admittedRelationshipStatement, ctx context.Context, pattern string) (nodePatternInfo, error) {
	return e.admittedPatternReader(statement).parseNodePattern(ctx, pattern)
}

func (e *StorageExecutor) parseNodePatternFromString(statement admittedRelationshipStatement, ctx context.Context, pattern string) (nodePatternInfo, error) {
	return e.admittedPatternReader(statement).parseNodePatternFromString(ctx, pattern)
}

func (e *StorageExecutor) parseRelationshipPattern(statement admittedRelationshipStatement, ctx context.Context, pattern string) (*RelationshipPattern, error) {
	return e.admittedPatternReader(statement).parseRelationshipPattern(ctx, pattern)
}

func (e *StorageExecutor) parseTraversalPattern(statement admittedRelationshipStatement, ctx context.Context, pattern string) *TraversalMatch {
	// Only the direct, unshadowed row frame can reuse runtime values. A
	// descendant context may change bindings, parameters or the read view.
	// The explicit typed statement remains the authority for this reader.
	if row, ok := ctx.(*relationshipRowContext); ok && row.executor == e && row.statement.program == statement.program && row.identity.key.pattern == pattern {
		if row.traversal == nil {
			row.traversal = e.admittedPatternReader(statement).parseTraversalPattern(ctx, pattern)
		}
		return row.traversal
	}
	return e.admittedPatternReader(statement).parseTraversalPattern(ctx, pattern)
}

func (e *StorageExecutor) parseChainedTraversalPattern(statement admittedRelationshipStatement, ctx context.Context, pattern string) *TraversalMatch {
	return e.admittedPatternReader(statement).parseChainedTraversalPattern(ctx, pattern)
}

func (e *StorageExecutor) parseTraversalPatternStateMachine(statement admittedRelationshipStatement, ctx context.Context, pattern string) *TraversalMatch {
	return e.admittedPatternReader(statement).parseTraversalPatternStateMachine(ctx, pattern)
}

func (e *StorageExecutor) isChainedPattern(pattern string) bool {
	return relationshipSyntaxReader().isChainedPattern(pattern)
}
