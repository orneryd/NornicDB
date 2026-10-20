package cypher

import "context"

// patternPropertyReader chooses the phase of the shared pattern reader.
// Execution supplies parsePropertyMap; inspection reads only property keys.
type patternPropertyReader func(context.Context, string, bool) map[string]interface{}

// patternPropertyKeys uses the existing property-map grammar without invoking
// value producers. The nil placeholders preserve property presence for access
// planning and validation; they must never be used to match stored values.
func (e *StorageExecutor) patternPropertyKeys(ctx context.Context, text string, _ bool) map[string]interface{} {
	return e.readPropertyMap(ctx, text, true, func(context.Context, string) interface{} { return nil })
}
