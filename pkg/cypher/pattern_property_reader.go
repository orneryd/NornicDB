package cypher

import "context"

// patternPropertyReader chooses the phase of the shared pattern reader.
// Execution supplies parsePropertyMap; inspection reads only property keys.
type patternPropertyReader func(context.Context, string, bool) (map[string]interface{}, error)

// patternPropertyKeys preserves property presence for access planning without
// evaluating any expression. Its nil placeholders are syntax, never values
// that may be used to match stored properties.
func patternPropertyKeys(_ context.Context, text string, _ bool) (map[string]interface{}, error) {
	expressions, err := parsePropertyExpressions(text)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]interface{}, len(expressions))
	for _, expression := range expressions {
		keys[expression.key] = nil
	}
	return keys, nil
}
