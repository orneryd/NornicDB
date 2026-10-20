package cypher

import "context"

// lowerLocalStatement is the backend boundary shared by ordinary and fragment
// execution. Map and label helpers are introduced after graph routing.
// Public clause normalization and the existing interpolation extension remain
// before graph planning; interpolation has no native grammar entry.
func (e *StorageExecutor) lowerLocalStatement(ctx context.Context, query string, params map[string]interface{}) (string, []*queryRewrite, bool, error) {
	cypher25 := cypherVersionFromContext(ctx) == "25"
	var rewrites []*queryRewrite
	if !rewriteFree(rewriteFreeCypher25Expressions, cypher25, query) {
		original := query
		var err error
		query, rewrites, err = rewriteCypher25Expressions(query, cypher25, expressionLocalExecution)
		if err != nil {
			return query, nil, false, err
		}
		if len(rewrites) == 0 {
			markRewriteFree(rewriteFreeCypher25Expressions, cypher25, original)
		}
	}
	lowered, labels, err := desugarLabelExpressions(query, params, cypher25)
	if err != nil {
		return query, nil, false, err
	}
	if labels != nil {
		rewrites = append(rewrites, labels)
	}
	return lowered, rewrites, labels != nil, nil
}

func restoreLocalStatement(result *ExecuteResult, err error, rewrites []*queryRewrite) (*ExecuteResult, error) {
	for index := len(rewrites) - 1; index >= 0; index-- {
		result, err = rewrites[index].restore(withoutGeneratedColumns(result), err)
	}
	return result, err
}
