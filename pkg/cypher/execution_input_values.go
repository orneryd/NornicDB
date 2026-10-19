package cypher

import (
	"context"
	"strings"
)

// These resolvers are compiled syntax, not a second expression evaluator.
// They can only read literals and actual parameters, never execute an expression.
type executionInputValue struct {
	literal any
	bound   bool
	path    []string
	entries []executionInputEntry
	items   []executionInputValue
}

type executionInputEntry struct {
	key   string
	value executionInputValue
}

// A stack of overrides has the same lookup as mergeInheritedQueryParams without
// allocating a merged map for admission of every nested statement.
type executionBindings struct {
	scopes [16]map[string]any
	count int
}

func newExecutionBindings(ctx context.Context) executionBindings {
    var bindings executionBindings
    bindings.scopes[0], bindings.count = getParamsFromContext(ctx), 1
    return bindings
}

func (bindings executionBindings) lookup(name string) (any, bool) {
	for i := bindings.count-1; i >= 0; i-- {
		if value, found := bindings.scopes[i][name]; found {
			return value, true
		}
	}
	return nil, false
}

func compileExecutionInput(text string, depth int) (executionInputValue, bool) {
	if depth >= 16 {
		return executionInputValue{}, false
	}
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "$") {
		var direct string
		scanParameterReferences(text, func(dollar, start, end int) {
			if dollar == 0 && end == len(text) {
				direct = text[start:end]
			}
		})
		if direct != "" {
			return executionInputValue{bound: true, path: []string{direct}}, true
		}
		path, known := parseParamPathParts(text[1:])
		return executionInputValue{bound: true, path: path}, known && len(path) > 0
	}
	if isWholeCypherQuotedString(text) {
		value, known := decodeCypherQuotedString(text)
		return executionInputValue{literal: value}, known
	}
	if value, literal := parseLiteralValueFromComputedRow(text); literal {
		return executionInputValue{literal: value}, true
	}
	if strings.HasPrefix(text, "{") && findMatchingDelimiter(text, 0, '{', '}') == len(text)-1 {
		result := executionInputValue{entries: []executionInputEntry{}}
		if strings.TrimSpace(text[1:len(text)-1]) != "" {
			for _, entry := range splitTopLevelComma(text[1 : len(text)-1]) {
				colon := findTopLevelMapKeyValueSeparator(entry)
				if colon < 0 {
					return executionInputValue{}, false
				}
				key := strings.TrimSpace(entry[:colon])
				if isWholeCypherQuotedString(key) {
					key, _ = decodeCypherQuotedString(key)
				} else if len(key) >= 2 && key[0] == '`' && key[len(key)-1] == '`' {
					key = strings.ReplaceAll(key[1:len(key)-1], "``", "`")
				}
				value, known := compileExecutionInput(entry[colon+1:], depth+1)
				if !known {
					return executionInputValue{}, false
				}
				result.bound = result.bound || value.bound
				result.entries = append(result.entries, executionInputEntry{key, value})
			}
		}
		return foldExecutionInput(result), true
	}
	if strings.HasPrefix(text, "[") && findMatchingDelimiter(text, 0, '[', ']') == len(text)-1 {
		result := executionInputValue{items: []executionInputValue{}}
		if strings.TrimSpace(text[1:len(text)-1]) != "" {
			for _, item := range splitTopLevelComma(text[1 : len(text)-1]) {
				value, known := compileExecutionInput(item, depth+1)
				if !known {
					return executionInputValue{}, false
				}
				result.bound = result.bound || value.bound
				result.items = append(result.items, value)
			}
		}
		return foldExecutionInput(result), true
	}
	return executionInputValue{}, false
}

func foldExecutionInput(value executionInputValue) executionInputValue {
	if value.bound {
		return value
	}
	value.bound = true // materialize this literal container once, at compilation
	literal, _ := value.resolve(executionBindings{})
	return executionInputValue{literal: literal}
}

func (input *executionInputValue) resolve(bindings executionBindings) (any, bool) {
	if !input.bound {
		return input.literal, true
	}
	if len(input.path) > 0 {
		value, known := bindings.lookup(input.path[0])
		if !known {
			return nil, false
		}
		for _, part := range input.path[1:] {
			switch container := value.(type) {
			case map[string]any:
				value, known = container[part]
			case map[any]any:
				value, known = container[part]
			default:
				return nil, false
			}
			if !known {
				return nil, false
			}
		}
		return value, true
	}
	if input.entries != nil {
		value := make(map[string]any, len(input.entries))
		for _, entry := range input.entries {
			item, known := entry.value.resolve(bindings)
			if !known {
				return nil, false
			}
			value[entry.key] = item
		}
		return value, true
	}
	value := make([]any, len(input.items))
	for i, item := range input.items {
		resolved, known := item.resolve(bindings)
		if !known {
			return nil, false
		}
		value[i] = resolved
	}
	return value, true
}

func resolveExecutionInput(ctx context.Context, text string, depth int) (any, bool) {
	value, known := compileExecutionInput(text, depth)
	if !known {
		return nil, false
	}
	return value.resolve(newExecutionBindings(ctx))
}
