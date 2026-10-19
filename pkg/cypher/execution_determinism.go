package cypher

import (
	"context"
	"errors"
	"strings"
)

var errNonDeterministicExecution = errors.New("execution is not deterministic")

var functionCacheDeclarations = func() map[string]functionCacheVolatility {
	result := make(map[string]functionCacheVolatility, len(cypherFunctionCatalog))
	for _, spec := range cypherFunctionCatalog {
		name := lowerASCII(spec.name)
		if previous, exists := result[name]; !exists || spec.cacheVolatility > previous {
			result[name] = spec.cacheVolatility
		}
	}
	return result
}()

// deterministicExecution is the sole semantic admission owner for local and
// Fabric result caches. It classifies the outer text and declared executable
// inputs recursively, with the same function/procedure rules and actual values.
// It never executes an expression to discover eligibility: unresolved row or
// computed inputs remain unknown and must execute outside result caching.
func deterministicExecution(ctx context.Context, text string, depth int) bool {
	if depth >= 16 {
		return false
	}
	text = classificationText(text)
	trimmed := strings.TrimSpace(text)
	if startsWithKeywordFold(trimmed, "SHOW") || startsWithKeywordFold(trimmed, "TERMINATE") || startsWithKeywordFold(trimmed, "PROFILE") || queryKeywords(text)["LOAD"] {
		return false
	}
	requirements := QueryPermissionRequirements(text)
	if requirements.Write || requirements.Schema || requirements.Admin {
		return false
	}
	if err := forEachFunctionInvocation(text, func(name, normalized string, open int) error {
		if PluginFunctionLookup != nil {
			if _, opaque := PluginFunctionLookup(name); opaque {
				return errNonDeterministicExecution
			}
		}
		volatility, known := functionCacheDeclarations[normalized]
		if !known || volatility == functionVolatile {
			return errNonDeterministicExecution
		}
		if volatility == functionStable {
			return nil
		}
		close := findMatchingCallParen(text, open)
		if close < 0 {
			return errNonDeterministicExecution
		}
		body := strings.TrimSpace(text[open+1 : close])
		if body == "" {
			return errNonDeterministicExecution
		}
		if volatility == functionTemporalTruncation && len(splitProcedureTopLevelComma(body)) < 2 {
			return errNonDeterministicExecution
		}
		if volatility == functionTemporalConstructor {
			// A temporal map may request the current instant (timezone only,
			// for example). Only resolved textual construction is proven pure.
			args := splitProcedureTopLevelComma(body)
			for _, argument := range args {
				value, resolved := resolveExecutionInput(ctx, argument, depth+1)
				if _, isText := value.(string); !resolved || !isText {
					return errNonDeterministicExecution
				}
			}
		}
		return nil
	}); err != nil {
		return false
	}

	ensureBuiltInProceduresRegistered()
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	for from := 0; from < len(text); {
		at := keywordIndexFrom(text, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, named := procedureNameAt(text, queryGapEnd(text, from))
		if !named {
			continue
		} // CALL { ... }: scan the body in this pass.
		procedure, known := globalProcedureRegistry.Get(name)
		if !known || procedure.User || procedure.Spec.Mode != ProcedureModeRead || !procedure.Spec.ResultCache.Deterministic {
			return false
		}
		declaration := procedure.Spec.ResultCache
		if len(declaration.CypherInputs) == 0 {
			continue
		}
		arguments := explicitProcedureArgumentTexts(text[at:])
		var request map[string]any
		if declaration.RetrievalRequest {
			if len(arguments) != 1 {
				return false
			}
			value, resolved := resolveExecutionInput(ctx, arguments[0], depth+1)
			if !resolved {
				return false
			}
			var err error
			request, err = ragProcedureRequest(name, []any{value})
			if err != nil || declaration.Continuation && readRetrievalContinuation(request).requested() {
				return false
			}
		}
		for _, input := range declaration.CypherInputs {
			innerContext := ctx
			var value any
			var resolved bool
			if input.StoredText != "" {
				value, resolved = input.StoredText, true
			} else if input.Field != "" {
				value, resolved = request[input.Field], true
				if value == nil {
					continue
				}
			} else {
				if input.Argument >= len(arguments) {
					return false
				}
				value, resolved = resolveExecutionInput(ctx, arguments[input.Argument], depth+1)
				if input.ParametersArgument > 0 {
					params := map[string]any{}
					if position := input.ParametersArgument - 1; position < len(arguments) {
						raw, known := resolveExecutionInput(ctx, arguments[position], depth+1)
						var isMap bool
						params, isMap = raw.(map[string]any)
						if !known || !isMap {
							return false
						}
					}
					innerContext = withQueryParams(ctx, params)
				}
			}
			inner, isText := value.(string)
			if !resolved || !isText || !deterministicExecution(innerContext, inner, depth+1) {
				return false
			}
		}
	}
	return true
}

// resolveExecutionInput reads only values already available at cache admission:
// literals, typed parameters/properties and maps/lists made entirely from those.
// Ordinary data strings are never interpreted unless the registry declares it.
func resolveExecutionInput(ctx context.Context, text string, depth int) (any, bool) {
	if depth >= 16 {
		return nil, false
	}
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "$") {
		params := getParamsFromContext(ctx)
		var value any
		resolved := false
		scanParameterReferences(text, func(dollar, start, end int) {
			if dollar == 0 && end == len(text) {
				value, resolved = params[text[start:end]]
			}
		})
		if resolved {
			return value, true
		}
		return resolveSetMergeSourceFromParams(params, text[1:])
	}
	if isWholeCypherQuotedString(text) {
		return decodeCypherQuotedString(text)
	}
	if value, literal := parseLiteralValueFromComputedRow(text); literal {
		return value, true
	}
	if strings.HasPrefix(text, "{") && findMatchingDelimiter(text, 0, '{', '}') == len(text)-1 {
		result := map[string]any{}
		if strings.TrimSpace(text[1:len(text)-1]) == "" {
			return result, true
		}
		for _, entry := range splitTopLevelComma(text[1 : len(text)-1]) {
			colon := findTopLevelMapKeyValueSeparator(entry)
			if colon < 0 {
				return nil, false
			}
			key := strings.TrimSpace(entry[:colon])
			if isWholeCypherQuotedString(key) {
				key, _ = decodeCypherQuotedString(key)
			} else if len(key) >= 2 && key[0] == '`' && key[len(key)-1] == '`' {
				key = strings.ReplaceAll(key[1:len(key)-1], "``", "`")
			}
			value, known := resolveExecutionInput(ctx, entry[colon+1:], depth+1)
			if !known {
				return nil, false
			}
			result[key] = value
		}
		return result, true
	}
	if strings.HasPrefix(text, "[") && findMatchingDelimiter(text, 0, '[', ']') == len(text)-1 {
		if strings.TrimSpace(text[1:len(text)-1]) == "" {
			return []any{}, true
		}
		result := []any{}
		for _, item := range splitTopLevelComma(text[1 : len(text)-1]) {
			value, known := resolveExecutionInput(ctx, item, depth+1)
			if !known {
				return nil, false
			}
			result = append(result, value)
		}
		return result, true
	}
	return nil, false
}
