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

// executionDeterminismPolicy is the one compiled semantic admission decision.
// QueryInfo owns the outer policy; parameter-supplied text uses the same compiler
// and a bounded text cache. No query syntax is parsed on a policy hit.
type executionDeterminismPolicy struct {
	text       string
	generation uint64
	cacheable  bool
	functions  []string // plugin loading can override a built-in after compilation
	temporal   []executionInputValue
	calls      []executionCallPolicy
}

type executionCallPolicy struct {
	name         string
	request      *executionInputValue
	continuation bool
	inputs       []executionTextPolicy
}

type executionTextPolicy struct {
	value      executionInputValue
	field      string
	parameters *executionInputValue
	child      *executionDeterminismPolicy
}

var executionPolicyCache = newBoundedCache[string, *executionDeterminismPolicy](4096, boundedCacheStringShardIndex)

func cachedExecutionPolicy(text string) *executionDeterminismPolicy {
	ensureBuiltInProceduresRegistered()
	generation := globalProcedureRegistry.generation.Load()
	if policy, found := executionPolicyCache.get(text); found && policy.generation == generation {
		return policy
	}
	policy := compileExecutionPolicy(text, 0)
	executionPolicyCache.put(text, policy)
	return policy
}

// compileExecutionPolicy performs the static work once: permissions, function
// classification, registry dependencies, argument positions and literal values.
// The binding phase below only reads the identified runtime inputs.
func compileExecutionPolicy(text string, depth int) *executionDeterminismPolicy {
	ensureBuiltInProceduresRegistered()
	policy := &executionDeterminismPolicy{text: text, generation: globalProcedureRegistry.generation.Load()}
	if depth >= 16 {
		return policy
	}
	text = classificationText(text)
	trimmed := strings.TrimSpace(text)
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	if startsWithKeywordFold(trimmed, "SHOW") || startsWithKeywordFold(trimmed, "TERMINATE") || startsWithKeywordFold(trimmed, "PROFILE") || keywordIndexFrom(text, "LOAD", 0, opts) >= 0 {
		return policy
	}
	requirements := QueryPermissionRequirements(text)
	if requirements.Write || requirements.Schema || requirements.Admin {
		return policy
	}
	if err := forEachFunctionInvocation(text, func(name, normalized string, open int) error {
		volatility, known := functionCacheDeclarations[normalized]
		if !known || volatility == functionVolatile {
			return errNonDeterministicExecution
		}
		policy.functions = append(policy.functions, name)
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
		args := splitProcedureTopLevelComma(body)
		if volatility == functionTemporalTruncation && len(args) < 2 {
			return errNonDeterministicExecution
		}
		if volatility == functionTemporalConstructor {
			for _, argument := range args {
				value, known := compileExecutionInput(argument, depth+1)
				if !known {
					return errNonDeterministicExecution
				}
				if value.bound {
					policy.temporal = append(policy.temporal, value)
				} else if _, text := value.literal.(string); !text {
					return errNonDeterministicExecution
				}
			}
		}
		return nil
	}); err != nil {
		return policy
	}
	for from := 0; from < len(text); {
		at := keywordIndexFrom(text, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, named := procedureNameAt(text, queryGapEnd(text, from))
		if !named {
			continue
		} // CALL { ... }: this pass also scans its body.
		procedure, known := globalProcedureRegistry.Get(name)
		if !known || procedure.User || procedure.Spec.Mode != ProcedureModeRead || !procedure.Spec.ResultCache.Deterministic {
			return policy
		}
		declaration := procedure.Spec.ResultCache
		if len(declaration.CypherInputs) == 0 {
			continue
		}
		arguments := explicitProcedureArgumentTexts(text[at:])
		call := executionCallPolicy{name: name, continuation: declaration.Continuation}
		var request map[string]any
		if declaration.RetrievalRequest {
			if len(arguments) != 1 {
				return policy
			}
			value, known := compileExecutionInput(arguments[0], depth+1)
			if !known {
				return policy
			}
			if value.bound {
				call.request = &value
			} else {
				var err error
				request, err = ragProcedureRequest(name, []any{value.literal})
				if err != nil || call.continuation && readRetrievalContinuation(request).requested() {
					return policy
				}
			}
		}
		for _, input := range declaration.CypherInputs {
			dependency := executionTextPolicy{}
			var known bool
			switch {
			case input.StoredText != "":
				dependency.value = executionInputValue{literal: input.StoredText}
			case input.Field != "":
				if call.request != nil {
					if field, fixedShape := call.request.mapField(input.Field); fixedShape {
						if !field.bound && field.literal == nil {
							continue
						}
						dependency.value = field
					} else {
						dependency.field = input.Field
					}
				} else {
					value := request[input.Field]
					if value == nil {
						continue
					}
					dependency.value = executionInputValue{literal: value}
				}
			default:
				if input.Argument >= len(arguments) {
					return policy
				}
				dependency.value, known = compileExecutionInput(arguments[input.Argument], depth+1)
				if !known {
					return policy
				}
			}
			if input.ParametersArgument > 0 {
				value := executionInputValue{literal: map[string]any{}}
				if position := input.ParametersArgument - 1; position < len(arguments) {
					value, known = compileExecutionInput(arguments[position], depth+1)
					if !known {
						return policy
					}
				}
				if !value.bound {
					if _, isMap := value.literal.(map[string]any); !isMap {
						return policy
					}
				}
				dependency.parameters = &value
			}
			if dependency.field == "" && !dependency.value.bound {
				inner, isText := dependency.value.literal.(string)
				if !isText {
					return policy
				}
				dependency.child = compileExecutionPolicy(inner, depth+1)
				if !dependency.child.cacheable {
					return policy
				}
				// Fold a fixed inner verdict, retaining the live plugin guards.
				if (dependency.parameters == nil || !dependency.parameters.bound) && len(dependency.child.calls) == 0 && len(dependency.child.temporal) == 0 {
					policy.functions = append(policy.functions, dependency.child.functions...)
					continue
				}
			}
			call.inputs = append(call.inputs, dependency)
		}
		if call.request != nil || len(call.inputs) > 0 {
			policy.calls = append(policy.calls, call)
		}
	}
	policy.cacheable = true
	return policy
}

func (policy *executionDeterminismPolicy) allows(bindings executionBindings, depth int) bool {
	if depth >= 16 {
		return false
	}
	if policy.generation != globalProcedureRegistry.generation.Load() {
		return cachedExecutionPolicy(policy.text).allows(bindings, depth)
	}
	if !policy.cacheable {
		return false
	}
	if PluginFunctionLookup != nil {
		for _, name := range policy.functions {
			if _, opaque := PluginFunctionLookup(name); opaque {
				return false
			}
		}
	}
	for _, input := range policy.temporal {
		value, known := input.resolve(bindings)
		if _, isText := value.(string); !known || !isText {
			return false
		}
	}
	for _, call := range policy.calls {
		var request map[string]any
		if call.request != nil {
			value, known := call.request.resolve(bindings)
			if !known {
				return false
			}
			var err error
			request, err = ragProcedureRequest(call.name, []any{value})
			if err != nil || call.continuation && readRetrievalContinuation(request).requested() {
				return false
			}
		}
		for _, input := range call.inputs {
			childBindings := bindings
			if input.parameters != nil {
				raw, known := input.parameters.resolve(bindings)
				params, isMap := raw.(map[string]any)
				if !known || !isMap {
					return false
				}
				// The same inheritance as executeInternal: the supplied map
				// overrides caller bindings. No merged map/context is needed.
				if childBindings.count >= len(childBindings.scopes) {
					return false
				}
				childBindings.scopes[childBindings.count] = params
				childBindings.count++
			}
			child := input.child
			if child == nil {
				var value any
				var known bool
				if input.field != "" {
					value, known = request[input.field], true
					if value == nil {
						continue
					}
				} else {
					value, known = input.value.resolve(bindings)
				}
				inner, isText := value.(string)
				if !known || !isText {
					return false
				}
				child = cachedExecutionPolicy(inner)
			}
			if !child.allows(childBindings, depth+1) {
				return false
			}
		}
	}
	return true
}

func deterministicExecution(ctx context.Context, text string, depth int) bool {
	return cachedExecutionPolicy(text).allows(newExecutionBindings(ctx), depth)
}
