package cypher

import (
	"context"
	"crypto/rand"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullFunctions(ctx context.Context, view braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	expr := view.text()
	ctx = withBraceView(ctx, view)
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// ========================================
	// Parenthesized Expressions - strip outer parens
	// ========================================
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		// Check if these parentheses wrap the entire expression
		depth := 0
		allWrapped := true
		for i, ch := range expr {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
			// If depth reaches 0 before the last character, parens don't wrap the whole thing
			if depth == 0 && i < len(expr)-1 {
				allWrapped = false
				break
			}
		}
		if allWrapped && depth == 0 {
			// Strip outer parentheses and re-evaluate
			return e.evaluateExpressionWithContextFull(ctx, view.window(1, len(expr)-1), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
	}

	// ========================================
	// Map Literals - handle { key: value, ... } patterns
	// ========================================
	if startsBrace(expr) && endsBrace(expr) {
		return e.evaluateMapLiteralFull(ctx, view, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// ========================================
	// CASE Expressions (must be checked first)
	// ========================================
	if isCaseExpression(view) {
		// The path bindings reach the CASE's WHEN / THEN / ELSE expressions
		// (CASE WHEN … THEN length(p) … END in a path WHERE, #699).
		return e.evaluateCaseExpression(ctx, view, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	lowerExpr := lowerASCII(expr)

	// ========================================
	// Plugin Functions (loaded dynamically from .so files)
	// ========================================
	// A plugin function overrides a built-in of the same name, so plugins
	// are asked first, as the row evaluator does (#1163).
	if call := inspectFunctionCall(expr, qualifiedFunctionCall, "", nil); call && strings.Contains(expr, ".") {
		if result, handled := e.tryCallPluginFunction(ctx, view, nodes, rels, paths, allPathEdges, allPathNodes, pathLength); handled {
			return result
		}
	}

	// ========================================
	// Registered Function Dispatch (Phase B: pkg/cypher/fn)
	// ========================================
	var callFacts1 functionCallFacts
	if ok := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1); ok {
		name := callFacts1.name
		args := view.functionArguments().trim().commaViews()
		fnCtx := e.registryFunctionContext(ctx, args, func(argument braceView) (interface{}, error) {
			value, err := e.checkedPropertyExpression(withBraceView(ctx, argument), argument, e.entityRowWithPaths(ctx, nodes, rels, paths))
			if (strings.EqualFold(name, "properties") || strings.EqualFold(name, "keys")) && e.queryPropertyDecayEnabled() {
				value = e.queryVisibleValue(value)
			}
			return value, err
		})
		fnCtx.Nodes = nodes
		fnCtx.Rels = rels

		if (strings.EqualFold(name, "properties") || strings.EqualFold(name, "keys")) && e.queryPropertyDecayEnabled() {
			fnCtx.Nodes = make(map[string]*storage.Node, len(nodes))
			for key, node := range nodes {
				fnCtx.Nodes[key] = e.queryVisibleValue(node).(*storage.Node)
			}
			fnCtx.Rels = make(map[string]*storage.Edge, len(rels))
			for key, rel := range rels {
				fnCtx.Rels[key] = e.queryVisibleValue(rel).(*storage.Edge)
			}
		}
		if v, found, err := cypherfn.EvaluateInvocation(name, functionArgumentTexts(args), fnCtx); found {
			if err != nil {
				functionEvaluationFailure(ctx, err)
				return nil
			}
			return v
		}
	}

	// ========================================
	// Cypher Functions (Neo4j compatible)
	// ========================================
	// Core scalar functions like id(), labels(), type(), keys(), properties(),
	// size(), coalesce(), toLower(), and toUpper() are dispatched above via
	// pkg/cypher/fn. Keep only evaluator-specific behavior here to avoid
	// duplicate implementations drifting apart.

	// Note: count(), sum(), avg(), etc. are aggregation functions and should NOT be
	// evaluated here. They must be handled by executeAggregation() in match.go or
	// executeMatchWithRelationshipsWithPathSeeded() in traversal.go. If we reach here with count(),
	// it means the query wasn't properly detected as an aggregation query - that's a bug
	// in the query router, not something we should handle here.

	// length(path) - same as size for compatibility, with special handling for paths
	if matchFuncStartAndSuffix(expr, "length") {
		inner := view.functionArguments().trim()

		// Check if this is a path variable - use the path length from context
		if paths != nil {
			if pathResult, ok := paths[inner.text()]; ok && pathResult != nil {
				return int64(pathResult.Length)
			}
		}
		// Also check if inner is a path variable and we have allPathEdges (variable-length patterns)
		// For MATCH path = (a)-[*1..2]-(b), the path variable gives us the length
		if pathLength > 0 {
			// If pathLength is set and inner looks like a simple variable (no dots, no parens)
			if !strings.Contains(inner.text(), ".") && !strings.Contains(inner.text(), "(") && !strings.Contains(inner.text(), "[") {
				// This might be a path variable reference, use stored pathLength
				return int64(pathLength)
			}
		}

		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if innerVal == nil {
			// length(null) is null in Neo4j; a missing/unbound argument must
			// not report a fabricated zero-length value (#581).
			return nil
		}
		if remote, ok := innerVal.(storage.RemotePathValue); ok {
			value, err := remotePathFunctionValue("length", remote)
			recordExpressionFailure(ctx, err)
			return value
		}
		if list, ok := valueconvert.ReadList(innerVal); ok {
			return int64(list.Len())
		}
		switch v := innerVal.(type) {
		case string:
			return int64(cyphertext.Length(v))
		case map[string]interface{}:
			// A path carried as a computed-row map (WITH p AS path): read the
			// recorded length, then the node list as a fallback.
			if length, ok := v["length"]; ok {
				if distance, valid := valueconvert.Int64(length, valueconvert.IntegerFloat); valid {
					return distance
				}
			}
			if nodesList, _, hasNodes, _ := pathValueParts(v); hasNodes {
				return int64(len(nodesList))
			}
		}
		return int64(0)
	}

	// exists(n.prop) - check if property exists; exists(pattern) - whether
	// the pattern matches, as EXISTS { pattern } (#907)
	if matchFuncStartAndSuffix(expr, "exists") {
		inner := view.functionArguments().trim()
		if trimmed := inner.trim().text(); strings.HasPrefix(trimmed, "(") {
			if chainEnd, chain := relationshipChainEnd(trimmed, 0, len(trimmed)); chain && chainEnd == len(trimmed) {
				value, _ := e.evaluateRowSubqueryValue(ctx, "EXISTS", inner.trim(), entityRow(ctx, nodes, rels))
				return value
			}
		}
		// Check for property access
		if dotIdx := strings.Index(inner.text(), "."); dotIdx > 0 {
			varName := inner.text()[:dotIdx]
			propName := inner.text()[dotIdx+1:]
			if node, ok := nodes[varName]; ok {
				_, exists := e.queryEntityProperties(node)[propName]
				return exists
			}
		}
		return false
	}

	// head(list) - return first element
	if matchFuncStartAndSuffix(expr, "head") {
		inner := view.functionArguments().trim()
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		result, err := evaluateListAccessFunction("head", innerVal)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return result
	}

	// last(list) - return last element
	if matchFuncStartAndSuffix(expr, "last") {
		inner := view.functionArguments().trim()
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		result, err := evaluateListAccessFunction("last", innerVal)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return result
	}

	// tail(list) - return list without first element
	if matchFuncStartAndSuffix(expr, "tail") {
		inner := view.functionArguments().trim()
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if list, ok := cypherListValue(innerVal); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}

	// reverse(list) - return reversed list
	if matchFuncStartAndSuffix(expr, "reverse") {
		inner := view.functionArguments().trim()
		innerVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		result, err := evaluateListAccessFunction("reverse", innerVal)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return result
	}

	// range(start, end) or range(start, end, step)
	if matchFuncStartAndSuffix(expr, "range") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		arguments := make([]interface{}, len(args))
		for index, argument := range args {
			arguments[index] = e.evaluateExpressionWithContextFull(ctx, argument.trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		result, err := evaluateCypherRange(arguments)
		if err != nil {
			return nil
		}
		return result
	}

	// slice(list, start, end) - get sublist from start to end (exclusive)
	if matchFuncStartAndSuffix(expr, "slice") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			startIdx, _ := strconv.ParseInt(args[1].text(), 10, 64)
			if list, ok := cypherListValue(listVal); ok {
				endIdx := int64(len(list))
				if len(args) >= 3 {
					endIdx, _ = strconv.ParseInt(args[2].text(), 10, 64)
				}
				if startIdx < 0 {
					startIdx = int64(len(list)) + startIdx
				}
				if endIdx < 0 {
					endIdx = int64(len(list)) + endIdx
				}
				if startIdx < 0 {
					startIdx = 0
				}
				if endIdx > int64(len(list)) {
					endIdx = int64(len(list))
				}
				if startIdx >= endIdx {
					return []interface{}{}
				}
				return list[startIdx:endIdx]
			}
		}
		return []interface{}{}
	}

	// indexOf(list, value) - get index of value in list, -1 if not found
	if matchFuncStartAndSuffix(expr, "indexof") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) == 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			searchVal := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if list, ok := cypherListValue(listVal); ok {
				for i, item := range list {
					if e.compareEqual(item, searchVal) {
						return int64(i)
					}
				}
			}
		}
		return int64(-1)
	}

	// degree(node) - total degree (in + out)
	if matchFuncStartAndSuffix(expr, "degree") {
		inner := view.functionArguments().trim()
		if node, ok := nodes[inner.text()]; ok {
			inDegree := e.storage.GetInDegree(node.ID)
			outDegree := e.storage.GetOutDegree(node.ID)
			return int64(inDegree + outDegree)
		}
		return int64(0)
	}

	// inDegree(node) - incoming edges count
	if matchFuncStartAndSuffix(expr, "indegree") {
		inner := view.functionArguments().trim()
		if node, ok := nodes[inner.text()]; ok {
			return int64(e.storage.GetInDegree(node.ID))
		}
		return int64(0)
	}

	// outDegree(node) - outgoing edges count
	if matchFuncStartAndSuffix(expr, "outdegree") {
		inner := view.functionArguments().trim()
		if node, ok := nodes[inner.text()]; ok {
			return int64(e.storage.GetOutDegree(node.ID))
		}
		return int64(0)
	}

	// hasLabels(node, labels) - check if node has all specified labels
	if matchFuncStartAndSuffix(expr, "haslabels") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			if node, ok := nodes[args[0].text()]; ok {
				labelsVal := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
				if labels, ok := cypherListValue(labelsVal); ok {
					for _, reqLabel := range labels {
						labelStr, _ := reqLabel.(string)
						found := false
						for _, nodeLabel := range node.Labels {
							if nodeLabel == labelStr {
								found = true
								break
							}
						}
						if !found {
							return false
						}
					}
					return true
				}
			}
		}
		return false
	}

	// ========================================
	// APOC Map Functions
	// ========================================

	// APOC map functions use the registered value/null/error owner.

	// ========================================
	// String Functions
	// ========================================

	// ========================================
	// List Conversion Functions
	// ========================================

	// ========================================
	// Additional Utility Functions
	// ========================================

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// sum(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "sum") {
		inner := view.functionArguments().trim()
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// avg(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "avg") {
		inner := view.functionArguments().trim()
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// min(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "min") {
		inner := view.functionArguments().trim()
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// max(expr) - in single row context, just returns the value
	if matchFuncStartAndSuffix(expr, "max") {
		inner := view.functionArguments().trim()
		return e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}

	// collect(expr) - in single row context, returns single-element list
	if matchFuncStartAndSuffix(expr, "collect") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if val == nil {
			return []interface{}{}
		}
		return []interface{}{val}
	}

	// replace(string, search, replacement)
	if matchFuncStartAndSuffix(expr, "replace") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 3 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			search := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			repl := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return strings.ReplaceAll(str, search, repl)
		}
		return nil
	}

	// split(string, delimiter)
	if matchFuncStartAndSuffix(expr, "split") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			delim := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			parts := strings.Split(str, delim)
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result
		}
		return nil
	}

	// lpad(string, length, padString) - left-pad string to specified length
	if matchFuncStartAndSuffix(expr, "lpad") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			length, err := strconv.Atoi(args[1].text())
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
				// Remove quotes if present
				padStr = strings.Trim(padStr, "'\"")
			}
			if len(str) >= length {
				return str[:length]
			}
			// Pad to the left
			padLen := length - len(str)
			padding := ""
			for len(padding) < padLen {
				padding += padStr
			}
			return padding[:padLen] + str
		}
		return nil
	}

	// rpad(string, length, padString) - right-pad string to specified length
	if matchFuncStartAndSuffix(expr, "rpad") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			length, err := strconv.Atoi(args[1].text())
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContextFull(ctx, args[2].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
				// Remove quotes if present
				padStr = strings.Trim(padStr, "'\"")
			}
			if len(str) >= length {
				return str[:length]
			}
			// Pad to the right
			padLen := length - len(str)
			padding := ""
			for len(padding) < padLen {
				padding += padStr
			}
			return str + padding[:padLen]
		}
		return nil
	}

	// ========================================
	// Date/Time Functions (Neo4j compatible)
	// ========================================
	if value, handled, callErr := e.evaluateTemporalConstructor(ctx, func(argument braceView) (interface{}, bool, error) {
		value := e.evaluateExpressionWithContextFull(withBraceView(ctx, argument), argument, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		err := getExpressionFailure(ctx)
		return value, err == nil, err
	}, view); handled {
		recordExpressionFailure(ctx, callErr)
		return value
	}

	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return time.Now().UnixMilli()
	}

	// date.<component>(value) and datetime.<component>(value): read as the
	// row evaluator reads them (temporalComponent), so both evaluators give
	// the same value for every accessor (#1105).
	var callFacts2 functionCallFacts
	if ok := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts2); ok {
		name := callFacts2.name

		if kind, component, dotted := strings.Cut(lowerASCII(name), "."); dotted && (kind == "date" || kind == "datetime") && isTemporalComponent(component) {
			value, _ := temporalComponent(component, e.evaluateExpressionWithContextFull(ctx, view.functionArguments(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength))
			return value
		}
	}

	// date.truncate(unit, date) - truncate date to specified unit
	if matchFuncStartAndSuffix(expr, "date.truncate") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			unit := strings.Trim(args[0].text(), "'\"")
			val := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				if t, err := time.Parse("2006-01-02", str); err == nil {
					switch strings.ToLower(unit) {
					case "year":
						return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "quarter":
						q := (int(t.Month())-1)/3*3 + 1
						return time.Date(t.Year(), time.Month(q), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "month":
						return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format("2006-01-02")
					case "week":
						// Go back to Monday of current week
						offset := int(t.Weekday())
						if offset == 0 {
							offset = 7
						}
						return t.AddDate(0, 0, -(offset - 1)).Format("2006-01-02")
					case "day":
						return t.Format("2006-01-02")
					}
				}
			}
		}
		return nil
	}

	// datetime.truncate(unit, datetime) - truncate datetime to specified unit
	if matchFuncStartAndSuffix(expr, "datetime.truncate") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			unit := strings.Trim(args[0].text(), "'\"")
			val := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				t := parseDateTime(str)
				if !t.IsZero() {
					switch strings.ToLower(unit) {
					case "year":
						return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "quarter":
						q := (int(t.Month())-1)/3*3 + 1
						return time.Date(t.Year(), time.Month(q), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "month":
						return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "week":
						offset := int(t.Weekday())
						if offset == 0 {
							offset = 7
						}
						return t.AddDate(0, 0, -(offset - 1)).Truncate(24 * time.Hour).Format(time.RFC3339)
					case "day":
						return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "hour":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Format(time.RFC3339)
					case "minute":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location()).Format(time.RFC3339)
					case "second":
						return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, t.Location()).Format(time.RFC3339)
					}
				}
			}
		}
		return nil
	}

	// time.truncate(unit, time) - truncate time to specified unit
	if matchFuncStartAndSuffix(expr, "time.truncate") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			unit := strings.Trim(args[0].text(), "'\"")
			val := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			if str, ok := val.(string); ok {
				str = strings.Trim(str, "'\"")
				if t, err := time.Parse("15:04:05", str); err == nil {
					switch strings.ToLower(unit) {
					case "hour":
						return time.Date(0, 1, 1, t.Hour(), 0, 0, 0, time.UTC).Format("15:04:05")
					case "minute":
						return time.Date(0, 1, 1, t.Hour(), t.Minute(), 0, 0, time.UTC).Format("15:04:05")
					case "second":
						return t.Format("15:04:05")
					}
				}
			}
		}
		return nil
	}

	// duration.inMonths(duration) - convert duration to months
	if matchFuncStartAndSuffix(expr, "duration.inmonths") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if d, ok := val.(*CypherDuration); ok {
			return d.Years*12 + d.Months
		}
		return nil
	}

	// duration() - create duration from ISO 8601 string (P1Y2M3DT4H5M6S)
	// Returns a CypherDuration struct that can be used in arithmetic
	if isFunctionCall(expr, "duration") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			return parseDuration(str)
		}
		// Handle map format: duration({days: 5, hours: 3})
		if m, ok := val.(map[string]interface{}); ok {
			return durationFromMap(m)
		}
		return nil
	}

	// duration.between(d1, d2) - calculate duration between two dates/datetimes
	if matchFuncStartAndSuffix(expr, "duration.between") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			d1 := e.evaluateExpressionWithContextFull(ctx, args[0].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			d2 := e.evaluateExpressionWithContextFull(ctx, args[1].trim(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return durationBetween(d1, d2)
		}
		return nil
	}

	// duration.inDays(duration) - convert duration to days
	if matchFuncStartAndSuffix(expr, "duration.indays") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalDays()
		}
		return nil
	}

	// duration.inSeconds(duration) - convert duration to seconds
	if matchFuncStartAndSuffix(expr, "duration.inseconds") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalSeconds()
		}
		return nil
	}

	// ========================================
	// Math Functions
	// ========================================

	// ceil(number)
	if matchFuncStartAndSuffix(expr, "ceil") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}

	// floor(number)
	if matchFuncStartAndSuffix(expr, "floor") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}

	// round(number)
	if matchFuncStartAndSuffix(expr, "round") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}

	// randomUUID()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}

	// rand() - random float between 0 and 1
	if lowerExpr == "rand()" {
		return randomCypherFloat()
	}

	// apoc.create.uuid() - Generate a UUID (alias for randomUUID)
	// TODO: Move to plugin
	if lowerExpr == "apoc.create.uuid()" {
		return e.generateUUID()
	}

	// apoc.text.join(list, separator) - Join list elements with separator
	if isFunctionCall(expr, "apoc.text.join") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			sepVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			sep := ""
			if s, ok := sepVal.(string); ok {
				sep = strings.Trim(s, "'\"")
			}
			items, _ := cypherListValue(listVal)
			parts := make([]string, len(items))
			for index, item := range items {
				parts[index] = fmt.Sprint(item)
			}
			return strings.Join(parts, sep)
		}
		return nil
	}

	// apoc.coll.flatten(list [, recursive]) (flattenList)
	if isFunctionCall(expr, "apoc.coll.flatten") {
		args := view.functionArguments().trim().commaViews()
		if len(args) == 0 || len(args) > 2 {
			return nil
		}
		listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		recursive := len(args) == 2 && e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength) == true
		return flattenList(listVal, recursive)
	}

	// apoc.coll.toSet(list) - Remove duplicates from list
	if isFunctionCall(expr, "apoc.coll.toset") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollToSet(listVal)
	}

	// apoc.coll.sum(list) - Sum numeric values in list
	if isFunctionCall(expr, "apoc.coll.sum") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollSum(listVal)
	}

	// apoc.coll.avg(list) - Average of numeric values in list
	if isFunctionCall(expr, "apoc.coll.avg") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollAvg(listVal)
	}

	// apoc.coll.min(list) - Minimum value in list
	if isFunctionCall(expr, "apoc.coll.min") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollMin(listVal)
	}

	// apoc.coll.max(list) - Maximum value in list
	if isFunctionCall(expr, "apoc.coll.max") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollMax(listVal)
	}

	// apoc.coll.sort(list) - Sort list in ascending order
	if isFunctionCall(expr, "apoc.coll.sort") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollSort(listVal)
	}

	// apoc.coll.sortNodes(nodes, property) - Sort nodes by property
	if isFunctionCall(expr, "apoc.coll.sortnodes") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			propName := strings.Trim(args[1].text(), "'\"")
			return apocCollSortNodesByProperty(listVal, propName, func(value interface{}, property string) interface{} {
				result, _ := rowPropertyValue(e.queryVisibleValue(value), property)
				return result
			})
		}
		return nil
	}

	// apoc.coll.reverse(list) - Reverse a list
	if isFunctionCall(expr, "apoc.coll.reverse") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollReverse(listVal)
	}

	// apoc.coll.union(list1, list2) - Union of two lists (removes duplicates)
	if isFunctionCall(expr, "apoc.coll.union") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollUnion(list1, list2)
		}
		return nil
	}

	// apoc.coll.unionAll(list1, list2) - Union of two lists (keeps duplicates)
	if isFunctionCall(expr, "apoc.coll.unionall") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollUnionAll(list1, list2)
		}
		return nil
	}

	// apoc.coll.intersection(list1, list2) - Intersection of two lists
	if isFunctionCall(expr, "apoc.coll.intersection") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollIntersection(list1, list2)
		}
		return nil
	}

	// apoc.coll.subtract(list1, list2) - Elements in list1 but not in list2
	if isFunctionCall(expr, "apoc.coll.subtract") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollSubtract(list1, list2)
		}
		return nil
	}

	// apoc.coll.contains(list, value) - Check if list contains value
	if isFunctionCall(expr, "apoc.coll.contains") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContains(listVal, value)
		}
		return false
	}

	// apoc.coll.containsAll(list1, list2) - Check if list1 contains all elements of list2
	if isFunctionCall(expr, "apoc.coll.containsall") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContainsAll(list1, list2)
		}
		return false
	}

	// apoc.coll.containsAny(list1, list2) - Check if list1 contains any element of list2
	if isFunctionCall(expr, "apoc.coll.containsany") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollContainsAny(list1, list2)
		}
		return false
	}

	// apoc.coll.indexOf(list, value) - Find index of value in list (-1 if not found)
	if isFunctionCall(expr, "apoc.coll.indexof") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollIndexOf(listVal, value)
		}
		return int64(-1)
	}

	// apoc.coll.split(list, value) - Split list at occurrences of value
	if isFunctionCall(expr, "apoc.coll.split") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollSplit(listVal, value)
		}
		return nil
	}

	// apoc.coll.partition(list, size) - Partition list into sublists of given size
	if isFunctionCall(expr, "apoc.coll.partition") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			sizeVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollPartition(listVal, sizeVal)
		}
		return nil
	}

	// apoc.coll.pairs(list) - Create pairs from consecutive elements [[a,b], [b,c], ...]
	if isFunctionCall(expr, "apoc.coll.pairs") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollPairs(listVal)
	}

	// apoc.coll.zip(list1, list2) - Zip two lists into pairs [[a1,b1], [a2,b2], ...]
	if isFunctionCall(expr, "apoc.coll.zip") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			list2 := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollZip(list1, list2)
		}
		return nil
	}

	// apoc.coll.frequencies(list) - Count frequency of each element
	if isFunctionCall(expr, "apoc.coll.frequencies") {
		inner := view.functionArguments().trim()
		listVal := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocCollFrequencies(listVal)
	}

	// apoc.coll.occurrences(list, value) - Count occurrences of value in list
	if isFunctionCall(expr, "apoc.coll.occurrences") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			value := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return apocCollOccurrences(listVal, value)
		}
		return int64(0)
	}

	// apoc.convert.toJson(value) - Convert value to JSON string
	if isFunctionCall(expr, "apoc.convert.tojson") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		text, ok := apocConvertToJSON(val)
		if !ok {
			return nil
		}
		return text
	}

	// apoc.convert.fromJsonMap / fromJsonList(json) (apocConvertFromJSON)
	for _, function := range [...]string{"apoc.convert.fromjsonmap", "apoc.convert.fromjsonlist"} {
		if isFunctionCall(expr, function) {
			inner := view.functionArguments().trim()
			value := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			result, err := apocConvertFromJSON(function == "apoc.convert.fromjsonmap", value)
			if err != nil {
				recordExpressionFailure(ctx, err)
				return nil
			}
			return result
		}
	}

	// apoc.meta.type(value) - Get the Cypher type name of a value
	if isFunctionCall(expr, "apoc.meta.type") {
		inner := view.functionArguments().trim()
		val := e.evaluateExpressionWithContextFull(ctx, inner, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		return apocValueTypeName(val)
	}

	// apoc.meta.isType(value, typeName) - Check if value is of given type
	if isFunctionCall(expr, "apoc.meta.istype") {
		inner := view.functionArguments().trim()
		args := inner.commaViews()
		if len(args) >= 2 {
			val := e.evaluateExpressionWithContextFull(ctx, args[0], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			typeVal := e.evaluateExpressionWithContextFull(ctx, args[1], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			typeName, ok := typeVal.(string)
			if !ok {
				return false
			}
			typeName = strings.Trim(typeName, "'\"")
			actualType := apocValueTypeName(val)
			return strings.EqualFold(actualType, typeName)
		}
		return false
	}

	if v, ok := e.evaluateKnowledgePolicyFunction(ctx, view, lowerExpr, nodes, rels); ok {
		return v
	}

	// Fall through to remaining evaluation logic.
	return e.evaluateExpressionWithContextFullMath(ctx, view, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func randomCypherFloat() float64 {
	var bytes [8]byte
	_, _ = rand.Read(bytes[:])
	return float64(bytes[0]^bytes[1]^bytes[2]^bytes[3]) / 256.0
}

// evaluateListAccessFunction is head(), last() or reverse() of value, as
// Neo4j computes them in both evaluators: null gives null; head and last take
// a list (its first or last item, null for an empty list); reverse takes a
// string or a list. Any other value is the function's TypeError (Neo4j 5.26
// and 2026.09; head('ab') too).
func evaluateListAccessFunction(function string, value interface{}) (interface{}, error) {
	if cypherValueKindOf(value) == valueKindNull {
		return nil, nil
	}
	if text, isString := value.(string); isString && function == "reverse" {
		runes := []rune(text)
		for left, right := 0, len(runes)-1; left < right; left, right = left+1, right-1 {
			runes[left], runes[right] = runes[right], runes[left]
		}
		return string(runes), nil
	}
	if _, isString := value.(string); !isString {
		if items, isList := cypherListValue(value); isList {
			switch function {
			case "head":
				if len(items) == 0 {
					return nil, nil
				}
				return items[0], nil
			case "last":
				if len(items) == 0 {
					return nil, nil
				}
				return items[len(items)-1], nil
			}
			reversed := make([]interface{}, len(items))
			for index := range items {
				reversed[len(items)-1-index] = items[index]
			}
			return reversed, nil
		}
	}
	expected := "a list"
	if function == "reverse" {
		expected = "a string or a list"
	}
	return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCoreFunctionArgumentInvalid(function, expected, neo4jValueRepr(value)))
}
