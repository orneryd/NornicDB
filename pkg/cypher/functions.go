// Cypher function implementations for NornicDB.
// This file contains evaluateExpressionWithContext and all Cypher functions.

package cypher

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// isFunctionCall checks if an expression is a standalone function call with balanced parentheses.
//
// This function validates that:
//  1. Expression starts with funcName + "("
//  2. All parentheses are properly balanced
//  3. Expression ends right after the closing parenthesis
//  4. Quotes are respected (parentheses inside quotes don't count)
//
// It's used to distinguish between standalone function calls like "date('2025-01-01')"
// and complex expressions like "date('2025-01-01') + duration('P5D')".
//
// Parameters:
//   - expr: The expression to check
//   - funcName: The function name to look for (case-insensitive)
//
// Returns:
//   - true if expr is a standalone call to funcName
//   - false if expr is part of a larger expression or not a function call
//
// Example 1 - Standalone Function Calls (returns true):
//
//	isFunctionCall("date('2025-01-01')", "date")           // true
//	isFunctionCall("toLower(n.name)", "tolower")           // true
//	isFunctionCall("count(n)", "count")                    // true
//	isFunctionCall("substring('hello', 0, 3)", "substring") // true
//
// Example 2 - Complex Expressions (returns false):
//
//	isFunctionCall("date('2025-01-01') + duration('P5D')", "date")  // false - has + after
//	isFunctionCall("toLower(n.name) + ' suffix'", "tolower")        // false - has + after
//	isFunctionCall("count(n) > 10", "count")                        // false - has > after
//
// Example 3 - Nested Calls (returns false for outer, true for specific):
//
//	expr := "toLower(substring(n.name, 0, 5))"
//	isFunctionCall(expr, "substring")  // false - it's wrapped in toLower
//	isFunctionCall(expr, "tolower")    // true - toLower is the outer function
//
// ELI12:
//
// Imagine you're reading math: "add(5, 3)"
// This function checks: "Is this JUST the add() function, or is there more?"
//
//   - "add(5, 3)" → YES, that's just the function
//   - "add(5, 3) * 2" → NO, there's multiplication after it
//   - "multiply(add(5, 3), 2)" → For "add": NO (it's inside multiply)
//     → For "multiply": YES (it's the whole thing)
//
// It's like asking "Is this sentence ONLY about one thing, or are there extra parts?"
//
// Use Cases:
//   - Distinguishing function calls from arithmetic expressions
//   - Parsing Cypher RETURN clauses
//   - Validating function arguments
func isFunctionCall(expr, funcName string) bool {
	lower := strings.ToLower(expr)
	if !strings.HasPrefix(lower, funcName+"(") {
		return false
	}

	// Find the matching closing parenthesis for the opening one
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range expr {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
		case ch == '(' && !inQuote:
			depth++
		case ch == ')' && !inQuote:
			depth--
			if depth == 0 {
				// Found the matching closing parenthesis
				// Check if this is the end of the expression
				return i == len(expr)-1
			}
		}
	}
	return false
}

// CypherDuration represents a Neo4j-compatible duration type.
// Supports ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
type CypherDuration struct {
	Years   int64
	Months  int64
	Days    int64
	Hours   int64
	Minutes int64
	Seconds int64
	Nanos   int64
}

// String returns ISO 8601 duration format
func (d *CypherDuration) String() string {
	var sb strings.Builder
	sb.WriteString("P")
	if d.Years > 0 {
		sb.WriteString(fmt.Sprintf("%dY", d.Years))
	}
	if d.Months > 0 {
		sb.WriteString(fmt.Sprintf("%dM", d.Months))
	}
	if d.Days > 0 {
		sb.WriteString(fmt.Sprintf("%dD", d.Days))
	}
	if d.Hours > 0 || d.Minutes > 0 || d.Seconds > 0 || d.Nanos > 0 {
		sb.WriteString("T")
		if d.Hours > 0 {
			sb.WriteString(fmt.Sprintf("%dH", d.Hours))
		}
		if d.Minutes > 0 {
			sb.WriteString(fmt.Sprintf("%dM", d.Minutes))
		}
		if d.Seconds > 0 || d.Nanos > 0 {
			if d.Nanos > 0 {
				sb.WriteString(fmt.Sprintf("%d.%09dS", d.Seconds, d.Nanos))
			} else {
				sb.WriteString(fmt.Sprintf("%dS", d.Seconds))
			}
		}
	}
	if sb.Len() == 1 {
		sb.WriteString("T0S") // Empty duration
	}
	return sb.String()
}

// TotalDays returns the total number of days (approximate for months/years)
func (d *CypherDuration) TotalDays() float64 {
	return float64(d.Years)*365.25 + float64(d.Months)*30.44 + float64(d.Days) +
		float64(d.Hours)/24 + float64(d.Minutes)/(24*60) + float64(d.Seconds)/(24*60*60)
}

// TotalSeconds returns the total number of seconds
func (d *CypherDuration) TotalSeconds() float64 {
	return float64(d.Years)*365.25*24*3600 + float64(d.Months)*30.44*24*3600 +
		float64(d.Days)*24*3600 + float64(d.Hours)*3600 + float64(d.Minutes)*60 +
		float64(d.Seconds) + float64(d.Nanos)/1e9
}

// ToTimeDuration converts to Go time.Duration (loses year/month precision)
func (d *CypherDuration) ToTimeDuration() time.Duration {
	totalNanos := d.Nanos +
		d.Seconds*1e9 +
		d.Minutes*60*1e9 +
		d.Hours*3600*1e9 +
		d.Days*24*3600*1e9 +
		int64(float64(d.Months)*30.44*24*3600*1e9) +
		int64(float64(d.Years)*365.25*24*3600*1e9)
	return time.Duration(totalNanos)
}

// parseDuration parses ISO 8601 duration format (P[n]Y[n]M[n]DT[n]H[n]M[n]S)
func parseDuration(s string) *CypherDuration {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(strings.ToUpper(s), "P") {
		return nil
	}

	d := &CypherDuration{}
	s = s[1:] // Remove 'P'

	// Split into date and time parts
	parts := strings.SplitN(strings.ToUpper(s), "T", 2)
	datePart := parts[0]
	timePart := ""
	if len(parts) > 1 {
		timePart = parts[1]
	}

	// Parse date part (Y, M, D) using pre-compiled pattern from regex_patterns.go
	for _, match := range durationDatePartPattern.FindAllStringSubmatch(datePart, -1) {
		val, _ := strconv.ParseInt(match[1], 10, 64)
		switch match[2] {
		case "Y":
			d.Years = val
		case "M":
			d.Months = val
		case "D":
			d.Days = val
		}
	}

	// Parse time part (H, M, S) using pre-compiled pattern from regex_patterns.go
	for _, match := range durationTimePartPattern.FindAllStringSubmatch(timePart, -1) {
		switch match[2] {
		case "H":
			val, _ := strconv.ParseInt(match[1], 10, 64)
			d.Hours = val
		case "M":
			val, _ := strconv.ParseInt(match[1], 10, 64)
			d.Minutes = val
		case "S":
			if strings.Contains(match[1], ".") {
				parts := strings.Split(match[1], ".")
				d.Seconds, _ = strconv.ParseInt(parts[0], 10, 64)
				// Parse fractional seconds as nanoseconds
				frac := parts[1]
				for len(frac) < 9 {
					frac += "0"
				}
				d.Nanos, _ = strconv.ParseInt(frac[:9], 10, 64)
			} else {
				d.Seconds, _ = strconv.ParseInt(match[1], 10, 64)
			}
		}
	}

	return d
}

// durationFromMap creates a duration from a map like {days: 5, hours: 3}
func durationFromMap(m map[string]interface{}) *CypherDuration {
	d := &CypherDuration{}

	if v, ok := m["years"]; ok {
		d.Years = toInt64(v)
	}
	if v, ok := m["months"]; ok {
		d.Months = toInt64(v)
	}
	if v, ok := m["days"]; ok {
		d.Days = toInt64(v)
	}
	if v, ok := m["hours"]; ok {
		d.Hours = toInt64(v)
	}
	if v, ok := m["minutes"]; ok {
		d.Minutes = toInt64(v)
	}
	if v, ok := m["seconds"]; ok {
		d.Seconds = toInt64(v)
	}

	return d
}

// toInt64 converts various numeric types to int64
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}

// durationBetween calculates the duration between two dates/datetimes
func durationBetween(d1, d2 interface{}) *CypherDuration {
	t1 := parseDateTime(d1)
	t2 := parseDateTime(d2)
	if t1.IsZero() || t2.IsZero() {
		return nil
	}

	diff := t2.Sub(t1)
	if diff < 0 {
		diff = -diff
	}

	return &CypherDuration{
		Days:    int64(diff.Hours()) / 24,
		Hours:   int64(diff.Hours()) % 24,
		Minutes: int64(diff.Minutes()) % 60,
		Seconds: int64(diff.Seconds()) % 60,
		Nanos:   int64(diff.Nanoseconds()) % 1e9,
	}
}

// parseDateTime parses various datetime formats
func parseDateTime(v interface{}) time.Time {
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		s := strings.Trim(val, "'\"")
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if parsed, err := time.Parse(layout, s); err == nil {
				return parsed
			}
		}
	}
	return time.Time{}
}

// addDurationToDate adds a CypherDuration to a date/datetime
func addDurationToDate(dateVal interface{}, dur *CypherDuration) string {
	t := parseDateTime(dateVal)
	if t.IsZero() || dur == nil {
		return ""
	}

	// Add the duration components
	t = t.AddDate(int(dur.Years), int(dur.Months), int(dur.Days))
	t = t.Add(time.Duration(dur.Hours)*time.Hour +
		time.Duration(dur.Minutes)*time.Minute +
		time.Duration(dur.Seconds)*time.Second +
		time.Duration(dur.Nanos)*time.Nanosecond)

	return t.Format(time.RFC3339)
}

// subtractDurationFromDate subtracts a CypherDuration from a date/datetime
func subtractDurationFromDate(dateVal interface{}, dur *CypherDuration) string {
	t := parseDateTime(dateVal)
	if t.IsZero() || dur == nil {
		return ""
	}

	// Subtract the duration components
	t = t.AddDate(-int(dur.Years), -int(dur.Months), -int(dur.Days))
	t = t.Add(-(time.Duration(dur.Hours)*time.Hour +
		time.Duration(dur.Minutes)*time.Minute +
		time.Duration(dur.Seconds)*time.Second +
		time.Duration(dur.Nanos)*time.Nanosecond))

	return t.Format(time.RFC3339)
}

// evaluateExpression evaluates an expression for a single node context.
func (e *StorageExecutor) evaluateExpression(expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(expr, map[string]*storage.Node{varName: node}, nil)
}
func (e *StorageExecutor) evaluateExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// ========================================
	// CASE Expressions (must be checked first)
	// ========================================
	if isCaseExpression(expr) {
		return e.evaluateCaseExpression(expr, nodes, rels)
	}

	lowerExpr := strings.ToLower(expr)

	// ========================================
	// Cypher Functions (Neo4j compatible)
	// ========================================

	// id(n) - return internal node/relationship ID
	if strings.HasPrefix(lowerExpr, "id(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[3 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return string(node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return string(rel.ID)
		}
		return nil
	}

	// elementId(n) - same as id() for compatibility
	if strings.HasPrefix(lowerExpr, "elementid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return fmt.Sprintf("4:nornicdb:%s", node.ID)
		}
		if rel, ok := rels[inner]; ok {
			return fmt.Sprintf("5:nornicdb:%s", rel.ID)
		}
		return nil
	}

	// labels(n) - return list of labels for a node
	if strings.HasPrefix(lowerExpr, "labels(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			// Return labels as a list of strings
			result := make([]interface{}, len(node.Labels))
			for i, label := range node.Labels {
				result[i] = label
			}
			return result
		}
		return nil
	}

	// type(r) - return relationship type
	if strings.HasPrefix(lowerExpr, "type(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			return rel.Type
		}
		return nil
	}

	// keys(n) - return list of property keys
	if strings.HasPrefix(lowerExpr, "keys(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			keys := make([]interface{}, 0, len(node.Properties))
			for k := range node.Properties {
				if !e.isInternalProperty(k) {
					keys = append(keys, k)
				}
			}
			return keys
		}
		if rel, ok := rels[inner]; ok {
			keys := make([]interface{}, 0, len(rel.Properties))
			for k := range rel.Properties {
				keys = append(keys, k)
			}
			return keys
		}
		return nil
	}

	// properties(n) - return all properties as a map
	if strings.HasPrefix(lowerExpr, "properties(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			props := make(map[string]interface{})
			for k, v := range node.Properties {
				if !e.isInternalProperty(k) {
					props[k] = v
				}
			}
			return props
		}
		if rel, ok := rels[inner]; ok {
			return rel.Properties
		}
		return nil
	}

	// count(*) or count(n) - simplified aggregation (returns 1 for single row context)
	if strings.HasPrefix(lowerExpr, "count(") && strings.HasSuffix(expr, ")") {
		return int64(1)
	}

	// size(list) or size(string) - return length
	if strings.HasPrefix(lowerExpr, "size(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		case []string:
			return int64(len(v))
		}
		return int64(0)
	}

	// length(path) - same as size for compatibility
	if strings.HasPrefix(lowerExpr, "length(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := innerVal.(type) {
		case string:
			return int64(len(v))
		case []interface{}:
			return int64(len(v))
		}
		return int64(0)
	}

	// exists(n.prop) - check if property exists
	if strings.HasPrefix(lowerExpr, "exists(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		// Check for property access
		if dotIdx := strings.Index(inner, "."); dotIdx > 0 {
			varName := inner[:dotIdx]
			propName := inner[dotIdx+1:]
			if node, ok := nodes[varName]; ok {
				_, exists := node.Properties[propName]
				return exists
			}
		}
		return false
	}

	// coalesce(val1, val2, ...) - return first non-null value
	if strings.HasPrefix(lowerExpr, "coalesce(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		for _, arg := range args {
			val := e.evaluateExpressionWithContext(strings.TrimSpace(arg), nodes, rels)
			if val != nil {
				return val
			}
		}
		return nil
	}

	// head(list) - return first element
	if strings.HasPrefix(lowerExpr, "head(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[0]
		}
		return nil
	}

	// last(list) - return last element
	if strings.HasPrefix(lowerExpr, "last(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 0 {
			return list[len(list)-1]
		}
		return nil
	}

	// tail(list) - return list without first element
	if strings.HasPrefix(lowerExpr, "tail(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok && len(list) > 1 {
			return list[1:]
		}
		return []interface{}{}
	}

	// reverse(list) - return reversed list
	if strings.HasPrefix(lowerExpr, "reverse(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		innerVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if list, ok := innerVal.([]interface{}); ok {
			result := make([]interface{}, len(list))
			for i, v := range list {
				result[len(list)-1-i] = v
			}
			return result
		}
		if str, ok := innerVal.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
		return nil
	}

	// range(start, end) or range(start, end, step)
	if strings.HasPrefix(lowerExpr, "range(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			start, _ := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
			end, _ := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			step := int64(1)
			if len(args) >= 3 {
				step, _ = strconv.ParseInt(strings.TrimSpace(args[2]), 10, 64)
			}
			if step == 0 {
				step = 1
			}
			var result []interface{}
			if step > 0 {
				for i := start; i <= end; i += step {
					result = append(result, i)
				}
			} else {
				for i := start; i >= end; i += step {
					result = append(result, i)
				}
			}
			return result
		}
		return []interface{}{}
	}

	// slice(list, start, end) - get sublist from start to end (exclusive)
	if strings.HasPrefix(lowerExpr, "slice(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			startIdx, _ := strconv.ParseInt(strings.TrimSpace(args[1]), 10, 64)
			if list, ok := listVal.([]interface{}); ok {
				endIdx := int64(len(list))
				if len(args) >= 3 {
					endIdx, _ = strconv.ParseInt(strings.TrimSpace(args[2]), 10, 64)
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
	if strings.HasPrefix(lowerExpr, "indexof(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			listVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			searchVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if list, ok := listVal.([]interface{}); ok {
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
	if strings.HasPrefix(lowerExpr, "degree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			inDegree := e.storage.GetInDegree(node.ID)
			outDegree := e.storage.GetOutDegree(node.ID)
			return int64(inDegree + outDegree)
		}
		return int64(0)
	}

	// inDegree(node) - incoming edges count
	if strings.HasPrefix(lowerExpr, "indegree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetInDegree(node.ID))
		}
		return int64(0)
	}

	// outDegree(node) - outgoing edges count
	if strings.HasPrefix(lowerExpr, "outdegree(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if node, ok := nodes[inner]; ok {
			return int64(e.storage.GetOutDegree(node.ID))
		}
		return int64(0)
	}

	// hasLabels(node, labels) - check if node has all specified labels
	if strings.HasPrefix(lowerExpr, "haslabels(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			if node, ok := nodes[strings.TrimSpace(args[0])]; ok {
				labelsVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
				if labels, ok := labelsVal.([]interface{}); ok {
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

	// apoc.map.fromPairs(list) - create map from [[key, value], ...] pairs
	if strings.HasPrefix(lowerExpr, "apoc.map.frompairs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		pairsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pairs, ok := pairsVal.([]interface{}); ok {
			result := make(map[string]interface{})
			for _, pair := range pairs {
				if pairList, ok := pair.([]interface{}); ok && len(pairList) >= 2 {
					if key, ok := pairList[0].(string); ok {
						result[key] = pairList[1]
					}
				}
			}
			return result
		}
		return map[string]interface{}{}
	}

	// apoc.map.merge(map1, map2) - merge two maps
	if strings.HasPrefix(lowerExpr, "apoc.map.merge(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			map1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			map2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			m1, ok1 := map1.(map[string]interface{})
			m2, ok2 := map2.(map[string]interface{})
			if ok1 && ok2 {
				result := make(map[string]interface{})
				for k, v := range m1 {
					result[k] = v
				}
				for k, v := range m2 {
					result[k] = v
				}
				return result
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.removeKey(map, key) - remove key from map
	if strings.HasPrefix(lowerExpr, "apoc.map.removekey(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) == 2 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			keyVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if m, ok := mapVal.(map[string]interface{}); ok {
				if key, ok := keyVal.(string); ok {
					result := make(map[string]interface{})
					for k, v := range m {
						if k != key {
							result[k] = v
						}
					}
					return result
				}
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.setKey(map, key, value) - set key in map
	if strings.HasPrefix(lowerExpr, "apoc.map.setkey(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) == 3 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			keyVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			value := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)
			if m, ok := mapVal.(map[string]interface{}); ok {
				if key, ok := keyVal.(string); ok {
					result := make(map[string]interface{})
					for k, v := range m {
						result[k] = v
					}
					result[key] = value
					return result
				}
			}
		}
		return map[string]interface{}{}
	}

	// apoc.map.clean(map, keys, values) - remove specified keys and entries with specified values
	if strings.HasPrefix(lowerExpr, "apoc.map.clean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			mapVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			var keysToRemove []string
			var valuesToRemove []interface{}

			if len(args) >= 2 {
				if keys, ok := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels).([]interface{}); ok {
					for _, k := range keys {
						if ks, ok := k.(string); ok {
							keysToRemove = append(keysToRemove, ks)
						}
					}
				}
			}
			if len(args) >= 3 {
				if vals, ok := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels).([]interface{}); ok {
					valuesToRemove = vals
				}
			}

			if m, ok := mapVal.(map[string]interface{}); ok {
				result := make(map[string]interface{})
				for k, v := range m {
					// Skip if key is in keysToRemove
					skip := false
					for _, kr := range keysToRemove {
						if k == kr {
							skip = true
							break
						}
					}
					if skip {
						continue
					}
					// Skip if value is in valuesToRemove
					for _, vr := range valuesToRemove {
						if e.compareEqual(v, vr) {
							skip = true
							break
						}
					}
					if !skip {
						result[k] = v
					}
				}
				return result
			}
		}
		return map[string]interface{}{}
	}

	// ========================================
	// String Functions
	// ========================================

	// toString(value)
	if strings.HasPrefix(lowerExpr, "tostring(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fmt.Sprintf("%v", val)
	}

	// toInteger(value) / toInt(value)
	if (strings.HasPrefix(lowerExpr, "tointeger(") || strings.HasPrefix(lowerExpr, "toint(")) && strings.HasSuffix(expr, ")") {
		startIdx := 10
		if strings.HasPrefix(lowerExpr, "toint(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil
	}

	// toFloat(value)
	if strings.HasPrefix(lowerExpr, "tofloat(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil
	}

	// toBoolean(value)
	if strings.HasPrefix(lowerExpr, "toboolean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return strings.EqualFold(v, "true")
		}
		return nil
	}

	// ========================================
	// OrNull Variants (return null instead of error)
	// ========================================

	// toIntegerOrNull(value)
	if strings.HasPrefix(lowerExpr, "tointegerornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
		return nil // Return null instead of error
	}

	// toFloatOrNull(value)
	if strings.HasPrefix(lowerExpr, "tofloatornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return nil
	}

	// toBooleanOrNull(value)
	if strings.HasPrefix(lowerExpr, "tobooleanornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case bool:
			return v
		case string:
			lower := strings.ToLower(v)
			if lower == "true" {
				return true
			}
			if lower == "false" {
				return false
			}
		}
		return nil
	}

	// toStringOrNull(value) - same as toString but explicit null handling
	if strings.HasPrefix(lowerExpr, "tostringornull(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if val == nil {
			return nil
		}
		return fmt.Sprintf("%v", val)
	}

	// ========================================
	// List Conversion Functions
	// ========================================

	// toIntegerList(list)
	if strings.HasPrefix(lowerExpr, "tointegerlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case int64:
				result[i] = v
			case int:
				result[i] = int64(v)
			case float64:
				result[i] = int64(v)
			case string:
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					result[i] = n
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toFloatList(list)
	if strings.HasPrefix(lowerExpr, "tofloatlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[12 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case float64:
				result[i] = v
			case float32:
				result[i] = float64(v)
			case int64:
				result[i] = float64(v)
			case int:
				result[i] = float64(v)
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					result[i] = f
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toBooleanList(list)
	if strings.HasPrefix(lowerExpr, "tobooleanlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			switch v := item.(type) {
			case bool:
				result[i] = v
			case string:
				lower := strings.ToLower(v)
				if lower == "true" {
					result[i] = true
				} else if lower == "false" {
					result[i] = false
				} else {
					result[i] = nil
				}
			default:
				result[i] = nil
			}
		}
		return result
	}

	// toStringList(list)
	if strings.HasPrefix(lowerExpr, "tostringlist(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[13 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		list, ok := val.([]interface{})
		if !ok {
			return nil
		}
		result := make([]interface{}, len(list))
		for i, item := range list {
			if item == nil {
				result[i] = nil
			} else {
				result[i] = fmt.Sprintf("%v", item)
			}
		}
		return result
	}

	// ========================================
	// Additional Utility Functions
	// ========================================

	// valueType(value) - returns the type of a value as a string
	if strings.HasPrefix(lowerExpr, "valuetype(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch val.(type) {
		case nil:
			return "NULL"
		case bool:
			return "BOOLEAN"
		case int, int64, int32:
			return "INTEGER"
		case float64, float32:
			return "FLOAT"
		case string:
			return "STRING"
		case []interface{}:
			return "LIST"
		case map[string]interface{}:
			return "MAP"
		default:
			return "ANY"
		}
	}

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// sum(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "sum(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// avg(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "avg(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// min(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "min(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// max(expr) - in single row context, just returns the value
	if strings.HasPrefix(lowerExpr, "max(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		return e.evaluateExpressionWithContext(inner, nodes, rels)
	}

	// collect(expr) - in single row context, returns single-element list
	if strings.HasPrefix(lowerExpr, "collect(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if val == nil {
			return []interface{}{}
		}
		return []interface{}{val}
	}

	// toLower(string) / lower(string)
	if (strings.HasPrefix(lowerExpr, "tolower(") || strings.HasPrefix(lowerExpr, "lower(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "lower(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
		return nil
	}

	// toUpper(string) / upper(string)
	if (strings.HasPrefix(lowerExpr, "toupper(") || strings.HasPrefix(lowerExpr, "upper(")) && strings.HasSuffix(expr, ")") {
		startIdx := 8
		if strings.HasPrefix(lowerExpr, "upper(") {
			startIdx = 6
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
		return nil
	}

	// trim(string) / ltrim(string) / rtrim(string)
	if strings.HasPrefix(lowerExpr, "trim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimSpace(str)
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "ltrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimLeft(str, " \t\n\r")
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "rtrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return strings.TrimRight(str, " \t\n\r")
		}
		return nil
	}

	// replace(string, search, replacement)
	if strings.HasPrefix(lowerExpr, "replace(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 3 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			search := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			repl := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
			return strings.ReplaceAll(str, search, repl)
		}
		return nil
	}

	// split(string, delimiter)
	if strings.HasPrefix(lowerExpr, "split(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			delim := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			parts := strings.Split(str, delim)
			result := make([]interface{}, len(parts))
			for i, p := range parts {
				result[i] = p
			}
			return result
		}
		return nil
	}

	// substring(string, start, [length])
	if strings.HasPrefix(lowerExpr, "substring(") && strings.HasSuffix(expr, ")") {
		return e.evaluateSubstring(expr)
	}

	// left(string, n) - return first n characters
	if strings.HasPrefix(lowerExpr, "left(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[:n]
		}
		return nil
	}

	// right(string, n) - return last n characters
	if strings.HasPrefix(lowerExpr, "right(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			n, _ := strconv.Atoi(strings.TrimSpace(args[1]))
			if n > len(str) {
				n = len(str)
			}
			return str[len(str)-n:]
		}
		return nil
	}

	// reverse(string) - reverse a string
	if strings.HasPrefix(lowerExpr, "reverse(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			runes := []rune(str)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
		return nil
	}

	// lpad(string, length, padString) - left-pad string to specified length
	if strings.HasPrefix(lowerExpr, "lpad(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
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
	if strings.HasPrefix(lowerExpr, "rpad(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			length, err := strconv.Atoi(strings.TrimSpace(args[1]))
			if err != nil {
				return nil
			}
			padStr := " " // default pad character is space
			if len(args) >= 3 {
				padStr = fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels))
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

	// format(template, ...args) - string formatting (printf-style)
	if strings.HasPrefix(lowerExpr, "format(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			template := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			// Remove quotes from template
			template = strings.Trim(template, "'\"")

			// Evaluate remaining arguments
			formatArgs := make([]interface{}, 0, len(args)-1)
			for i := 1; i < len(args); i++ {
				val := e.evaluateExpressionWithContext(strings.TrimSpace(args[i]), nodes, rels)
				formatArgs = append(formatArgs, val)
			}

			// Simple format string replacement
			// Supports %s (string), %d (integer), %f (float), %v (any)
			return fmt.Sprintf(template, formatArgs...)
		}
		return nil
	}

	// ========================================
	// Date/Time Functions (Neo4j compatible)
	// ========================================

	// timestamp() - current Unix timestamp in milliseconds
	if lowerExpr == "timestamp()" {
		return time.Now().UnixMilli()
	}

	// datetime() - current datetime as ISO 8601 string or parse from argument
	if isFunctionCall(expr, "datetime") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		if inner == "" {
			// No argument - return current datetime
			return time.Now().Format(time.RFC3339)
		}
		// Try to parse argument as ISO 8601 string
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			// Try parsing various formats
			for _, layout := range []string{
				time.RFC3339,
				"2006-01-02T15:04:05",
				"2006-01-02 15:04:05",
				"2006-01-02",
			} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format(time.RFC3339)
				}
			}
		}
		return nil
	}

	// localdatetime() - current local datetime
	if lowerExpr == "localdatetime()" {
		return time.Now().Format("2006-01-02T15:04:05")
	}

	// date() - current date or parse from argument
	if isFunctionCall(expr, "date") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if inner == "" {
			// No argument - return current date
			return time.Now().Format("2006-01-02")
		}
		// Try to parse argument
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return t.Format("2006-01-02")
			}
			// Try parsing datetime and extracting date
			for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format("2006-01-02")
				}
			}
		}
		return nil
	}

	// time() - current time or parse from argument
	if isFunctionCall(expr, "time") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		if inner == "" {
			// No argument - return current time
			return time.Now().Format("15:04:05")
		}
		// Try to parse argument
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			// Try parsing various time formats
			for _, layout := range []string{"15:04:05", "15:04:05.000", "15:04"} {
				if t, err := time.Parse(layout, str); err == nil {
					return t.Format("15:04:05")
				}
			}
		}
		return nil
	}

	// localtime() - current local time
	if lowerExpr == "localtime()" {
		return time.Now().Format("15:04:05")
	}

	// date.year(date), date.month(date), date.day(date) - extract components
	if strings.HasPrefix(lowerExpr, "date.year(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Year())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "date.month(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Month())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "date.day(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.Day())
			}
		}
		return nil
	}

	// date.week(date) - ISO week number (1-53)
	if strings.HasPrefix(lowerExpr, "date.week(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				_, week := t.ISOWeek()
				return int64(week)
			}
		}
		return nil
	}

	// date.quarter(date) - quarter of year (1-4)
	if strings.HasPrefix(lowerExpr, "date.quarter(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[13 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64((int(t.Month())-1)/3 + 1)
			}
		}
		return nil
	}

	// date.dayOfWeek(date) - day of week (1=Monday, 7=Sunday, ISO 8601)
	if strings.HasPrefix(lowerExpr, "date.dayofweek(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				dow := int64(t.Weekday())
				if dow == 0 {
					dow = 7 // Sunday = 7 in ISO 8601
				}
				return dow
			}
		}
		return nil
	}

	// date.dayOfYear(date) - day of year (1-366)
	if strings.HasPrefix(lowerExpr, "date.dayofyear(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.YearDay())
			}
		}
		return nil
	}

	// date.ordinalDay(date) - same as dayOfYear
	if strings.HasPrefix(lowerExpr, "date.ordinalday(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				return int64(t.YearDay())
			}
		}
		return nil
	}

	// date.weekYear(date) - ISO week year (may differ from calendar year at year boundaries)
	if strings.HasPrefix(lowerExpr, "date.weekyear(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			if t, err := time.Parse("2006-01-02", str); err == nil {
				year, _ := t.ISOWeek()
				return int64(year)
			}
		}
		return nil
	}

	// date.truncate(unit, date) - truncate date to specified unit
	if strings.HasPrefix(lowerExpr, "date.truncate(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
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
	if strings.HasPrefix(lowerExpr, "datetime.truncate(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
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
	if strings.HasPrefix(lowerExpr, "time.truncate(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			unit := strings.Trim(strings.TrimSpace(args[0]), "'\"")
			val := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
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

	// datetime.hour(datetime), datetime.minute(datetime), datetime.second(datetime)
	if strings.HasPrefix(lowerExpr, "datetime.hour(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Hour())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "datetime.minute(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Minute())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "datetime.second(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Second())
			}
		}
		return nil
	}

	// datetime.year(datetime), datetime.month(datetime), datetime.day(datetime)
	if strings.HasPrefix(lowerExpr, "datetime.year(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Year())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "datetime.month(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Month())
			}
		}
		return nil
	}
	if strings.HasPrefix(lowerExpr, "datetime.day(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[13 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			str = strings.Trim(str, "'\"")
			t := parseDateTime(str)
			if !t.IsZero() {
				return int64(t.Day())
			}
		}
		return nil
	}

	// duration.inMonths(duration) - convert duration to months
	if strings.HasPrefix(lowerExpr, "duration.inmonths(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.Years*12 + d.Months
		}
		return nil
	}

	// duration() - create duration from ISO 8601 string (P1Y2M3DT4H5M6S)
	// Returns a CypherDuration struct that can be used in arithmetic
	if isFunctionCall(expr, "duration") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
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
	if strings.HasPrefix(lowerExpr, "duration.between(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			d1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			d2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			return durationBetween(d1, d2)
		}
		return nil
	}

	// duration.inDays(duration) - convert duration to days
	if strings.HasPrefix(lowerExpr, "duration.indays(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalDays()
		}
		return nil
	}

	// duration.inSeconds(duration) - convert duration to seconds
	if strings.HasPrefix(lowerExpr, "duration.inseconds(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if d, ok := val.(*CypherDuration); ok {
			return d.TotalSeconds()
		}
		return nil
	}

	// ========================================
	// Math Functions
	// ========================================

	// abs(number)
	if strings.HasPrefix(lowerExpr, "abs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case int64:
			if v < 0 {
				return -v
			}
			return v
		case float64:
			if v < 0 {
				return -v
			}
			return v
		}
		return nil
	}

	// ceil(number)
	if strings.HasPrefix(lowerExpr, "ceil(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.999999999)
		}
		return nil
	}

	// floor(number)
	if strings.HasPrefix(lowerExpr, "floor(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f)
		}
		return nil
	}

	// round(number)
	if strings.HasPrefix(lowerExpr, "round(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return int64(f + 0.5)
		}
		return nil
	}

	// sign(number)
	if strings.HasPrefix(lowerExpr, "sign(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			if f > 0 {
				return int64(1)
			} else if f < 0 {
				return int64(-1)
			}
			return int64(0)
		}
		return nil
	}

	// randomUUID()
	if lowerExpr == "randomuuid()" {
		return e.generateUUID()
	}

	// rand() - random float between 0 and 1
	if lowerExpr == "rand()" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		// Convert to float between 0 and 1
		val := float64(b[0]^b[1]^b[2]^b[3]) / 256.0
		return val
	}

	// ========================================
	// APOC Functions (Neo4j Community Extensions)
	// ========================================

	// apoc.create.uuid() - Generate a UUID (alias for randomUUID)
	if lowerExpr == "apoc.create.uuid()" {
		return e.generateUUID()
	}

	// apoc.text.join(list, separator) - Join list elements with separator
	if isFunctionCall(expr, "apoc.text.join") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			sepVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			sep := ""
			if s, ok := sepVal.(string); ok {
				sep = strings.Trim(s, "'\"")
			}
			// Convert list to string slice
			var parts []string
			switch v := listVal.(type) {
			case []interface{}:
				for _, item := range v {
					parts = append(parts, fmt.Sprintf("%v", item))
				}
			case []string:
				parts = v
			}
			return strings.Join(parts, sep)
		}
		return nil
	}

	// apoc.coll.flatten(list) - Flatten nested lists into a single list
	if isFunctionCall(expr, "apoc.coll.flatten") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return flattenList(listVal)
	}

	// apoc.coll.toSet(list) - Remove duplicates from list
	if isFunctionCall(expr, "apoc.coll.toset") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return toSet(listVal)
	}

	// apoc.coll.sum(list) - Sum numeric values in list
	if isFunctionCall(expr, "apoc.coll.sum") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollSum(listVal)
	}

	// apoc.coll.avg(list) - Average of numeric values in list
	if isFunctionCall(expr, "apoc.coll.avg") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollAvg(listVal)
	}

	// apoc.coll.min(list) - Minimum value in list
	if isFunctionCall(expr, "apoc.coll.min") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollMin(listVal)
	}

	// apoc.coll.max(list) - Maximum value in list
	if isFunctionCall(expr, "apoc.coll.max") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollMax(listVal)
	}

	// apoc.coll.sort(list) - Sort list in ascending order
	if isFunctionCall(expr, "apoc.coll.sort") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollSort(listVal)
	}

	// apoc.coll.sortNodes(nodes, property) - Sort nodes by property
	if isFunctionCall(expr, "apoc.coll.sortnodes") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			propName := strings.Trim(args[1], "'\"")
			return apocCollSortNodes(listVal, propName)
		}
		return nil
	}

	// apoc.coll.reverse(list) - Reverse a list
	if isFunctionCall(expr, "apoc.coll.reverse") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollReverse(listVal)
	}

	// apoc.coll.union(list1, list2) - Union of two lists (removes duplicates)
	if isFunctionCall(expr, "apoc.coll.union") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollUnion(list1, list2)
		}
		return nil
	}

	// apoc.coll.unionAll(list1, list2) - Union of two lists (keeps duplicates)
	if isFunctionCall(expr, "apoc.coll.unionall") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollUnionAll(list1, list2)
		}
		return nil
	}

	// apoc.coll.intersection(list1, list2) - Intersection of two lists
	if isFunctionCall(expr, "apoc.coll.intersection") {
		inner := strings.TrimSpace(expr[23 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollIntersection(list1, list2)
		}
		return nil
	}

	// apoc.coll.subtract(list1, list2) - Elements in list1 but not in list2
	if isFunctionCall(expr, "apoc.coll.subtract") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollSubtract(list1, list2)
		}
		return nil
	}

	// apoc.coll.contains(list, value) - Check if list contains value
	if isFunctionCall(expr, "apoc.coll.contains") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContains(listVal, value)
		}
		return false
	}

	// apoc.coll.containsAll(list1, list2) - Check if list1 contains all elements of list2
	if isFunctionCall(expr, "apoc.coll.containsall") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContainsAll(list1, list2)
		}
		return false
	}

	// apoc.coll.containsAny(list1, list2) - Check if list1 contains any element of list2
	if isFunctionCall(expr, "apoc.coll.containsany") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollContainsAny(list1, list2)
		}
		return false
	}

	// apoc.coll.indexOf(list, value) - Find index of value in list (-1 if not found)
	if isFunctionCall(expr, "apoc.coll.indexof") {
		inner := strings.TrimSpace(expr[18 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollIndexOf(listVal, value)
		}
		return int64(-1)
	}

	// apoc.coll.split(list, value) - Split list at occurrences of value
	if isFunctionCall(expr, "apoc.coll.split") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollSplit(listVal, value)
		}
		return nil
	}

	// apoc.coll.partition(list, size) - Partition list into sublists of given size
	if isFunctionCall(expr, "apoc.coll.partition") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			sizeVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollPartition(listVal, sizeVal)
		}
		return nil
	}

	// apoc.coll.pairs(list) - Create pairs from consecutive elements [[a,b], [b,c], ...]
	if isFunctionCall(expr, "apoc.coll.pairs") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollPairs(listVal)
	}

	// apoc.coll.zip(list1, list2) - Zip two lists into pairs [[a1,b1], [a2,b2], ...]
	if isFunctionCall(expr, "apoc.coll.zip") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			list1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			list2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollZip(list1, list2)
		}
		return nil
	}

	// apoc.coll.frequencies(list) - Count frequency of each element
	if isFunctionCall(expr, "apoc.coll.frequencies") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return apocCollFrequencies(listVal)
	}

	// apoc.coll.occurrences(list, value) - Count occurrences of value in list
	if isFunctionCall(expr, "apoc.coll.occurrences") {
		inner := strings.TrimSpace(expr[22 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			listVal := e.evaluateExpressionWithContext(args[0], nodes, rels)
			value := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return apocCollOccurrences(listVal, value)
		}
		return int64(0)
	}

	// apoc.convert.toJson(value) - Convert value to JSON string
	if isFunctionCall(expr, "apoc.convert.tojson") {
		inner := strings.TrimSpace(expr[20 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return string(jsonBytes)
	}

	// apoc.convert.fromJsonMap(json) - Parse JSON string to map
	if isFunctionCall(expr, "apoc.convert.fromjsonmap") {
		inner := strings.TrimSpace(expr[24 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonStr, ok := val.(string)
		if !ok {
			return nil
		}
		jsonStr = strings.Trim(jsonStr, "'\"")
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
			return nil
		}
		return result
	}

	// apoc.convert.fromJsonList(json) - Parse JSON string to list
	if isFunctionCall(expr, "apoc.convert.fromjsonlist") {
		inner := strings.TrimSpace(expr[25 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		jsonStr, ok := val.(string)
		if !ok {
			return nil
		}
		jsonStr = strings.Trim(jsonStr, "'\"")
		var result []interface{}
		if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
			return nil
		}
		return result
	}

	// apoc.meta.type(value) - Get the Cypher type name of a value
	if isFunctionCall(expr, "apoc.meta.type") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		return getCypherType(val)
	}

	// apoc.meta.isType(value, typeName) - Check if value is of given type
	if isFunctionCall(expr, "apoc.meta.istype") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val := e.evaluateExpressionWithContext(args[0], nodes, rels)
			typeVal := e.evaluateExpressionWithContext(args[1], nodes, rels)
			typeName, ok := typeVal.(string)
			if !ok {
				return false
			}
			typeName = strings.Trim(typeName, "'\"")
			actualType := getCypherType(val)
			return strings.EqualFold(actualType, typeName)
		}
		return false
	}

	// apoc.map.merge(map1, map2) - Merge two maps (map2 values override map1)
	if isFunctionCall(expr, "apoc.map.merge") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			map1 := e.evaluateExpressionWithContext(args[0], nodes, rels)
			map2 := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return mergeMaps(map1, map2)
		}
		return nil
	}

	// apoc.map.fromPairs(list) - Create map from list of [key, value] pairs
	if isFunctionCall(expr, "apoc.map.frompairs") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		listVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		return fromPairs(listVal)
	}

	// apoc.map.fromLists(keys, values) - Create map from parallel lists
	if isFunctionCall(expr, "apoc.map.fromlists") {
		inner := strings.TrimSpace(expr[19 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			keys := e.evaluateExpressionWithContext(args[0], nodes, rels)
			values := e.evaluateExpressionWithContext(args[1], nodes, rels)
			return fromLists(keys, values)
		}
		return nil
	}

	// ========================================
	// Trigonometric Functions
	// ========================================

	// sin(x) - sine of x (radians)
	if strings.HasPrefix(lowerExpr, "sin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sin(f)
		}
		return nil
	}

	// cos(x) - cosine of x (radians)
	if strings.HasPrefix(lowerExpr, "cos(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cos(f)
		}
		return nil
	}

	// tan(x) - tangent of x (radians)
	if strings.HasPrefix(lowerExpr, "tan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tan(f)
		}
		return nil
	}

	// cot(x) - cotangent of x (radians)
	if strings.HasPrefix(lowerExpr, "cot(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return 1.0 / math.Tan(f)
		}
		return nil
	}

	// asin(x) - arc sine
	if strings.HasPrefix(lowerExpr, "asin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Asin(f)
		}
		return nil
	}

	// acos(x) - arc cosine
	if strings.HasPrefix(lowerExpr, "acos(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Acos(f)
		}
		return nil
	}

	// atan(x) - arc tangent
	if strings.HasPrefix(lowerExpr, "atan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Atan(f)
		}
		return nil
	}

	// atan2(y, x) - arc tangent of y/x
	if strings.HasPrefix(lowerExpr, "atan2(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			y, ok1 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			x, ok2 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Atan2(y, x)
			}
		}
		return nil
	}

	// ========================================
	// Exponential and Logarithmic Functions
	// ========================================

	// exp(x) - e^x
	if strings.HasPrefix(lowerExpr, "exp(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Exp(f)
		}
		return nil
	}

	// log(x) - natural logarithm
	if strings.HasPrefix(lowerExpr, "log(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log(f)
		}
		return nil
	}

	// log10(x) - base-10 logarithm
	if strings.HasPrefix(lowerExpr, "log10(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Log10(f)
		}
		return nil
	}

	// sqrt(x) - square root
	if strings.HasPrefix(lowerExpr, "sqrt(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sqrt(f)
		}
		return nil
	}

	// ========================================
	// Angle Conversion Functions
	// ========================================

	// radians(degrees) - convert degrees to radians
	if strings.HasPrefix(lowerExpr, "radians(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * math.Pi / 180.0
		}
		return nil
	}

	// degrees(radians) - convert radians to degrees
	if strings.HasPrefix(lowerExpr, "degrees(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return f * 180.0 / math.Pi
		}
		return nil
	}

	// haversin(x) - half of versine = (1 - cos(x))/2
	if strings.HasPrefix(lowerExpr, "haversin(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return (1 - math.Cos(f)) / 2
		}
		return nil
	}

	// sinh(x) - hyperbolic sine (Neo4j 2025.06+)
	if strings.HasPrefix(lowerExpr, "sinh(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Sinh(f)
		}
		return nil
	}

	// cosh(x) - hyperbolic cosine (Neo4j 2025.06+)
	if strings.HasPrefix(lowerExpr, "cosh(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Cosh(f)
		}
		return nil
	}

	// tanh(x) - hyperbolic tangent (Neo4j 2025.06+)
	if strings.HasPrefix(lowerExpr, "tanh(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.Tanh(f)
		}
		return nil
	}

	// coth(x) - hyperbolic cotangent (Neo4j 2025.06+)
	if strings.HasPrefix(lowerExpr, "coth(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			sinh := math.Sinh(f)
			if sinh == 0 {
				return math.NaN()
			}
			return math.Cosh(f) / sinh
		}
		return nil
	}

	// power(base, exponent) - raise base to power of exponent
	if strings.HasPrefix(lowerExpr, "power(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			base, ok1 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			exp, ok2 := toFloat64(e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
			if ok1 && ok2 {
				return math.Pow(base, exp)
			}
		}
		return nil
	}

	// ========================================
	// Mathematical Constants
	// ========================================

	// pi() - mathematical constant π
	if lowerExpr == "pi()" {
		return math.Pi
	}

	// e() - mathematical constant e
	if lowerExpr == "e()" {
		return math.E
	}

	// ========================================
	// Relationship Functions
	// ========================================

	// startNode(r) - return start node of relationship
	if strings.HasPrefix(lowerExpr, "startnode(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.StartNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// endNode(r) - return end node of relationship
	if strings.HasPrefix(lowerExpr, "endnode(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		if rel, ok := rels[inner]; ok {
			node, err := e.storage.GetNode(rel.EndNode)
			if err == nil {
				return e.nodeToMap(node)
			}
		}
		return nil
	}

	// nodes(path) - return list of nodes in a path
	if strings.HasPrefix(lowerExpr, "nodes(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// For now, return nodes from node context
		if node, ok := nodes[inner]; ok {
			return []interface{}{e.nodeToMap(node)}
		}
		return []interface{}{}
	}

	// relationships(path) - return list of relationships in a path
	if strings.HasPrefix(lowerExpr, "relationships(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[14 : len(expr)-1])
		// For now, return rels from rel context
		if rel, ok := rels[inner]; ok {
			return []interface{}{map[string]interface{}{
				"id":         string(rel.ID),
				"type":       rel.Type,
				"properties": rel.Properties,
			}}
		}
		return []interface{}{}
	}

	// ========================================
	// Null Check Functions
	// ========================================

	// isEmpty(list/map/string) - check if empty
	if strings.HasPrefix(lowerExpr, "isempty(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := val.(type) {
		case nil:
			return true
		case string:
			return len(v) == 0
		case []interface{}:
			return len(v) == 0
		case map[string]interface{}:
			return len(v) == 0
		}
		return false
	}

	// isNaN(number) - check if not a number
	if strings.HasPrefix(lowerExpr, "isnan(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if f, ok := toFloat64(val); ok {
			return math.IsNaN(f)
		}
		return false
	}

	// nullIf(val1, val2) - return null if val1 = val2
	if strings.HasPrefix(lowerExpr, "nullif(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			val1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			val2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
			if fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2) {
				return nil
			}
			return val1
		}
		return nil
	}

	// ========================================
	// String Functions (additional)
	// ========================================

	// btrim(string) / btrim(string, chars) - trim both sides
	if strings.HasPrefix(lowerExpr, "btrim(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 1 {
			str := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels))
			if len(args) >= 2 {
				chars := fmt.Sprintf("%v", e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels))
				return strings.Trim(str, chars)
			}
			return strings.TrimSpace(str)
		}
		return nil
	}

	// char_length(string) / character_length(string)
	if (strings.HasPrefix(lowerExpr, "char_length(") || strings.HasPrefix(lowerExpr, "character_length(")) && strings.HasSuffix(expr, ")") {
		startIdx := 12
		if strings.HasPrefix(lowerExpr, "character_length(") {
			startIdx = 17
		}
		inner := strings.TrimSpace(expr[startIdx : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			return int64(len([]rune(str))) // Character count, not byte count
		}
		return nil
	}

	// normalize(string) - Unicode normalization
	if strings.HasPrefix(lowerExpr, "normalize(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if str, ok := val.(string); ok {
			// Simple normalization - just return the string (full Unicode normalization would require unicode package)
			return str
		}
		return nil
	}

	// ========================================
	// Aggregation Functions (in expression context)
	// ========================================

	// percentileCont(expr, percentile) - continuous percentile
	if strings.HasPrefix(lowerExpr, "percentilecont(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// percentileDisc(expr, percentile) - discrete percentile
	if strings.HasPrefix(lowerExpr, "percentiledisc(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			// In single-row context, just return the value
			return e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		}
		return nil
	}

	// stDev(expr) - standard deviation
	if strings.HasPrefix(lowerExpr, "stdev(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// stDevP(expr) - population standard deviation
	if strings.HasPrefix(lowerExpr, "stdevp(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		// In single-row context, return 0
		_ = inner
		return float64(0)
	}

	// ========================================
	// Reduce Function
	// ========================================

	// reduce(acc = initial, x IN list | expr) - reduce a list
	if strings.HasPrefix(lowerExpr, "reduce(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])

		// Parse: acc = initial, x IN list | expr
		eqIdx := strings.Index(inner, "=")
		commaIdx := strings.Index(inner, ",")
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		pipeIdx := strings.Index(inner, "|")

		if eqIdx > 0 && commaIdx > eqIdx && inIdx > commaIdx && pipeIdx > inIdx {
			accName := strings.TrimSpace(inner[:eqIdx])
			initialExpr := strings.TrimSpace(inner[eqIdx+1 : commaIdx])
			varName := strings.TrimSpace(inner[commaIdx+1 : inIdx])
			listExpr := strings.TrimSpace(inner[inIdx+4 : pipeIdx])
			reduceExpr := strings.TrimSpace(inner[pipeIdx+1:])

			// Get initial value
			acc := e.evaluateExpressionWithContext(initialExpr, nodes, rels)

			// Get list
			list := e.evaluateExpressionWithContext(listExpr, nodes, rels)

			var items []interface{}
			switch v := list.(type) {
			case []interface{}:
				items = v
			default:
				items = []interface{}{list}
			}

			// Apply reduce
			for _, item := range items {
				// Create context with acc and item
				tempNodes := make(map[string]*storage.Node)
				for k, v := range nodes {
					tempNodes[k] = v
				}

				// Store acc and item as pseudo-properties (simplified)
				// For a proper implementation, we'd need to handle this more comprehensively
				substitutedExpr := strings.ReplaceAll(reduceExpr, accName, fmt.Sprintf("%v", acc))
				substitutedExpr = strings.ReplaceAll(substitutedExpr, varName, fmt.Sprintf("%v", item))

				acc = e.evaluateExpressionWithContext(substitutedExpr, tempNodes, rels)
			}

			return acc
		}
		return nil
	}

	// ========================================
	// Vector Functions
	// ========================================

	// vector.similarity.cosine(v1, v2)
	if strings.HasPrefix(lowerExpr, "vector.similarity.cosine(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[25 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return cosineSimilarity(vec1, vec2)
			}
		}
		return nil
	}

	// vector.similarity.euclidean(v1, v2)
	if strings.HasPrefix(lowerExpr, "vector.similarity.euclidean(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[28 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			v1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			v2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			vec1, ok1 := toFloat64Slice(v1)
			vec2, ok2 := toFloat64Slice(v2)

			if ok1 && ok2 && len(vec1) == len(vec2) {
				return euclideanSimilarity(vec1, vec2)
			}
		}
		return nil
	}

	// ========================================
	// Point/Spatial Functions (basic support)
	// ========================================

	// point({x: val, y: val}) or point({latitude: val, longitude: val})
	if strings.HasPrefix(lowerExpr, "point(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[6 : len(expr)-1])
		// Return the point as a map
		if strings.HasPrefix(inner, "{") && strings.HasSuffix(inner, "}") {
			props := e.parseProperties(inner)
			return props
		}
		return nil
	}

	// distance(p1, p2) - Euclidean distance between two points
	if strings.HasPrefix(lowerExpr, "distance(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[9 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// withinBBox(point, lowerLeft, upperRight) - checks if point is within bounding box
	if strings.HasPrefix(lowerExpr, "withinbbox(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Try x/y coordinates
		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		// Try lat/lon
		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.x(point) - get x coordinate
	if strings.HasPrefix(lowerExpr, "point.x(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if x, ok := m["x"]; ok {
				if v, ok := toFloat64(x); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.y(point) - get y coordinate
	if strings.HasPrefix(lowerExpr, "point.y(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if y, ok := m["y"]; ok {
				if v, ok := toFloat64(y); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.z(point) - get z coordinate (3D points)
	if strings.HasPrefix(lowerExpr, "point.z(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.latitude(point) - get latitude
	if strings.HasPrefix(lowerExpr, "point.latitude(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lat, ok := m["latitude"]; ok {
				if v, ok := toFloat64(lat); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.longitude(point) - get longitude
	if strings.HasPrefix(lowerExpr, "point.longitude(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[16 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if lon, ok := m["longitude"]; ok {
				if v, ok := toFloat64(lon); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.srid(point) - get SRID (Spatial Reference System Identifier)
	if strings.HasPrefix(lowerExpr, "point.srid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			if srid, ok := m["srid"]; ok {
				return srid
			}
			// Default SRID based on coordinate type
			if _, ok := m["latitude"]; ok {
				return int64(4326) // WGS84
			}
			return int64(7203) // Cartesian 2D
		}
		return nil
	}

	// point.distance(p1, p2) - alias for distance(p1, p2)
	if strings.HasPrefix(lowerExpr, "point.distance(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) >= 2 {
			p1 := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
			p2 := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)

			m1, ok1 := p1.(map[string]interface{})
			m2, ok2 := p2.(map[string]interface{})

			if ok1 && ok2 {
				// Try x/y coordinates
				x1, y1, hasXY1 := getXY(m1)
				x2, y2, hasXY2 := getXY(m2)
				if hasXY1 && hasXY2 {
					return math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
				}

				// Try lat/long (haversine distance in meters)
				lat1, lon1, hasLatLon1 := getLatLon(m1)
				lat2, lon2, hasLatLon2 := getLatLon(m2)
				if hasLatLon1 && hasLatLon2 {
					return haversineDistance(lat1, lon1, lat2, lon2)
				}
			}
		}
		return nil
	}

	// point.withinBBox(point, lowerLeft, upperRight) - alias for withinBBox
	if strings.HasPrefix(lowerExpr, "point.withinbbox(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		lowerLeft := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		upperRight := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		llm, ok2 := lowerLeft.(map[string]interface{})
		urm, ok3 := upperRight.(map[string]interface{})
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		px, py, hasXY := getXY(pm)
		llx, lly, hasLL := getXY(llm)
		urx, ury, hasUR := getXY(urm)

		if hasXY && hasLL && hasUR {
			return px >= llx && px <= urx && py >= lly && py <= ury
		}

		plat, plon, hasLatLon := getLatLon(pm)
		lllat, lllon, hasLLLatLon := getLatLon(llm)
		urlat, urlon, hasURLatLon := getLatLon(urm)

		if hasLatLon && hasLLLatLon && hasURLatLon {
			return plat >= lllat && plat <= urlat && plon >= lllon && plon <= urlon
		}

		return false
	}

	// point.withinDistance(point, center, distance) - check if point is within distance of center
	if strings.HasPrefix(lowerExpr, "point.withindistance(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[21 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) < 3 {
			return false
		}
		point := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		center := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		maxDist := e.evaluateExpressionWithContext(strings.TrimSpace(args[2]), nodes, rels)

		pm, ok1 := point.(map[string]interface{})
		cm, ok2 := center.(map[string]interface{})
		dist, ok3 := toFloat64(maxDist)
		if !ok1 || !ok2 || !ok3 {
			return false
		}

		// Calculate distance between point and center
		x1, y1, hasXY1 := getXY(pm)
		x2, y2, hasXY2 := getXY(cm)
		if hasXY1 && hasXY2 {
			actualDist := math.Sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1))
			return actualDist <= dist
		}

		lat1, lon1, hasLatLon1 := getLatLon(pm)
		lat2, lon2, hasLatLon2 := getLatLon(cm)
		if hasLatLon1 && hasLatLon2 {
			actualDist := haversineDistance(lat1, lon1, lat2, lon2)
			return actualDist <= dist
		}

		return false
	}

	// point.height(point) - get height/altitude (alias for z coordinate)
	if strings.HasPrefix(lowerExpr, "point.height(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[13 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Try z first (3D Cartesian)
			if z, ok := m["z"]; ok {
				if v, ok := toFloat64(z); ok {
					return v
				}
			}
			// Try height (geographic)
			if h, ok := m["height"]; ok {
				if v, ok := toFloat64(h); ok {
					return v
				}
			}
			// Try altitude (alternative name)
			if alt, ok := m["altitude"]; ok {
				if v, ok := toFloat64(alt); ok {
					return v
				}
			}
		}
		return nil
	}

	// point.crs(point) - get Coordinate Reference System name
	if strings.HasPrefix(lowerExpr, "point.crs(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[10 : len(expr)-1])
		val := e.evaluateExpressionWithContext(inner, nodes, rels)
		if m, ok := val.(map[string]interface{}); ok {
			// Check if CRS is explicitly set
			if crs, ok := m["crs"]; ok {
				return crs
			}
			// Infer CRS from coordinate type
			if _, ok := m["latitude"]; ok {
				if _, ok := m["height"]; ok {
					return "wgs-84-3d"
				}
				return "wgs-84"
			}
			if _, ok := m["z"]; ok {
				return "cartesian-3d"
			}
			return "cartesian"
		}
		return nil
	}

	// polygon(points) - create a polygon geometry from a list of points
	if strings.HasPrefix(lowerExpr, "polygon(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		
		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)
			
			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}
			
			// Validate that we have at least 3 points for a valid polygon
			if len(pointList) < 3 {
				return nil
			}
			
			// Return a polygon structure
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}
		
		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 3 {
				return nil
			}
			return map[string]interface{}{
				"type":   "polygon",
				"points": pointList,
			}
		}
		return nil
	}

	// lineString(points) - create a lineString geometry from a list of points
	if strings.HasPrefix(lowerExpr, "linestring(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[11 : len(expr)-1])
		
		// Check if inner is a list literal [...]
		if strings.HasPrefix(inner, "[") && strings.HasSuffix(inner, "]") {
			// Parse and evaluate list elements manually
			listContent := inner[1 : len(inner)-1]
			pointExprs := e.splitFunctionArgs(listContent)
			
			pointList := make([]interface{}, 0, len(pointExprs))
			for _, pointExpr := range pointExprs {
				evalPoint := e.evaluateExpressionWithContext(strings.TrimSpace(pointExpr), nodes, rels)
				pointList = append(pointList, evalPoint)
			}
			
			// Validate that we have at least 2 points for a valid lineString
			if len(pointList) < 2 {
				return nil
			}
			
			// Return a lineString structure
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}
		
		// Otherwise try evaluating as variable or expression
		pointsVal := e.evaluateExpressionWithContext(inner, nodes, rels)
		if pointList, ok := pointsVal.([]interface{}); ok {
			if len(pointList) < 2 {
				return nil
			}
			return map[string]interface{}{
				"type":   "linestring",
				"points": pointList,
			}
		}
		return nil
	}

	// point.intersects(point, polygon) - check if point intersects with polygon
	if strings.HasPrefix(lowerExpr, "point.intersects(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[17 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}
		
		pointVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		polygonVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		
		pm, ok1 := pointVal.(map[string]interface{})
		polygonMap, ok2 := polygonVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}
		
		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}
		
		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}
		
		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// point.contains(polygon, point) - check if polygon contains point
	if strings.HasPrefix(lowerExpr, "point.contains(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[15 : len(expr)-1])
		args := e.splitFunctionArgs(inner)
		if len(args) < 2 {
			return false
		}
		
		polygonVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[0]), nodes, rels)
		pointVal := e.evaluateExpressionWithContext(strings.TrimSpace(args[1]), nodes, rels)
		
		polygonMap, ok1 := polygonVal.(map[string]interface{})
		pm, ok2 := pointVal.(map[string]interface{})
		if !ok1 || !ok2 {
			return false
		}
		
		// Extract polygon points
		polygonPoints := extractPolygonPoints(polygonMap)
		if polygonPoints == nil {
			return false
		}
		
		// Get point coordinates
		px, py, hasXY := getXY(pm)
		if !hasXY {
			// Try lat/lon
			var hasLatLon bool
			px, py, hasLatLon = getLatLon(pm)
			if !hasLatLon {
				return false
			}
		}
		
		// Use point-in-polygon algorithm
		return pointInPolygon(px, py, polygonPoints)
	}

	// ========================================
	// List Predicate Functions
	// ========================================

	// all(variable IN list WHERE predicate) - check if all elements match
	if strings.HasPrefix(lowerExpr, "all(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		// Parse "variable IN list WHERE predicate"
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			// Create temporary context with variable
			tempNodes := make(map[string]*storage.Node)
			for k, v := range nodes {
				tempNodes[k] = v
			}
			// For simple values, we need to substitute in the predicate
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, tempNodes, rels)
			if result != true {
				return false
			}
		}
		return true
	}

	// any(variable IN list WHERE predicate) - check if any element matches
	if strings.HasPrefix(lowerExpr, "any(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[4 : len(expr)-1])
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				return true
			}
		}
		return false
	}

	// none(variable IN list WHERE predicate) - check if no element matches
	if strings.HasPrefix(lowerExpr, "none(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[5 : len(expr)-1])
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return true
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return true
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return true
		}

		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				return false
			}
		}
		return true
	}

	// single(variable IN list WHERE predicate) - check if exactly one element matches
	if strings.HasPrefix(lowerExpr, "single(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return false
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return false
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return false
		}

		matchCount := 0
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			result := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if result == true {
				matchCount++
				if matchCount > 1 {
					return false
				}
			}
		}
		return matchCount == 1
	}

	// ========================================
	// Additional List Functions
	// ========================================

	// filter(variable IN list WHERE predicate) - filter list elements
	if strings.HasPrefix(lowerExpr, "filter(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[7 : len(expr)-1])
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		whereIdx := strings.Index(strings.ToLower(rest), " where ")
		if whereIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:whereIdx])
		predicate := strings.TrimSpace(rest[whereIdx+7:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, 0)
		for _, item := range listVal {
			predWithVal := strings.ReplaceAll(predicate, varName, fmt.Sprintf("%v", item))
			res := e.evaluateExpressionWithContext(predWithVal, nodes, rels)
			if res == true {
				result = append(result, item)
			}
		}
		return result
	}

	// extract(variable IN list | expression) - transform list elements
	if strings.HasPrefix(lowerExpr, "extract(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[8 : len(expr)-1])
		inIdx := strings.Index(strings.ToLower(inner), " in ")
		if inIdx == -1 {
			return []interface{}{}
		}
		varName := strings.TrimSpace(inner[:inIdx])
		rest := inner[inIdx+4:]
		pipeIdx := strings.Index(rest, " | ")
		if pipeIdx == -1 {
			return []interface{}{}
		}
		listExpr := strings.TrimSpace(rest[:pipeIdx])
		transform := strings.TrimSpace(rest[pipeIdx+3:])

		list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
		listVal, ok := list.([]interface{})
		if !ok {
			return []interface{}{}
		}

		result := make([]interface{}, len(listVal))
		for i, item := range listVal {
			// Simple variable substitution for primitive values
			transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
			result[i] = e.evaluateExpressionWithContext(transformWithVal, nodes, rels)
		}
		return result
	}

	// [x IN list | expression] - list comprehension
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") && strings.Contains(expr, " IN ") && strings.Contains(expr, " | ") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		inIdx := strings.Index(strings.ToUpper(inner), " IN ")
		if inIdx > 0 {
			varName := strings.TrimSpace(inner[:inIdx])
			rest := inner[inIdx+4:]
			pipeIdx := strings.Index(rest, " | ")
			if pipeIdx > 0 {
				listExpr := strings.TrimSpace(rest[:pipeIdx])
				transform := strings.TrimSpace(rest[pipeIdx+3:])

				list := e.evaluateExpressionWithContext(listExpr, nodes, rels)
				listVal, ok := list.([]interface{})
				if !ok {
					return []interface{}{}
				}

				result := make([]interface{}, len(listVal))
				for i, item := range listVal {
					transformWithVal := strings.ReplaceAll(transform, varName, fmt.Sprintf("%v", item))
					result[i] = e.evaluateExpressionWithContext(transformWithVal, nodes, rels)
				}
				return result
			}
		}
	}

	// ========================================
	// CASE WHEN Expressions (must be before operators)
	// ========================================
	if strings.HasPrefix(lowerExpr, "case") && strings.HasSuffix(lowerExpr, "end") {
		return e.evaluateCaseExpression(expr, nodes, rels)
	}

	// ========================================
	// Boolean/Comparison Operators (must be before property access)
	// ========================================

	// NOT expr
	if strings.HasPrefix(lowerExpr, "not ") {
		inner := strings.TrimSpace(expr[4:])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		if b, ok := result.(bool); ok {
			return !b
		}
		return nil
	}

	// BETWEEN must be checked before AND (because BETWEEN x AND y uses AND)
	if e.hasStringPredicate(expr, " BETWEEN ") {
		betweenParts := e.splitByOperator(expr, " BETWEEN ")
		if len(betweenParts) == 2 {
			value := e.evaluateExpressionWithContext(betweenParts[0], nodes, rels)
			// For BETWEEN, we need to split by AND but only in the range part
			rangeParts := strings.SplitN(strings.ToUpper(betweenParts[1]), " AND ", 2)
			if len(rangeParts) == 2 {
				// Get the actual case-preserved parts
				andIdx := strings.Index(strings.ToUpper(betweenParts[1]), " AND ")
				minPart := strings.TrimSpace(betweenParts[1][:andIdx])
				maxPart := strings.TrimSpace(betweenParts[1][andIdx+5:])
				minVal := e.evaluateExpressionWithContext(minPart, nodes, rels)
				maxVal := e.evaluateExpressionWithContext(maxPart, nodes, rels)
				return (e.compareGreater(value, minVal) || e.compareEqual(value, minVal)) &&
					(e.compareLess(value, maxVal) || e.compareEqual(value, maxVal))
			}
		}
	}

	// AND operator
	if e.hasLogicalOperator(expr, " AND ") {
		return e.evaluateLogicalAnd(expr, nodes, rels)
	}

	// OR operator
	if e.hasLogicalOperator(expr, " OR ") {
		return e.evaluateLogicalOr(expr, nodes, rels)
	}

	// XOR operator
	if e.hasLogicalOperator(expr, " XOR ") {
		return e.evaluateLogicalXor(expr, nodes, rels)
	}

	// Comparison operators (=, <>, <, >, <=, >=)
	if e.hasComparisonOperator(expr) {
		return e.evaluateComparisonExpr(expr, nodes, rels)
	}

	// Arithmetic operators (*, /, %, -, +)
	// NOTE: Arithmetic is checked BEFORE string concatenation to support date/duration arithmetic
	if e.hasArithmeticOperator(expr) {
		result := e.evaluateArithmeticExpr(expr, nodes, rels)
		if result != nil {
			return result
		}
		// If arithmetic returned nil, fall through to string concatenation for + operator
	}

	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// This is a fallback when arithmetic didn't apply (e.g., string + string)
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(expr, nodes, rels)
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		switch v := result.(type) {
		case int64:
			return -v
		case float64:
			return -v
		case int:
			return -v
		}
	}

	// ========================================
	// Null Predicates (IS NULL, IS NOT NULL)
	// ========================================
	if strings.HasSuffix(lowerExpr, " is null") {
		inner := strings.TrimSpace(expr[:len(expr)-8])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		return result == nil
	}
	if strings.HasSuffix(lowerExpr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContext(inner, nodes, rels)
		return result != nil
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	if e.hasStringPredicate(expr, " STARTS WITH ") {
		parts := e.splitByOperator(expr, " STARTS WITH ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.HasPrefix(leftStr, rightStr)
			}
			return false
		}
	}
	if e.hasStringPredicate(expr, " ENDS WITH ") {
		parts := e.splitByOperator(expr, " ENDS WITH ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.HasSuffix(leftStr, rightStr)
			}
			return false
		}
	}
	if e.hasStringPredicate(expr, " CONTAINS ") {
		parts := e.splitByOperator(expr, " CONTAINS ")
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			leftStr, ok1 := left.(string)
			rightStr, ok2 := right.(string)
			if ok1 && ok2 {
				return strings.Contains(leftStr, rightStr)
			}
			return false
		}
	}

	// ========================================
	// IN Operator (value IN list)
	// ========================================
	if e.hasStringPredicate(expr, " IN ") {
		parts := e.splitByOperator(expr, " IN ")
		if len(parts) == 2 {
			value := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			listVal := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			if list, ok := listVal.([]interface{}); ok {
				for _, item := range list {
					if e.compareEqual(value, item) {
						return true
					}
				}
				return false
			}
			return false
		}
	}

	// ========================================
	// Property Access: n.property
	// ========================================
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node, ok := nodes[varName]; ok {
			// Don't return internal properties like embeddings
			if e.isInternalProperty(propName) {
				return nil
			}
			if val, ok := node.Properties[propName]; ok {
				return val
			}
			return nil
		}
		if rel, ok := rels[varName]; ok {
			if val, ok := rel.Properties[propName]; ok {
				return val
			}
			return nil
		}
	}

	// ========================================
	// Variable Reference - return whole node/rel
	// ========================================
	if node, ok := nodes[expr]; ok {
		// Check if this is a scalar wrapper (pseudo-node created for YIELD variables)
		// If it only has a "value" property, return that value directly
		if len(node.Properties) == 1 {
			if val, hasValue := node.Properties["value"]; hasValue {
				return val
			}
		}
		return e.nodeToMap(node)
	}
	if rel, ok := rels[expr]; ok {
		return map[string]interface{}{
			"id":         string(rel.ID),
			"type":       rel.Type,
			"properties": rel.Properties,
		}
	}

	// ========================================
	// Literals
	// ========================================

	// null
	if lowerExpr == "null" {
		return nil
	}

	// Boolean
	if lowerExpr == "true" {
		return true
	}
	if lowerExpr == "false" {
		return false
	}

	// String literal (single or double quotes)
	if len(expr) >= 2 {
		if (expr[0] == '\'' && expr[len(expr)-1] == '\'') ||
			(expr[0] == '"' && expr[len(expr)-1] == '"') {
			return expr[1 : len(expr)-1]
		}
	}

	// Number literal
	if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return num
	}
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num
	}

	// Array literal [a, b, c]
	if strings.HasPrefix(expr, "[") && strings.HasSuffix(expr, "]") {
		return e.parseArrayValue(expr)
	}

	// Map literal {key: value}
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return e.parseProperties(expr)
	}

	// Check if this looks like a variable reference (identifier pattern)
	// If it's a valid identifier and not found in nodes/rels, it should be null
	// This handles cases like OPTIONAL MATCH where the variable might not exist
	isValidIdentifier := true
	for i, ch := range expr {
		if i == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
				isValidIdentifier = false
				break
			}
		} else {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
				isValidIdentifier = false
				break
			}
		}
	}
	if isValidIdentifier && len(expr) > 0 {
		// This looks like an unresolved variable reference - return null
		return nil
	}

	// Unknown - return as string (for string literals without quotes, etc.)
	return expr
}

// evaluateStringConcatWithContext handles string concatenation with + operator.
func (e *StorageExecutor) evaluateStringConcatWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) string {
	var result strings.Builder

	// Split by + but respect quotes and parentheses
	parts := e.splitByPlus(expr)

	for _, part := range parts {
		val := e.evaluateExpressionWithContext(part, nodes, rels)
		result.WriteString(fmt.Sprintf("%v", val))
	}

	return result.String()
}

// ========================================
// Logical Operators
// ========================================

// hasLogicalOperator checks if the expression has a logical operator outside of quotes/parentheses
func (e *StorageExecutor) hasLogicalOperator(expr, op string) bool {
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)

	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i <= len(upperExpr)-len(upperOp); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case !inQuote && parenDepth == 0:
			if upperExpr[i:i+len(upperOp)] == upperOp {
				return true
			}
		}
	}
	return false
}

// evaluateLogicalAnd evaluates expr1 AND expr2
func (e *StorageExecutor) evaluateLogicalAnd(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " AND ")
	if len(parts) < 2 {
		return nil
	}

	for _, part := range parts {
		result := e.evaluateExpressionWithContext(part, nodes, rels)
		if result != true {
			return false
		}
	}
	return true
}

// evaluateLogicalOr evaluates expr1 OR expr2
func (e *StorageExecutor) evaluateLogicalOr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " OR ")
	if len(parts) < 2 {
		return nil
	}

	for _, part := range parts {
		result := e.evaluateExpressionWithContext(part, nodes, rels)
		if result == true {
			return true
		}
	}
	return false
}

// evaluateLogicalXor evaluates expr1 XOR expr2
func (e *StorageExecutor) evaluateLogicalXor(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	parts := e.splitByOperator(expr, " XOR ")
	if len(parts) != 2 {
		return nil
	}

	left := e.evaluateExpressionWithContext(parts[0], nodes, rels) == true
	right := e.evaluateExpressionWithContext(parts[1], nodes, rels) == true
	return left != right
}

// ========================================
// Comparison Operators
// ========================================

// hasComparisonOperator checks if the expression has a comparison operator
func (e *StorageExecutor) hasComparisonOperator(expr string) bool {
	ops := []string{"<>", "<=", ">=", "=~", "!=", "=", "<", ">"}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// hasOperatorOutsideQuotes checks if operator exists outside quotes and parentheses
func (e *StorageExecutor) hasOperatorOutsideQuotes(expr, op string) bool {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0

	for i := 0; i <= len(expr)-len(op); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case !inQuote && parenDepth == 0:
			if expr[i:i+len(op)] == op {
				// Make sure = is not part of <= or >=
				if op == "=" {
					if i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
						continue
					}
					if i < len(expr)-1 && expr[i+1] == '~' {
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

// evaluateComparisonExpr evaluates comparison expressions
func (e *StorageExecutor) evaluateComparisonExpr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	// Try operators in order of specificity
	ops := []struct {
		op   string
		eval func(left, right interface{}) bool
	}{
		{"<>", func(l, r interface{}) bool { return !e.compareEqual(l, r) }},
		{"!=", func(l, r interface{}) bool { return !e.compareEqual(l, r) }},
		{"<=", func(l, r interface{}) bool { return e.compareLess(l, r) || e.compareEqual(l, r) }},
		{">=", func(l, r interface{}) bool { return e.compareGreater(l, r) || e.compareEqual(l, r) }},
		{"=~", e.compareRegex},
		{"=", e.compareEqual},
		{"<", e.compareLess},
		{">", e.compareGreater},
	}

	for _, op := range ops {
		parts := e.splitByOperator(expr, op.op)
		if len(parts) == 2 {
			left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
			right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
			return op.eval(left, right)
		}
	}

	return nil
}

// ========================================
// Arithmetic Operators
// ========================================

// hasArithmeticOperator checks if the expression has arithmetic operators (*, /, %, -)
func (e *StorageExecutor) hasArithmeticOperator(expr string) bool {
	// Include + for date arithmetic (date + duration)
	ops := []string{" + ", "*", "/", "%", " - "}
	for _, op := range ops {
		if e.hasOperatorOutsideQuotes(expr, op) {
			return true
		}
	}
	return false
}

// evaluateArithmeticExpr evaluates arithmetic expressions
func (e *StorageExecutor) evaluateArithmeticExpr(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	// Handle + operator (date + duration, or numeric addition)
	if parts := e.splitByOperator(expr, " + "); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.add(left, right)
	}

	// Handle * operator
	if parts := e.splitByOperator(expr, "*"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.multiply(left, right)
	}

	// Handle / operator
	if parts := e.splitByOperator(expr, "/"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.divide(left, right)
	}

	// Handle % operator
	if parts := e.splitByOperator(expr, "%"); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.modulo(left, right)
	}

	// Handle - operator (binary subtraction, not unary minus)
	if parts := e.splitByOperator(expr, " - "); len(parts) == 2 {
		left := e.evaluateExpressionWithContext(parts[0], nodes, rels)
		right := e.evaluateExpressionWithContext(parts[1], nodes, rels)
		return e.subtract(left, right)
	}

	return nil
}

// splitByOperator splits expression by operator respecting quotes and parentheses
func (e *StorageExecutor) splitByOperator(expr, op string) []string {
	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)

	for i := 0; i <= len(expr)-len(op); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case !inQuote && parenDepth == 0:
			if upperExpr[i:i+len(upperOp)] == upperOp {
				// Additional check for = not being part of <= or >=
				if op == "=" {
					if i > 0 && (expr[i-1] == '<' || expr[i-1] == '>' || expr[i-1] == '!') {
						continue
					}
				}
				left := strings.TrimSpace(expr[:i])
				right := strings.TrimSpace(expr[i+len(op):])
				return []string{left, right}
			}
		}
	}
	return []string{expr}
}

// Arithmetic helper functions

// add handles addition including date + duration
func (e *StorageExecutor) add(left, right interface{}) interface{} {
	// Handle date/datetime + duration
	if dur, ok := right.(*CypherDuration); ok {
		result := addDurationToDate(left, dur)
		if result != "" {
			return result
		}
	}
	// Handle duration + date/datetime (commutative)
	if dur, ok := left.(*CypherDuration); ok {
		result := addDurationToDate(right, dur)
		if result != "" {
			return result
		}
	}

	// Standard numeric addition
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l + r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

func (e *StorageExecutor) multiply(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l * r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

func (e *StorageExecutor) divide(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return l / r
}

func (e *StorageExecutor) modulo(left, right interface{}) interface{} {
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR || r == 0 {
		return nil
	}
	return int64(l) % int64(r)
}

func (e *StorageExecutor) subtract(left, right interface{}) interface{} {
	// Handle date - duration = date
	if dur, ok := right.(*CypherDuration); ok {
		result := subtractDurationFromDate(left, dur)
		if result != "" {
			return result
		}
	}

	// Handle date - date = duration
	leftTime := parseDateTime(left)
	rightTime := parseDateTime(right)
	if !leftTime.IsZero() && !rightTime.IsZero() {
		return durationBetween(left, right)
	}

	// Standard numeric subtraction
	l, okL := toFloat64(left)
	r, okR := toFloat64(right)
	if !okL || !okR {
		return nil
	}
	result := l - r
	// Return integer if both were integers
	if _, isInt := left.(int64); isInt {
		if _, isInt := right.(int64); isInt {
			return int64(result)
		}
	}
	return result
}

// hasStringPredicate checks if expression has a string predicate (case-insensitive)
func (e *StorageExecutor) hasStringPredicate(expr, predicate string) bool {
	upperExpr := strings.ToUpper(expr)
	upperPred := strings.ToUpper(predicate)

	inQuote := false
	quoteChar := rune(0)
	parenDepth := 0
	bracketDepth := 0

	for i := 0; i <= len(upperExpr)-len(upperPred); i++ {
		c := rune(expr[i])
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		case c == '(' && !inQuote:
			parenDepth++
		case c == ')' && !inQuote:
			parenDepth--
		case c == '[' && !inQuote:
			bracketDepth++
		case c == ']' && !inQuote:
			bracketDepth--
		case !inQuote && parenDepth == 0 && bracketDepth == 0:
			if upperExpr[i:i+len(upperPred)] == upperPred {
				return true
			}
		}
	}
	return false
}

// ========================================
// APOC Helper Functions
// ========================================

// flattenList recursively flattens nested lists into a single list
func flattenList(val interface{}) []interface{} {
	var result []interface{}
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			// Check if item is also a list
			switch inner := item.(type) {
			case []interface{}:
				result = append(result, flattenList(inner)...)
			default:
				result = append(result, item)
			}
		}
	case []string:
		for _, s := range v {
			result = append(result, s)
		}
	default:
		result = append(result, val)
	}
	return result
}

// toSet removes duplicates from a list while preserving order
func toSet(val interface{}) []interface{} {
	var result []interface{}
	seen := make(map[string]bool)

	addUnique := func(item interface{}) {
		key := fmt.Sprintf("%T:%v", item, item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}

	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			addUnique(item)
		}
	case []string:
		for _, s := range v {
			addUnique(s)
		}
	}
	return result
}

// ========================================
// APOC Collection Function Helpers
// ========================================

// apocCollSum sums numeric values in a list
func apocCollSum(val interface{}) float64 {
	var sum float64
	switch v := val.(type) {
	case []interface{}:
		for _, item := range v {
			if f, ok := toFloat64(item); ok {
				sum += f
			}
		}
	}
	return sum
}

// apocCollAvg calculates average of numeric values in a list
func apocCollAvg(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		var sum float64
		for _, item := range v {
			if f, ok := toFloat64(item); ok {
				sum += f
			}
		}
		return sum / float64(len(v))
	}
	return nil
}

// apocCollMin finds minimum value in a list
func apocCollMin(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		min := v[0]
		minFloat, _ := toFloat64(min)
		for _, item := range v[1:] {
			if f, ok := toFloat64(item); ok && f < minFloat {
				min = item
				minFloat = f
			}
		}
		return min
	}
	return nil
}

// apocCollMax finds maximum value in a list
func apocCollMax(val interface{}) interface{} {
	switch v := val.(type) {
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		max := v[0]
		maxFloat, _ := toFloat64(max)
		for _, item := range v[1:] {
			if f, ok := toFloat64(item); ok && f > maxFloat {
				max = item
				maxFloat = f
			}
		}
		return max
	}
	return nil
}

// apocCollSort sorts a list in ascending order
func apocCollSort(val interface{}) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		copy(result, v)
		// Sort by converting to float64 for comparison
		for i := 0; i < len(result)-1; i++ {
			for j := i + 1; j < len(result); j++ {
				fj, _ := toFloat64(result[j])
				fi, _ := toFloat64(result[i])
				if fj < fi {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
		return result
	}
	return nil
}

// apocCollSortNodes sorts nodes by a property value
func apocCollSortNodes(val interface{}, propName string) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		copy(result, v)
		// Sort by property value
		for i := 0; i < len(result)-1; i++ {
			for j := i + 1; j < len(result); j++ {
				vi := getNodeProperty(result[i], propName)
				vj := getNodeProperty(result[j], propName)
				fj, _ := toFloat64(vj)
				fi, _ := toFloat64(vi)
				if fj < fi {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
		return result
	}
	return nil
}

// getNodeProperty extracts a property from a node (map or *storage.Node)
func getNodeProperty(node interface{}, propName string) interface{} {
	switch n := node.(type) {
	case map[string]interface{}:
		if props, ok := n["properties"].(map[string]interface{}); ok {
			return props[propName]
		}
		return n[propName]
	case *storage.Node:
		return n.Properties[propName]
	}
	return nil
}

// apocCollReverse reverses a list
func apocCollReverse(val interface{}) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[len(v)-1-i] = item
		}
		return result
	}
	return nil
}

// apocCollUnion returns union of two lists (unique elements)
func apocCollUnion(list1, list2 interface{}) []interface{} {
	combined := apocCollUnionAll(list1, list2)
	return toSet(combined)
}

// apocCollUnionAll returns union of two lists (keeps duplicates)
func apocCollUnionAll(list1, list2 interface{}) []interface{} {
	var result []interface{}
	if l1, ok := list1.([]interface{}); ok {
		result = append(result, l1...)
	}
	if l2, ok := list2.([]interface{}); ok {
		result = append(result, l2...)
	}
	return result
}

// apocCollIntersection returns elements present in both lists
func apocCollIntersection(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	// Build set from list2
	set2 := make(map[string]bool)
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		set2[key] = true
	}

	// Find elements in list1 that are also in list2
	seen := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		if set2[key] && !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}
	return result
}

// apocCollSubtract returns elements in list1 but not in list2
func apocCollSubtract(list1, list2 interface{}) []interface{} {
	var result []interface{}
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 {
		return result
	}

	// Build set from list2
	set2 := make(map[string]bool)
	if ok2 {
		for _, item := range l2 {
			key := fmt.Sprintf("%T:%v", item, item)
			set2[key] = true
		}
	}

	// Find elements in list1 that are not in list2
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		if !set2[key] {
			result = append(result, item)
		}
	}
	return result
}

// apocCollContains checks if list contains value
func apocCollContains(listVal, value interface{}) bool {
	l, ok := listVal.([]interface{})
	if !ok {
		return false
	}
	targetKey := fmt.Sprintf("%T:%v", value, value)
	for _, item := range l {
		key := fmt.Sprintf("%T:%v", item, item)
		if key == targetKey {
			return true
		}
	}
	return false
}

// apocCollContainsAll checks if list1 contains all elements of list2
func apocCollContainsAll(list1, list2 interface{}) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Build set from list1
	set1 := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		set1[key] = true
	}

	// Check all elements of list2 are in list1
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		if !set1[key] {
			return false
		}
	}
	return true
}

// apocCollContainsAny checks if list1 contains any element of list2
func apocCollContainsAny(list1, list2 interface{}) bool {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return false
	}

	// Build set from list1
	set1 := make(map[string]bool)
	for _, item := range l1 {
		key := fmt.Sprintf("%T:%v", item, item)
		set1[key] = true
	}

	// Check if any element of list2 is in list1
	for _, item := range l2 {
		key := fmt.Sprintf("%T:%v", item, item)
		if set1[key] {
			return true
		}
	}
	return false
}

// apocCollIndexOf finds index of value in list (-1 if not found)
func apocCollIndexOf(listVal, value interface{}) int64 {
	l, ok := listVal.([]interface{})
	if !ok {
		return -1
	}
	targetKey := fmt.Sprintf("%T:%v", value, value)
	for i, item := range l {
		key := fmt.Sprintf("%T:%v", item, item)
		if key == targetKey {
			return int64(i)
		}
	}
	return -1
}

// apocCollSplit splits list at occurrences of value
func apocCollSplit(listVal, value interface{}) []interface{} {
	l, ok := listVal.([]interface{})
	if !ok {
		return nil
	}
	targetKey := fmt.Sprintf("%T:%v", value, value)
	var result []interface{}
	var current []interface{}

	for _, item := range l {
		key := fmt.Sprintf("%T:%v", item, item)
		if key == targetKey {
			result = append(result, current)
			current = nil
		} else {
			current = append(current, item)
		}
	}
	result = append(result, current)
	return result
}

// apocCollPartition partitions list into sublists of given size
func apocCollPartition(listVal, sizeVal interface{}) []interface{} {
	l, ok := listVal.([]interface{})
	if !ok {
		return nil
	}
	size := int(toInt64(sizeVal))
	if size <= 0 {
		return nil
	}

	var result []interface{}
	for i := 0; i < len(l); i += size {
		end := i + size
		if end > len(l) {
			end = len(l)
		}
		result = append(result, l[i:end])
	}
	return result
}

// apocCollPairs creates pairs from consecutive elements
func apocCollPairs(val interface{}) []interface{} {
	l, ok := val.([]interface{})
	if !ok || len(l) < 2 {
		return nil
	}

	var result []interface{}
	for i := 0; i < len(l)-1; i++ {
		pair := []interface{}{l[i], l[i+1]}
		result = append(result, pair)
	}
	return result
}

// apocCollZip zips two lists into pairs
func apocCollZip(list1, list2 interface{}) []interface{} {
	l1, ok1 := list1.([]interface{})
	l2, ok2 := list2.([]interface{})
	if !ok1 || !ok2 {
		return nil
	}

	// Use shorter length
	length := len(l1)
	if len(l2) < length {
		length = len(l2)
	}

	result := make([]interface{}, length)
	for i := 0; i < length; i++ {
		result[i] = []interface{}{l1[i], l2[i]}
	}
	return result
}

// apocCollFrequencies counts frequency of each element
func apocCollFrequencies(val interface{}) map[string]interface{} {
	l, ok := val.([]interface{})
	if !ok {
		return nil
	}

	counts := make(map[string]int64)
	for _, item := range l {
		key := fmt.Sprintf("%v", item)
		counts[key]++
	}

	// Convert to result format
	result := make(map[string]interface{})
	for k, v := range counts {
		result[k] = v
	}
	return result
}

// apocCollOccurrences counts occurrences of value in list
func apocCollOccurrences(listVal, value interface{}) int64 {
	l, ok := listVal.([]interface{})
	if !ok {
		return 0
	}
	targetKey := fmt.Sprintf("%T:%v", value, value)
	var count int64
	for _, item := range l {
		key := fmt.Sprintf("%T:%v", item, item)
		if key == targetKey {
			count++
		}
	}
	return count
}

// getCypherType returns the Cypher type name for a value
func getCypherType(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "FLOAT"
	case string:
		return "STRING"
	case []interface{}, []string:
		return "LIST"
	case map[string]interface{}:
		return "MAP"
	case *storage.Node:
		return "NODE"
	case *storage.Edge:
		return "RELATIONSHIP"
	case *CypherDuration:
		return "DURATION"
	default:
		_ = v
		return "ANY"
	}
}

// mergeMaps merges two maps, with map2 values overriding map1
func mergeMaps(map1, map2 interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy map1
	if m1, ok := map1.(map[string]interface{}); ok {
		for k, v := range m1 {
			result[k] = v
		}
	}

	// Override with map2
	if m2, ok := map2.(map[string]interface{}); ok {
		for k, v := range m2 {
			result[k] = v
		}
	}

	return result
}

// fromPairs creates a map from a list of [key, value] pairs
func fromPairs(val interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	list, ok := val.([]interface{})
	if !ok {
		return result
	}

	for _, item := range list {
		pair, ok := item.([]interface{})
		if !ok || len(pair) < 2 {
			continue
		}
		key, ok := pair[0].(string)
		if !ok {
			key = fmt.Sprintf("%v", pair[0])
		}
		result[key] = pair[1]
	}

	return result
}

// fromLists creates a map from parallel lists of keys and values
func fromLists(keys, values interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	keyList, ok1 := keys.([]interface{})
	valList, ok2 := values.([]interface{})
	if !ok1 || !ok2 {
		return result
	}

	for i := 0; i < len(keyList) && i < len(valList); i++ {
		key, ok := keyList[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyList[i])
		}
		result[key] = valList[i]
	}

	return result
}
