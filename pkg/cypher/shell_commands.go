package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/util"
)

func (e *StorageExecutor) preprocessShellCommands(ctx context.Context, cypher string, explicitParams map[string]interface{}) (string, context.Context, *ExecuteResult, error) {
	remaining := strings.TrimSpace(cypher)
	var lastResult *ExecuteResult

	for {
		cmd, rest, ok := nextShellCommand(remaining)
		if !ok {
			break
		}

		result, nextCtx, err := e.executeShellCommand(ctx, cmd, explicitParams)
		if err != nil {
			return "", ctx, nil, err
		}
		ctx = nextCtx
		lastResult = result
		remaining = strings.TrimSpace(rest)
		if remaining == "" || !strings.HasPrefix(remaining, ":") {
			break
		}
	}

	if strings.HasPrefix(remaining, ":") {
		return "", ctx, nil, fmt.Errorf("unknown command: %s", strings.Split(strings.TrimSpace(remaining), "\n")[0])
	}

	if lastResult == nil {
		lastResult = &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
	}
	return remaining, ctx, lastResult, nil
}

func nextShellCommand(input string) (string, string, bool) {
	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(trimmed, ":") {
		return "", input, false
	}

	lineEnd := strings.IndexByte(trimmed, '\n')
	firstLine := trimmed
	if lineEnd >= 0 {
		firstLine = trimmed[:lineEnd]
	}
	fields := strings.Fields(firstLine)
	if len(fields) == 0 {
		return "", input, false
	}

	commandName := strings.ToLower(fields[0])
	if (commandName == ":param" || commandName == ":params") && isMapStyleShellParam(trimmed) {
		command, consumed, ok := consumeShellMapCommand(trimmed)
		if !ok {
			return "", input, false
		}
		return command, trimmed[consumed:], true
	}

	if lineEnd == -1 {
		return trimmed, "", true
	}
	return strings.TrimSpace(trimmed[:lineEnd]), trimmed[lineEnd+1:], true
}

func isMapStyleShellParam(command string) bool {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return false
	}
	name := strings.ToLower(fields[0])
	if name != ":param" && name != ":params" {
		return false
	}
	arg := strings.TrimSpace(command[len(fields[0]):])
	return strings.HasPrefix(arg, "{")
}

func consumeShellMapCommand(input string) (string, int, bool) {
	start := strings.Index(input, "{")
	if start == -1 {
		return "", 0, false
	}

	depth := 0
	inSingle := false
	inDouble := false
	inLineComment := false
	inBlockComment := false
	escaped := false

	for i := start; i < len(input); i++ {
		ch := input[i]
		next := byte(0)
		if i+1 < len(input) {
			next = input[i+1]
		}

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if ch == '*' && next == '/' {
				inBlockComment = false
				i++
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if (inSingle || inDouble) && ch == '\\' {
			escaped = true
			continue
		}
		if !inSingle && !inDouble && ch == '/' && next == '/' {
			inLineComment = true
			i++
			continue
		}
		if !inSingle && !inDouble && ch == '/' && next == '*' {
			inBlockComment = true
			i++
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}

		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end := i + 1
				for end < len(input) && (input[end] == ';' || input[end] == ' ' || input[end] == '\t' || input[end] == '\r') {
					end++
				}
				if end < len(input) && input[end] == '\n' {
					end++
				}
				return strings.TrimSpace(input[:i+1]), end, true
			}
		}
	}

	return "", 0, false
}

func (e *StorageExecutor) executeShellCommand(ctx context.Context, command string, explicitParams map[string]interface{}) (*ExecuteResult, context.Context, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(command, ";"))
	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return nil, ctx, fmt.Errorf("empty command")
	}

	name := strings.ToLower(parts[0])
	args := ""
	if len(trimmed) > len(parts[0]) {
		args = strings.TrimSpace(trimmed[len(parts[0]):])
	}

	switch name {
	case ":use":
		if args == "" {
			return nil, ctx, fmt.Errorf(":use requires a database name")
		}
		dbName := strings.Fields(args)[0]
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, dbName)
		return &ExecuteResult{
			Columns: []string{"database"},
			Rows:    [][]interface{}{{"switched"}},
		}, ctx, nil
	case ":param", ":params":
		result, err := e.executeParamCommand(ctx, args, explicitParams)
		return result, ctx, err
	default:
		return nil, ctx, fmt.Errorf("unknown command: %s", parts[0])
	}
}

func (e *StorageExecutor) executeParamCommand(ctx context.Context, args string, explicitParams map[string]interface{}) (*ExecuteResult, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(args, ";"))
	if trimmed == "" || strings.EqualFold(trimmed, "list") {
		return e.listShellParams(), nil
	}
	if strings.EqualFold(trimmed, "clear") {
		e.clearShellParams()
		return &ExecuteResult{
			Columns: []string{"status"},
			Rows:    [][]interface{}{{"Parameters cleared"}},
		}, nil
	}

	mapExpr, err := parseParamCommandToMapExpression(trimmed)
	if err != nil {
		return nil, err
	}

	effectiveParams := e.mergeShellParams(explicitParams)
	paramMap, err := e.evaluateParamMapExpression(ctx, mapExpr, effectiveParams)
	if err != nil {
		return nil, err
	}
	if len(paramMap) == 0 {
		return nil, fmt.Errorf("parameter expression must evaluate to a map")
	}

	e.setShellParams(paramMap)
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{fmt.Sprintf("Parameters set: %d", len(paramMap))}},
	}, nil
}

func parseParamCommandToMapExpression(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", fmt.Errorf(":param requires an argument")
	}
	if strings.HasPrefix(trimmed, "{") {
		return trimmed, nil
	}

	patterns := []struct {
		sep string
		fn  func(string) (string, string, bool)
	}{
		{sep: "=>", fn: splitArrowParamSyntax},
		{sep: "legacy", fn: splitLegacyParamSyntax},
	}

	for _, pattern := range patterns {
		key, value, ok := pattern.fn(trimmed)
		if !ok {
			continue
		}
		if value == "" {
			return "", fmt.Errorf(":param value cannot be empty")
		}
		return fmt.Sprintf("{%s: %s}", key, value), nil
	}

	return "", fmt.Errorf("incorrect usage: expected :param clear, :param list, :param {a: 1}, or :param key => value")
}

func splitArrowParamSyntax(input string) (string, string, bool) {
	idx := strings.Index(input, "=>")
	if idx == -1 {
		return "", "", false
	}
	left := strings.TrimSpace(input[:idx])
	right := strings.TrimSpace(input[idx+2:])
	if strings.HasSuffix(left, ":") {
		return "", "", false
	}
	key := normalizePropertyKey(left)
	if key == "" {
		return "", "", false
	}
	return left, right, true
}

func splitLegacyParamSyntax(input string) (string, string, bool) {
	fields := strings.Fields(input)
	if len(fields) < 2 {
		return "", "", false
	}
	key := fields[0]
	rest := strings.TrimSpace(input[len(key):])
	rest = strings.TrimSpace(strings.TrimPrefix(rest, ":"))
	if rest == "" {
		return "", "", false
	}
	if normalizePropertyKey(key) == "" {
		return "", "", false
	}
	return key, rest, true
}

func normalizeParamMap(value interface{}) (map[string]interface{}, error) {
	switch typed := value.(type) {
	case map[string]interface{}:
		return typed, nil
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, val := range typed {
			keyStr, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("parameter map keys must be strings")
			}
			out[keyStr] = val
		}
		return out, nil
	default:
		return nil, fmt.Errorf("parameter expression must evaluate to a map")
	}
}

func (e *StorageExecutor) evaluateParamMapExpression(ctx context.Context, mapExpr string, params map[string]interface{}) (map[string]interface{}, error) {
	trimmed := strings.TrimSpace(mapExpr)
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return nil, fmt.Errorf("parameter expression must evaluate to a map")
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if inner == "" {
		return map[string]interface{}{}, nil
	}

	pairs := splitTopLevelCommaKeepEmpty(inner)
	out := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return nil, fmt.Errorf("empty map entry")
		}
		colonIdx := topLevelColonIndex(pair)
		if colonIdx <= 0 || colonIdx >= len(pair)-1 {
			return nil, fmt.Errorf("invalid map entry %q", pair)
		}

		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])
		if key == "" || valueExpr == "" {
			return nil, fmt.Errorf("invalid map entry %q", pair)
		}

		result, err := e.executeInternal(ctx, "RETURN "+valueExpr+" AS value", params)
		if err != nil {
			return nil, fmt.Errorf("%w: parameter %s: %w", nerrors.ErrExpressionEvaluationFailed, key, err)
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, fmt.Errorf("%w: parameter %s produced no value", nerrors.ErrExpressionEvaluationFailed, key)
		}
		value := result.Rows[0][0]
		if isUnevaluatedParameterExpression(valueExpr, value) {
			return nil, fmt.Errorf("%w: parameter %s unresolved expression %q", nerrors.ErrExpressionEvaluationFailed, key, strings.TrimSpace(valueExpr))
		}
		out[key] = value
	}
	return out, nil
}

func isUnevaluatedParameterExpression(expr string, value interface{}) bool {
	got, ok := value.(string)
	if !ok {
		return false
	}
	trimmed := strings.TrimSpace(expr)
	if strings.TrimSpace(got) != trimmed {
		return false
	}
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "'") || strings.HasPrefix(trimmed, "\"") {
		return false
	}
	if strings.EqualFold(trimmed, "true") || strings.EqualFold(trimmed, "false") || strings.EqualFold(trimmed, "null") {
		return false
	}
	if isNumericLiteral(trimmed) {
		return false
	}
	return true
}

func topLevelColonIndex(input string) int {
	inSingle := false
	inDouble := false
	depth := 0
	for index, r := range input {
		switch r {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(', '[', '{':
			if !inSingle && !inDouble {
				depth++
			}
		case ')', ']', '}':
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		case ':':
			if !inSingle && !inDouble && depth == 0 {
				return index
			}
		}
	}
	return -1
}

func (e *StorageExecutor) mergeShellParams(explicitParams map[string]interface{}) map[string]interface{} {
	e.shellParamsMu.RLock()
	shellLen := len(e.shellParams)
	if shellLen == 0 {
		e.shellParamsMu.RUnlock()
		if len(explicitParams) == 0 {
			return nil
		}
		// Fast path: no shell params to merge, so return explicit params directly.
		return explicitParams
	}

	merged := make(map[string]interface{}, util.SafePreallocSum(shellLen, len(explicitParams)))
	for key, value := range e.shellParams {
		merged[key] = value
	}
	e.shellParamsMu.RUnlock()

	for key, value := range explicitParams {
		merged[key] = value
	}
	return merged
}

func (e *StorageExecutor) getShellParamsSnapshot() map[string]interface{} {
	e.shellParamsMu.RLock()
	defer e.shellParamsMu.RUnlock()
	if len(e.shellParams) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(e.shellParams))
	for key, value := range e.shellParams {
		out[key] = value
	}
	return out
}

func (e *StorageExecutor) setShellParams(params map[string]interface{}) {
	e.shellParamsMu.Lock()
	defer e.shellParamsMu.Unlock()
	if e.shellParams == nil {
		e.shellParams = make(map[string]interface{}, len(params))
	}
	for key, value := range params {
		e.shellParams[key] = value
	}
}

func (e *StorageExecutor) clearShellParams() {
	e.shellParamsMu.Lock()
	defer e.shellParamsMu.Unlock()
	clear(e.shellParams)
}

func (e *StorageExecutor) listShellParams() *ExecuteResult {
	params := e.getShellParamsSnapshot()
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, []interface{}{key, params[key]})
	}
	return &ExecuteResult{
		Columns: []string{"name", "value"},
		Rows:    rows,
	}
}
