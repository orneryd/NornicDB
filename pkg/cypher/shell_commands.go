package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"strings"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
)

func (e *StorageExecutor) preprocessShellCommands(ctx context.Context, cypher string, explicitParams map[string]interface{}) (string, context.Context, *ExecuteResult, error) {
	remaining := statementBraceView(ctx, cypher).trim()
	ctx = withBraceView(ctx, remaining)
	var lastResult *ExecuteResult

	for {
		command, rest, ok := shellCommandSpans(remaining.text(), remaining.close)
		cmd, restView := remaining.window(command.Start, command.End), remaining.window(rest.Start, rest.End)
		if !ok {
			break
		}

		result, nextCtx, err := e.executeShellCommand(ctx, cmd, restView, explicitParams)
		if err != nil {
			return "", ctx, nil, err
		}
		ctx = nextCtx
		lastResult = result
		remaining = restView.trim()
		ctx = withBraceView(ctx, remaining)
		if remaining.empty() || !strings.HasPrefix(remaining.text(), ":") {
			break
		}
	}

	if strings.HasPrefix(remaining.text(), ":") {
		command := strings.Split(remaining.text(), "\n")[0]
		return "", ctx, nil, localizedError(localization.CypherCommandRoutingUnknownCommand(command), nil)
	}

	if lastResult == nil {
		lastResult = &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
	}
	return remaining.text(), ctx, lastResult, nil
}

func (e *StorageExecutor) executeShellCommand(ctx context.Context, commandView, queryView braceView, explicitParams map[string]interface{}) (*ExecuteResult, context.Context, error) {
	commandView = commandView.trim()
	if strings.HasSuffix(commandView.text(), ";") {
		commandView = commandView.window(0, len(commandView.text())-1).trim()
	}
	trimmed := commandView.text()
	query := queryView.text()
	ctx = withBraceView(ctx, commandView)
	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return nil, ctx, localizedError(localization.CypherCommandRoutingEmptyCommand(), nil)
	}

	name := lowerASCII(parts[0])
	args := ""
	if len(trimmed) > len(parts[0]) {
		args = strings.TrimSpace(trimmed[len(parts[0]):])
	}

	switch name {
	case ":use":
		if args == "" {
			return nil, ctx, localizedError(localization.CypherCommandRoutingShellUseDatabaseRequired(), nil)
		}
		dbName := strings.Fields(args)[0]
		// The query follows any remaining shell commands. Select its database
		// with its own requirements, just as a Cypher USE clause does.
		for {
			_, rest, ok := shellCommandSpans(queryView.text(), queryView.close)
			if !ok {
				break
			}
			queryView = queryView.window(rest.Start, rest.End)
			query = queryView.text()
		}
		if err := e.authorizeSelectedDatabase(withBraceView(ctx, queryView), dbName, query); err != nil {
			return nil, ctx, err
		}
		if e.dbManager != nil {
			// Resolve shell selections before the statement starts, including
			// selections with no following query. Reuse USE's canonical resolver.
			_, resolved, err := e.scopedExecutorForUse(dbName, GetAuthTokenFromContext(ctx))
			if err != nil {
				return nil, ctx, nerrors.MarkCompileTime(err)
			}
			dbName = resolved
		}
		ctx = withExecutionDatabase(ctx, dbName)
		return &ExecuteResult{
			Columns: []string{"database"},
			Rows:    [][]interface{}{{"switched"}},
		}, ctx, nil
	case ":param", ":params":
		result, err := e.executeParamCommand(ctx, commandView.window(len(parts[0]), len(trimmed)).trim(), explicitParams)
		return result, ctx, err
	default:
		return nil, ctx, localizedError(localization.CypherCommandRoutingUnknownCommand(parts[0]), nil)
	}
}

func (e *StorageExecutor) executeParamCommand(ctx context.Context, args braceView, explicitParams map[string]interface{}) (*ExecuteResult, error) {
	args = args.trim()
	if strings.HasSuffix(args.text(), ";") {
		args = args.window(0, len(args.text())-1).trim()
	}
	trimmed := args.text()
	if trimmed == "" || strings.EqualFold(trimmed, "list") {
		return e.listShellParams(ctx), nil
	}
	if strings.EqualFold(trimmed, "clear") {
		e.clearShellParams(ctx)
		return &ExecuteResult{
			Columns: []string{"status"},
			Rows:    [][]interface{}{{"Parameters cleared"}},
		}, nil
	}

	input, err := readShellParameterInput(trimmed)
	if err != nil {
		return nil, err
	}

	effectiveParams := e.mergeShellParams(ctx, explicitParams)
	var paramMap map[string]interface{}
	if input.mapForm {
		paramMap, err = e.evaluateParamMapExpression(ctx, args.spanWindow(input.value), effectiveParams)
	} else {
		key := trimmed[input.key.Start:input.key.End]
		paramMap, err = e.evaluateShellParameterEntries(ctx, []recordedMapEntry{{name: key, value: args.spanWindow(input.value)}}, effectiveParams)
	}
	if err != nil {
		return nil, err
	}
	if len(paramMap) == 0 {
		return nil, localizedError(localization.CypherCommandRoutingParameterMapRequired(), nil)
	}

	e.setShellParams(ctx, paramMap)
	return &ExecuteResult{
		Columns: []string{"status"},
		Rows:    [][]interface{}{{fmt.Sprintf("Parameters set: %d", len(paramMap))}},
	}, nil
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
				return nil, localizedError(localization.CypherCommandRoutingParameterMapKeysStrings(), nil)
			}
			out[keyStr] = val
		}
		return out, nil
	default:
		return nil, localizedError(localization.CypherCommandRoutingParameterMapRequired(), nil)
	}
}

func (e *StorageExecutor) evaluateParamMapExpression(ctx context.Context, view braceView, params map[string]interface{}) (map[string]interface{}, error) {
	entries, valid := view.keyedEntries()
	if !valid {
		return nil, localizedError(localization.CypherCommandRoutingParameterMapRequired(), nil)
	}
	return e.evaluateShellParameterEntries(ctx, entries, params)
}

func (e *StorageExecutor) evaluateShellParameterEntries(ctx context.Context, entries []recordedMapEntry, params map[string]interface{}) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(entries))
	for _, entry := range entries {
		key, valueExpr := entry.key(), entry.value.text()
		value := entry.value.wrap("RETURN ", " AS value")
		result, err := e.executeInternal(withBraceView(ctx, value), value.text(), params)
		if err != nil {
			if cyphertext.IsIdentifier(valueExpr) {
				return nil, localizedError(localization.CypherCommandRoutingParameterExpressionUnresolved(key, valueExpr), nerrors.ErrExpressionEvaluationFailed)
			}
			cause := fmt.Errorf("%w: parameter %s: %w", nerrors.ErrExpressionEvaluationFailed, key, err)
			return nil, localizedError(localization.CypherCommandRoutingParameterEvaluationFailed(key, err), cause)
		}
		if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			return nil, localizedError(localization.CypherCommandRoutingParameterProducedNoValue(key), nerrors.ErrExpressionEvaluationFailed)
		}
		evaluated := result.Rows[0][0]
		if isUnevaluatedParameterExpression(valueExpr, evaluated) {
			return nil, localizedError(localization.CypherCommandRoutingParameterExpressionUnresolved(key, strings.TrimSpace(valueExpr)), nerrors.ErrExpressionEvaluationFailed)
		}
		out[key] = evaluated
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

func (e *StorageExecutor) mergeShellParams(ctx context.Context, explicitParams map[string]interface{}) map[string]interface{} {
	shell := e.shellParamsFor(ctx)
	e.shellParamsMu.RLock()
	shellLen := len(shell)
	if shellLen == 0 {
		e.shellParamsMu.RUnlock()
		if len(explicitParams) == 0 {
			return nil
		}
		// Fast path: no shell params to merge, so return explicit params directly.
		return explicitParams
	}

	merged := make(map[string]interface{}, util.SafePreallocSum(shellLen, len(explicitParams)))
	for key, value := range shell {
		merged[key] = value
	}
	e.shellParamsMu.RUnlock()

	for key, value := range explicitParams {
		merged[key] = value
	}
	return merged
}

// shellParamsFor returns the caller-scoped shell-param bucket for ctx: the
// authenticated principal's own bucket, else the auth token's, else the
// shared legacy bucket for contexts without a caller identity. One client's
// :param values are never visible to another client's statements.
func (e *StorageExecutor) shellParamsFor(ctx context.Context) map[string]interface{} {
	bucket := shellParamsBucket(ctx)
	if bucket == "" {
		return e.shellParams
	}
	return e.shellParamsByToken[bucket]
}

func shellParamsBucket(ctx context.Context) string {
	if principal := GetAuthenticatedPrincipalFromContext(ctx); principal != "" {
		return "principal:" + principal
	}
	if token := GetAuthTokenFromContext(ctx); token != "" {
		return "token:" + token
	}
	return ""
}

// cloneShellParamsByToken copies every per-caller shell-param bucket for a
// derived executor (a request-scoped clone; each clone serves one caller).
func cloneShellParamsByToken(e *StorageExecutor) map[string]map[string]interface{} {
	e.shellParamsMu.RLock()
	defer e.shellParamsMu.RUnlock()
	if len(e.shellParamsByToken) == 0 {
		return nil
	}
	out := make(map[string]map[string]interface{}, len(e.shellParamsByToken))
	for bucket, params := range e.shellParamsByToken {
		copyParams := make(map[string]interface{}, len(params))
		for key, value := range params {
			copyParams[key] = value
		}
		out[bucket] = copyParams
	}
	return out
}

func (e *StorageExecutor) getShellParamsSnapshot(ctx context.Context) map[string]interface{} {
	shell := e.shellParamsFor(ctx)
	e.shellParamsMu.RLock()
	defer e.shellParamsMu.RUnlock()
	if len(shell) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(shell))
	for key, value := range shell {
		out[key] = value
	}
	return out
}

func (e *StorageExecutor) setShellParams(ctx context.Context, params map[string]interface{}) {
	e.shellParamsMu.Lock()
	defer e.shellParamsMu.Unlock()
	shell := e.shellParamsFor(ctx)
	if bucket := shellParamsBucket(ctx); bucket != "" {
		if shell == nil {
			shell = make(map[string]interface{}, len(params))
			if e.shellParamsByToken == nil {
				e.shellParamsByToken = make(map[string]map[string]interface{})
			}
			e.shellParamsByToken[bucket] = shell
		}
	} else if e.shellParams == nil {
		e.shellParams = make(map[string]interface{}, len(params))
		shell = e.shellParams
	}
	for key, value := range params {
		shell[key] = value
	}
}

func (e *StorageExecutor) clearShellParams(ctx context.Context) {
	e.shellParamsMu.Lock()
	defer e.shellParamsMu.Unlock()
	shell := e.shellParamsFor(ctx)
	if shell == nil {
		return
	}
	clear(shell)
}

func (e *StorageExecutor) listShellParams(ctx context.Context) *ExecuteResult {
	params := e.getShellParamsSnapshot(ctx)
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
