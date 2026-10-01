package cypher

import (
	"context"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var txCaseRollbackPattern = regexp.MustCompile(`(?is)^CASE\s+WHEN\s+(.+?)\s+THEN\s+ROLLBACK\s+ELSE\s+RETURN\s+(.+?)\s+COMMIT\s*$`)

// transactionScriptShape reports whether query is one of the one-statement
// transaction script forms — BEGIN … COMMIT / ROLLBACK, including the
// CALL … CASE WHEN … THEN ROLLBACK ELSE RETURN … COMMIT form. A bare BEGIN
// or BEGIN TRANSACTION is not a script (parseTransactionStatement owns it).
func transactionScriptShape(cypher string) bool {
	trimmed := strings.TrimSpace(cypher)
	upper := upperASCII(trimmed)
	if !strings.HasPrefix(upper, "BEGIN") {
		return false
	}
	if upper == "BEGIN" || upper == "BEGIN TRANSACTION" {
		return false
	}
	bodyAfterBegin, ok := stripBeginTransactionPrefix(trimmed)
	if !ok {
		return false
	}
	if strings.HasPrefix(upperASCII(strings.TrimSpace(bodyAfterBegin)), "CALL") && strings.Contains(upperASCII(bodyAfterBegin), "CASE") {
		return true
	}
	_, _, ok = splitTransactionScriptTailAction(bodyAfterBegin)
	return ok
}

// executeTransactionScript handles Nornic transaction script syntax:
// - BEGIN TRANSACTION ... COMMIT
// - BEGIN ... COMMIT (shorthand)
// - BEGIN TRANSACTION ... CASE WHEN ... THEN ROLLBACK ELSE RETURN ... COMMIT
func (e *StorageExecutor) executeTransactionScript(ctx context.Context, cypher string) (*ExecuteResult, error) {
	trimmed := strings.TrimSpace(cypher)
	upper := upperASCII(trimmed)
	if !strings.HasPrefix(upper, "BEGIN") {
		return nil, nil
	}
	// Single-statement transaction commands are handled by parseTransactionStatement.
	if upper == "BEGIN" || upper == "BEGIN TRANSACTION" {
		return nil, nil
	}

	bodyAfterBegin, ok := stripBeginTransactionPrefix(trimmed)
	if !ok {
		return nil, nil
	}

	// CASE rollback script form.
	if strings.HasPrefix(upperASCII(strings.TrimSpace(bodyAfterBegin)), "CALL") && strings.Contains(upperASCII(bodyAfterBegin), "CASE") {
		return e.executeCaseRollbackTransactionScript(ctx, bodyAfterBegin)
	}

	// Generic BEGIN ... <query> ... COMMIT|ROLLBACK.
	queryBody, action, ok := splitTransactionScriptTailAction(bodyAfterBegin)
	if !ok {
		return nil, nil
	}
	return e.executeSimpleTransactionScript(ctx, queryBody, action)
}

func stripBeginTransactionPrefix(query string) (string, bool) {
	trimmed := strings.TrimSpace(query)
	upper := upperASCII(trimmed)
	if strings.HasPrefix(upper, "BEGIN TRANSACTION") {
		return strings.TrimSpace(trimmed[len("BEGIN TRANSACTION"):]), true
	}
	if strings.HasPrefix(upper, "BEGIN") {
		return strings.TrimSpace(trimmed[len("BEGIN"):]), true
	}
	return "", false
}

func splitTransactionScriptTailAction(script string) (string, string, bool) {
	trimmed := strings.TrimSpace(script)
	upper := upperASCII(trimmed)
	if strings.HasSuffix(upper, "COMMIT") {
		body := strings.TrimSpace(trimmed[:len(trimmed)-len("COMMIT")])
		if body == "" {
			return "", "", false
		}
		return body, "COMMIT", true
	}
	if strings.HasSuffix(upper, "ROLLBACK") {
		body := strings.TrimSpace(trimmed[:len(trimmed)-len("ROLLBACK")])
		if body == "" {
			return "", "", false
		}
		return body, "ROLLBACK", true
	}
	return "", "", false
}

func (e *StorageExecutor) executeSimpleTransactionScript(ctx context.Context, queryBody string, action string) (*ExecuteResult, error) {
	if _, err := e.handleBegin(); err != nil {
		return nil, err
	}

	queryUpper := upperASCII(strings.TrimSpace(queryBody))
	result, err := e.executeInTransaction(ctx, queryBody, queryUpper)
	if err != nil {
		return nil, e.abortTransaction(err)
	}

	switch action {
	case "COMMIT":
		if _, err := e.handleCommit(); err != nil {
			return nil, err
		}
		return result, nil
	case "ROLLBACK":
		return e.handleRollback()
	default:
		return nil, e.abortTransaction(localizedError(localization.CypherTransactionsInvalidScriptAction(action), nil))
	}
}

func (e *StorageExecutor) executeCaseRollbackTransactionScript(ctx context.Context, scriptBody string) (*ExecuteResult, error) {
	upper := upperASCII(scriptBody)
	caseIdx := strings.Index(upper, "CASE")
	if caseIdx <= 0 {
		return nil, localizedError(localization.CypherTransactionsCaseBlockMissing(), nil)
	}

	callQuery := strings.TrimSpace(scriptBody[:caseIdx])
	caseBlock := strings.TrimSpace(scriptBody[caseIdx:])
	matches := txCaseRollbackPattern.FindStringSubmatch(caseBlock)
	if len(matches) != 3 {
		return nil, localizedError(localization.CypherTransactionsCaseSyntaxInvalid(), nil)
	}
	conditionExpr := strings.TrimSpace(matches[1])
	returnExpr := strings.TrimSpace(matches[2])

	if _, err := e.handleBegin(); err != nil {
		return nil, err
	}

	callUpper := upperASCII(strings.TrimSpace(callQuery))
	callResult, err := e.executeInTransaction(ctx, callQuery, callUpper)
	if err != nil {
		return nil, e.abortTransaction(err)
	}

	rollbackRequired := false
	for _, row := range callResult.Rows {
		nodes, rels := buildRowGraphContext(callResult.Columns, row)
		shouldRollback, err := e.evaluateConditionExpression(ctx, conditionExpr, nodes, rels)
		if err != nil {
			return nil, e.abortTransaction(err)
		}
		if shouldRollback {
			rollbackRequired = true
			break
		}
	}
	if rollbackRequired {
		return e.handleRollback()
	}

	projected, err := e.projectTransactionReturn(ctx, callResult, returnExpr)
	if err != nil {
		return nil, e.abortTransaction(err)
	}

	if _, err := e.handleCommit(); err != nil {
		return nil, err
	}
	return projected, nil
}

func (e *StorageExecutor) projectTransactionReturn(ctx context.Context, input *ExecuteResult, returnExpr string) (*ExecuteResult, error) {
	rows := make([]pipelineRow, 0, len(input.Rows))
	for _, row := range input.Rows {
		rows = append(rows, pipelineRow(buildRowValueMap(input.Columns, row)))
	}
	if strings.TrimSpace(returnExpr) == "" {
		returnExpr = "*"
	}
	projected, err := e.projectMergeReturn(ctx, rows, "RETURN "+returnExpr)
	if err != nil {
		return nil, err
	}
	projected.Stats = input.Stats
	return projected, nil
}

func buildRowGraphContext(cols []string, row []interface{}) (map[string]*storage.Node, map[string]*storage.Edge) {
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)
	for i, col := range cols {
		if i >= len(row) {
			continue
		}
		switch v := row[i].(type) {
		case *storage.Node:
			nodes[col] = v
		case *storage.Edge:
			rels[col] = v
		}
	}
	return nodes, rels
}

func buildRowValueMap(cols []string, row []interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		if i < len(row) {
			out[col] = row[i]
		}
	}
	return out
}

func (e *StorageExecutor) evaluateConditionExpression(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (bool, error) {
	v := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
	switch b := v.(type) {
	case bool:
		return b, nil
	case nil:
		return false, nil
	default:
		return false, localizedError(localization.CypherTransactionsConditionNotBoolean(v), nil)
	}
}
