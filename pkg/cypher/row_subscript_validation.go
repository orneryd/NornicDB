package cypher

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// validatePipelineProjectionSubscripts applies Cypher's runtime type rules to
// every postfix subscript before a WITH or RETURN projection materializes it.
// The expression evaluator and optimized physical plans therefore share one
// validation boundary instead of silently converting invalid access to null.
func (e *StorageExecutor) validatePipelineProjectionSubscripts(rows []pipelineRow, clause, keyword string) error {
	body := strings.TrimSpace(clause)
	if len(body) < len(keyword) || !strings.EqualFold(body[:len(keyword)], keyword) {
		return nil
	}
	body = strings.TrimSpace(body[len(keyword):])
	if strings.HasPrefix(strings.ToUpper(body), "DISTINCT ") {
		body = strings.TrimSpace(body[len("DISTINCT "):])
	}
	end := len(body)
	for _, suffix := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(body, suffix); index >= 0 && index < end {
			end = index
		}
	}
	body = strings.TrimSpace(body[:end])
	if body == "" || body == "*" {
		return nil
	}
	for _, item := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		for _, row := range rows {
			if err := e.validateRowSubscriptTypes(expression, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateRowSubscriptTypes(expression string, row pipelineRow) error {
	expression = strings.TrimSpace(expression)
	open := strings.LastIndexByte(expression, '[')
	if open <= 0 || !strings.HasSuffix(expression, "]") {
		return nil
	}
	baseExpression := strings.TrimSpace(expression[:open])
	indexExpression := strings.TrimSpace(expression[open+1 : len(expression)-1])
	if err := e.validateRowSubscriptTypes(baseExpression, row); err != nil {
		return err
	}
	base, baseOK := e.evaluateRowExpression(baseExpression, row)
	if rangeIndex := strings.Index(indexExpression, ".."); rangeIndex >= 0 {
		if !baseOK || base == nil {
			return nil
		}
		baseType := reflect.TypeOf(base)
		if baseType == nil || (baseType.Kind() != reflect.Slice && baseType.Kind() != reflect.Array) {
			return invalidSubscriptTypeError("slice receiver must be a LIST", base)
		}
		for _, boundExpression := range []string{strings.TrimSpace(indexExpression[:rangeIndex]), strings.TrimSpace(indexExpression[rangeIndex+2:])} {
			if boundExpression == "" {
				continue
			}
			bound, ok := e.evaluateRowExpression(boundExpression, row)
			if ok && bound != nil && !isCypherInteger(bound) {
				return invalidSubscriptTypeError("list slice bound requires an INTEGER", bound)
			}
		}
		return nil
	}
	index, indexOK := e.evaluateRowExpression(indexExpression, row)
	if !baseOK || !indexOK || base == nil || index == nil {
		return nil
	}

	if object, isMap := toStringAnyMap(base); isMap {
		_ = object
		if _, valid := index.(string); !valid {
			return invalidSubscriptTypeError("map subscript requires a STRING key", index)
		}
		return nil
	}
	switch base.(type) {
	case *storage.Node, *storage.Edge:
		if _, valid := index.(string); !valid {
			return invalidSubscriptTypeError("entity subscript requires a STRING property key", index)
		}
		return nil
	}

	baseType := reflect.TypeOf(base)
	if baseType == nil || (baseType.Kind() != reflect.Slice && baseType.Kind() != reflect.Array) {
		return invalidSubscriptTypeError("subscript receiver must be a LIST or MAP", base)
	}
	if !isCypherInteger(index) {
		return invalidSubscriptTypeError("list subscript requires an INTEGER index", index)
	}
	return nil
}

func isCypherInteger(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

func invalidSubscriptTypeError(message string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentType",
		fmt.Sprintf("%s, got %T", message, value),
	)
}
