package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Dynamic labels and property keys ($(expr) and n[expr], Neo4j 5.26) take
// their names from a value at run time. One rule reads every such value, in
// every clause that takes one (#907):
//
//   - a label value is a STRING (one name) or a LIST<STRING> (each name; an
//     empty list names none);
//   - a property key is a STRING;
//   - a name must be non-empty and hold no null byte (TokenNameError), and
//     any other non-empty string is a valid name ('A B' and '`x`' included);
//   - any other value, null included, and a list holding a null or a
//     non-string, is a TypeError.
//
// A value whose type is known when the statement is compiled is checked
// then, as Neo4j does (staticDynamicTokenError).

// dynamicLabelNames reads the value of a dynamic label $(expr).
func dynamicLabelNames(value interface{}) ([]string, error) {
	switch typed := value.(type) {
	case string:
		if err := tokenNameError(typed); err != nil {
			return nil, err
		}
		return []string{typed}, nil
	case []string:
		for _, name := range typed {
			if err := tokenNameError(name); err != nil {
				return nil, err
			}
		}
		return typed, nil
	case []interface{}:
		names := make([]string, 0, len(typed))
		for _, item := range typed {
			name, ok := item.(string)
			if !ok {
				return nil, dynamicLabelValueError()
			}
			if err := tokenNameError(name); err != nil {
				return nil, err
			}
			names = append(names, name)
		}
		return names, nil
	}
	return nil, dynamicLabelValueError()
}

func dynamicLabelValueError() error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", localization.CypherCoreDynamicLabelValueInvalid())
}

// dynamicPropertyKey reads the value of a dynamic property key n[expr] that
// a SET writes or a REMOVE removes. REMOVE n[''] removes nothing, as in
// Neo4j: for removing, the empty name is returned without an error.
func dynamicPropertyKey(value interface{}, removing bool) (string, error) {
	name, ok := value.(string)
	if !ok {
		return "", localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreEntityPropertyKeyTypeMismatch(cypherTypeName(value)))
	}
	if removing && name == "" {
		return "", nil
	}
	return name, tokenNameError(name)
}

// tokenNameError is the TokenNameError of a name a token can't have: empty,
// or holding a null byte.
func tokenNameError(name string) error {
	if name == "" || strings.IndexByte(name, 0) >= 0 {
		return localizedStatusError("Neo.ClientError.Schema.TokenNameError", "TokenNameError", localization.CypherCoreTokenNameInvalid(name))
	}
	return nil
}

// dynamicTokenValue evaluates the expression of a dynamic label or property
// key in a row's context, as SET evaluates a value: a parameter that wasn't
// supplied is an error (requireSetParameter).
func (e *StorageExecutor) dynamicTokenValue(ctx context.Context, expression string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, error) {
	if err := requireSetParameter(ctx, expression); err != nil {
		return nil, err
	}
	if value, ok := resolveDirectParamRef(ctx, expression); ok {
		return value, nil
	}
	if value, ok := resolveContextPathRef(ctx, expression); ok {
		return value, nil
	}
	value := e.evaluateSetExpressionWithContext(ctx, expression, nodes, rels)
	if text, isText := value.(string); isText && strings.TrimSpace(text) == strings.TrimSpace(expression) {
		// A literal the evaluator hands back as its own text.
		value = e.parseValue(ctx, strings.TrimSpace(expression))
	}
	return value, nil
}

// chainLabelNames resolves the items of a SET or REMOVE label chain
// (setLabelChainItems) to label names, in order: a $(expr) item names what
// its value names (dynamicLabelNames).
func (e *StorageExecutor) chainLabelNames(ctx context.Context, items []labelChainItem, nodes map[string]*storage.Node, rels map[string]*storage.Edge) ([]string, error) {
	names := make([]string, 0, len(items))
	for _, item := range items {
		if item.expression == "" {
			names = append(names, item.name)
			continue
		}
		value, err := e.dynamicTokenValue(ctx, item.expression, nodes, rels)
		if err != nil {
			return nil, err
		}
		dynamic, err := dynamicLabelNames(value)
		if err != nil {
			return nil, err
		}
		names = append(names, dynamic...)
	}
	return names, nil
}

// dynamicPropertyKeyOf evaluates the key expression of n[expr] for a SET
// (removing false) or a REMOVE (removing true) and reads it as a property key
// (dynamicPropertyKey).
func (e *StorageExecutor) dynamicPropertyKeyOf(ctx context.Context, expression string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, removing bool) (string, error) {
	value, err := e.dynamicTokenValue(ctx, expression, nodes, rels)
	if err != nil {
		return "", err
	}
	return dynamicPropertyKey(value, removing)
}

// labelTargetTypeError is the TypeError of a SET or REMOVE label item on a
// value that isn't a node (typeName): only nodes have labels.
func labelTargetTypeError(typeName string) error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidType", localization.CypherCoreLabelTargetTypeMismatch(typeName))
}

// staticWriteTokenError is the error Neo4j reports when it compiles a SET,
// REMOVE or MERGE action item: a label item whose target isn't a node, a
// property write whose target isn't a node or relationship, or a dynamic
// label or property key whose value is known not to name one
// (staticDynamicTokenError). Everything else is read at run time.
func staticWriteTokenError(clause pipelineClause, scope staticTypeScope) error {
	switch clause.kind {
	case pipelineClauseSet:
		return staticSetItemsTokenError(splitSetAssignments(collapseChainedSetClauses(pipelineClauseBody(clause.text, "SET"))), scope)
	case pipelineClauseMerge:
		actions := splitMergeClauseActions(pipelineClauseBody(clause.text, "MERGE"))
		if err := staticSetItemsTokenError(mergeActionAssignments(actions.onCreate), scope); err != nil {
			return err
		}
		return staticSetItemsTokenError(mergeActionAssignments(actions.onMatch), scope)
	case pipelineClauseRemove:
		items, err := parseRemoveItems(pipelineClauseBody(clause.text, "REMOVE"))
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.key != "" {
				if err := staticDynamicTokenError(item.key, scope, dynamicTokenKey); err != nil {
					return err
				}
				continue
			}
			if len(item.labels) == 0 {
				continue
			}
			if err := staticLabelTargetError(item.variable, scope); err != nil {
				return err
			}
			if err := staticLabelChainTokenError(item.labels, scope); err != nil {
				return err
			}
		}
	}
	return nil
}

func staticSetItemsTokenError(assignments []string, scope staticTypeScope) error {
	for _, assignment := range assignments {
		target, property, operator, right := splitSetAssignment(assignment)
		if operator != ":" && operator != "" {
			// A property write (x.p = v, x[k] = v, x = m, x += m) needs a node
			// or a relationship.
			switch typeName := scope.typeOf(target); typeName {
			case "", "Node", "Relationship":
			default:
				return typeNameMismatchError("Node or Relationship", typeName)
			}
		}
		switch operator {
		case "[]=":
			if err := staticDynamicTokenError(property, scope, dynamicTokenKey); err != nil {
				return err
			}
		case ":":
			if err := staticLabelTargetError(target, scope); err != nil {
				return err
			}
			items, err := setLabelChainItems(right)
			if err != nil {
				return err
			}
			if err := staticLabelChainTokenError(items, scope); err != nil {
				return err
			}
		}
	}
	return nil
}

func staticLabelChainTokenError(items []labelChainItem, scope staticTypeScope) error {
	for _, item := range items {
		if item.expression == "" {
			continue
		}
		if err := staticDynamicTokenError(item.expression, scope, dynamicTokenLabel); err != nil {
			return err
		}
	}
	return nil
}

// staticLabelTargetError rejects a label item on a variable whose static type
// isn't a node: only nodes have labels.
func staticLabelTargetError(variable string, scope staticTypeScope) error {
	if typeName := scope.typeOf(variable); typeName != "" && typeName != "Node" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreLabelTargetTypeMismatch(typeName))
	}
	return nil
}

// dynamicTokenUse is where a dynamic token's value is read.
type dynamicTokenUse int

const (
	dynamicTokenLabel dynamicTokenUse = iota // $(e) in a label chain
	dynamicTokenKey                          // x[e] in SET or REMOVE
)

// staticDynamicTokenError is the compile-time error of a dynamic token whose
// value is known: a literal null, a literal '' (in REMOVE x[''] too, though a
// '' computed at run time removes nothing), a list literal holding either
// (for a label), or an expression whose static type a label (STRING,
// LIST<STRING>) or a key (STRING) can't take.
func staticDynamicTokenError(expression string, scope staticTypeScope, use dynamicTokenUse) error {
	expression = strings.TrimSpace(expression)
	for {
		inner, wrapped := stripEnclosingExpressionParentheses(expression)
		if !wrapped {
			break
		}
		expression = strings.TrimSpace(inner)
	}
	literals := []string{expression}
	if inner, isList := stripEnclosingRowDelimiter(expression, '[', ']'); isList && use == dynamicTokenLabel {
		literals = splitTopLevelComma(inner)
	}
	for _, literal := range literals {
		literal = strings.TrimSpace(literal)
		if strings.EqualFold(literal, "null") {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreTokenNameNull())
		}
		if value, ok := parseLiteralValueFromComputedRow(literal); ok {
			if text, isText := value.(string); isText && text == "" {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreTokenNameInvalid(""))
			}
		}
	}
	typeName := scope.staticExpressionType(expression)
	switch {
	case typeName == "" || typeName == "String" || typeName == "Any":
		return nil
	case use == dynamicTokenLabel:
		if strings.HasPrefix(typeName, "List<") && (typeName == "List<String>" || typeName == "List<T>" || typeName == "List<Any>") {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreDynamicLabelTypeMismatch(typeName))
	default:
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreEntityPropertyKeyTypeMismatch(typeName))
	}
}
