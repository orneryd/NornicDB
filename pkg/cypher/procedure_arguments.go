package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Procedure argument readers. A built-in procedure's handler receives the
// call's evaluated arguments (literals, parameters, variables, any
// expression) after the registry checked their count and, for statically
// typed ones, their types; these read them by position. As in Neo4j, null
// is a value of every type, so a null the procedure needs is the
// procedure's own failure (reported as ProcedureCallFailed), and a value of
// another type, known only when the statement runs, is a TypeError.

// procedureArgumentNullError is the failure of a procedure called with null
// (or nothing) for an argument it needs.
func procedureArgumentNullError(procedure, argument string) error {
	return localizedError(localization.CypherProceduresArgumentNull(procedure, argument), nil)
}

// procedureArgumentTypeError is the TypeError of an argument value of
// another type than the procedure takes.
func procedureArgumentTypeError(procedure, argument, expected string, value interface{}) error {
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherProceduresArgumentType(procedure, argument, expected, cypherTypeSystemName(value)))
}

// procedureArgument is the index-th argument, nil when the call passed none.
func procedureArgument(args []interface{}, index int) interface{} {
	if index < len(args) {
		return args[index]
	}
	return nil
}

// requiredProcedureString reads a STRING argument the procedure needs.
func requiredProcedureString(procedure string, args []interface{}, index int, argument string) (string, error) {
	value := procedureArgument(args, index)
	if value == nil {
		return "", procedureArgumentNullError(procedure, argument)
	}
	text, isString := value.(string)
	if !isString {
		return "", procedureArgumentTypeError(procedure, argument, "STRING", value)
	}
	return text, nil
}

// requiredProcedureInteger reads an INTEGER argument the procedure needs.
func requiredProcedureInteger(procedure string, args []interface{}, index int, argument string) (int64, error) {
	value := procedureArgument(args, index)
	if value == nil {
		return 0, procedureArgumentNullError(procedure, argument)
	}
	if !isIntegerProcedureValue(value) {
		return 0, procedureArgumentTypeError(procedure, argument, "INTEGER", value)
	}
	return toInt64(value), nil
}

// requiredProcedureStringList reads a LIST<STRING> argument the procedure
// needs; a single STRING is NornicDB's kept one-element form
// (createNodeIndex('i', 'Label', 'name')).
func requiredProcedureStringList(procedure string, args []interface{}, index int, argument string) ([]string, error) {
	value := procedureArgument(args, index)
	if value == nil {
		return nil, procedureArgumentNullError(procedure, argument)
	}
	switch typed := value.(type) {
	case string:
		return []string{typed}, nil
	case []string:
		return append([]string(nil), typed...), nil
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, procedureArgumentTypeError(procedure, argument, "LIST<STRING>", value)
	}
	texts := make([]string, len(items))
	for position, item := range items {
		text, isString := item.(string)
		if !isString {
			return nil, procedureArgumentTypeError(procedure, argument, "LIST<STRING>", value)
		}
		texts[position] = text
	}
	return texts, nil
}

// optionalProcedureMap reads an optional MAP argument: none or null is an
// empty map.
func optionalProcedureMap(procedure string, args []interface{}, index int, argument string) (map[string]interface{}, error) {
	value := procedureArgument(args, index)
	if value == nil {
		return map[string]interface{}{}, nil
	}
	entries, isMap := value.(map[string]interface{})
	if !isMap || cypherValueKindOf(value) != valueKindMap {
		return nil, procedureArgumentTypeError(procedure, argument, "MAP", value)
	}
	return entries, nil
}
