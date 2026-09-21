package cypher

import "fmt"

// SemanticError describes a Cypher validation failure that must retain its
// Neo4j-compatible Bolt classification and conformance detail.
type SemanticError struct {
	Code    string
	Detail  string
	Message string
}

// Error formats the error so non-Bolt callers retain the Neo4j status code.
func (e *SemanticError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// BoltErrorCode returns the Neo4j-compatible Bolt error code.
func (e *SemanticError) BoltErrorCode() string { return e.Code }

// BoltErrorDetail returns the openCypher conformance error detail.
func (e *SemanticError) BoltErrorDetail() string { return e.Detail }

func invalidBooleanOperandError(value interface{}) error {
	return &SemanticError{
		Code:    "Neo.ClientError.Statement.SyntaxError",
		Detail:  "InvalidArgumentType",
		Message: fmt.Sprintf("boolean operator requires BOOLEAN or NULL operands, got %T", value),
	}
}
