package cypher

import (
	"fmt"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// SemanticError describes a Cypher validation failure that must retain its
// Neo4j-compatible Bolt classification and conformance detail.
type SemanticError struct {
	Code    string
	Detail  string
	Message string
}

// classifiedCypherError adds Neo4j/TCK classification to an existing typed
// error without changing its localized message or unwrap identity.
type classifiedCypherError struct {
	cause  error
	code   string
	detail string
}

func (e *classifiedCypherError) Error() string           { return e.cause.Error() }
func (e *classifiedCypherError) Unwrap() error           { return e.cause }
func (e *classifiedCypherError) BoltErrorCode() string   { return e.code }
func (e *classifiedCypherError) BoltErrorDetail() string { return e.detail }

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

// procedureCallFailedError is Neo4j's Procedure.ProcedureCallFailed: a
// procedure failed while running, with a cause that carries no status of its
// own (#657).
type procedureCallFailedError struct {
	procedure string
	cause     error
}

func (e *procedureCallFailedError) Error() string {
	return fmt.Sprintf("Failed to invoke procedure `%s`: Caused by: %s", e.procedure, e.cause.Error())
}
func (e *procedureCallFailedError) Unwrap() error { return e.cause }
func (e *procedureCallFailedError) BoltErrorCode() string {
	return "Neo.ClientError.Procedure.ProcedureCallFailed"
}

// procedureRuntimeError is the error of procedure failing while running with
// err: ProcedureCallFailed, as in Neo4j, unless err carries its own status
// (a Cypher error of a statement the procedure ran, a transient failure, ...).
// Every CALL dispatch reports procedure failures through it.
func procedureRuntimeError(procedure string, err error) error {
	if nornicerrors.HasNeo4jStatus(err) {
		return err
	}
	return &procedureCallFailedError{procedure: procedure, cause: err}
}

func newSemanticError(code, detail, message string) error {
	return &SemanticError{Code: code, Detail: detail, Message: message}
}
