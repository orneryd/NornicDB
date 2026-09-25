package errors

import (
	stderrors "errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// StatementSyntaxError is the status of a statement error with no more
	// specific classification.
	StatementSyntaxError = "Neo.ClientError.Statement.SyntaxError"
	// ConstraintValidationFailed is the status of a write that violates a
	// schema constraint (uniqueness, existence, key, type), as in Neo4j.
	ConstraintValidationFailed = "Neo.ClientError.Schema.ConstraintValidationFailed"
	// TransactionCommitFailed is the status of a COMMIT that failed for a
	// reason with no more specific classification.
	TransactionCommitFailed = "Neo.ClientError.Transaction.TransactionCommitFailed"
)

// compileTimeStatuses are the statuses Neo4j raises while compiling a
// statement, before it runs.
var compileTimeStatuses = map[string]struct{}{
	StatementSyntaxError:                               {},
	"Neo.ClientError.Statement.SemanticError":          {},
	"Neo.ClientError.Statement.ParameterMissing":       {},
	"Neo.ClientError.Procedure.ProcedureNotFound":      {},
	"Neo.ClientError.Statement.NotSystemDatabaseError": {},
}

// IsCompileTimeStatus reports whether code is raised while a statement is
// compiled. A statement failing with any other status compiled, so it has a
// result with its columns, as Neo4j's HTTP API reports next to the error
// (#668).
func IsCompileTimeStatus(code string) bool {
	_, ok := compileTimeStatuses[code]
	return ok
}

// ErrCommitRolledBack marks a COMMIT failure after which nothing the
// transaction wrote is stored (the failure was detected before any write), so
// the outcome is known and the client connection can stay usable.
var ErrCommitRolledBack = stderrors.New("transaction rolled back at commit")

// statusCoder is an error that carries its Neo4j status code.
type statusCoder interface {
	BoltErrorCode() string
}

// statusMessenger is an error that supplies the message clients see for it,
// instead of the text of the error chain that wrapped it.
type statusMessenger interface {
	StatusMessage() string
}

// Neo4jStatus returns the Neo4j status code and message a client gets for a
// statement error. Bolt and HTTP both report statement errors through it, so
// a failure has the same code and message on every protocol:
//   - an error that carries its code (BoltErrorCode) keeps it;
//   - transaction conflicts and MVCC pressure are transient
//     (MapTransientTransactionError);
//   - a constraint violation is Schema.ConstraintValidationFailed;
//   - a message that starts with (or contains) a "Neo.…: " code keeps that code;
//   - anything else is Statement.SyntaxError.
//
// The message is the error's text, or its StatusMessage when it has one, and
// never repeats the code: a leading "<code>: " is removed.
func Neo4jStatus(err error) (code, message string) {
	code, message, _ = neo4jStatus(err)
	return code, message
}

// HasNeo4jStatus reports whether err carries its own Neo4j status (a status
// code, a transient transaction failure, a constraint violation or a "Neo."
// prefix), rather than getting Neo4jStatus's SyntaxError default.
func HasNeo4jStatus(err error) bool {
	_, _, classified := neo4jStatus(err)
	return classified
}

// neo4jStatus is Neo4jStatus, and whether err's status came from err itself.
func neo4jStatus(err error) (code, message string, classified bool) {
	if err == nil {
		return StatementSyntaxError, "", false
	}
	message = err.Error()
	var coded statusCoder
	if stderrors.As(err, &coded) {
		if code = coded.BoltErrorCode(); code != "" {
			var messenger statusMessenger
			if stderrors.As(err, &messenger) {
				return code, messenger.StatusMessage(), true
			}
			return code, trimStatusPrefix(message, code), true
		}
	}
	if transientCode, ok := MapTransientTransactionError(err); ok {
		return transientCode, message, true
	}
	var violation *storage.ConstraintViolationError
	if stderrors.As(err, &violation) && violation != nil {
		return ConstraintValidationFailed, message, true
	}
	if start := strings.Index(message, "Neo."); start >= 0 {
		rest := message[start:]
		if separator := strings.Index(rest, ":"); separator > 0 {
			return strings.TrimSpace(rest[:separator]), strings.TrimSpace(rest[separator+1:]), true
		}
		if start == 0 {
			return message, message, true
		}
	}
	return StatementSyntaxError, message, false
}

// Neo4jCommitStatus is Neo4jStatus for a failed COMMIT: a failure with no more
// specific classification is Transaction.TransactionCommitFailed.
func Neo4jCommitStatus(err error) (code, message string) {
	code, message = Neo4jStatus(err)
	if code == StatementSyntaxError {
		code = TransactionCommitFailed
	}
	return code, message
}

// trimStatusPrefix removes a leading "<code>: " from message.
func trimStatusPrefix(message, code string) string {
	if rest, ok := strings.CutPrefix(message, code); ok {
		if rest, ok = strings.CutPrefix(rest, ":"); ok {
			return strings.TrimSpace(rest)
		}
	}
	return message
}

type commitRolledBackError struct {
	err error
}

func (e *commitRolledBackError) Error() string { return e.err.Error() }

func (e *commitRolledBackError) Unwrap() error { return e.err }

func (e *commitRolledBackError) Is(target error) bool { return target == ErrCommitRolledBack }

// MarkCommitRolledBack tags a COMMIT failure as rolled back (ErrCommitRolledBack)
// without changing its message. Only the caller that ran the commit knows
// nothing was written, so it decides.
func MarkCommitRolledBack(err error) error {
	if err == nil || stderrors.Is(err, ErrCommitRolledBack) {
		return err
	}
	return &commitRolledBackError{err: err}
}

// IsCommitRolledBack reports whether a COMMIT failure was tagged by
// MarkCommitRolledBack.
func IsCommitRolledBack(err error) bool {
	return stderrors.Is(err, ErrCommitRolledBack)
}
