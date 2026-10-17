package errors

import (
	"context"
	stderrors "errors"
)

// statementDeadline is the cancellation cause a statement deadline's owner
// installs it with (context.WithTimeoutCause): the cause records which owner's
// deadline ended the statement, whatever expires after it. It is a deadline,
// so errors.Is(cause, context.DeadlineExceeded) holds.
type statementDeadline struct {
	text, code, message string
}

func (d *statementDeadline) Error() string { return d.text }
func (d *statementDeadline) Unwrap() error { return context.DeadlineExceeded }

// The owned statement deadlines. A deadline installed with any other cause
// (an embedder's request budget, an inherited HTTP request deadline, a
// dependency's own timeout) is not a transaction timeout.
var (
	// ErrDatabaseQueryTimeLimit ends a statement at its database's query
	// time limit (multidb query limits).
	ErrDatabaseQueryTimeLimit error = &statementDeadline{"database query time limit exceeded", TransactionTimedOut, TransactionTimedOutMessage}
	// ErrServerStatementTimeout ends a Bolt statement at the server's
	// statement timeout, for a client that set no tx_timeout.
	ErrServerStatementTimeout error = &statementDeadline{"server statement timeout exceeded", TransactionTimedOut, TransactionTimedOutMessage}
	// ErrClientTransactionTimeout ends a Bolt statement at its client's
	// tx_timeout.
	ErrClientTransactionTimeout error = &statementDeadline{"client transaction timeout exceeded", TransactionTimedOutClientConfiguration, TransactionTimedOutClientConfigurationMessage}
)

// statementTimeoutError is a statement's error after one of its owned
// deadlines stopped it: Neo4j's transaction timeout, with the status of the
// deadline that ended it first.
type statementTimeoutError struct {
	err      error
	deadline *statementDeadline
}

func (e *statementTimeoutError) Error() string         { return e.err.Error() }
func (e *statementTimeoutError) Unwrap() error         { return e.err }
func (e *statementTimeoutError) BoltErrorCode() string { return e.deadline.code }
func (e *statementTimeoutError) StatusMessage() string { return e.deadline.message }

// MarkStatementTimeout marks err, a statement's deadline error, with the
// status of the owned deadline that ended its context first: cause is
// context.Cause of the statement's context. Any other cause leaves err as it
// is. nil stays nil; marking is idempotent.
func MarkStatementTimeout(err, cause error) error {
	if err == nil || !stderrors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var deadline *statementDeadline
	if !stderrors.As(cause, &deadline) {
		return err
	}
	var marked *statementTimeoutError
	if stderrors.As(err, &marked) {
		return err
	}
	return &statementTimeoutError{err: err, deadline: deadline}
}
