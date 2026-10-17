package errors

import stderrors "errors"

// statementTimeoutError is a statement's error after the statement's own
// deadline stopped it: Neo4j's transaction timeout. Only the statement's owner
// (the executor, which knows the statement's context) marks one; a
// dependency's own deadline (an embedder, a remote call) keeps its status.
// callerDeadline records which deadline stopped it: the caller's (a Bolt
// client's tx_timeout or the server's statement timeout, an HTTP request's)
// or only the database's query time limit the executor applied.
type statementTimeoutError struct {
	cause          error
	callerDeadline bool
}

func (e *statementTimeoutError) Error() string         { return e.cause.Error() }
func (e *statementTimeoutError) Unwrap() error         { return e.cause }
func (e *statementTimeoutError) BoltErrorCode() string { return TransactionTimedOut }
func (e *statementTimeoutError) StatusMessage() string { return TransactionTimedOutMessage }

// MarkStatementTimeout marks err as the error of a statement its own deadline
// stopped (Transaction.TransactionTimedOut); callerDeadline says the caller's
// deadline expired, not only the database's query time limit. nil stays nil.
func MarkStatementTimeout(err error, callerDeadline bool) error {
	if err == nil {
		return nil
	}
	var marked *statementTimeoutError
	if stderrors.As(err, &marked) {
		return err
	}
	return &statementTimeoutError{cause: err, callerDeadline: callerDeadline}
}

// CallerDeadlineTimeout reports whether err is a statement timeout its
// caller's deadline caused (MarkStatementTimeout with callerDeadline): a Bolt
// client's tx_timeout then reports TransactionTimedOutClientConfiguration.
func CallerDeadlineTimeout(err error) bool {
	var marked *statementTimeoutError
	return stderrors.As(err, &marked) && marked.callerDeadline
}

// statementDeadline is the cancellation cause a statement deadline's owner
// installs it with (context.WithTimeoutCause).
type statementDeadline struct {
	text, code, message string
}

func (d *statementDeadline) Error() string { return d.text }

// The owned statement deadlines.
var (
	ErrDatabaseQueryTimeLimit   error = &statementDeadline{"database query time limit exceeded", TransactionTimedOut, TransactionTimedOutMessage}
	ErrServerStatementTimeout   error = &statementDeadline{"server statement timeout exceeded", TransactionTimedOut, TransactionTimedOutMessage}
	ErrClientTransactionTimeout error = &statementDeadline{"client transaction timeout exceeded", TransactionTimedOutClientConfiguration, TransactionTimedOutClientConfigurationMessage}
)
