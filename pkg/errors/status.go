// Author: Codex, NOR-180 status-diagnostic propagation, 2026-10-27.
package errors

import (
	stderrors "errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
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
	TransactionCommitFailed = "Neo.DatabaseError.Transaction.TransactionCommitFailed"
	// TransactionRollbackFailed is the status of a ROLLBACK that failed for a
	// reason with no more specific classification.
	TransactionRollbackFailed = "Neo.DatabaseError.Transaction.TransactionRollbackFailed"
	// TransactionStartFailed is the status of a BEGIN that failed for a reason
	// with no more specific classification.
	TransactionStartFailed = "Neo.DatabaseError.Transaction.TransactionStartFailed"
	// TransactionTimedOut is the status of a statement stopped by a timeout
	// the server configures (Neo4j's db.transaction.timeout; NornicDB's query
	// time limit and Bolt statement timeout).
	TransactionTimedOut = "Neo.ClientError.Transaction.TransactionTimedOut"
	// TransactionTimedOutClientConfiguration is the status of a statement or
	// transaction stopped by the timeout its client set when it started.
	TransactionTimedOutClientConfiguration = "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"

	// TransactionTimedOutMessage and TransactionTimedOutClientConfigurationMessage
	// are Neo4j 5.26's messages for those statuses, trailing space included.
	TransactionTimedOutMessage                    = "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. The transaction has not completed within the specified timeout (db.transaction.timeout). You may want to retry with a longer timeout. "
	TransactionTimedOutClientConfigurationMessage = "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. The transaction has not completed within the timeout specified at its start by the client. You may want to retry with a longer timeout. "
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

type compileTimeError struct{ error }

func (err *compileTimeError) Unwrap() error { return err.error }

// MarkCompileTime records that an error occurred during statement preparation.
// For example, conflicting execution options are ArgumentError at compile time,
// while invalid function arguments may use the same status at runtime.
func MarkCompileTime(err error) error {
	if err == nil {
		return nil
	}
	var marked *compileTimeError
	if stderrors.As(err, &marked) {
		return err
	}
	return &compileTimeError{err}
}

// IsCompileTimeError reports a preparation error, including wrapped errors,
// or an error with a status that always occurs at compile time.
// Use it when deciding whether a failed HTTP statement has a result shell.
func IsCompileTimeError(err error) bool {
	if err == nil {
		return false
	}
	var marked *compileTimeError
	if stderrors.As(err, &marked) {
		return true
	}
	code, _ := Neo4jStatus(err)
	return IsCompileTimeStatus(code)
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
//     (transientTransactionStatus);
//   - a transaction over the storage size limit is
//     General.TransactionOutOfMemoryError, a ClientError drivers don't retry,
//     with a message saying to split it;
//   - a statement an owned deadline stopped (MarkStatementTimeout, from its
//     context's cause) is Transaction.TransactionTimedOut, or
//     TransactionTimedOutClientConfiguration for a Bolt client's tx_timeout;
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

// RenderStatus renders the immutable outcome at every protocol's existing
// response boundary. Localization changes display only,not classification.
func RenderStatus(err error, render func(localization.Message) string) (code, message string) {
	code, message, _ = renderStatus(err, statusRendering{render: render})
	return code, message
}

// RenderStatusWithFallback preserves a protocol's ordinary-error policies while
// keeping them outside the already-owned terminal outcome's classification:
// classify returns ok with the protocol's own status and message for an error
// it classifies; any other error gets Neo4jStatus's classification.
func RenderStatusWithFallback(err error, render func(localization.Message) string, classify func(error) (code, message string, ok bool)) (code, message string) {
	code, message, _ = renderStatus(err, statusRendering{render: render, classify: classify})
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
	return renderStatus(err, statusRendering{})
}

// neo4jStatusPlain is the existing classification owner for an original primary.
// Terminal producers snapshot it once;protocols never apply it to a child again.
func neo4jStatusPlain(err error) (code, message string, classified bool) {
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
	if transientCode, ok := transientTransactionStatus(err); ok {
		return transientCode, message, true
	}
	if stderrors.Is(err, storage.ErrStorageRestoring) {
		return TransientDatabaseUnavailable, message, true
	}
	if storage.IsTransactionTooBig(err) {
		return ClientTransactionOutOfMemory, transactionTooBigMessage(message), true
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

type statusDiagnostic interface{ StatusDiagnostic() localization.Message }

// HasStatusDiagnostic distinguishes a retained diagnostic from ordinary cleanup
// uncertainty. Protocols keep their existing fatal policy for both outcomes.
func HasStatusDiagnostic(err error) bool {
	var diagnostic statusDiagnostic
	return stderrors.As(err, &diagnostic)
}

// AppendStatusDiagnostics preserves supplementary failures when a boundary
// replaces the primary error's message. Ordinary wrapped causes retain their
// existing formatting. A marked error owns its descriptor and is visited once.
func AppendStatusDiagnostics(err error, message string, render func(localization.Message) string) string {
	_, message, _ = renderStatus(err, statusRendering{render: render, diagnosticsOnto: &message})
	return message
}

// transactionControlStatus is the single transaction-control status rule,
// applied where an operation's failure is produced (TransactionControlFailure,
// NewTerminalFailure): a status of the failure's own is kept; only an
// unclassified failure takes the operation's status (none: unchanged).
// Protocols then render every outcome with the one status renderer.
func transactionControlStatus(code string, classified bool, fallback string) (string, bool) {
	if classified || fallback == "" {
		return code, classified
	}
	return fallback, true
}

// renderTerminalView is the single representation of an owned finish outcome.
// Child identity controls composition;equal messages from different children
// remain separate classified outcomes rather than being deduplicated as text.
func renderTerminalView(view *terminalView, render func(localization.Message) string) string {
	var result strings.Builder
	appendText := func(text string) {
		if text == "" {
			return
		}
		if result.Len() != 0 {
			result.WriteByte('\n')
		}
		result.WriteString(text)
	}
	messageText := func(message outcomeMessage) string {
		if render != nil && message.descriptor != nil {
			return render(cloneOutcomeMessage(*message.descriptor))
		}
		return message.text
	}
	for _, preface := range view.prefaces {
		text := messageText(preface.message)
		if preface.code != "" {
			text = preface.code + ": " + text
		}
		appendText(text)
	}
	for _, record := range view.records {
		primary, diagnostic := messageText(record.primary), messageText(record.diagnostic)
		if primary != "" {
			appendText(record.code + ": " + primary)
			appendText(messageText(record.detail))
			appendText(diagnostic)
		} else if diagnostic != "" {
			appendText(record.code + ": " + diagnostic)
		}
	}
	return result.String()
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
