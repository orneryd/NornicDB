// Author: Codex,NOR-180 immutable terminal outcomes,29October2026; discovery and
// composition through the one error walk (statusScan) by Claude, 30 October 2026;
// the committed-outcome retry rule (committedStatus), the one committed
// representation and the invocation's committed work by Claude, 30 October 2026.
package errors

import (
	stderrors "errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// TerminalOperation identifies the logical operation whose finish record failed.
// It is supplied by that operation's owner,never inferred from a later response.
type TerminalOperation uint8

const (
	TerminalCommit TerminalOperation = iota
	TerminalRollback
	TerminalEmptyCompletion
	// TerminalAbort ends a transaction because one of its statements failed.
	// That statement's failure is the primary and keeps its own status.
	TerminalAbort
	// TerminalBegin is a transaction's start.
	TerminalBegin
)

// fallbackStatus is the status of the operation's own unclassified failure;
// a statement abort has none, so its statement keeps its status.
func (op TerminalOperation) fallbackStatus() string {
	switch op {
	case TerminalRollback:
		return TransactionRollbackFailed
	case TerminalAbort:
		return ""
	case TerminalBegin:
		return TransactionStartFailed
	default:
		return TransactionCommitFailed
	}
}

type outcomeMessage struct {
	text       string
	descriptor *localization.Message
	// Retain producer-owned source facts for the existing source mapper.
	// Rendering reads only the frozen text/descriptor,never this cause.
	source error
}

// A record is immutable. identity remains the original produced failure even
// when a source/localization boundary creates a different display projection.
type terminalRecord struct {
	identity   *TerminalFailure
	operation  TerminalOperation
	committed  bool
	code       string
	primary    outcomeMessage
	detail     outcomeMessage
	diagnostic outcomeMessage
}

type outcomePreface struct {
	identity error
	code     string
	message  outcomeMessage
}

type terminalView struct {
	code     string
	prefaces []outcomePreface
	records  []terminalRecord
	// committedWork is the completion owner's fact that the invocation this
	// outcome completes committed native work, whatever its children returned
	// (TerminalOutcomes.Retain). A record's committed is its own finish's.
	committedWork bool
}

// settleCode applies the committed-outcome rule to the view's overall code:
// once the outcome contains committed work, it is never safe to retry. Each
// record and preface keeps its own code.
func (v *terminalView) settleCode() {
	committed := v.committedWork
	for i := range v.records {
		committed = committed || v.records[i].committed
	}
	if committed {
		v.code = committedStatus(v.code)
	}
}

// withCommittedWork gives a terminal outcome its invocation's committed truth.
// Ordinary errors and nil are returned unchanged: committed work alone never
// makes a failure, and an ordinary error keeps its own status policy. An
// outcome that already carries committed truth is returned as it is.
func withCommittedWork(err error) error {
	if err == nil {
		return nil
	}
	found := lookupTerminalView(err)
	if found == nil || found.committedWork {
		return err
	}
	for i := range found.records {
		if found.records[i].committed {
			return err
		}
	}
	view := collectTerminalView(err)
	view.committedWork = true
	view.settleCode()
	return &terminalOutcomeError{cause: err, view: view}
}

// TerminalFailure owns one failed finish and its immutable classified outcome.
// Unwrap retains the actual causes for errors.Is/As and existing retry policy.
type TerminalFailure struct {
	cause      error
	diagnostic localization.Message
	view       terminalView
}

func (e *TerminalFailure) Error() string                      { return renderTerminalView(&e.view, nil) }
func (e *TerminalFailure) Unwrap() error                      { return e.cause }
func (e *TerminalFailure) BoltErrorCode() string              { return e.view.code }
func (e *TerminalFailure) StatusMessage() string              { return renderTerminalView(&e.view, nil) }
func (e *TerminalFailure) terminalOutcomeView() *terminalView { return &e.view }

// StatusDiagnostic retains the existing structural diagnostic contract. The
// response renderer consumes the complete immutable outcome instead of this leaf.
func (e *TerminalFailure) StatusDiagnostic() localization.Message {
	return cloneOutcomeMessage(e.diagnostic)
}

type terminalOutcomeCarrier interface {
	error
	terminalOutcomeView() *terminalView
}

type terminalOutcomeError struct {
	cause error
	view  terminalView
}

type ownedStatusError struct {
	cause   error
	code    string
	message localization.Message
}

func (e *ownedStatusError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message.Fallback
}
func (e *ownedStatusError) Unwrap() error         { return e.cause }
func (e *ownedStatusError) BoltErrorCode() string { return e.code }
func (e *ownedStatusError) StatusMessage() string { return e.message.Fallback }
func (e *ownedStatusError) As(target any) bool {
	if localized, ok := target.(**localization.LocalizedError); ok {
		*localized = localization.NewLocalizedError(e.code, cloneOutcomeMessage(e.message), e.cause)
		return true
	}
	return false
}

// controlFailure is a transaction-control operation's own failure, classified
// where the operation produced it.
type controlFailure struct {
	cause error
	code  string
}

func (e *controlFailure) Error() string         { return e.cause.Error() }
func (e *controlFailure) Unwrap() error         { return e.cause }
func (e *controlFailure) BoltErrorCode() string { return e.code }

// TransactionControlFailure classifies the failure of a BEGIN, COMMIT,
// ROLLBACK or post-commit step where that operation produces it: a failure
// with a status of its own (including a produced terminal outcome) is
// returned unchanged; an unclassified one takes the operation's status. Every
// consumer then renders it with the one status renderer, whatever route it
// takes to the client.
func TransactionControlFailure(op TerminalOperation, err error) error {
	if err == nil {
		return nil
	}
	if _, _, classified := neo4jStatusPlain(err); classified || lookupTerminalView(err) != nil {
		return err
	}
	code, _ := transactionControlStatus(StatementSyntaxError, false, op.fallbackStatus())
	return &controlFailure{cause: err, code: code}
}

// IsRetriableStatus reports whether a Neo4j driver retries a transaction that
// failed with code: its classification is TransientError, or it is one of the
// cluster and authorization codes drivers retry (neo4j-go-driver
// Neo4jError.IsRetriable, which also treats the pre-5.x transient
// Terminated and LockClientStopped codes as ClientError).
func IsRetriableStatus(code string) bool {
	switch code {
	case "Neo.ClientError.Cluster.NotALeader", "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase", "Neo.ClientError.Security.AuthorizationExpired":
		return true
	case "Neo.TransientError.Transaction.Terminated", "Neo.TransientError.Transaction.LockClientStopped":
		return false
	}
	return strings.HasPrefix(code, "Neo.TransientError.")
}

// committedStatus is the one status decision for an outcome that contains
// committed work: it never tells a client to retry, since a retry repeats work
// that is already stored. A retriable status becomes the COMMIT operation's
// own status (TransactionCommitFailed, a DatabaseError, which drivers never
// retry), taken from its owner; any other status is kept.
func committedStatus(code string) string {
	if IsRetriableStatus(code) {
		return TerminalCommit.fallbackStatus()
	}
	return code
}

// WithStatus records a separate enclosing rejection at its existing policy
// owner. It never changes a child's already-produced operation or classification.
func WithStatus(cause error, code string, message localization.Message) error {
	status := &ownedStatusError{cause: cause, code: code, message: cloneOutcomeMessage(message)}
	if lookupTerminalView(cause) == nil {
		return status
	}
	primary := &ownedStatusError{code: code, message: cloneOutcomeMessage(message)}
	return CombineTerminalOutcomes(primary, cause)
}

func (e *terminalOutcomeError) Error() string                      { return renderTerminalView(&e.view, nil) }
func (e *terminalOutcomeError) Unwrap() error                      { return e.cause }
func (e *terminalOutcomeError) BoltErrorCode() string              { return e.view.code }
func (e *terminalOutcomeError) StatusMessage() string              { return renderTerminalView(&e.view, nil) }
func (e *terminalOutcomeError) terminalOutcomeView() *terminalView { return &e.view }

func cloneOutcomeMessage(message localization.Message) localization.Message {
	if message.Data != nil {
		data := make(map[string]any, len(message.Data))
		for key, value := range message.Data {
			data[key] = value
		}
		message.Data = data
	}
	return message
}

func snapshotOutcomeMessage(err error, text string) outcomeMessage {
	message := outcomeMessage{text: text, source: err}
	var localized *localization.LocalizedError
	if stderrors.As(err, &localized) && localized.Message.Fallback == text {
		descriptor := cloneOutcomeMessage(localized.Message)
		message.descriptor = &descriptor
	}
	return message
}

// NewTerminalFailure freezes classification and native truth at the logical
// finish owner. Later cancellation,RESET or aggregation cannot relabel a child.
// terminal is the failed logical record; it is nil, with an empty diagnostic,
// for a committed transaction whose record was written but whose post-commit
// step failed. Every committed outcome is this one value, so composition and
// retention see its committed truth whether or not its record failed.
func NewTerminalFailure(operation TerminalOperation, committed bool, primary, terminal error, diagnostic localization.Message) *TerminalFailure {
	code, message, classified := neo4jStatusPlain(primary)
	code, _ = transactionControlStatus(code, classified, operation.fallbackStatus())
	failure := &TerminalFailure{cause: compose.Outcomes(primary, terminal), diagnostic: cloneOutcomeMessage(diagnostic)}
	view := collectTerminalView(primary)
	detail := outcomeMessage{}
	if primary != nil && len(view.records) == 0 && primary.Error() != message {
		detail = snapshotOutcomeMessage(primary, primary.Error())
	}
	if len(view.records) != 0 {
		code = view.code
		// Existing child outcomes already carry their messages individually.
		// Do not copy their rendered diagnostics into this record's primary.
		message = ""
	} else {
		// This record already owns the original primary;it is not a separate
		// enclosing outcome in addition to itself.
		view.prefaces = nil
	}
	// A produced record's classification is frozen here: a later
	// transaction-control boundary never replaces it with its own status.
	record := terminalRecord{
		identity: failure, operation: operation, committed: committed, code: code,
		primary: snapshotOutcomeMessage(primary, message),
		detail:  detail,
	}
	if diagnostic.ID != "" || diagnostic.Fallback != "" {
		record.diagnostic = outcomeMessage{text: diagnostic.Fallback, descriptor: &failure.diagnostic}
	}
	failure.view = terminalView{code: code, prefaces: view.prefaces, records: append(view.records, record)}
	failure.view.settleCode()
	return failure
}

// lookupTerminalView is the view of the first terminal outcome carrier in err,
// found by the one error walk (statusScan) in discovery-only mode.
func lookupTerminalView(err error) *terminalView {
	scan := statusScan{carrierOnly: true}
	scan.walk(err)
	if !scan.carrier {
		return nil
	}
	return scan.found.terminalOutcomeView()
}

// HasTerminalOutcome reports an actual owner-produced failed finish value,
// including wrapped and composed values,without deriving a status from text.
func HasTerminalOutcome(err error) bool { return lookupTerminalView(err) != nil }

// responseTerminalView consumes a complete error forest, including wrappers
// added after completion. Composition copies the children's owned statuses;
// it never derives a child's status from its wrapper or cancellation reason.
func responseTerminalView(err error) *terminalView {
	scan := statusScan{carrierOnly: true}
	scan.walk(err)
	return scan.terminalView(err)
}

// collectTerminalView is the single composition owner. It combines immutable
// child values and separate primary outcomes;it never reclassifies a child.
func collectTerminalView(err error) terminalView {
	// The one error walk lists the forest in order: every carrier, and every
	// separate enclosing primary next to them (statusScan compose mode).
	scan := statusScan{compose: true}
	scan.walk(err)
	view := terminalView{}
	seen := map[*TerminalFailure]bool{}
	for _, item := range scan.items {
		if item.carrier != nil {
			child := item.carrier.terminalOutcomeView()
			if view.code == "" {
				view.code = child.code
			}
			view.committedWork = view.committedWork || child.committedWork
			for _, preface := range child.prefaces {
				found := false
				for _, old := range view.prefaces {
					if stderrors.Is(old.identity, preface.identity) {
						found = true
						break
					}
				}
				if !found {
					view.prefaces = append(view.prefaces, preface)
				}
			}
			for _, record := range child.records {
				if !seen[record.identity] {
					seen[record.identity] = true
					view.records = append(view.records, record)
				}
			}
			continue
		}
		// This is a separate enclosing primary,not a status projection of a
		// finished child. Keep its original status/message next to the children.
		current := item.primary
		code, message, classified := neo4jStatusPlain(current)
		if !classified {
			code = ""
		}
		if view.code == "" && code != "" {
			view.code = code
		}
		view.prefaces = append(view.prefaces, outcomePreface{current, code, snapshotOutcomeMessage(current, message)})
	}
	view.settleCode()
	return view
}

// NormalizeTerminalOutcome seals a complete return value at its existing owner
// boundary before an immediate or deferred consumer receives it. Ordinary errors
// keep their release behaviour and allocate no terminal value.
func NormalizeTerminalOutcome(err error) error {
	if err == nil || lookupTerminalView(err) == nil {
		return err
	}
	if _, ok := err.(terminalOutcomeCarrier); ok {
		return err
	}
	view := collectTerminalView(err)
	return &terminalOutcomeError{cause: err, view: view}
}

// CombineTerminalOutcomes is the one composition of a primary failure with
// its cleanup/rollback failures: the primary stays first, every other cause
// is retained once, and terminal values compose through the same owner.
func CombineTerminalOutcomes(primary error, required ...error) error {
	return NormalizeTerminalOutcome(compose.Outcomes(primary, required...))
}

// ProjectTerminalOutcomeMessages consumes the existing display owner;status,
// operation,native truth and child identity remain unchanged in the new view.
func ProjectTerminalOutcomeMessages(err error, project func(error) error) error {
	if err == nil || lookupTerminalView(err) == nil {
		return err
	}
	normalized := NormalizeTerminalOutcome(err)
	old := lookupTerminalView(normalized)
	view := terminalView{code: old.code, prefaces: make([]outcomePreface, len(old.prefaces)), records: make([]terminalRecord, len(old.records)), committedWork: old.committedWork}
	projectMessage := func(message outcomeMessage) outcomeMessage {
		if message.text == "" {
			return message
		}
		descriptor := localization.ExternalDiagnostic(stderrors.New(message.text))
		if message.descriptor != nil {
			descriptor = cloneOutcomeMessage(*message.descriptor)
		}
		// A text-only replacement would lose typed source-offset facts. Supply
		// the frozen display alongside the original Is/As source identity.
		localized := localization.NewLocalizedError(string(descriptor.ID), descriptor, nil)
		display := localization.ProjectError(message.source, message.text, localized, nil)
		projected := project(display)
		return snapshotOutcomeMessage(projected, projected.Error())
	}
	for i, preface := range old.prefaces {
		preface.message = projectMessage(preface.message)
		view.prefaces[i] = preface
	}
	for i, record := range old.records {
		record.primary = projectMessage(record.primary)
		record.detail = projectMessage(record.detail)
		// The terminal owner supplied this operational transaction/WAL
		// descriptor. Source-detail suppression must not erase that outcome.
		view.records[i] = record
	}
	return &terminalOutcomeError{cause: err, view: view}
}
