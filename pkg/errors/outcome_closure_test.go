// Author: Claude, NOR-180 committed truth by construction, 30 October 2026;
// independent per-child oracle and subtree pairs after the 9f16 review;
// every terminal operation and the exact population after the 656f review.
package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// childExpect is the test's own model of one terminal child record: the line
// carrying its own classification (code + ": " + primary text), when it has a
// primary, and a WAL-diagnostic tag that only this record renders. The oracle
// never asks the product which children an outcome holds.
type childExpect struct {
	id      any    // record identity: a leaf, or one finish application
	line    string // "code: primary" ("" when the record has no own primary line)
	diagTag string // unique text of its WAL diagnostic ("" when it has none)
}

// closureValue is an outcome built by the closure test, with what its
// construction guarantees according to the model.
type closureValue struct {
	err       error
	name      string
	committed bool
	retained  []error       // identities that must stay reachable (errors.Is)
	children  []childExpect // terminal child records that must each render
	// ownCommitStatus: a child's own status is DatabaseTransactionCommitFailed (a
	// failed COMMIT record, or a COMMIT or empty completion's own fallback).
	ownCommitStatus bool
}

// terminal is the model's own view: an outcome is terminal when it holds a
// terminal child record.
func (v closureValue) terminal() bool { return len(v.children) != 0 }

// closureConstructor is one construction of the outcome API. name is the
// census's form of the function (outcome-constructor); arity is how many
// outcomes it takes.
type closureConstructor struct {
	name  string
	arity int
	build func(args []closureValue) closureValue
}

const closureMemory = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

var closureTag int

// uniqueWAL is a WAL failure whose text no other record renders.
func uniqueWAL(owner string) (error, localization.Message, string) {
	closureTag++
	tag := fmt.Sprintf("wal closed <%s#%d>", owner, closureTag)
	err := stderrors.New(tag)
	return err, localization.ExternalDiagnostic(err), tag
}

func closureLeaves() []closureValue {
	memory := func(text string) error { return &statusError{code: closureMemory, message: text} }
	abortedLeaf := func(name string) closureValue {
		wal, diag, tag := uniqueWAL(name)
		err := NewTerminalFailure(TerminalAbort, false, memory("memory "+name), wal, diag)
		return closureValue{err: err, name: name, retained: []error{err},
			children: []childExpect{{err, closureMemory + ": memory " + name, tag}}}
	}
	// An invocation whose committed child returned nothing, beside an
	// aborted child whose WAL failed (TerminalOutcomes.Retain).
	siblingWAL, siblingDiag, siblingTag := uniqueWAL("committed_sibling")
	inner := NewTerminalFailure(TerminalAbort, false, memory("memory committed_sibling"), siblingWAL, siblingDiag)
	journal, _ := TerminalOutcomes{}.RecordCommit()
	journal, _ = journal.Record(inner)
	committedSibling := journal.Retain(nil)
	written := NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, nil, localization.Message{})
	failedWAL, failedDiag, failedTag := uniqueWAL("committed_record_failed")
	failedRecord := NewTerminalFailure(TerminalCommit, true, nil, failedWAL, failedDiag)
	timeoutWAL, timeoutDiag, timeoutTag := uniqueWAL("aborted_non_retriable")
	nonRetriable := NewTerminalFailure(TerminalAbort, false, &statusError{code: TransactionTimedOut, message: "timeout aborted_non_retriable"}, timeoutWAL, timeoutDiag)
	ordinary := memory("memory ordinary_transient")
	plain := stderrors.New("plain ordinary_plain")
	return []closureValue{
		{err: committedSibling, name: "committed_sibling", committed: true, retained: []error{committedSibling, inner},
			children: []childExpect{{inner, closureMemory + ": memory committed_sibling", siblingTag}}},
		{err: written, name: "committed_record_written", committed: true, retained: []error{written},
			children: []childExpect{{written, TransientDatabaseUnavailable + ": " + storage.ErrStorageRestoring.Error(), ""}}},
		{err: failedRecord, name: "committed_record_failed", committed: true, retained: []error{failedRecord}, ownCommitStatus: true,
			children: []childExpect{{failedRecord, DatabaseTransactionCommitFailed + ": " + failedTag, failedTag}}},
		abortedLeaf("aborted_transient"),
		abortedLeaf("aborted_transient_twin"), // a second, distinct child with the same status
		{err: nonRetriable, name: "aborted_non_retriable", retained: []error{nonRetriable},
			children: []childExpect{{nonRetriable, TransactionTimedOut + ": timeout aborted_non_retriable", timeoutTag}}},
		{err: ordinary, name: "ordinary_transient", retained: []error{ordinary}},
		{err: plain, name: "ordinary_plain", retained: []error{plain}},
	}
}

// fallbackCode is the model's status for a finish record whose primary is an
// ordinary error: the primary's own status when it has one, otherwise the
// operation's own status (Neo4j 5.26 Status.Transaction; an abort has none,
// so its unclassified statement keeps the default statement status).
func fallbackCode(op TerminalOperation, primary error) string {
	var coded *statusError
	if stderrors.As(primary, &coded) {
		return coded.code
	}
	switch op {
	case TerminalRollback:
		return TransactionRollbackFailed
	case TerminalAbort:
		return StatementSyntaxError
	case TerminalBegin:
		return TransactionStartFailed
	}
	return DatabaseTransactionCommitFailed
}

// closureConstructors is the registry. It must equal the census's
// outcome-constructor list for pkg/errors (compared after the run), so every
// construction the API allows is composed here. NewTerminalFailure is taken
// with every combination of terminal operation (all five), committed and WAL
// record outcome.
func closureConstructors() []closureConstructor {
	join := func(name string, committed bool, err error, args []closureValue, terminalOnly ...bool) closureValue {
		v := closureValue{err: err, name: name, committed: committed}
		for i, arg := range args {
			if len(terminalOnly) > i && terminalOnly[i] && !arg.terminal() {
				continue // a journal records terminal outcomes only, by design
			}
			v.committed = v.committed || arg.committed
			v.ownCommitStatus = v.ownCommitStatus || arg.ownCommitStatus
			v.retained = append(v.retained, arg.retained...)
			v.children = append(v.children, arg.children...)
		}
		return v
	}
	finish := func(op TerminalOperation, committed, walFailed bool) func([]closureValue) closureValue {
		return func(a []closureValue) closureValue {
			name := fmt.Sprintf("finish(%d,%v,%v,%s)", op, committed, walFailed, a[0].name)
			var wal error
			var diag localization.Message
			tag := ""
			if walFailed {
				wal, diag, tag = uniqueWAL(name)
			}
			failure := NewTerminalFailure(op, committed, a[0].err, wal, diag)
			v := join(name, committed, failure, a)
			own := childExpect{id: failure, diagTag: tag}
			if a[0].name == "ordinary_transient" || a[0].name == "ordinary_plain" {
				// A bare ordinary primary is this record's own: its line
				// carries the record's own classification. (A primary with an
				// enclosing status of its own renders that status instead.)
				own.line = fallbackCode(op, a[0].err) + ": " + a[0].err.Error()
			}
			if !a[0].terminal() {
				v.ownCommitStatus = v.ownCommitStatus || op == TerminalCommit || op == TerminalEmptyCompletion
			}
			// Every finish is a terminal child record, whether or not the model
			// predicts its line or it has a WAL diagnostic: a journal keeps it,
			// and its committed fact belongs to every outcome built from it.
			v.children = append(v.children, own)
			return v
		}
	}
	var constructors []closureConstructor
	for _, op := range []TerminalOperation{TerminalAbort, TerminalCommit, TerminalRollback, TerminalEmptyCompletion, TerminalBegin} {
		for _, committed := range []bool{false, true} {
			for _, walFailed := range []bool{false, true} {
				constructors = append(constructors, closureConstructor{"pkg/errors.NewTerminalFailure", 1, finish(op, committed, walFailed)})
			}
		}
	}
	return append(constructors,
		closureConstructor{"pkg/errors.NormalizeTerminalOutcome", 1, func(a []closureValue) closureValue {
			return join("normalize("+a[0].name+")", false, NormalizeTerminalOutcome(a[0].err), a)
		}},
		closureConstructor{"pkg/errors.CombineTerminalOutcomes", 2, func(a []closureValue) closureValue {
			return join("combine("+a[0].name+","+a[1].name+")", false, CombineTerminalOutcomes(a[0].err, a[1].err), a)
		}},
		closureConstructor{"pkg/errors.ProjectTerminalOutcomeMessages", 1, func(a []closureValue) closureValue {
			return join("project("+a[0].name+")", false, ProjectTerminalOutcomeMessages(a[0].err, func(e error) error { return e }), a)
		}},
		closureConstructor{"pkg/errors.WithStatus", 1, func(a []closureValue) closureValue {
			err := WithStatus(a[0].err, TransientOutdated, localization.ExternalDiagnostic(stderrors.New("enclosing rejection")))
			return join("status("+a[0].name+")", false, err, a)
		}},
		closureConstructor{"pkg/errors.MarkStatementTimeout", 1, func(a []closureValue) closureValue {
			return join("timeout("+a[0].name+")", false, MarkStatementTimeout(a[0].err, ErrDatabaseQueryTimeLimit), a)
		}},
		closureConstructor{"(pkg/errors.TerminalOutcomes).Retain", 2, func(a []closureValue) closureValue {
			journal, _ := TerminalOutcomes{}.Record(a[1].err)
			return join("retain("+a[0].name+","+a[1].name+")", false, journal.Retain(a[0].err), a, false, true)
		}},
		closureConstructor{"(*pkg/errors.TerminalOutcomeScope).Complete", 2, func(a []closureValue) closureValue {
			scope := NewTerminalOutcomeScope(context.Background())
			RecordTerminalOutcome(scope, a[1].err)
			return join("complete("+a[0].name+","+a[1].name+")", false, scope.Complete(a[0].err), a, false, true)
		}},
		// Not an outcome constructor; the ordinary wrapping every caller applies.
		closureConstructor{"fmt.Errorf", 1, func(a []closureValue) closureValue {
			return join("wrap("+a[0].name+")", false, fmt.Errorf("caller context: %w", a[0].err), a)
		}},
	)
}

// requireClosure checks one constructed outcome against the model:
//   - committed work never invites a driver retry (the pinned driver decides);
//   - every retained identity stays reachable;
//   - every terminal child record renders: its unique WAL diagnostic exactly
//     once, and its own classification line at least as often as children
//     expect it (prefaces may repeat an ordinary primary's text);
//   - no committed status without committed work or a child's own commit status.
func requireClosure(t *testing.T, v closureValue) {
	t.Helper()
	code, message := Neo4jStatus(v.err)
	if v.committed && (&db.Neo4jError{Code: code}).IsRetriable() {
		t.Fatalf("%s: committed work, but a driver retries %s", v.name, code)
	}
	for _, leaf := range v.retained {
		if !stderrors.Is(v.err, leaf) {
			t.Fatalf("%s: lost a child's identity: %v", v.name, leaf)
		}
	}
	lines := strings.Split(message, "\n")
	seen := map[any]bool{}
	wantLine := map[string]int{}
	for _, child := range v.children {
		if seen[child.id] {
			continue // one identity is one record, however often it is composed
		}
		seen[child.id] = true
		if child.diagTag != "" {
			if got := strings.Count(message, child.diagTag); got != 1 {
				t.Fatalf("%s: a child's WAL diagnostic %q renders %d times, want 1, in %q", v.name, child.diagTag, got, message)
			}
		}
		if child.line != "" {
			wantLine[child.line]++
		}
	}
	for line, want := range wantLine {
		got := 0
		for _, l := range lines {
			if l == line {
				got++
			}
		}
		if got < want {
			t.Fatalf("%s: a child's own classification line %q renders %d times, want at least %d, in %q", v.name, line, got, want, message)
		}
	}
	if !v.committed && code == DatabaseTransactionCommitFailed && !v.ownCommitStatus {
		t.Fatalf("%s: no committed work, but given the committed status", v.name)
	}
}

// The population: every construction of the outcome API (the registry), with
// NewTerminalFailure over all five operations × committed × WAL outcome;
//   - every constructor spine to depth 3, each binary node's other operand a
//     committed sibling or a transient abort, in both positions;
//   - every ordered pair of the spine values of depth ≤ 1 under each binary
//     constructor. Those come from the spine population, so a binary subtree
//     whose operands are both outside the two fixed siblings is not paired.
//
// It is finite; it is not every tree. The records-only representation is what
// carries the claim beyond it: a construction keeps a child's record, so it
// keeps that child's facts.
func TestTerminalOutcomeConstructionClosure(t *testing.T) {
	leaves := closureLeaves()
	constructors := closureConstructors()
	names := map[string]bool{}
	for _, c := range constructors {
		if strings.HasPrefix(c.name, "pkg/errors.") || strings.HasPrefix(c.name, "(pkg/errors.") || strings.HasPrefix(c.name, "(*pkg/errors.") {
			names[c.name] = true
		}
	}
	registry := make([]string, 0, len(names))
	for name := range names {
		registry = append(registry, name)
	}
	sort.Strings(registry)
	t.Logf("NOR180_CONSTRUCTORS %s", strings.Join(registry, ","))
	siblings := []closureValue{leaves[0], leaves[3]}
	spines := 0
	var depthOne []closureValue
	var extend func(v closureValue, depth int)
	extend = func(v closureValue, depth int) {
		requireClosure(t, v)
		spines++
		if depth <= 1 {
			depthOne = append(depthOne, v)
		}
		if depth == 3 {
			return
		}
		for _, c := range constructors {
			if c.arity == 1 {
				extend(c.build([]closureValue{v}), depth+1)
				continue
			}
			for _, other := range siblings {
				extend(c.build([]closureValue{v, other}), depth+1)
				extend(c.build([]closureValue{other, v}), depth+1)
			}
		}
	}
	for _, leaf := range leaves {
		extend(leaf, 0)
	}
	pairs := 0
	for _, c := range constructors {
		if c.arity != 2 {
			continue
		}
		for _, a := range depthOne {
			for _, b := range depthOne {
				requireClosure(t, c.build([]closureValue{a, b}))
				pairs++
			}
		}
	}
	t.Logf("NOR180_CLOSURE spines=%d subtree_pairs=%d depth_le_1=%d", spines, pairs, len(depthOne))
}

// The 5b9c review's construction: a retained outcome with an error-free
// committed sibling becomes the primary of a new failed enclosing abort, then
// is composed with another transient aborted outcome, in both orders.
func TestCommittedSiblingSurvivesEnclosingFinish(t *testing.T) {
	leaves := closureLeaves()
	walClosed := stderrors.New("enclosing wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	enclosing := NewTerminalFailure(TerminalAbort, false, leaves[0].err, walClosed, diag)
	other := NewTerminalFailure(TerminalAbort, false, &statusError{code: closureMemory, message: "other"}, walClosed, diag)
	for name, err := range map[string]error{
		"other_first":     CombineTerminalOutcomes(other, enclosing),
		"enclosing_first": CombineTerminalOutcomes(enclosing, other),
	} {
		code, message := Neo4jStatus(err)
		require.False(t, (&db.Neo4jError{Code: code}).IsRetriable(), "%s: committed work, retriable %s", name, code)
		require.Equal(t, DatabaseTransactionCommitFailed, code, name)
		require.Equal(t, 2, strings.Count(message, closureMemory+": "), "%s: each aborted child keeps its own status", name)
		require.ErrorIs(t, err, leaves[0].err)
		require.ErrorIs(t, err, other)
	}
}
