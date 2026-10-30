// Author: Claude, NOR-180 committed truth by construction, 30 October 2026.
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

// closureValue is an outcome built by the closure test, with what its
// construction guarantees: the leaves it must retain, whether it contains
// committed work, and the own status codes its rendering must keep.
type closureValue struct {
	err       error
	name      string
	committed bool
	retained  []error
	codes     []string
	// ownCommitStatus: a child's own status is TransactionCommitFailed (a
	// failed COMMIT record, or a COMMIT or empty completion's own fallback).
	ownCommitStatus bool
}

// closureConstructor is one construction of the outcome API. name is the
// census's form of the function (outcome-constructor); arity is how many
// outcomes it takes.
type closureConstructor struct {
	name  string
	arity int
	build func(args []closureValue) closureValue
}

const closureMemory = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

func closureLeaves() []closureValue {
	walClosed := stderrors.New("wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	memory := func() error { return &statusError{code: closureMemory, message: "memory"} }
	timeout := &statusError{code: TransactionTimedOut, message: "timeout"}
	aborted := NewTerminalFailure(TerminalAbort, false, memory(), walClosed, diag)
	sibling, _ := TerminalOutcomes{}.RecordCommit()
	sibling, _ = sibling.Record(NewTerminalFailure(TerminalAbort, false, memory(), walClosed, diag))
	committedSibling := sibling.Retain(nil)
	written := NewTerminalFailure(TerminalCommit, true, storage.ErrStorageRestoring, nil, localization.Message{})
	failedRecord := NewTerminalFailure(TerminalCommit, true, nil, walClosed, diag)
	nonRetriable := NewTerminalFailure(TerminalAbort, false, timeout, walClosed, diag)
	ordinary := memory()
	plain := stderrors.New("plain failure")
	return []closureValue{
		{committedSibling, "committed_sibling", true, []error{committedSibling}, []string{closureMemory}, false},
		{written, "committed_record_written", true, []error{written}, []string{TransientDatabaseUnavailable}, false},
		{failedRecord, "committed_record_failed", true, []error{failedRecord}, []string{TransactionCommitFailed}, true},
		{aborted, "aborted_transient", false, []error{aborted}, []string{closureMemory}, false},
		{nonRetriable, "aborted_non_retriable", false, []error{nonRetriable}, []string{TransactionTimedOut}, false},
		{ordinary, "ordinary_transient", false, []error{ordinary}, nil, false},
		{plain, "ordinary_plain", false, []error{plain}, nil, false},
	}
}

// closureConstructors is the registry. It must equal the census's
// outcome-constructor list for pkg/errors (compared after the run), so every
// construction the API allows is composed here.
func closureConstructors() []closureConstructor {
	walClosed := stderrors.New("enclosing wal closed")
	diag := localization.ExternalDiagnostic(walClosed)
	join := func(name string, committed bool, err error, args []closureValue, terminalOnly ...bool) closureValue {
		v := closureValue{err: err, name: name, committed: committed}
		for i, arg := range args {
			if len(terminalOnly) > i && terminalOnly[i] && !HasTerminalOutcome(arg.err) {
				continue // a journal records terminal outcomes only, by design
			}
			v.committed = v.committed || arg.committed
			v.ownCommitStatus = v.ownCommitStatus || arg.ownCommitStatus
			v.retained = append(v.retained, arg.retained...)
			v.codes = append(v.codes, arg.codes...)
		}
		return v
	}
	finish := func(op TerminalOperation, committed bool) func([]closureValue) closureValue {
		return func(a []closureValue) closureValue {
			name := fmt.Sprintf("finish(%d,%v,%s)", op, committed, a[0].name)
			v := join(name, committed, NewTerminalFailure(op, committed, a[0].err, walClosed, diag), a)
			v.ownCommitStatus = v.ownCommitStatus || op == TerminalCommit || op == TerminalEmptyCompletion
			return v
		}
	}
	return []closureConstructor{
		{"pkg/errors.NewTerminalFailure", 1, finish(TerminalAbort, false)},
		{"pkg/errors.NewTerminalFailure", 1, finish(TerminalCommit, true)},
		{"pkg/errors.NewTerminalFailure", 1, finish(TerminalRollback, false)},
		{"pkg/errors.NewTerminalFailure", 1, finish(TerminalEmptyCompletion, false)},
		{"pkg/errors.NormalizeTerminalOutcome", 1, func(a []closureValue) closureValue {
			return join("normalize("+a[0].name+")", false, NormalizeTerminalOutcome(a[0].err), a)
		}},
		{"pkg/errors.CombineTerminalOutcomes", 2, func(a []closureValue) closureValue {
			return join("combine("+a[0].name+","+a[1].name+")", false, CombineTerminalOutcomes(a[0].err, a[1].err), a)
		}},
		{"pkg/errors.ProjectTerminalOutcomeMessages", 1, func(a []closureValue) closureValue {
			return join("project("+a[0].name+")", false, ProjectTerminalOutcomeMessages(a[0].err, func(e error) error { return e }), a)
		}},
		{"pkg/errors.WithStatus", 1, func(a []closureValue) closureValue {
			err := WithStatus(a[0].err, TransientOutdated, localization.ExternalDiagnostic(stderrors.New("enclosing rejection")))
			return join("status("+a[0].name+")", false, err, a)
		}},
		{"pkg/errors.MarkStatementTimeout", 1, func(a []closureValue) closureValue {
			return join("timeout("+a[0].name+")", false, MarkStatementTimeout(a[0].err, ErrDatabaseQueryTimeLimit), a)
		}},
		{"(pkg/errors.TerminalOutcomes).Retain", 2, func(a []closureValue) closureValue {
			journal, _ := TerminalOutcomes{}.Record(a[1].err)
			return join("retain("+a[0].name+","+a[1].name+")", false, journal.Retain(a[0].err), a, false, true)
		}},
		{"(*pkg/errors.TerminalOutcomeScope).Complete", 2, func(a []closureValue) closureValue {
			scope := NewTerminalOutcomeScope(context.Background())
			RecordTerminalOutcome(scope, a[1].err)
			return join("complete("+a[0].name+","+a[1].name+")", false, scope.Complete(a[0].err), a, false, true)
		}},
		// Not an outcome constructor; the ordinary wrapping every caller applies.
		{"fmt.Errorf", 1, func(a []closureValue) closureValue {
			return join("wrap("+a[0].name+")", false, fmt.Errorf("caller context: %w", a[0].err), a)
		}},
	}
}

// requireClosure checks one constructed outcome: committed work never invites
// a driver retry; every retained leaf keeps its identity and own status; and an
// outcome without committed work is not given the committed status.
func requireClosure(t *testing.T, v closureValue) {
	t.Helper()
	code, message := Neo4jStatus(v.err)
	retries := (&db.Neo4jError{Code: code}).IsRetriable()
	if v.committed && retries {
		t.Fatalf("%s: committed work, but a driver retries %s", v.name, code)
	}
	for _, leaf := range v.retained {
		if !stderrors.Is(v.err, leaf) {
			t.Fatalf("%s: lost a leaf's identity: %v", v.name, leaf)
		}
	}
	if HasTerminalOutcome(v.err) {
		for _, own := range v.codes {
			if !strings.Contains(message, own+": ") {
				t.Fatalf("%s: lost a child's own status %s in %q", v.name, own, message)
			}
		}
	}
	if !v.committed && code == TransactionCommitFailed && !v.ownCommitStatus {
		t.Fatalf("%s: no committed work, but given the committed status", v.name)
	}
}

// Every construction the outcome API allows, composed to depth 3 along every
// spine of constructors, with each binary constructor's other operand a
// committed sibling or a transient aborted outcome, in both positions.
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
	checked := 0
	var extend func(v closureValue, depth int)
	extend = func(v closureValue, depth int) {
		requireClosure(t, v)
		checked++
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
	t.Logf("NOR180_CLOSURE checked=%d", checked)
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
		require.Equal(t, TransactionCommitFailed, code, name)
		require.Equal(t, 2, strings.Count(message, closureMemory+": "), "%s: each aborted child keeps its own status", name)
		require.ErrorIs(t, err, leaves[0].err)
		require.ErrorIs(t, err, other)
	}
}
