// Author: Codex, NOR-180 immutable outcome composition, 29 October 2026.
package errors

import (
	"context"
	stderrors "errors"
	"sync/atomic"
)

type terminalOutcomeEntry struct {
	identity *TerminalFailure
	err      error
}

// TerminalOutcomes is an immutable collection of produced terminal values.
// Statement and protocol completion owners use the same composition operation;
// neither an aggregation loop nor a protocol reconstructs a child's status.
// The zero value is empty. Record returns a new collection when it changes.
type TerminalOutcomes struct{ entries []terminalOutcomeEntry }

// Empty reports whether the owner has recorded any failed terminal outcome.
func (o TerminalOutcomes) Empty() bool { return len(o.entries) == 0 }

// Record retains the complete returned value for each original finish identity.
// A later display projection replaces its earlier view without changing order,
// causes, classification or native truth. No published collection is mutated.
func (o TerminalOutcomes) Record(outcome error) (TerminalOutcomes, bool) {
	view := responseTerminalView(outcome)
	if view == nil {
		return o, false
	}
	changed := false
	for _, record := range view.records {
		index := -1
		for i, previous := range o.entries {
			if previous.identity == record.identity {
				index = i
				break
			}
		}
		value := outcome
		if index >= 0 {
			previous := o.entries[index].err
			if stderrors.Is(previous, value) {
				continue
			}
			if !stderrors.Is(value, previous) {
				value = CombineTerminalOutcomes(value, previous)
			}
		}
		if !changed {
			o.entries = append([]terminalOutcomeEntry(nil), o.entries...)
			changed = true
		}
		entry := terminalOutcomeEntry{identity: record.identity, err: value}
		if index < 0 {
			o.entries = append(o.entries, entry)
		} else {
			o.entries[index] = entry
		}
	}
	return o, changed
}

// Retain composes the primary return and every recorded immutable outcome.
// Healthy completion keeps the original result/error contract.
func (o TerminalOutcomes) Retain(primary error) error {
	for _, entry := range o.entries {
		primary = CombineTerminalOutcomes(primary, entry.err)
	}
	return primary
}

type terminalOutcomeScopeKey struct{}

// TerminalOutcomeScope is one synchronous client operation's completion owner.
// It also carries its parent's context, so one allocation installs the scope.
// Its single state writer publishes immutable TerminalOutcomes snapshots.
type TerminalOutcomeScope struct {
	context.Context
	state atomic.Pointer[TerminalOutcomes]
}

// NewTerminalOutcomeScope starts an independent client operation. For example,
// a GraphQL root field can issue several public statements and aggregate counts;
// Complete must retain their failed terminal records even if a loop drops errors.
func NewTerminalOutcomeScope(ctx context.Context) *TerminalOutcomeScope {
	return &TerminalOutcomeScope{Context: ctx}
}

func (s *TerminalOutcomeScope) Value(key any) any {
	if _, ok := key.(terminalOutcomeScopeKey); ok {
		return s
	}
	return s.Context.Value(key)
}

func (s *TerminalOutcomeScope) record(outcome error) {
	for {
		previous := s.state.Load()
		var next TerminalOutcomes
		if previous != nil {
			next = *previous
		}
		next, changed := next.Record(outcome)
		if !changed {
			return
		}
		if s.state.CompareAndSwap(previous, &next) {
			return
		}
	}
}

// RecordTerminalOutcome records an already-projected statement return with its
// enclosing client operation. The statement owner decides scope eligibility;
// this function neither classifies a cause nor starts or ends a transaction.
func RecordTerminalOutcome(ctx context.Context, outcome error) {
	if !HasTerminalOutcome(outcome) {
		return
	}
	if scope, ok := ctx.Value(terminalOutcomeScopeKey{}).(*TerminalOutcomeScope); ok {
		scope.record(outcome)
	}
}

// Complete returns the operation's primary and every required child outcome.
func (s *TerminalOutcomeScope) Complete(primary error) error {
	if state := s.state.Load(); state != nil {
		return state.Retain(primary)
	}
	return NormalizeTerminalOutcome(primary)
}
