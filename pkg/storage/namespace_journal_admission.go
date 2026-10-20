package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type namespaceJournalState struct {
	pending int
	closed  *NamespaceDrop
	drained chan struct{}
}

// BeginNamespaceJournalMutation enrolls a publication before any wrapper journal
// or snapshot wait. No mutex is held across I/O. DROP closes admission and waits
// only for this fixed set of publications, before taking wrapper barriers.
func BeginNamespaceJournalMutation(ctx context.Context, engine Engine, namespaces []string) (func(), error) {
	b := namespaceLifecycleOwner(engine)
	if b == nil {
		return func() {}, nil
	}
	revision := b.namespaceLifetimeRevision()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.namespaceJournalMu.Lock()
	if b.namespaceJournalStates == nil {
		b.namespaceJournalStates = make(map[string]*namespaceJournalState)
	}
	var states []*namespaceJournalState
	seen := make(map[string]bool, len(namespaces))
	conflict := func() (func(), error) {
		b.namespaceJournalMu.Unlock()
		return nil, fmt.Errorf("%w: namespace journal admission is closed", ErrConflict)
	}
	if len(namespaces) == 0 {
		if len(b.namespaceJournalPreparations) != 0 {
			return conflict()
		}
		states = append(states, &b.namespaceJournalGlobal)
	} else {
		for _, namespace := range namespaces {
			if seen[namespace] {
				continue
			}
			seen[namespace] = true
			state := b.namespaceJournalStates[namespace]
			if state == nil {
				state = &namespaceJournalState{}
				b.namespaceJournalStates[namespace] = state
			}
			if state.closed != nil {
				return conflict()
			}
			if err := b.checkNamespaceLifetimeRevision(namespace, revision); err != nil {
				b.namespaceJournalMu.Unlock()
				return nil, err
			}
			states = append(states, state)
		}
	}
	for _, state := range states {
		state.pending++
	}
	b.namespaceJournalMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			b.namespaceJournalMu.Lock()
			defer b.namespaceJournalMu.Unlock()
			for _, state := range states {
				state.pending--
				if state.pending == 0 && state.drained != nil {
					close(state.drained)
					state.drained = nil
				}
			}
		})
	}, nil
}

// PrepareNamespaceJournal closes target admission before any long wrapper wait.
// A global legacy Cypher command has whole-engine scope, so it participates in
// every target frontier; ordinary namespaced commands remain independent.
func PrepareNamespaceJournal(ctx context.Context, engine Engine, prefix string, op *NamespaceDrop) error {
	if op.releaseJournalAdmission != nil {
		return nil
	}
	namespace, whole := strings.CutSuffix(prefix, ":")
	if !whole || namespace == "" || strings.Contains(namespace, ":") {
		return nil
	}
	b := namespaceLifecycleOwner(engine)
	if b == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.namespaceJournalMu.Lock()
	if b.namespaceJournalStates == nil {
		b.namespaceJournalStates = make(map[string]*namespaceJournalState)
	}
	state := b.namespaceJournalStates[namespace]
	if state == nil {
		state = &namespaceJournalState{}
		b.namespaceJournalStates[namespace] = state
	}
	if state.closed != nil {
		b.namespaceJournalMu.Unlock()
		return ErrConflict
	}
	if b.namespaceJournalPreparations == nil {
		b.namespaceJournalPreparations = make(map[string]*NamespaceDrop)
	}
	state.closed = op
	b.namespaceJournalPreparations[namespace] = op
	var waits []<-chan struct{}
	for _, frontier := range []*namespaceJournalState{state, &b.namespaceJournalGlobal} {
		if frontier.pending != 0 {
			if frontier.drained == nil {
				frontier.drained = make(chan struct{})
			}
			waits = append(waits, frontier.drained)
		}
	}
	op.releaseJournalAdmission = func() {
		b.namespaceJournalMu.Lock()
		defer b.namespaceJournalMu.Unlock()
		if state.closed == op {
			state.closed = nil
		}
		if b.namespaceJournalPreparations[namespace] == op {
			delete(b.namespaceJournalPreparations, namespace)
		}
	}
	b.namespaceJournalMu.Unlock()
	for _, done := range waits {
		select {
		case <-done:
		case <-ctx.Done():
			op.Abort()
			return ctx.Err()
		}
	}
	return nil
}

func (w *WALEngine) admitJournalIDs(ids ...string) (func(), error) {
	if len(ids) == 0 {
		return func() {}, nil
	}
	if w.journalNamespace != "" {
		return BeginNamespaceJournalMutation(context.Background(), w.engine, []string{w.journalNamespace})
	}
	namespaces := make([]string, 0, len(ids))
	for _, id := range ids {
		namespace, _, ok := ParseDatabasePrefix(id)
		if !ok {
			namespace = w.getDatabaseName()
		}
		namespaces = append(namespaces, namespace)
	}
	// Empty bulk operations retain their ordinary no-op behavior.
	return BeginNamespaceJournalMutation(context.Background(), w.engine, namespaces)
}

func namespaceJournalNodeIDs(nodes []*Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node != nil {
			ids = append(ids, string(node.ID))
		}
	}
	return ids
}

func namespaceJournalEdgeIDs(edges []*Edge) []string {
	ids := make([]string, 0, len(edges)*3)
	for _, edge := range edges {
		if edge != nil {
			ids = append(ids, string(edge.ID), string(edge.StartNode), string(edge.EndNode))
		}
	}
	return ids
}

func namespaceJournalIDs[T ~string](ids []T) []string {
	result := make([]string, len(ids))
	for i, id := range ids {
		result[i] = string(id)
	}
	return result
}
