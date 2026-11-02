package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// A schema definition replacement owns its preparation through ctx. A canceled
// or rejected definition installs nothing, so it must also leave the manager
// and the pending-write source attached to it alone: no schema work and no
// source tracking belong to a candidate that was never accepted. Only an
// accepted installation changes the tracked (label, property) pairs, and that
// completion then belongs to the accepted schema change rather than to the
// caller's preparation.

// trackedPairsForTest returns the attachment's tracked (label, property)
// pairs under its own lock.
func trackedPairsForTest(buffer *WriteBehindBuffer) []nodePropertyKey {
	buffer.mu.RLock()
	defer buffer.mu.RUnlock()
	return append([]nodePropertyKey(nil), buffer.trackedPairs...)
}

// attachedPreparedSchema returns a schema with one live UNIQUE constraint pair
// attached to a real write-behind buffer that tracks that pair, and a buffered
// node the attachment would index.
func attachedPreparedSchema(t *testing.T) (*SchemaManager, *WriteBehindBuffer) {
	t.Helper()
	sm := NewSchemaManager()
	require.NoError(t, sm.AddUniqueConstraint("doc_slug", "Doc", "slug"))
	buffer := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(buffer, "target")
	buffer.active.addNode(&Node{ID: "target:one", Labels: []string{"Doc"}, Properties: map[string]interface{}{"slug": "one"}})
	require.Equal(t, []nodePropertyKey{{label: "Doc", property: "slug"}}, trackedPairsForTest(buffer))
	return sm, buffer
}

// TestReplaceFromDefinitionCanceledOrRejectedPreparationDoesNoSchemaWork pins
// that a definition which is never installed performs none of the accepted
// installation's work. A concurrent accepted schema mutation holds the schema
// write lock and an indexing write holds the attachment's lock, so any pair
// refresh for an unaccepted candidate would wait on one of them; the call must
// return its own result while both are still held.
func TestReplaceFromDefinitionCanceledOrRejectedPreparationDoesNoSchemaWork(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	cases := map[string]struct {
		ctx    context.Context
		def    *SchemaDefinition
		assert func(*testing.T, error)
	}{
		"canceled preparation": {
			ctx:    canceledCtx,
			def:    &SchemaDefinition{Version: schemaDefinitionVersion},
			assert: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		"rejected definition": {
			ctx: t.Context(),
			def: &SchemaDefinition{DecayProfileBindings: []knowledgepolicy.DecayProfileBinding{
				{Name: "bad_binding", IsWildcard: true, ProfileRef: "missing"},
			}},
			assert: func(t *testing.T, err error) { require.Error(t, err) },
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sm, buffer := attachedPreparedSchema(t)
			before, tracked := schemaDefinitionForTest(t, sm), trackedPairsForTest(buffer)

			sm.mu.Lock()
			buffer.mu.Lock()
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					buffer.mu.Unlock()
					sm.mu.Unlock()
				})
			}
			defer release()

			done := make(chan error, 1)
			go func() { done <- sm.ReplaceFromDefinition(c.ctx, c.def) }()

			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				release()
				<-done
				t.Fatal("a definition that is never installed waited on schema or attachment work")
			}
			release()

			c.assert(t, err)
			require.Equal(t, before, schemaDefinitionForTest(t, sm))
			require.Equal(t, tracked, trackedPairsForTest(buffer))
		})
	}
}

// TestReplaceFromDefinitionAcceptedPreparationTracksPendingPairs pins that an
// accepted installation still refreshes the attached source's tracked pairs:
// that completion belongs to the accepted schema change, so it runs whatever
// the caller's preparation context was.
func TestReplaceFromDefinitionAcceptedPreparationTracksPendingPairs(t *testing.T) {
	sm := NewSchemaManager()
	buffer := &WriteBehindBuffer{active: newCommitBuffer()}
	sm.attachPendingWrites(buffer, "target")
	require.Empty(t, trackedPairsForTest(buffer))

	def := &SchemaDefinition{
		Version:     schemaDefinitionVersion,
		Constraints: []Constraint{{Name: "doc_slug", Type: ConstraintUnique, Label: "Doc", Properties: []string{"slug"}}},
	}
	require.NoError(t, sm.ReplaceFromDefinition(t.Context(), def))

	require.Equal(t, []nodePropertyKey{{label: "Doc", property: "slug"}}, trackedPairsForTest(buffer))
}

// TestReplaceFromDefinitionPreparationCancellationStopsConstraintIndexDiscovery
// pins that the constraint-owned index discovery a replacement runs while
// building its private candidate honors the preparation context. This control
// cancels the real context on a counted poll inside that loading work
// (newIndexPollCancelContext), so it proves cancellation is observed at a
// poll boundary during preparation; it is not evidence about cancellation while
// the replacement waits for live schema admission.
func TestReplaceFromDefinitionPreparationCancellationStopsConstraintIndexDiscovery(t *testing.T) {
	sm := NewSchemaManager()
	before := schemaDefinitionForTest(t, sm)

	def := &SchemaDefinition{
		Version:     schemaDefinitionVersion,
		Constraints: []Constraint{{Name: "doc_slug", Type: ConstraintUnique, Label: "Doc", Properties: []string{"slug"}}},
	}
	for i := 0; i < 8; i++ {
		property := fmt.Sprintf("p%d", i)
		def.PropertyIndexes = append(def.PropertyIndexes, SchemaPropertyIndexDef{Name: "doc_" + property, Label: "Doc", Properties: []string{property}})
	}

	err := sm.ReplaceFromDefinition(newIndexPollCancelContext(t, 4), def)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, schemaDefinitionForTest(t, sm))
}

// TestReplaceFromDefinitionCanceledWhileWaitingForSchemaAdmission pins the
// admission half of the same rule through the public entry point: a
// replacement whose preparation context is canceled while it waits for the
// live schema lock returns the caller's own cancellation and installs nothing.
// The wait is proved at the actual admission boundary (the wait observer
// reports a wait on &sm.mu carrying the caller's context), so cancellation is
// scheduled by the real cancel and not by elapsed time; the replacement
// returning while this test still holds sm.mu is itself the proof that it
// stopped instead of being admitted. The old definition and the tracked pairs
// stay exactly as they were, and a healthy retry installs the same definition
// and tracks the pairs it constrains.
func TestReplaceFromDefinitionCanceledWhileWaitingForSchemaAdmission(t *testing.T) {
	sm, buffer := attachedPreparedSchema(t)
	before, tracked := schemaDefinitionForTest(t, sm), trackedPairsForTest(buffer)
	def := &SchemaDefinition{
		Version:     schemaDefinitionVersion,
		Constraints: []Constraint{{Name: "doc_other", Type: ConstraintUnique, Label: "Doc", Properties: []string{"other"}}},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Hold the live schema write lock for the whole admission attempt: the
	// replacement can only be installed by waiting here first.
	sm.mu.Lock()
	var held atomic.Bool
	held.Store(true)
	releaseSchema := func() {
		if held.CompareAndSwap(true, false) {
			sm.mu.Unlock()
		}
	}
	t.Cleanup(releaseSchema)

	admission := make(chan context.Context, 1)
	var observed atomic.Bool
	restoreObserver := setAdmissionWaitObserver(func(owner any, waitCtx context.Context) {
		if owner == &sm.mu && observed.CompareAndSwap(false, true) {
			admission <- waitCtx
		}
	})
	t.Cleanup(restoreObserver)

	done := make(chan error, 1)
	go func() { done <- sm.ReplaceFromDefinition(ctx, def) }()

	var waitCtx context.Context
	select {
	case waitCtx = <-admission:
	case err := <-done:
		t.Fatalf("the replacement finished before waiting on the held live schema lock: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the replacement never waited on the held live schema lock")
	}

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled,
			"a replacement canceled at live schema admission must return the caller's cancellation")
	case <-time.After(30 * time.Second):
		t.Fatal("a replacement canceled at live schema admission did not return")
	}
	if sm.mu.TryLock() {
		sm.mu.Unlock()
		t.Fatal("the canceled replacement returned only after the live schema lock was released")
	}
	require.True(t, held.Load(), "the canceled replacement must return while this test still holds sm.mu")
	require.Same(t, ctx, waitCtx,
		"the live schema admission must wait on the caller's own context, not a context-free lifetime")

	// Every worker is joined above; release the hold and check that the
	// canceled attempt changed nothing.
	releaseSchema()
	require.Equal(t, before, schemaDefinitionForTest(t, sm),
		"a canceled admission must install nothing")
	require.Equal(t, tracked, trackedPairsForTest(buffer),
		"a canceled admission must track no pending pairs")

	require.NoError(t, sm.ReplaceFromDefinition(t.Context(), def),
		"a healthy retry of the canceled definition must install it")
	installed := schemaDefinitionForTest(t, sm)
	require.NotEqual(t, before, installed, "the healthy retry must install the definition")
	constraintNames := make([]string, 0, len(installed.Constraints))
	for _, constraint := range installed.Constraints {
		constraintNames = append(constraintNames, constraint.Name)
	}
	require.Equal(t, []string{"doc_other"}, constraintNames)
	require.ElementsMatch(t,
		[]nodePropertyKey{{label: "Doc", property: "slug"}, {label: "Doc", property: "other"}},
		trackedPairsForTest(buffer),
		"the accepted retry must index the pair the installed definition constrains")
}
