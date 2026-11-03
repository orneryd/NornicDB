package storage

// Controls for the one canonical knowledge-policy binding-table compiler
// (compileBindingTableLocked): a candidate that does not compile is rejected
// where it is introduced instead of being persisted or published, the compiled
// table changes only when a mutation is accepted, and a canceled schema
// admission publishes and persists nothing.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const bindingOwnerNamespace = "bindingowner"

// bindingOwnerBundle is a valid decay profile bundle a candidate binding can
// reference.
func bindingOwnerBundle(name string) knowledgepolicy.DecayProfileBundle {
	return knowledgepolicy.DecayProfileBundle{
		Name:                name,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.1,
		Function:            knowledgepolicy.DecayFunctionExponential,
		Scope:               knowledgepolicy.ScopeNode,
		DecayEnabled:        true,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	}
}

func bindingOwnerHasConstraint(schema *SchemaManager, name string) bool {
	for _, constraint := range schema.GetAllConstraints() {
		if constraint.Name == name {
			return true
		}
	}
	return false
}

// TestReplaceDefinitionDurableRejectsInvalidBindingBeforeWriting pins the
// replacement's compile boundary: a definition whose decay binding references a
// profile it does not carry fails while the detached candidate loads, so the
// persister is never called and the live definition, the compiled binding table
// and the published families stay exactly as they were.
func TestReplaceDefinitionDurableRejectsInvalidBindingBeforeWriting(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	target := NewNamespacedEngine(engine, bindingOwnerNamespace)
	schema := engine.GetSchemaForNamespace(bindingOwnerNamespace)

	require.NoError(t, schema.CreateDecayProfileBundle(bindingOwnerBundle("present_bundle")))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "present_binding", IsWildcard: true, ProfileRef: "present_bundle",
	}))

	beforeTable := schema.GetBindingTable()
	require.NotNil(t, beforeTable, "the seed must have published a compiled binding table")
	beforeDefinition := schemaDefinitionForTest(t, schema)

	var attempts atomic.Int64
	schema.SetPersister(func(*SchemaDefinition) error {
		attempts.Add(1)
		return nil
	})

	invalid := &SchemaDefinition{
		DecayProfileBindings: []knowledgepolicy.DecayProfileBinding{{
			Name: "unknown_profile_binding", IsWildcard: true, ProfileRef: "absent_bundle",
		}},
	}

	err := schema.ReplaceDefinitionDurable(context.Background(), target, invalid)
	require.Error(t, err, "a definition referencing a missing decay profile must be rejected")
	assert.Contains(t, err.Error(), "absent_bundle")
	assert.Zero(t, attempts.Load(), "a rejected definition must be rejected before it is persisted")
	assert.Same(t, beforeTable, schema.GetBindingTable(),
		"a rejected definition must leave the compiled binding table serving")
	assert.Equal(t, beforeDefinition, schemaDefinitionForTest(t, schema),
		"a rejected definition must leave every published schema family unchanged")

	bundles, bindings := schema.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	require.Len(t, bindings, 1)
	assert.Equal(t, "present_binding", bindings[0].Name)

	// The loader is the compiler's other caller: a fresh manager reports the
	// same compile failure and keeps no compiled table.
	fresh := NewSchemaManager()
	require.Error(t, fresh.ReplaceFromDefinition(context.Background(), invalid))
	assert.Nil(t, fresh.GetBindingTable())
}

// TestKnowledgePolicyMutationCompileErrorRestoresPublishedState pins the
// staged-mutation rejection: a binding whose property rule references a missing
// profile compiles to an error before the persister runs, so the staged family
// is restored, nothing is written and the previously compiled table keeps
// serving.
func TestKnowledgePolicyMutationCompileErrorRestoresPublishedState(t *testing.T) {
	schema := NewSchemaManager()
	var persists atomic.Int64
	schema.SetPersister(func(*SchemaDefinition) error {
		persists.Add(1)
		return nil
	})
	require.NoError(t, schema.CreateDecayProfileBundle(bindingOwnerBundle("present_bundle")))
	persists.Store(0)

	beforeTable := schema.GetBindingTable()
	require.NotNil(t, beforeTable)

	err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "rule_unknown_profile",
		TargetLabels: []string{"Doc"},
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{
			PropertyPath: "summary",
			ProfileRef:   "absent_bundle",
		}},
	})
	require.Error(t, err, "a property rule referencing a missing profile must not be accepted")
	assert.Contains(t, err.Error(), "absent_bundle")
	assert.Zero(t, persists.Load(), "an uncompilable candidate must not be persisted")
	assert.Same(t, beforeTable, schema.GetBindingTable(),
		"the previously compiled table must keep serving")
	_, bindings := schema.ShowDecayProfiles()
	assert.Empty(t, bindings, "the rejected binding must not be published")
}

// TestKnowledgePolicyMutationPersisterRejectionKeepsCompiledTable pins the
// accepted-candidate ordering: a compiling candidate is still not published when
// the persister refuses it, so the published families and the compiled table
// pointer stay the ones that were already accepted.
func TestKnowledgePolicyMutationPersisterRejectionKeepsCompiledTable(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.CreateDecayProfileBundle(bindingOwnerBundle("present_bundle")))

	beforeTable := schema.GetBindingTable()
	require.NotNil(t, beforeTable)

	rejection := errors.New("policy persistence rejected")
	schema.SetPersister(func(*SchemaDefinition) error { return rejection })

	err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "rejected_wildcard", IsWildcard: true, ProfileRef: "present_bundle",
	})
	require.ErrorIs(t, err, rejection)
	assert.Same(t, beforeTable, schema.GetBindingTable(),
		"a rejected write must leave the compiled table serving")
	_, bindings := schema.ShowDecayProfiles()
	assert.Empty(t, bindings, "a rejected binding must not be published")
}

// TestKnowledgePolicyMutationPublishesCompiledCandidate pins the accepted half:
// the compiled candidate is assigned only after acceptance, and the previously
// published table is left untouched.
func TestKnowledgePolicyMutationPublishesCompiledCandidate(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.CreateDecayProfileBundle(bindingOwnerBundle("present_bundle")))

	beforeTable := schema.GetBindingTable()
	require.NotNil(t, beforeTable)
	require.False(t, beforeTable.HasWildNode())

	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name: "accepted_wildcard", IsWildcard: true, ProfileRef: "present_bundle",
	}))

	published := schema.GetBindingTable()
	require.NotNil(t, published)
	assert.NotSame(t, beforeTable, published,
		"an accepted mutation publishes the candidate it compiled")
	assert.True(t, published.HasWildNode(), "the published table carries the accepted binding")
	assert.False(t, beforeTable.HasWildNode(), "the previously published table is unchanged")
}

// TestReplaceDefinitionDurableCanceledAdmissionPublishesNothing pins the
// cancellation boundary of the replacement: a caller canceled while its schema
// admission waits for the publication gate, or before it is admitted at all,
// persists and publishes nothing.
func TestReplaceDefinitionDurableCanceledAdmissionPublishesNothing(t *testing.T) {
	replacement := &SchemaDefinition{
		Constraints: []Constraint{{
			Name: "probe_unique", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"},
		}},
	}

	prepare := func(t *testing.T) (*SchemaManager, *NamespacedEngine, *SchemaDefinition, *atomic.Int64) {
		t.Helper()
		engine := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
		target := NewNamespacedEngine(engine, "cancelwait")
		schema := engine.GetSchemaForNamespace("cancelwait")
		require.NoError(t, schema.AddUniqueConstraint("baseline_unique", "Person", "id"))
		before := schemaDefinitionForTest(t, schema)
		var attempts atomic.Int64
		schema.SetPersister(func(*SchemaDefinition) error {
			attempts.Add(1)
			return nil
		})
		return schema, target, before, &attempts
	}

	t.Run("canceled while waiting for the publication gate", func(t *testing.T) {
		schema, target, before, attempts := prepare(t)
		beforeTable := schema.GetBindingTable()

		schema.publicationGate.Lock()
		var held atomic.Bool
		held.Store(true)
		releaseGate := func() {
			if held.CompareAndSwap(true, false) {
				schema.publicationGate.Unlock()
			}
		}
		t.Cleanup(releaseGate)

		gateWait := make(chan struct{})
		var claimed atomic.Bool
		restoreObserver := setAdmissionWaitObserver(func(owner any, _ context.Context) {
			if owner == &schema.publicationGate && claimed.CompareAndSwap(false, true) {
				close(gateWait)
			}
		})
		t.Cleanup(restoreObserver)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- schema.ReplaceDefinitionDurable(ctx, target, replacement) }()

		select {
		case <-gateWait:
			// Observe the actual admission wait instead of inferring it from time.
		case err := <-done:
			t.Fatalf("the replacement returned before waiting on the held publication gate: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the replacement never waited on the held publication gate")
		}

		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(30 * time.Second):
			t.Fatal("the canceled replacement did not return")
		}
		releaseGate()

		assert.Zero(t, attempts.Load(), "a canceled admission must not persist")
		assert.False(t, bindingOwnerHasConstraint(schema, "probe_unique"),
			"a canceled admission must not publish the replacement")
		assert.Equal(t, before, schemaDefinitionForTest(t, schema))
		assert.Same(t, beforeTable, schema.GetBindingTable())
	})

	t.Run("canceled before admission", func(t *testing.T) {
		schema, target, before, attempts := prepare(t)
		beforeTable := schema.GetBindingTable()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := schema.ReplaceDefinitionDurable(ctx, target, replacement)
		require.ErrorIs(t, err, context.Canceled)

		assert.Zero(t, attempts.Load(), "a canceled replacement must not persist")
		assert.False(t, bindingOwnerHasConstraint(schema, "probe_unique"))
		assert.Equal(t, before, schemaDefinitionForTest(t, schema))
		assert.Same(t, beforeTable, schema.GetBindingTable())
	})
}

func TestReplaceDefinitionDurableCanceledSchemaReaderWaitPublishesNothing(t *testing.T) {
	engine := newTestMemoryEngine(t)
	target := NewNamespacedEngine(engine, "cancelreader")
	schema := target.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("before", "Person", "id"))
	before := schemaDefinitionForTest(t, schema)
	var attempts atomic.Int64
	schema.SetPersister(func(*SchemaDefinition) error { attempts.Add(1); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schema.mu.RLock()
	held := true
	release := func() {
		if held {
			held = false
			schema.mu.RUnlock()
		}
	}
	defer release()
	waiting := make(chan struct{})
	var observed atomic.Bool
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &schema.mu && observed.CompareAndSwap(false, true) {
			cancel()
			close(waiting)
		}
	})
	defer restore()
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); done <- schema.ReplaceDefinitionDurable(ctx, target, &SchemaDefinition{}) }()
	defer func() {
		cancel()
		release()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("replacement did not join after release")
		}
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement never waited on schema reader")
	}
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		release()
		err := <-done
		t.Fatalf("cancellation failed to retire held-schema admission within 100ms (result %v)", err)
	}
	release()
	<-exited
	require.Zero(t, attempts.Load())
	require.Equal(t, before, schemaDefinitionForTest(t, schema))
	require.NoError(t, schema.ReplaceDefinitionDurable(context.Background(), target, &SchemaDefinition{}))
	require.Equal(t, int64(1), attempts.Load())
}

func TestReplaceFromDefinitionCompileErrorPreservesPublishedDefinition(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.CreateDecayProfileBundle(bindingOwnerBundle("old_bundle")))
	require.NoError(t, schema.AddPropertyIndex("old_index", "Doc", []string{"id"}))
	before := schemaDefinitionForTest(t, schema)
	table := schema.GetBindingTable()
	err := schema.ReplaceFromDefinition(context.Background(), &SchemaDefinition{DecayProfileBindings: []knowledgepolicy.DecayProfileBinding{{Name: "bad_binding", IsWildcard: true, ProfileRef: "missing"}}})
	require.Error(t, err)
	require.Equal(t, before, schemaDefinitionForTest(t, schema))
	require.Same(t, table, schema.GetBindingTable())
}
