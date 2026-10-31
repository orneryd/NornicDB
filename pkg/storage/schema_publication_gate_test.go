package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// schemaPublicationGateMutation is one direct persisting schema mutation and
// the published state it must leave behind.
type schemaPublicationGateMutation struct {
	name      string
	apply     func(*SchemaManager) error
	published func(*SchemaManager) bool
}

// schemaPublicationGateMutations is the population of direct persisting schema
// APIs a native schema publication must not be overtaken by.
func schemaPublicationGateMutations() []schemaPublicationGateMutation {
	hasConstraint := func(sm *SchemaManager, name string) bool {
		for _, constraint := range sm.GetAllConstraints() {
			if constraint.Name == name {
				return true
			}
		}
		return false
	}
	hasToken := func(names []string, name string) bool {
		for _, token := range names {
			if token == name {
				return true
			}
		}
		return false
	}
	return []schemaPublicationGateMutation{
		{
			name: "fulltext index",
			apply: func(sm *SchemaManager) error {
				return sm.AddFulltextIndex("direct_fulltext", []string{"Doc"}, []string{"body"}, ConstraintEntityNode)
			},
			published: func(sm *SchemaManager) bool { _, ok := sm.GetFulltextIndex("direct_fulltext"); return ok },
		},
		{
			name:      "property index",
			apply:     func(sm *SchemaManager) error { return sm.AddPropertyIndex("direct_property", "Doc", []string{"body"}) },
			published: func(sm *SchemaManager) bool { _, ok := sm.GetCompositeIndex("direct_property"); return ok },
		},
		{
			name:      "range index",
			apply:     func(sm *SchemaManager) error { return sm.AddRangeIndex("direct_range", "Doc", "body") },
			published: func(sm *SchemaManager) bool { _, ok := sm.GetRangeIndex("direct_range"); return ok },
		},
		{
			name: "vector index",
			apply: func(sm *SchemaManager) error {
				return sm.AddVectorIndex("direct_vector", "Doc", "body", 3, "cosine", ConstraintEntityNode)
			},
			published: func(sm *SchemaManager) bool { _, ok := sm.GetVectorIndex("direct_vector"); return ok },
		},
		{
			name:      "unique constraint",
			apply:     func(sm *SchemaManager) error { return sm.AddUniqueConstraint("direct_unique", "Doc", "body") },
			published: func(sm *SchemaManager) bool { return hasConstraint(sm, "direct_unique") },
		},
		{
			name:  "token registration",
			apply: func(sm *SchemaManager) error { return sm.RegisterTokens([]string{"Direct"}, []string{"DIRECT"}) },
			published: func(sm *SchemaManager) bool {
				definition := sm.ExportDefinition()
				return hasToken(definition.LabelTokens, "Direct") && hasToken(definition.RelationshipTypeTokens, "DIRECT")
			},
		},
	}
}

// TestNativeSchemaPublicationGateSerializesDirectSchemaMutations parks a native
// transaction that publishes a promotion policy in the physical-commit-to-live-
// publication interval and runs one direct persisting schema mutation
// concurrently. The direct mutation must wait on the manager's publication gate,
// so it observes the accepted native policy, persists a definition containing
// both changes, and both ACKs stay readable live and after reopen.
func TestNativeSchemaPublicationGateSerializesDirectSchemaMutations(t *testing.T) {
	for _, mutation := range schemaPublicationGateMutations() {
		t.Run(mutation.name, func(t *testing.T) {
			dir := t.TempDir()
			engine := openPolicyPersistenceEngine(t, dir)
			const namespace = "publicationgate"
			schema := engine.GetSchemaForNamespace(namespace)

			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace(namespace))
			view, err := tx.Schema(KnowledgePolicyChanges)
			require.NoError(t, err)
			require.NoError(t, view.CreatePromotionPolicy(policyPersistenceFixturePolicy("native")))

			parked, release := make(chan struct{}), make(chan struct{})
			var claimed, released atomic.Bool
			unpark := func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}
			restoreHook := setPublicationStageHook(func(uint64) {
				if claimed.CompareAndSwap(false, true) {
					close(parked)
					<-release
				}
			})
			// Unpark before removing the hook so a failed subtest leaves no parked
			// publication behind.
			t.Cleanup(restoreHook)
			t.Cleanup(unpark)

			nativeDone := make(chan error, 1)
			go func() { nativeDone <- tx.Commit() }()
			select {
			case <-parked:
			case err := <-nativeDone:
				t.Fatalf("native commit finished before its pre-publication park: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("native commit did not reach its pre-publication park")
			}
			require.Empty(t, schema.ShowPromotionPolicies(),
				"the native physical write must still await live schema publication")

			gateWait := make(chan struct{})
			var gateClaimed atomic.Bool
			restoreObserver := setAdmissionWaitObserver(func(owner any, _ context.Context) {
				if owner == &schema.publicationGate && gateClaimed.CompareAndSwap(false, true) {
					close(gateWait)
				}
			})
			t.Cleanup(restoreObserver)
			directDone := make(chan error, 1)
			go func() { directDone <- mutation.apply(schema) }()
			select {
			case <-gateWait:
				// Observe the actual admission wait, rather than infer it from elapsed time.
			case err := <-directDone:
				t.Fatalf("a direct schema mutation finished while the native commit still held its publication park: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("the direct schema mutation never waited on its publication gate")
			}

			unpark()
			select {
			case err := <-nativeDone:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("native commit did not finish after its park was released")
			}
			select {
			case err := <-directDone:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("direct schema mutation did not finish after the native publication completed")
			}

			policies := map[string]bool{}
			for _, policy := range schema.ShowPromotionPolicies() {
				policies[policy.Name] = true
			}
			require.True(t, policies["native"], "the direct mutation must observe the accepted native policy")
			require.True(t, mutation.published(schema), "the direct schema change is missing from the live schema")

			require.NoError(t, engine.Close())
			reopened := openPolicyPersistenceEngine(t, dir)
			reopenedSchema := reopened.GetSchemaForNamespace(namespace)
			durablePolicies := map[string]bool{}
			for _, policy := range reopenedSchema.ShowPromotionPolicies() {
				durablePolicies[policy.Name] = true
			}
			require.True(t, durablePolicies["native"], "the durable definition must retain the accepted native policy")
			require.True(t, mutation.published(reopenedSchema), "the durable definition must retain the direct schema change")
		})
	}
}

// TestNativeSchemaCommitCanceledWaitingForPublicationGate cancels a native
// schema commit while it waits for a held publication gate: the commit returns
// the context error, writes no schema definition and runs no publication
// callback.
func TestNativeSchemaCommitCanceledWaitingForPublicationGate(t *testing.T) {
	dir := t.TempDir()
	engine := openPolicyPersistenceEngine(t, dir)
	const namespace = "canceledpublication"
	schema := engine.GetSchemaForNamespace(namespace)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace(namespace))
	view, err := tx.Schema(KnowledgePolicyChanges)
	require.NoError(t, err)
	require.NoError(t, view.CreatePromotionPolicy(policyPersistenceFixturePolicy("canceled")))

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
	var afterPublication atomic.Bool
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- tx.PublishCommit(ctx, CommitPublication{AfterPublication: func() { afterPublication.Store(true) }})
	}()
	select {
	case <-gateWait:
	case err := <-commitDone:
		t.Fatalf("native schema commit finished before waiting on the held publication gate: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("native schema commit never waited on the held publication gate")
	}
	cancel()
	select {
	case err := <-commitDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("canceled native schema commit did not return")
	}
	releaseGate()

	require.False(t, afterPublication.Load(), "a canceled native schema commit must not run its publication callback")
	require.Equal(t, TxStatusRolledBack, tx.Status)
	require.Empty(t, schema.ShowPromotionPolicies(), "a canceled native schema commit must not publish its definition")

	require.NoError(t, engine.Close())
	reopened := openPolicyPersistenceEngine(t, dir)
	require.Empty(t, reopened.GetSchemaForNamespace(namespace).ShowPromotionPolicies(),
		"a canceled native schema commit must not write its definition")
}

// TestSchemaPublicationGateLeavesEntityOnlyCommitsFree holds the publication
// gate while a known-token registration and an entity-only commit run: neither
// takes the gate, so ordinary writes never wait for a native schema
// publication.
func TestSchemaPublicationGateLeavesEntityOnlyCommitsFree(t *testing.T) {
	engine := openPolicyPersistenceEngine(t, t.TempDir())
	schema := engine.GetSchemaForNamespace("entitycommit")
	require.NoError(t, schema.RegisterTokens([]string{"Doc"}, nil))

	schema.publicationGate.Lock()
	defer schema.publicationGate.Unlock()

	done := make(chan error, 1)
	go func() {
		if err := schema.RegisterTokens([]string{"Doc"}, nil); err != nil {
			done <- err
			return
		}
		_, err := engine.CreateNode(&Node{ID: "entitycommit:1", Labels: []string{"Doc"}, Properties: map[string]any{"body": "x"}})
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("a known-token registration or an entity-only commit waited on the schema publication gate")
	}
}
