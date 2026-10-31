package storage

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// policyPersistenceFixturePolicy builds a valid promotion policy for these
// persistence-ordering tests.
func policyPersistenceFixturePolicy(name string) knowledgepolicy.PromotionPolicyDef {
	return knowledgepolicy.PromotionPolicyDef{
		Name:         name,
		TargetLabels: []string{"Fact"},
		Enabled:      true,
	}
}

// openPolicyPersistenceEngine opens a disk-backed engine whose durable schema
// key can be read back by a reopen. Close is idempotent, so a test may close it
// early to reopen the same directory.
func openPolicyPersistenceEngine(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	return engine
}

// TestPolicyMutationWaitsForNativePublicationBeforeReadingSchema pins the
// ordering between a direct policy mutation and a native transaction that
// publishes a policy: the direct mutation must wait at the schema manager's
// publication gate (not at the global schema lock, which the native commit
// takes during publication), and after the native publication completes it must
// see the native policy and persist a definition containing both changes.
func TestPolicyMutationWaitsForNativePublicationBeforeReadingSchema(t *testing.T) {
	dir := t.TempDir()
	engine := openPolicyPersistenceEngine(t, dir)
	const namespace = "policyadmission"
	committed := engine.GetSchemaForNamespace(namespace)

	parked := make(chan struct{})
	release := make(chan struct{})
	var parkOnce sync.Once
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	restoreTailHook := setPublicationStageHook(func(uint64) {
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
	})
	t.Cleanup(restoreTailHook)

	nativeDone := make(chan error, 1)
	go func() {
		tx, err := engine.BeginTransaction()
		if err != nil {
			nativeDone <- err
			return
		}
		if err := tx.SetNamespace(namespace); err != nil {
			_ = tx.Rollback()
			nativeDone <- err
			return
		}
		view, err := tx.Schema(KnowledgePolicyChanges)
		if err != nil {
			_ = tx.Rollback()
			nativeDone <- err
			return
		}
		if err := view.CreatePromotionPolicy(policyPersistenceFixturePolicy("native")); err != nil {
			_ = tx.Rollback()
			nativeDone <- err
			return
		}
		nativeDone <- tx.Commit()
	}()
	select {
	case <-parked:
	case err := <-nativeDone:
		t.Fatalf("native commit finished before its pre-publication park: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("native commit did not reach its pre-publication park")
	}

	require.Empty(t, committed.ShowPromotionPolicies(), "the native physical write must still await live schema publication")

	gateWait := make(chan struct{})
	var gateClaimed atomic.Bool
	restoreObserver := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &committed.publicationGate && gateClaimed.CompareAndSwap(false, true) {
			close(gateWait)
		}
	})
	t.Cleanup(restoreObserver)

	directDone := make(chan error, 1)
	go func() {
		directDone <- committed.CreatePromotionPolicy(policyPersistenceFixturePolicy("direct"))
	}()
	select {
	case <-gateWait:
	case err := <-directDone:
		t.Fatalf("direct mutation finished while the native commit still held its publication park: %v", err)
	case <-time.After(30 * time.Second):
		unpark()
		t.Fatal("direct mutation never waited on the schema publication gate")
	}
	require.True(t, committed.mu.TryLock(),
		"a direct mutation that already waits on the publication gate must not hold the global schema lock")
	committed.mu.Unlock()

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
		t.Fatal("direct mutation did not finish after the native publication completed")
	}

	published := map[string]bool{}
	for _, policy := range committed.ShowPromotionPolicies() {
		published[policy.Name] = true
	}
	require.True(t, published["native"], "the direct mutation must observe the accepted native policy")
	require.True(t, published["direct"])

	require.NoError(t, engine.Close())
	reopened := openPolicyPersistenceEngine(t, dir)
	durable := map[string]bool{}
	for _, policy := range reopened.GetSchemaForNamespace(namespace).ShowPromotionPolicies() {
		durable[policy.Name] = true
	}
	require.True(t, durable["native"], "the durable definition must retain the accepted native policy")
	require.True(t, durable["direct"], "the durable definition must retain the accepted direct policy")
}

// TestPolicyAcceptedDirectWritersKeepEveryAcceptedValue runs successful direct
// writes on one engine-bound manager while an earlier accepted write is still
// being persisted and, in a second phase, while its changed callback is parked.
// Every accepted value must survive in memory and in the durable definition;
// they must not overwrite each other.
func TestPolicyAcceptedDirectWritersKeepEveryAcceptedValue(t *testing.T) {
	dir := t.TempDir()
	engine := openPolicyPersistenceEngine(t, dir)
	const namespace = "policywriters"
	committed := engine.GetSchemaForNamespace(namespace)

	persistEntered := make(chan struct{})
	persistRelease := make(chan struct{})
	var persistOnce sync.Once
	var persistReleaseOnce sync.Once
	unparkPersist := func() { persistReleaseOnce.Do(func() { close(persistRelease) }) }
	t.Cleanup(unparkPersist)
	realPersist := committed.persist
	require.NotNil(t, realPersist)
	committed.SetPersister(func(def *SchemaDefinition) error {
		persistOnce.Do(func() {
			close(persistEntered)
			<-persistRelease
		})
		return realPersist(def)
	})

	firstDone := make(chan error, 1)
	go func() { firstDone <- committed.CreatePromotionPolicy(policyPersistenceFixturePolicy("first")) }()
	select {
	case <-persistEntered:
	case err := <-firstDone:
		t.Fatalf("first accepted write finished before its persistence park: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("first accepted write never reached its persistence park")
	}
	if committed.mu.TryLock() {
		committed.mu.Unlock()
		t.Fatal("an accepted knowledge-policy write released the schema lock before its persistence resolved")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- committed.CreatePromotionPolicy(policyPersistenceFixturePolicy("second")) }()
	select {
	case err := <-secondDone:
		t.Fatalf("a second accepted writer finished while the first accepted write was still being persisted: %v", err)
	case <-time.After(250 * time.Millisecond):
		// The second writer waits for the accepted persistence, which is allowed.
	}

	unparkPersist()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("second accepted write did not finish after the first persistence resolved")
	}
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("first accepted write did not finish")
	}

	// Second phase: the accepted callback of the third write is parked while a
	// fourth accepted write runs, which may wait or proceed; neither value may
	// be lost.
	callbackParked := make(chan struct{})
	callbackRelease := make(chan struct{})
	var callbackOnce sync.Once
	var callbackReleaseOnce sync.Once
	unparkCallback := func() { callbackReleaseOnce.Do(func() { close(callbackRelease) }) }
	t.Cleanup(unparkCallback)
	committed.SetKnowledgePolicyChangedHook(func() {
		callbackOnce.Do(func() {
			close(callbackParked)
			<-callbackRelease
		})
	})
	thirdDone := make(chan error, 1)
	go func() { thirdDone <- committed.CreatePromotionPolicy(policyPersistenceFixturePolicy("third")) }()
	select {
	case <-callbackParked:
	case err := <-thirdDone:
		t.Fatalf("third accepted write finished before its callback park: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("third accepted write never reached its callback park")
	}
	fourthDone := make(chan error, 1)
	fourthFinished := false
	go func() { fourthDone <- committed.CreatePromotionPolicy(policyPersistenceFixturePolicy("fourth")) }()
	select {
	case err := <-fourthDone:
		require.NoError(t, err)
		fourthFinished = true
	case <-time.After(250 * time.Millisecond):
		// A serialized writer may wait behind the parked accepted callback.
	}
	unparkCallback()
	select {
	case err := <-thirdDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("third accepted write did not finish")
	}
	if !fourthFinished {
		select {
		case err := <-fourthDone:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("fourth accepted write did not finish")
		}

	}
	names := []string{"first", "second", "third", "fourth"}
	inMemory := map[string]bool{}
	for _, policy := range committed.ShowPromotionPolicies() {
		inMemory[policy.Name] = true
	}
	for _, name := range names {
		require.True(t, inMemory[name], "accepted policy %q is missing from memory", name)
	}

	require.NoError(t, engine.Close())
	reopened := openPolicyPersistenceEngine(t, dir)
	durable := map[string]bool{}
	for _, policy := range reopened.GetSchemaForNamespace(namespace).ShowPromotionPolicies() {
		durable[policy.Name] = true
	}
	for _, name := range names {
		require.True(t, durable[name], "accepted policy %q is missing from the durable definition", name)
	}
}

// TestPolicyRejectedMutationDoesNotRideLaterWrite rejects one mutation of every
// knowledge-policy form and then performs a successful write: the rejected
// change must not appear in that write's definition.
func TestPolicyRejectedMutationDoesNotRideLaterWrite(t *testing.T) {
	rejected := errors.New("policy persistence rejected")
	for _, form := range []string{"create", "alter", "drop"} {
		t.Run(form, func(t *testing.T) {
			schema := NewSchemaManager()
			require.NoError(t, schema.CreatePromotionPolicy(policyPersistenceFixturePolicy("policy")))
			schema.SetPersister(func(*SchemaDefinition) error { return rejected })

			var err error
			switch form {
			case "create":
				err = schema.CreatePromotionProfile(validPromoProfile("ghost"))
			case "alter":
				err = schema.AlterPromotionPolicy("policy", map[string]any{"enabled": false})
			case "drop":
				err = schema.DropPromotionPolicy("policy")
			}
			require.ErrorIs(t, err, rejected)

			var written *SchemaDefinition
			schema.SetPersister(func(def *SchemaDefinition) error {
				written = def
				return nil
			})
			require.NoError(t, schema.CreatePromotionProfile(validPromoProfile("later")))
			require.NotNil(t, written)
			require.Equal(t, written, schema.ExportDefinition())

			profileNames := map[string]bool{}
			for _, profile := range written.PromotionProfiles {
				profileNames[profile.Name] = true
			}
			require.False(t, profileNames["ghost"], "a rejected profile creation rode a later successful write")
			require.True(t, profileNames["later"])

			require.Len(t, written.PromotionPolicies, 1)
			require.Equal(t, "policy", written.PromotionPolicies[0].Name,
				"a rejected drop rode a later successful write")
			require.True(t, written.PromotionPolicies[0].Enabled,
				"a rejected alter rode a later successful write")
		})
	}
}

// TestPolicyRejectedMutationKeepsDerivedSchemaState rejects a policy mutation
// on a manager whose unique-constraint cache, property-index values and
// compiled binding table are populated: the rejected change must leave those
// allocations and values published, and must not rebuild the binding table.
func TestPolicyRejectedMutationKeepsDerivedSchemaState(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("unique_id", "Doc", "id"))
	require.NoError(t, schema.AddPropertyIndex("doc_name", "Doc", []string{"name"}))
	require.NoError(t, schema.CreatePromotionPolicy(policyPersistenceFixturePolicy("published")))

	schema.ApplyUniqueValue("Doc", "id", "kept", "policy:1", ValidatedUniqueClaim)
	unique := schema.uniqueConstraints["Doc:id"]
	require.NotNil(t, unique)
	unique.mu.Lock()
	unique.valuesCacheComplete = true
	unique.mu.Unlock()
	schema.MarkPropertyIndexesFilled()
	require.NoError(t, schema.PropertyIndexInsert("Doc", "name", "policy:1", "kept"))
	index, ok := schema.GetPropertyIndex("Doc", "name")
	require.True(t, ok)
	bindingTable := schema.GetBindingTable()
	require.NotNil(t, bindingTable)
	before := schema.ExportDefinition()

	rejected := errors.New("policy persistence rejected")
	changed := 0
	schema.SetPersister(func(*SchemaDefinition) error { return rejected })
	schema.SetKnowledgePolicyChangedHook(func() { changed++ })
	require.ErrorIs(t, schema.CreatePromotionProfile(validPromoProfile("ghost")), rejected)

	require.Equal(t, before, schema.ExportDefinition(), "a rejected change must not become the readable schema")
	require.Zero(t, changed)
	require.True(t, schema.mu.TryLock(), "a rejected change must release the schema lock")
	schema.mu.Unlock()

	require.Same(t, unique, schema.uniqueConstraints["Doc:id"],
		"a rejected change must keep the unique constraint and its value cache")
	unique.mu.RLock()
	value, kept := unique.values["kept"]
	complete := unique.valuesCacheComplete
	unique.mu.RUnlock()
	require.Equal(t, NodeID("policy:1"), value)
	require.True(t, kept)
	require.True(t, complete)

	after, ok := schema.GetPropertyIndex("Doc", "name")
	require.True(t, ok)
	require.Same(t, index, after, "a rejected change must keep the property index and its values")
	require.Equal(t, []NodeID{"policy:1"}, after.LookupFull("kept"))

	require.Same(t, bindingTable, schema.GetBindingTable(),
		"a rejected change must not rebuild the compiled binding table")
}

// TestPolicyRejectedMutationIsNeverReadableDuringPersistence samples the
// published policies while a rejected persistence is in flight. Blocking until
// the persistence resolves is allowed; reading the staged change is not.
func TestPolicyRejectedMutationIsNeverReadableDuringPersistence(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.CreatePromotionProfile(validPromoProfile("published")))
	before := schema.ExportDefinition()

	rejected := errors.New("policy persistence rejected")
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	schema.SetPersister(func(*SchemaDefinition) error {
		close(entered)
		<-release
		return rejected
	})

	mutationDone := make(chan error, 1)
	go func() { mutationDone <- schema.CreatePromotionProfile(validPromoProfile("ghost")) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the rejected mutation never reached persistence")
	}

	readDone := make(chan []knowledgepolicy.PromotionProfileDef, 1)
	go func() {
		profiles := schema.ShowPromotionProfiles()
		_ = schema.ExportDefinition()
		readDone <- profiles
	}()
	sampled := false
	select {
	case profiles := <-readDone:
		sampled = true
		for _, profile := range profiles {
			require.NotEqual(t, "ghost", profile.Name, "a reader observed a rejected change during persistence")
		}
	case <-time.After(100 * time.Millisecond):
		// Blocking for the whole persistence window is allowed.
	}

	unpark()
	select {
	case err := <-mutationDone:
		require.ErrorIs(t, err, rejected)
	case <-time.After(30 * time.Second):
		t.Fatal("the rejected mutation did not finish")
	}
	if !sampled {
		select {
		case profiles := <-readDone:
			for _, profile := range profiles {
				require.NotEqual(t, "ghost", profile.Name, "a reader observed a rejected change")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the concurrent reader did not finish")
		}
	}
	require.Equal(t, before, schema.ExportDefinition())
	require.True(t, schema.mu.TryLock(), "a rejected change must release the schema lock")
	schema.mu.Unlock()
}

// TestPolicyIfNotExistsNoOpKeepsPublishedAllocation pins the IF NOT EXISTS
// no-op: no persistence, no callback, and no replacement of the published
// family allocation.
func TestPolicyIfNotExistsNoOpKeepsPublishedAllocation(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.CreatePromotionProfile(validPromoProfile("kept")))
	before := schema.promotionProfiles
	persists, changed := 0, 0
	schema.SetPersister(func(*SchemaDefinition) error {
		persists++
		return nil
	})
	schema.SetKnowledgePolicyChangedHook(func() { changed++ })

	require.NoError(t, schema.CreatePromotionProfile(validPromoProfile("kept"), true))
	require.Zero(t, persists)
	require.Zero(t, changed)
	require.Equal(t, reflect.ValueOf(before).Pointer(), reflect.ValueOf(schema.promotionProfiles).Pointer(),
		"an IF NOT EXISTS no-op must keep the published family allocation")
}

// constraintRuntimeCacheFixture is the live state a rejected constraint add
// must leave exactly as it was: the accepted unique constraint with its
// complete value cache, the filled equality index holding the same value, and
// an unrelated knowledge policy with its compiled binding table.
type constraintRuntimeCacheFixture struct {
	schema  *SchemaManager
	unique  *UniqueConstraint
	index   *PropertyIndex
	binding *knowledgepolicy.BindingTable
}

// newConstraintRuntimeCacheFixture seeds that state once, so every rejected add
// in the table below is rejected against the same live caches.
func newConstraintRuntimeCacheFixture(t *testing.T) *constraintRuntimeCacheFixture {
	t.Helper()

	schema := NewSchemaManager()
	require.NoError(t, schema.AddUniqueConstraint("existing_id", "Doc", "id"))
	schema.ApplyUniqueValue("Doc", "id", "kept", "constraint:1", ValidatedUniqueClaim)
	unique := schema.uniqueConstraints["Doc:id"]
	require.NotNil(t, unique)
	unique.mu.Lock()
	unique.valuesCacheComplete = true
	unique.mu.Unlock()
	schema.MarkPropertyIndexesFilled()
	require.NoError(t, schema.PropertyIndexInsert("Doc", "id", "constraint:1", "kept"))
	index, ok := schema.GetPropertyIndex("Doc", "id")
	require.True(t, ok)
	require.NoError(t, schema.CreatePromotionProfile(validPromoProfile("kept_policy")))
	binding := schema.GetBindingTable()
	require.NotNil(t, binding, "the fixture needs a compiled binding table to observe a rollback recompiling it")

	return &constraintRuntimeCacheFixture{schema: schema, unique: unique, index: index, binding: binding}
}

// requireUnchanged asserts a rejected add published nothing and kept every live
// allocation the fixture seeded.
func (f *constraintRuntimeCacheFixture) requireUnchanged(t *testing.T, before *SchemaDefinition) {
	t.Helper()

	require.Equal(t, before, f.schema.ExportDefinition(), "a rejected constraint must not become the readable schema")
	require.Same(t, f.unique, f.schema.uniqueConstraints["Doc:id"],
		"a rejected constraint add must keep the live unique constraint and its value cache")
	nodeID, valueFound, constrained, complete := f.schema.LookupUniqueConstraintValue("Doc", "id", "kept")
	require.True(t, constrained)
	require.True(t, valueFound, "a rejected constraint add must keep the cached unique value")
	require.Equal(t, NodeID("constraint:1"), nodeID)
	require.True(t, complete, "a rejected constraint add must keep the value cache complete")

	after, ok := f.schema.GetPropertyIndex("Doc", "id")
	require.True(t, ok)
	require.Same(t, f.index, after, "a rejected constraint add must keep the live property index")
	require.Equal(t, []NodeID{"constraint:1"}, after.LookupFull("kept"))

	require.Same(t, f.binding, f.schema.GetBindingTable(),
		"a rejected constraint add must keep the compiled binding table instead of recompiling it")

	require.True(t, f.schema.mu.TryLock(), "a rejected constraint add must release the schema lock")
	f.schema.mu.Unlock()
	gate := &f.schema.publicationGate
	gate.mu.Lock()
	held := gate.writer || gate.readers != 0
	gate.mu.Unlock()
	require.False(t, held, "a rejected constraint add must release the publication gate")
}

// TestRejectedConstraintAddKeepsRuntimeCaches reproduces the derived-state loss
// in the constraint add rollback: rejecting the add must restore the published
// rules and leave every live unique-constraint value cache, index value and
// compiled binding table in place. The standard constraint add and the contract
// bundle that compiles it are rejected against the same live caches, because
// both roll back the same families.
func TestRejectedConstraintAddKeepsRuntimeCaches(t *testing.T) {
	rejected := errors.New("constraint persistence rejected")

	contract := func() ConstraintContract {
		return ConstraintContract{
			Name:              "person_contract",
			TargetEntityType:  string(ConstraintEntityNode),
			TargetLabelOrType: "Person",
			Entries: []ConstraintContractEntry{
				{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintUnique), Property: "email", Properties: []string{"email"}},
				{Kind: ConstraintContractKindPrimitiveNode, PrimitiveType: string(ConstraintPropertyType), Property: "age", Properties: []string{"age"}, ExpectedType: string(PropertyTypeInteger)},
			},
		}
	}
	compiledEmail := Constraint{
		Name: ConstraintContractEntryName("person_contract", 0), Type: ConstraintUnique, EntityType: ConstraintEntityNode,
		Label: "Person", Properties: []string{"email"},
	}
	compiledAge := PropertyTypeConstraint{
		Name: ConstraintContractEntryName("person_contract", 1), EntityType: ConstraintEntityNode,
		Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger,
	}
	// duplicateDocID is equivalent to the fixture's live unique constraint, so
	// the bundle admits its first compiled entry and rejects this one.
	duplicateDocID := Constraint{
		Name: "duplicate_doc_id", Type: ConstraintUnique, EntityType: ConstraintEntityNode,
		Label: "Doc", Properties: []string{"id"},
	}

	cases := []struct {
		name string
		// seed prepares state the add meets, before the rejecting persister is
		// installed.
		seed func(t *testing.T, schema *SchemaManager)
		add  func(t *testing.T, schema *SchemaManager) error
		// assertErr states what the add returns: the persister's rejection, an
		// admission rejection, or nothing for an IF NOT EXISTS no-op.
		assertErr func(t *testing.T, err error)
		// wantPersists is how many definitions the add attempts to persist.
		wantPersists int
		noPersister  bool
		// assertNotPublished states that the rejected add's own rules are absent.
		assertNotPublished func(t *testing.T, schema *SchemaManager)
	}{
		{
			name: "standard unique constraint",
			add: func(t *testing.T, schema *SchemaManager) error {
				return schema.AddUniqueConstraint("rejected_email", "Person", "email")
			},
			assertErr:    func(t *testing.T, err error) { require.ErrorIs(t, err, rejected) },
			wantPersists: 1,
			assertNotPublished: func(t *testing.T, schema *SchemaManager) {
				require.NotContains(t, schema.constraints, "rejected_email")
				require.NotContains(t, schema.compositeIndexes, "rejected_email")
			},
		},
		{
			name: "contract bundle persist rejected",
			add: func(t *testing.T, schema *SchemaManager) error {
				return schema.AddConstraintContractBundle(contract(), []Constraint{compiledEmail},
					[]PropertyTypeConstraint{compiledAge}, false)
			},
			assertErr:    func(t *testing.T, err error) { require.ErrorIs(t, err, rejected) },
			wantPersists: 1,
			assertNotPublished: func(t *testing.T, schema *SchemaManager) {
				require.Empty(t, schema.GetAllConstraintContracts())
				require.NotContains(t, schema.constraints, compiledEmail.Name)
				require.NotContains(t, schema.uniqueConstraints, "Person:email")
				require.NotContains(t, schema.compositeIndexes, compiledEmail.Name)
				require.NotContains(t, schema.rangeIndexes, compiledEmail.Name)
				require.Empty(t, schema.GetAllPropertyTypeConstraints())
			},
		},
		{
			name:        "contract bundle partial compiled admission rejected",
			noPersister: true,
			add: func(t *testing.T, schema *SchemaManager) error {
				return schema.AddConstraintContractBundle(contract(), []Constraint{compiledEmail, duplicateDocID}, nil, false)
			},
			assertErr: func(t *testing.T, err error) {
				require.Error(t, err, "the bundle's second compiled constraint conflicts with the live unique constraint")
			},
			wantPersists: 0,
			assertNotPublished: func(t *testing.T, schema *SchemaManager) {
				require.Empty(t, schema.GetAllConstraintContracts())
				require.NotContains(t, schema.constraints, compiledEmail.Name)
				require.NotContains(t, schema.uniqueConstraints, "Person:email")
				require.NotContains(t, schema.compositeIndexes, compiledEmail.Name)
				require.Len(t, schema.constraints, 1, "the admitted compiled entry must not survive the rejection")
			},
		},
		{
			name: "contract bundle if not exists no-op",
			seed: func(t *testing.T, schema *SchemaManager) {
				require.NoError(t, schema.AddConstraintContractBundle(contract(), nil, nil, false))
			},
			add: func(t *testing.T, schema *SchemaManager) error {
				return schema.AddConstraintContractBundle(contract(), []Constraint{compiledEmail}, nil, true)
			},
			assertErr: func(t *testing.T, err error) {
				require.NoError(t, err, "an IF NOT EXISTS contract add is a no-op")
			},
			wantPersists: 0,
			assertNotPublished: func(t *testing.T, schema *SchemaManager) {
				require.Len(t, schema.GetAllConstraintContracts(), 1)
				require.NotContains(t, schema.constraints, compiledEmail.Name)
				require.NotContains(t, schema.uniqueConstraints, "Person:email")
				require.Len(t, schema.constraints, 1, "a no-op must not apply the bundle's compiled entries")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newConstraintRuntimeCacheFixture(t)
			if tc.seed != nil {
				tc.seed(t, fixture.schema)
			}
			before := fixture.schema.ExportDefinition()

			persists := 0
			if !tc.noPersister {
				fixture.schema.SetPersister(func(*SchemaDefinition) error {
					persists++
					return rejected
				})
			}

			tc.assertErr(t, tc.add(t, fixture.schema))
			require.Equal(t, tc.wantPersists, persists, "unexpected persistence attempts")
			fixture.requireUnchanged(t, before)
			tc.assertNotPublished(t, fixture.schema)
		})
	}
}
