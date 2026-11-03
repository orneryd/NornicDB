package adminimport_test

// Public-contract reproduction for a JSON schema document whose knowledge-policy
// binding references a decay profile the document does not define.
//
// The import target's accepted definition replacement loads the document into a
// detached prospective manager before anything is written. Compiling that
// candidate's binding table is where an unknown profile reference is detected,
// so the import must fail before the document is persisted or published and the
// live, runtime and durable schema must stay as it was. The data rows imported
// before the document keep their accepted-data contract, as in the cancellation
// case: only the document's schema publication is forbidden. Those rows also
// register their label token through the ordinary accepted token path, which is
// why the token order is not part of the state this test holds fixed.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const invalidBindingNamespace = "invalidbinding"

const (
	invalidBindingProbeName    = "person_missing_profile"
	invalidBindingMissingRef   = "absent_decay_bundle"
	invalidBindingBaselineName = "baseline_binding"
	invalidBindingBaselineRef  = "baseline_bundle"
)

// writeInvalidBindingFixture writes two accepted CSV data rows and a JSON schema
// whose only knowledge-policy binding references a decay profile the document
// does not carry. The document contains no constraints: the pre-existing
// namespace schema is the definition the import must leave in place.
func writeInvalidBindingFixture(t *testing.T) (nodesPath, schemaPath string) {
	t.Helper()

	dir := t.TempDir()
	nodesPath = filepath.Join(dir, "nodes.csv")
	schemaPath = filepath.Join(dir, "schema.json")

	require.NoError(t, os.WriteFile(nodesPath, []byte(
		":ID,name,:LABEL\n"+
			"n1,Alice,Person\n"+
			"n2,Bob,Person\n",
	), 0o600))

	definition := storage.SchemaDefinition{
		DecayProfileBindings: []knowledgepolicy.DecayProfileBinding{{
			Name:       invalidBindingProbeName,
			IsWildcard: true,
			ProfileRef: invalidBindingMissingRef,
		}},
	}
	raw, err := json.Marshal(definition)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(schemaPath, raw, 0o600))
	return nodesPath, schemaPath
}

// seedPublishedKnowledgePolicy publishes one valid bundle and its wildcard
// binding so the test can observe that a rejected document leaves the live
// family and its compiled binding table in place.
func seedPublishedKnowledgePolicy(t *testing.T, schema *storage.SchemaManager) {
	t.Helper()
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                invalidBindingBaselineRef,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.1,
		Function:            knowledgepolicy.DecayFunctionExponential,
		Scope:               knowledgepolicy.ScopeNode,
		DecayEnabled:        true,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:       invalidBindingBaselineName,
		IsWildcard: true,
		ProfileRef: invalidBindingBaselineRef,
	}))
}

// requireSchemaApplicationFailed asserts the import surfaced the document as a
// schema application failure whose cause names the missing profile reference.
func requireSchemaApplicationFailed(t *testing.T, importErr error) {
	t.Helper()
	require.Error(t, importErr)
	var schemaErr *adminimport.Error
	require.ErrorAs(t, importErr, &schemaErr)
	assert.Equal(t, adminimport.ExitConstraintViolation, schemaErr.ExitCode,
		"an unacceptable schema document must fail the import as a schema application failure")
	assert.Contains(t, importErr.Error(), invalidBindingMissingRef,
		"the failure must name the unresolved decay profile reference")
}

// assertSchemaFamiliesUnchanged asserts the rejected document published none of
// its families: every definition family except the token order the accepted data
// rows registered is the pre-import one, and the compiled binding table is the
// very pointer the seed compiled.
func assertSchemaFamiliesUnchanged(t *testing.T, schema *storage.SchemaManager, beforeTable *knowledgepolicy.BindingTable, before *storage.SchemaDefinition) {
	t.Helper()
	assert.Same(t, beforeTable, schema.GetBindingTable(),
		"a rejected document must leave the compiled binding table serving")
	live := schemaDefinitionForTest(t, schema)
	want := *before
	// Accepted data rows may extend token order, but no other definition field
	// may change on rejection, including fields added to SchemaDefinition later.
	want.LabelTokens = live.LabelTokens
	want.RelationshipTypeTokens = live.RelationshipTypeTokens
	assert.Equal(t, &want, live)

	bundles, bindings := schema.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	require.Len(t, bindings, 1)
	assert.Equal(t, invalidBindingBaselineRef, bundles[0].Name)
	assert.Equal(t, invalidBindingBaselineName, bindings[0].Name)
}

// requireNoPersistedProbeBinding asserts no durable schema write commissioned
// during the import carried the document's rejected binding. The accepted data
// import may write the schema it legitimately extended (its label token).
func requireNoPersistedProbeBinding(t *testing.T, persisted []*storage.SchemaDefinition) {
	t.Helper()
	require.NotEmpty(t, persisted, "the accepted data import must still persist its token registration")
	for _, definition := range persisted {
		require.NotNil(t, definition)
		for _, binding := range definition.DecayProfileBindings {
			require.NotEqual(t, invalidBindingProbeName, binding.Name,
				"no durable schema write may carry the rejected document's binding")
		}
	}
}

func TestImportFullJSONSchemaInvalidBindingIsRejectedBeforePublication(t *testing.T) {
	t.Run("memory live definition", func(t *testing.T) {
		nodesPath, schemaPath := writeInvalidBindingFixture(t)

		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		schema := base.GetSchemaForNamespace(invalidBindingNamespace)
		require.NoError(t, schema.AddUniqueConstraint(
			schemaCancellationBaselineConstraint, "Person", "id"))
		seedPublishedKnowledgePolicy(t, schema)

		var persistedMu sync.Mutex
		var persisted []*storage.SchemaDefinition
		schema.SetPersister(func(def *storage.SchemaDefinition) error {
			persistedMu.Lock()
			defer persistedMu.Unlock()
			persisted = append(persisted, def)
			return nil
		})
		beforeTable := schema.GetBindingTable()
		require.NotNil(t, beforeTable, "the seed must have published a compiled binding table")
		beforeDefinition := schemaDefinitionForTest(t, schema)

		report, importErr := adminimport.ImportFull(t.Context(), base, adminimport.Options{
			DatabaseName: invalidBindingNamespace,
			NodeSources:  []string{nodesPath},
			SchemaFile:   schemaPath,
		})

		requireSchemaApplicationFailed(t, importErr)

		persistedMu.Lock()
		persistedDefinitions := append([]*storage.SchemaDefinition(nil), persisted...)
		persistedMu.Unlock()
		requireNoPersistedProbeBinding(t, persistedDefinitions)

		live := base.GetSchemaForNamespace(invalidBindingNamespace)
		assert.True(t, hasSchemaConstraint(live, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain published")
		assertSchemaFamiliesUnchanged(t, live, beforeTable, beforeDefinition)

		namespaced := storage.NewNamespacedEngine(base, invalidBindingNamespace)
		count, err := namespaced.NodeCount(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(2), report.NodesImported,
			"both accepted rows must have been counted as imported")
		assert.Equal(t, int64(2), count,
			"accepted data writes need not be rolled back when the document is rejected")
	})

	t.Run("badger live definition and native reopen", func(t *testing.T) {
		nodesPath, schemaPath := writeInvalidBindingFixture(t)
		dir := filepath.Join(t.TempDir(), "graph")

		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
			storage.NewBadgerEngine(dir))
		require.NoError(t, err)

		schema := base.GetSchemaForNamespace(invalidBindingNamespace)
		require.NoError(t, schema.AddUniqueConstraint(
			schemaCancellationBaselineConstraint, "Person", "id"))
		seedPublishedKnowledgePolicy(t, schema)
		beforeTable := schema.GetBindingTable()
		require.NotNil(t, beforeTable, "the seed must have published a compiled binding table")
		beforeDefinition := schemaDefinitionForTest(t, schema)

		report, importErr := adminimport.ImportFull(t.Context(), base, adminimport.Options{
			DatabaseName: invalidBindingNamespace,
			NodeSources:  []string{nodesPath},
			SchemaFile:   schemaPath,
		})

		requireSchemaApplicationFailed(t, importErr)

		live := base.GetSchemaForNamespace(invalidBindingNamespace)
		assert.True(t, hasSchemaConstraint(live, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain published")
		assertSchemaFamiliesUnchanged(t, live, beforeTable, beforeDefinition)

		namespaced := storage.NewNamespacedEngine(base, invalidBindingNamespace)
		count, err := namespaced.NodeCount(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(2), report.NodesImported)
		assert.Equal(t, int64(2), count)

		// Read the durable definition through a native reopen: an invalid binding
		// must not reach storage even though the live definition looked right.
		require.NoError(t, base.Close())
		reopened, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
			storage.NewBadgerEngine(dir))
		require.NoError(t, err)

		durable := reopened.GetSchemaForNamespace(invalidBindingNamespace)
		t.Logf("durable constraints after native reopen: %v", schemaConstraintNames(durable))
		assert.True(t, hasSchemaConstraint(durable, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain the durable definition")
		durableDefinition := schemaDefinitionForTest(t, durable)
		require.Len(t, durableDefinition.DecayProfileBundles, 1)
		require.Len(t, durableDefinition.DecayProfileBindings, 1)
		assert.Equal(t, invalidBindingBaselineName, durableDefinition.DecayProfileBindings[0].Name,
			"the durable definition must be the seed binding, not the rejected document")

		reopenedNamespaced := storage.NewNamespacedEngine(reopened, invalidBindingNamespace)
		count, err = reopenedNamespaced.NodeCount(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(2), count,
			"accepted data writes need not be rolled back across a native reopen")
	})
}
