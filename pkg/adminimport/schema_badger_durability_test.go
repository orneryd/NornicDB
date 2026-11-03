package adminimport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/internal/jsontext"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// adminImportSeedRelationshipPair seeds the second node and one edge so the
// exported schema population also carries a relationship type token.
func adminImportSeedRelationshipPair(t *testing.T, engine storage.Engine) {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &storage.Node{
		ID:         "u2",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob"},
	})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{
		ID:        "e1",
		Type:      "KNOWS",
		StartNode: "u1",
		EndNode:   "u2",
	}))
}

// adminImportSeedIndependentSchemaFamilies seeds the schema document families
// that carry no unique constraint, property index or promotion policy, under a
// caller-chosen name tail. The escaped-name population needs this split because
// its constraint/index/profile/policy identities come from
// adminImportSeedEscapedSchema.
func adminImportSeedIndependentSchemaFamilies(t *testing.T, schema *storage.SchemaManager, tail string) {
	t.Helper()
	require.NoError(t, schema.AddCompositeIndex(
		"person_identity_idx"+tail, "Person"+tail, []string{"given", "family"},
	))
	require.NoError(t, schema.AddFulltextIndex(
		"person_bio_ft"+tail, []string{"Person" + tail}, []string{"bio"}, storage.ConstraintEntityNode,
	))
	require.NoError(t, schema.AddVectorIndex(
		"person_embedding_vec"+tail, "Person"+tail, "embedding", 8, "cosine", storage.ConstraintEntityNode,
	))
	require.NoError(t, schema.AddRangeIndex(
		"person_born_range"+tail, "Person"+tail, "born",
	))
	require.NoError(t, schema.AddPropertyTypeConstraint(
		"person_age_type"+tail, "Person"+tail, "age", storage.PropertyTypeInteger,
		storage.PropertyTypeConstraintOptions{EntityType: storage.ConstraintEntityNode},
	))
	require.NoError(t, schema.AddConstraintContractBundle(storage.ConstraintContract{
		Name:              "person_profile" + tail,
		TargetEntityType:  string(storage.ConstraintEntityNode),
		TargetLabelOrType: "Person" + tail,
		Definition:        "person profile properties",
	}, nil, nil, false))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "person_decay" + tail,
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
		DecayEnabled:        true,
	}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "person_decay_binding" + tail,
		TargetLabels: []string{"Person" + tail},
		ProfileRef:   "person_decay" + tail,
	}))
}

// adminImportSeedPlainSchemaPopulation seeds one complete schema document
// population under ordinary names, including the constraint, property index and
// promotion identities adminImportSeedEscapedSchema seeds for escaped names.
func adminImportSeedPlainSchemaPopulation(t *testing.T, schema *storage.SchemaManager) {
	t.Helper()
	require.NoError(t, schema.AddUniqueConstraint("person_email_unique", "Person", "email"))
	require.NoError(t, schema.AddPropertyIndex("person_name_idx", "Person", []string{"name"}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       "person_boost_profile",
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 1.5,
		ScoreFloor: 0.20,
		ScoreCap:   1.0,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         "person_boost_policy",
		TargetLabels: []string{"Person"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{
			Predicate:  "mutationCount > 3",
			ProfileRef: "person_boost_profile",
			Order:      1,
		}},
	}))
	adminImportSeedIndependentSchemaFamilies(t, schema, "")
}

// adminImportReadSchemaDocument decodes the shipped schema document the public
// export wrote, so the fixture's completeness is asserted on the exact document
// an operator hands to the importer.
func adminImportReadSchemaDocument(t *testing.T, path string) *storage.SchemaDefinition {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var document storage.SchemaDefinition
	require.NoError(t, jsontext.Unmarshal(raw, &document))
	return &document
}

// adminImportRequireCompleteSchemaDocument asserts the fixture document carries
// every persisted schema family, so a later equality assertion covers the whole
// document instead of whichever subset the fixture happened to seed.
func adminImportRequireCompleteSchemaDocument(t *testing.T, document *storage.SchemaDefinition) {
	t.Helper()
	require.NotNil(t, document.LookupIndexes)
	families := map[string]int{
		"label tokens":              len(document.LabelTokens),
		"relationship type tokens":  len(document.RelationshipTypeTokens),
		"lookup indexes":            len(*document.LookupIndexes),
		"constraints":               len(document.Constraints),
		"constraint contracts":      len(document.ConstraintContracts),
		"property type constraints": len(document.PropertyTypeConstraints),
		"property indexes":          len(document.PropertyIndexes),
		"composite indexes":         len(document.CompositeIndexes),
		"fulltext indexes":          len(document.FulltextIndexes),
		"vector indexes":            len(document.VectorIndexes),
		"range indexes":             len(document.RangeIndexes),
		"decay profile bundles":     len(document.DecayProfileBundles),
		"decay profile bindings":    len(document.DecayProfileBindings),
		"promotion profiles":        len(document.PromotionProfiles),
		"promotion policies":        len(document.PromotionPolicies),
	}
	for name, count := range families {
		require.NotZero(t, count, "fixture schema document must carry %s", name)
	}
}

// adminImportSchemaSummary reports which schema document families a definition
// carries, so a lost-rule failure states what the destination actually retains.
func adminImportSchemaSummary(def *storage.SchemaDefinition) string {
	lookupIndexes := 0
	if def.LookupIndexes != nil {
		lookupIndexes = len(*def.LookupIndexes)
	}
	return fmt.Sprintf(
		"labelTokens=%d relationshipTypeTokens=%d lookupIndexes=%d constraints=%d constraintContracts=%d "+
			"propertyTypeConstraints=%d propertyIndexes=%d compositeIndexes=%d fulltextIndexes=%d "+
			"vectorIndexes=%d rangeIndexes=%d decayProfileBundles=%d decayProfileBindings=%d "+
			"promotionProfiles=%d promotionPolicies=%d",
		len(def.LabelTokens), len(def.RelationshipTypeTokens), lookupIndexes, len(def.Constraints),
		len(def.ConstraintContracts), len(def.PropertyTypeConstraints), len(def.PropertyIndexes),
		len(def.CompositeIndexes), len(def.FulltextIndexes), len(def.VectorIndexes), len(def.RangeIndexes),
		len(def.DecayProfileBundles), len(def.DecayProfileBindings), len(def.PromotionProfiles),
		len(def.PromotionPolicies),
	)
}

// adminImportReExportSchemaDocument writes one schema document through the
// shipped exporter's canonical writer and returns its exact bytes, so a
// destination's document is compared with the exporter's own output instead of
// a hand-built rendering.
func adminImportReExportSchemaDocument(t *testing.T, schema *storage.SchemaManager) string {
	t.Helper()
	def := schemaDefinitionForTest(t, schema)
	require.True(t, hasSchemaDefinitionContent(def),
		"schema manager carries no schema document content (%s): the imported definition is absent",
		adminImportSchemaSummary(def))
	path := filepath.Join(t.TempDir(), Neo4jCSVNornicSchemaFileName)
	require.NoError(t, exportNornicSchemaDefinition(path, schema))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(raw)
}

// adminImportImportIntoBadger imports one Neo4j CSV export and its schema
// document through the public ImportFull consumer into a native Badger engine
// and returns that engine with its data directory still open.
func adminImportImportIntoBadger(t *testing.T, dir, outDir, schemaFile, database string) *storage.BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)
	report, err := ImportFull(context.Background(), engine, Options{
		DatabaseName: database,
		NodeSources:  nodeSources,
		RelSources:   relSources,
		SchemaFile:   schemaFile,
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, "success", report.Status)
	return engine
}

// adminImportReopenBadger reopens the data directory the closed destination
// owned, so the reopened schema is the one a later process would serve.
func adminImportReopenBadger(t *testing.T, dir string) *storage.BadgerEngine {
	t.Helper()
	reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

// adminImportSeedDurableDestination primes one native Badger destination with
// the durable schema its import runs against and returns the definition a
// reopened destination serves.
//
// The imported document's label and relationship-type tokens are registered
// through the namespace schema writer first, so the data phase of the import
// needs no durable token write of its own: the only durable schema write an
// import can attempt afterwards is the schema replacement under test. One
// constraint the imported document does not carry keeps the pre-import
// definition distinguishable from the prospective one, and the definition is
// reopened from disk before it is returned, so the caller compares against the
// durable definition the destination actually serves.
func adminImportSeedDurableDestination(t *testing.T, dir, database string, imported *storage.SchemaDefinition) *storage.SchemaDefinition {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	schema := engine.GetSchemaForNamespace(database)
	require.NoError(t, schema.RegisterTokens(t.Context(), imported.LabelTokens, imported.RelationshipTypeTokens))
	require.NoError(t, schema.AddUniqueConstraint("legacy_key_unique", "Legacy", "key"))
	durable := schemaDefinitionForTest(t, schema)
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	served := schemaDefinitionForTest(t, reopened.GetSchemaForNamespace(database))
	require.NoError(t, reopened.Close())
	require.Equal(t, durable, served,
		"the destination's pre-import schema must already be durable before the rejection case starts")
	return served
}

// TestImportFullSchemaDefinitionSurvivesNativeBadgerReopen pins the durable half
// of the shipped schema document contract: importing schema.nornic.json through
// the public ImportFull consumer into a native Badger destination and reopening
// that destination must retain the exact complete schema document, under both
// ordinary names and the escaped lone UTF16 identities the export/import codec
// already preserves.
//
// The compared document is the shipped exporter's own output: a destination
// whose schema still carries the imported document re-exports exactly the bytes
// the operator imported.
//
// The rejection case pins the other half of the same replacement: a destination
// whose namespace schema writer refuses the imported document must fail the
// import and keep the pre-import definition, both live and durable, so a
// rejected replacement never publishes a schema it did not persist.
func TestImportFullSchemaDefinitionSurvivesNativeBadgerReopen(t *testing.T) {
	high := adminImportEscapedUnit(t, "d800")
	low := adminImportEscapedUnit(t, "dc00")

	t.Run("plain names", func(t *testing.T) {
		dir := t.TempDir()
		source := storage.NewNamespacedEngine(
			fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			"source",
		)
		adminImportSeedNode(t, source)
		adminImportSeedRelationshipPair(t, source)
		adminImportSeedPlainSchemaPopulation(t, source.GetSchema())

		outDir := adminImportExportSchema(t, source)
		schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
		adminImportRequireCompleteSchemaDocument(t, adminImportReadSchemaDocument(t, schemaFile))
		document, err := os.ReadFile(schemaFile)
		require.NoError(t, err)

		engine := adminImportImportIntoBadger(t, dir, outDir, schemaFile, "dest")
		require.Equal(t, string(document), adminImportReExportSchemaDocument(t, engine.GetSchemaForNamespace("dest")),
			"the public import must apply the whole schema document in memory before the destination is closed")

		require.NoError(t, engine.Close())
		reopenedSchema := adminImportReopenBadger(t, dir).GetSchemaForNamespace("dest")
		t.Logf("schema retained after close/reopen: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, reopenedSchema)))
		require.Equal(t, string(document), adminImportReExportSchemaDocument(t, reopenedSchema),
			"a native Badger destination must retain the imported schema document across close and reopen")
	})

	t.Run("surrogate names", func(t *testing.T) {
		dir := t.TempDir()
		source := storage.NewNamespacedEngine(
			fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			"source",
		)
		adminImportSeedNode(t, source)
		adminImportSeedRelationshipPair(t, source)
		identities := adminImportSeedEscapedSchema(t, source.GetSchema(), high, low)
		adminImportSeedIndependentSchemaFamilies(t, source.GetSchema(), high)

		outDir := adminImportExportSchema(t, source)
		schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
		adminImportRequireCompleteSchemaDocument(t, adminImportReadSchemaDocument(t, schemaFile))
		document, err := os.ReadFile(schemaFile)
		require.NoError(t, err)

		engine := adminImportImportIntoBadger(t, dir, outDir, schemaFile, "dest")
		require.Equal(t, string(document), adminImportReExportSchemaDocument(t, engine.GetSchemaForNamespace("dest")),
			"the public import must apply the whole escaped-identity schema document in memory before the destination is closed")

		require.NoError(t, engine.Close())
		reopenedSchema := adminImportReopenBadger(t, dir).GetSchemaForNamespace("dest")
		t.Logf("schema retained after close/reopen: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, reopenedSchema)))
		adminImportRequireEscapedSchema(t, reopenedSchema, identities)
		require.Equal(t, string(document), adminImportReExportSchemaDocument(t, reopenedSchema),
			"a native Badger destination must retain the imported escaped-identity schema document across close and reopen")
	})

	t.Run("rejected persistence", func(t *testing.T) {
		source := storage.NewNamespacedEngine(
			fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			"source",
		)
		adminImportSeedNode(t, source)
		adminImportSeedRelationshipPair(t, source)
		adminImportSeedPlainSchemaPopulation(t, source.GetSchema())

		outDir := adminImportExportSchema(t, source)
		schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
		document, err := os.ReadFile(schemaFile)
		require.NoError(t, err)
		var imported storage.SchemaDefinition
		require.NoError(t, jsontext.Unmarshal(document, &imported))
		adminImportRequireCompleteSchemaDocument(t, &imported)

		dir := t.TempDir()
		oldDefinition := adminImportSeedDurableDestination(t, dir, "dest", &imported)
		require.NotEqual(t, oldDefinition, &imported,
			"the imported document must differ from the destination's durable schema")

		nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
		require.NoError(t, err)

		engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
		require.NoError(t, err)
		destination := engine.GetSchemaForNamespace("dest")
		require.Equal(t, oldDefinition, schemaDefinitionForTest(t, destination),
			"the reopened destination must serve its durable schema before the import")

		rejection := errors.New("schema persistence rejected")
		var attempted []*storage.SchemaDefinition
		destination.SetPersister(func(def *storage.SchemaDefinition) error {
			attempted = append(attempted, def)
			return rejection
		})

		report, err := ImportFull(context.Background(), engine, Options{
			DatabaseName: "dest",
			NodeSources:  nodeSources,
			RelSources:   relSources,
			SchemaFile:   schemaFile,
			BuildIndexes: false,
			Now:          fixedImportTime,
		})
		t.Logf("live schema after the rejected import: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, destination)))

		// A rejected durable write is a failed import: the destination must not
		// publish, and must not report success for, a schema it never persisted.
		assert.Error(t, err, "a destination whose durable schema write is rejected must not report a successful import")
		assert.ErrorIs(t, err, rejection, "the import must surface the schema persistence rejection itself")
		assert.Equal(t, "failed", report.Status)
		// The published schema keeps serving the pre-import definition.
		assert.Equal(t, oldDefinition, schemaDefinitionForTest(t, destination),
			"a rejected schema replacement must leave the published schema unchanged")
		// One durable write of the prospective document is what the replacement
		// commits, so a rejection is attributable to that write alone. The write
		// carries the accepted prospective definition, whose durable encoding is
		// the imported document (an empty family stays omitted, as the document
		// keeps it).
		if assert.Len(t, attempted, 1, "the replacement must make exactly one durable schema write") {
			attemptedJSON, marshalErr := jsontext.Marshal(attempted[0])
			require.NoError(t, marshalErr)
			assert.JSONEq(t, string(document), string(attemptedJSON),
				"the rejected write must be the prospective imported definition")
		}

		require.NoError(t, engine.Close())
		reopened := adminImportReopenBadger(t, dir)
		reopenedSchema := reopened.GetSchemaForNamespace("dest")
		t.Logf("schema retained after close/reopen: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, reopenedSchema)))
		assert.Equal(t, oldDefinition, schemaDefinitionForTest(t, reopenedSchema),
			"a rejected schema replacement must leave the durable definition unchanged")

		// The pre-import runtime index and UNIQUE caches are the replaced
		// manager's, so the definition the rejection left published must still
		// enforce its constraint through them.
		probe := storage.NewNamespacedEngine(reopened, "dest")
		_, err = probe.CreateNode(t.Context(), &storage.Node{
			ID: "legacy-1", Labels: []string{"Legacy"}, Properties: map[string]any{"key": "k1"},
		})
		require.NoError(t, err, "the pre-import schema must still accept a first constrained value")
		_, err = probe.CreateNode(t.Context(), &storage.Node{
			ID: "legacy-2", Labels: []string{"Legacy"}, Properties: map[string]any{"key": "k1"},
		})
		require.Error(t, err,
			"a rejected schema replacement must leave the pre-import UNIQUE constraint enforced")
	})

	t.Run("rejected validation", func(t *testing.T) {
		// The imported nodes cannot satisfy the document's UNIQUE constraint, so
		// the accepted replacement must be rejected before any durable write: a
		// definition the data cannot satisfy is neither persisted nor published.
		source := storage.NewNamespacedEngine(
			fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
			"source",
		)
		for _, id := range []string{"u1", "u2"} {
			_, err := source.CreateNode(t.Context(), &storage.Node{
				ID:         storage.NodeID(id),
				Labels:     []string{"Person"},
				Properties: map[string]any{"email": "shared@example.com"},
			})
			require.NoError(t, err)
		}
		outDir := adminImportExportSchema(t, source)

		// The shipped exporter's own writer produces the document, so the
		// rejection is decided on a document an operator can hand to the
		// importer.
		documentSchema := storage.NewSchemaManager()
		require.NoError(t, documentSchema.RegisterTokens(t.Context(), []string{"Person"}, nil))
		require.NoError(t, documentSchema.AddUniqueConstraint("person_email_unique", "Person", "email"))
		schemaFile := filepath.Join(t.TempDir(), Neo4jCSVNornicSchemaFileName)
		require.NoError(t, exportNornicSchemaDefinition(schemaFile, documentSchema))

		dir := t.TempDir()
		oldDefinition := adminImportSeedDurableDestination(t, dir, "dest", schemaDefinitionForTest(t, documentSchema))

		nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
		require.NoError(t, err)

		engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
		require.NoError(t, err)
		destination := engine.GetSchemaForNamespace("dest")
		var attempted []*storage.SchemaDefinition
		destination.SetPersister(func(def *storage.SchemaDefinition) error {
			attempted = append(attempted, def)
			return nil
		})

		report, err := ImportFull(context.Background(), engine, Options{
			DatabaseName: "dest",
			NodeSources:  nodeSources,
			RelSources:   relSources,
			SchemaFile:   schemaFile,
			BuildIndexes: false,
			Now:          fixedImportTime,
		})
		t.Logf("live schema after the unsatisfiable import: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, destination)))

		// The existing classification, unchanged: an unapplied schema document
		// fails the import under the constraint-violation exit.
		var importErr *Error
		require.ErrorAs(t, err, &importErr)
		assert.Equal(t, ExitConstraintViolation, importErr.ExitCode)
		assert.Equal(t, "failed", report.Status)
		// Acceptance checks run before the durable write, so a document the data
		// cannot satisfy attempts no schema write at all.
		assert.Empty(t, attempted,
			"a definition the data cannot satisfy must be rejected before it is persisted")
		assert.Equal(t, oldDefinition, schemaDefinitionForTest(t, destination),
			"a rejected schema replacement must leave the published schema unchanged")

		require.NoError(t, engine.Close())
		reopenedSchema := adminImportReopenBadger(t, dir).GetSchemaForNamespace("dest")
		t.Logf("schema retained after close/reopen: %s", adminImportSchemaSummary(schemaDefinitionForTest(t, reopenedSchema)))
		assert.Equal(t, oldDefinition, schemaDefinitionForTest(t, reopenedSchema),
			"a rejected schema replacement must leave the durable definition unchanged")
	})
}
