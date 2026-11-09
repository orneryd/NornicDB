package adminimport

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/jsontext"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// adminImportEscapedUnit returns the exact in-memory string identity the
// canonical internal/jsontext codec admits for the standard JSON escape of one
// lone UTF16 unit. It is derived from JSON text so the fixture is the identity
// the shipped export/import codec must preserve, not a hand-built surrogate.
func adminImportEscapedUnit(t *testing.T, unit string) string {
	t.Helper()
	var payload struct {
		Value string `json:"value"`
	}
	require.NoError(t, jsontext.Unmarshal([]byte(`{"value":"\u`+unit+`"}`), &payload))
	require.NotEqual(t, "\ufffd", payload.Value, "fixture must not already be a replacement character")
	return payload.Value
}

// adminImportSchemaIdentities names one seeded schema population whose names
// carry escaped lone UTF16 units. Every schema document consumer below must
// keep these exact identities instead of normalizing them.
type adminImportSchemaIdentities struct {
	constraintName string
	label          string
	constraintProp string
	indexName      string
	indexLabel     string
	indexProp      string
	profileName    string
	policyName     string
	policyLabel    string
	predicate      string
}

func adminImportSeedNode(t *testing.T, engine storage.Engine) {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &storage.Node{
		ID:         "u1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	require.NoError(t, err)
}

func adminImportSeedEscapedSchema(t *testing.T, schema *storage.SchemaManager, high, low string) adminImportSchemaIdentities {
	t.Helper()
	identities := adminImportSchemaIdentities{
		constraintName: "person_email_unique" + high,
		label:          "Person" + high,
		constraintProp: "email" + high,
		indexName:      "person_name_idx" + low,
		indexLabel:     "Person" + low,
		indexProp:      "name" + low,
		profileName:    "boost_profile" + high,
		policyName:     "boost_policy" + low,
		policyLabel:    "PolicyLabel" + high,
		predicate:      "mutationCount > 3 OR name = '" + high + "'",
	}
	require.NoError(t, schema.AddUniqueConstraint(identities.constraintName, identities.label, identities.constraintProp))
	require.NoError(t, schema.AddPropertyIndex(identities.indexName, identities.indexLabel, []string{identities.indexProp}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       identities.profileName,
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 1.5,
		ScoreFloor: 0.20,
		ScoreCap:   1.0,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         identities.policyName,
		TargetLabels: []string{identities.policyLabel},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{
			Predicate:  identities.predicate,
			ProfileRef: identities.profileName,
			Order:      1,
		}},
	}))
	return identities
}

// adminImportExportSchema writes one Neo4j CSV export directory for engine.
func adminImportExportSchema(t *testing.T, engine storage.Engine) string {
	t.Helper()
	outDir := filepath.Join(t.TempDir(), "neo4j-csv")
	require.NoError(t, ExportNeo4jCSV(engine, Neo4jCSVExportOptions{OutputDir: outDir}))
	return outDir
}

// adminImportImportSchema imports outDir's node sources with the given schema
// document through the public ImportFull consumer.
func adminImportImportSchema(t *testing.T, outDir, schemaFile, database string) storage.Engine {
	t.Helper()
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, err = ImportFull(context.Background(), base, Options{
		DatabaseName: database,
		NodeSources:  nodeSources,
		RelSources:   relSources,
		SchemaFile:   schemaFile,
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	return storage.NewNamespacedEngine(base, database)
}

func adminImportRequireEscapedSchema(t *testing.T, schema *storage.SchemaManager, identities adminImportSchemaIdentities) {
	t.Helper()
	constraints := schema.GetAllConstraints()
	require.Len(t, constraints, 1)
	require.Equal(t, identities.constraintName, constraints[0].Name)
	require.Equal(t, identities.label, constraints[0].Label)
	require.Equal(t, []string{identities.constraintProp}, constraints[0].Properties)

	foundIndex := false
	for _, index := range schema.GetIndexes() {
		listed, ok := index.(map[string]interface{})
		if !ok || listed["name"] != identities.indexName {
			continue
		}
		foundIndex = true
		require.Equal(t, identities.indexLabel, listed["label"])
		require.Equal(t, []string{identities.indexProp}, listed["properties"])
	}
	require.True(t, foundIndex, "property index %q must keep its exact identity", identities.indexName)

	policies := schema.ShowPromotionPolicies()
	require.Len(t, policies, 1)
	require.Equal(t, identities.policyName, policies[0].Name)
	require.Equal(t, []string{identities.policyLabel}, policies[0].TargetLabels)
	require.Len(t, policies[0].WhenClauses, 1)
	require.Equal(t, identities.predicate, policies[0].WhenClauses[0].Predicate)
	profiles := schema.ShowPromotionProfiles()
	require.Len(t, profiles, 1)
	require.Equal(t, identities.profileName, profiles[0].Name)
}

// TestNeo4jCSVSchemaDefinitionPreservesEscapedSchemaIdentities pins the
// shipped export -> schema document -> import contract for already-approved
// exact UTF16 identities: a surrogate-bearing constraint name, label, property,
// index identity and promotion profile/policy identity must survive the
// standard-JSON export document and the public import consumer unchanged.
func TestNeo4jCSVSchemaDefinitionPreservesEscapedSchemaIdentities(t *testing.T) {
	high := adminImportEscapedUnit(t, "d800")
	low := adminImportEscapedUnit(t, "dc00")

	source := storage.NewNamespacedEngine(
		fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		"source",
	)
	adminImportSeedNode(t, source)
	identities := adminImportSeedEscapedSchema(t, source.GetSchema(), high, low)

	outDir := adminImportExportSchema(t, source)
	schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
	raw, err := os.ReadFile(schemaFile)
	require.NoError(t, err)
	require.True(t, json.Valid(raw), "exported schema document must stay standard JSON")
	require.True(t, utf8.Valid(raw), "exported schema document must stay valid UTF8")
	require.Contains(t, string(raw), `\ud800`, "escaped lone unit must stay a standard JSON escape")
	require.NotContains(t, string(raw), "\ufffd", "escaped lone unit must not be normalized on export")
	require.NotContains(t, string(raw), `\ufffd`, "escaped lone unit must not be normalized on export")

	dest := adminImportImportSchema(t, outDir, schemaFile, "dest")
	require.Equal(t, schemaDefinitionForTest(t, source.GetSchema()), schemaDefinitionForTest(t, dest.GetSchema()))
	adminImportRequireEscapedSchema(t, dest.GetSchema(), identities)
}

// TestNeo4jCSVSchemaDefinitionKeepsOrdinaryBytesAndAcceptsLegacyDocument pins
// the compatibility halves of the same contract: a schema population with no
// escaped lone units still produces the previous standard encoding/json bytes,
// indentation, trailing newline and file permissions, and a legacy document
// that already carries a replacement identity still imports unchanged.
func TestNeo4jCSVSchemaDefinitionKeepsOrdinaryBytesAndAcceptsLegacyDocument(t *testing.T) {
	source := storage.NewNamespacedEngine(
		fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)),
		"source",
	)
	adminImportSeedNode(t, source)
	schema := source.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("person_email_unique", "Café", "email"))
	require.NoError(t, schema.AddPropertyIndex("person_name_idx", "Café", []string{"name"}))
	require.NoError(t, schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name:       "boost_profile",
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 1.5,
		ScoreFloor: 0.20,
		ScoreCap:   1.0,
		Enabled:    true,
	}))
	require.NoError(t, schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         "boost_policy",
		TargetLabels: []string{"Café"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{
			Predicate:  "n.age >= 30",
			ProfileRef: "boost_profile",
			Order:      1,
		}},
	}))

	outDir := adminImportExportSchema(t, source)
	schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
	raw, err := os.ReadFile(schemaFile)
	require.NoError(t, err)
	require.True(t, json.Valid(raw))
	require.True(t, utf8.Valid(raw))

	def := schemaDefinitionForTest(t, schema)
	want, err := json.MarshalIndent(def, "", "  ")
	require.NoError(t, err)
	want = append(want, '\n')
	require.Equal(t, string(want), string(raw), "ordinary schema documents must keep the previous bytes")
	info, err := os.Stat(schemaFile)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	dest := adminImportImportSchema(t, outDir, schemaFile, "dest")
	require.Equal(t, def, schemaDefinitionForTest(t, dest.GetSchema()))

	// A legacy document produced for identities an older exporter normalized
	// keeps that stored replacement identity instead of failing the import.
	legacy := storage.SchemaDefinition{Constraints: []storage.Constraint{{
		Name:       "legacy_email_unique",
		Label:      "Legacy\ufffd",
		Type:       storage.ConstraintUnique,
		Properties: []string{"email"},
	}}}
	legacyRaw, err := json.MarshalIndent(legacy, "", "  ")
	require.NoError(t, err)
	legacyRaw = append(legacyRaw, '\n')
	legacyFile := filepath.Join(t.TempDir(), "legacy.nornic.json")
	require.NoError(t, os.WriteFile(legacyFile, legacyRaw, 0o600))
	legacyDest := adminImportImportSchema(t, outDir, legacyFile, "legacy")
	legacyConstraints := legacyDest.GetSchema().GetAllConstraints()
	require.Len(t, legacyConstraints, 1)
	require.Equal(t, "Legacy\ufffd", legacyConstraints[0].Label)
}

// TestNeo4jCSVSchemaDefinitionPreservesEscapedIdentitiesAfterBadgerReopen
// keeps the same exact identities through a real persistent consumer: the
// schema names survive a durable Badger reopen, the reopened consumer's export
// document stays standard JSON, and the public import consumer restores them.
//
// The reopen assertion is owned by the persistent source consumer; the import
// destination below is an in-memory engine, which publishes the document
// through the same accepted definition replacement the native Badger
// destination persists (schema_badger_durability_test.go).
func TestNeo4jCSVSchemaDefinitionPreservesEscapedIdentitiesAfterBadgerReopen(t *testing.T) {
	high := adminImportEscapedUnit(t, "d800")
	low := adminImportEscapedUnit(t, "dc00")

	sourceDir := t.TempDir()
	sourceEngine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(sourceDir))
	require.NoError(t, err)
	source := storage.NewNamespacedEngine(sourceEngine, "source")
	adminImportSeedNode(t, source)
	identities := adminImportSeedEscapedSchema(t, source.GetSchema(), high, low)
	require.NoError(t, sourceEngine.Close())

	reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(sourceDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	stored := reopened.GetSchemaForNamespace("source")
	adminImportRequireEscapedSchema(t, stored, identities)

	outDir := adminImportExportSchema(t, storage.NewNamespacedEngine(reopened, "source"))
	schemaFile := filepath.Join(outDir, Neo4jCSVNornicSchemaFileName)
	raw, err := os.ReadFile(schemaFile)
	require.NoError(t, err)
	require.True(t, json.Valid(raw), "exported schema document must stay standard JSON")
	require.True(t, utf8.Valid(raw), "exported schema document must stay valid UTF8")
	require.Contains(t, string(raw), `\ud800`, "escaped lone unit must stay a standard JSON escape")
	require.NotContains(t, string(raw), "\ufffd", "escaped lone unit must not be normalized on export")

	dest := adminImportImportSchema(t, outDir, schemaFile, "dest")
	require.Equal(t, schemaDefinitionForTest(t, stored), schemaDefinitionForTest(t, dest.GetSchema()))
	adminImportRequireEscapedSchema(t, dest.GetSchema(), identities)
}
