package adminimport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestImporterFullLoadsNeo4jCSVWithVectorsEmbeddingsAndRelationships(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	relsPath := filepath.Join(dir, "rels.csv")
	schemaPath := filepath.Join(dir, "schema.cypher")

	require.NoError(t, os.WriteFile(nodesPath, []byte(`id:ID(User),name:string,age:int,active:boolean,tags:string[],"embedding:vector{coordinateType:float,dimensions:3}",:EMBEDDING(default),:LABEL,:IGNORE
u1,Alice,34,true,"agent;graph","0.1;0.2;0.3","0.4;0.5;0.6",Person;User,ignored
u2,Bob,29,false,"graph","0.7;0.8;0.9","1.0;1.1;1.2",Person,ignored
`), 0o600))
	require.NoError(t, os.WriteFile(relsPath, []byte(`:START_ID(User),:END_ID(User),:TYPE,since:int
u1,u2,KNOWS,2024
`), 0o600))
	require.NoError(t, os.WriteFile(schemaPath, []byte(`CREATE CONSTRAINT person_name_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS UNIQUE;
`), 0o600))

	base := storage.NewMemoryEngine()
	report, err := ImportFull(context.Background(), base, Options{
		DatabaseName: "mydb",
		NodeSources:  []string{"Imported=" + nodesPath},
		RelSources:   []string{relsPath},
		SchemaFile:   schemaPath,
		DataDir:      dir,
		BuildIndexes: true,
		ChunkSize:    2,
		Now:          fixedImportTime,
		ReportFile:   filepath.Join(dir, "report.json"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), report.NodesImported)
	require.Equal(t, int64(1), report.RelationshipsImported)

	engine := storage.NewNamespacedEngine(base, "mydb")
	alice, err := engine.GetNode("u1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Imported", "Person", "User"}, alice.Labels)
	require.Equal(t, "Alice", alice.Properties["name"])
	require.Equal(t, int64(34), alice.Properties["age"])
	require.Equal(t, true, alice.Properties["active"])
	require.Equal(t, []any{"agent", "graph"}, alice.Properties["tags"])
	require.Equal(t, []float32{0.1, 0.2, 0.3}, alice.Properties["embedding"])
	require.Equal(t, []float32{0.4, 0.5, 0.6}, alice.NamedEmbeddings["default"])
	require.Equal(t, "u1", alice.Properties["id"])

	edges, err := engine.GetOutgoingEdges("u1")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, storage.NodeID("u1"), edges[0].StartNode)
	require.Equal(t, storage.NodeID("u2"), edges[0].EndNode)
	require.Equal(t, "KNOWS", edges[0].Type)
	require.Equal(t, int64(2024), edges[0].Properties["since"])
	require.True(t, report.IndexesBuilt)

	constraints := engine.GetSchema().GetConstraintsForLabels([]string{"Person"})
	require.Len(t, constraints, 1)
	require.Equal(t, "person_name_unique", constraints[0].Name)
	require.Equal(t, storage.ConstraintUnique, constraints[0].Type)
	require.Equal(t, []string{"name"}, constraints[0].Properties)

	_, err = os.Stat(filepath.Join(dir, "report.json"))
	require.NoError(t, err)
}

func TestImporterSkipsDuplicateNodesWhenRequested(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(nodesPath, []byte(`:ID,name
n1,Alice
n1,Bob
`), 0o600))

	base := storage.NewMemoryEngine()
	report, err := ImportFull(context.Background(), base, Options{
		DatabaseName:       "mydb",
		NodeSources:        []string{nodesPath},
		BuildIndexes:       false,
		SkipDuplicateNodes: true,
		Now:                fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), report.NodesImported)
	require.Equal(t, int64(1), report.DuplicateNodesSkipped)

	engine := storage.NewNamespacedEngine(base, "mydb")
	node, err := engine.GetNode("n1")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, "Alice", node.Properties["name"])
}

func TestDiscoverNeo4jCSVSourcesFromDirectory(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	relsPath := filepath.Join(dir, "relationships.csv")
	require.NoError(t, os.WriteFile(nodesPath, []byte(`:ID,name,:LABEL
n1,Alice,Person
`), 0o600))
	require.NoError(t, os.WriteFile(relsPath, []byte(`:START_ID,:END_ID,:TYPE,since:int
n1,n2,KNOWS,2024
`), 0o600))

	nodeSources, relSources, err := DiscoverNeo4jCSVSources(dir, Options{})
	require.NoError(t, err)
	require.Equal(t, []string{nodesPath}, nodeSources)
	require.Equal(t, []string{relsPath}, relSources)
}

func TestNeo4jCSVExportRoundTripsThroughImporter(t *testing.T) {
	base := storage.NewMemoryEngine()
	source := storage.NewNamespacedEngine(base, "source")
	_, err := source.CreateNode(&storage.Node{
		ID:     "u1",
		Labels: []string{"Person", "User"},
		Properties: map[string]any{
			"name": "Alice",
			"age":  int64(34),
			"tags": []any{"agent", "graph"},
			"vec":  []float32{0.1, 0.2, 0.3},
		},
		NamedEmbeddings: map[string][]float32{"default": {0.4, 0.5, 0.6}},
	})
	require.NoError(t, err)
	_, err = source.CreateNode(&storage.Node{
		ID:     "u2",
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Bob",
			"age":  int64(29),
		},
	})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(&storage.Edge{
		ID:        "r1",
		StartNode: "u1",
		EndNode:   "u2",
		Type:      "KNOWS",
		Properties: map[string]any{
			"since": int64(2024),
		},
	}))
	require.NoError(t, source.GetSchema().AddUniqueConstraint("person_name_unique", "Person", "name"))
	require.NoError(t, source.GetSchema().AddPropertyIndex("person_age_idx", "Person", []string{"age"}))

	outDir := filepath.Join(t.TempDir(), "neo4j-csv")
	require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: outDir}))
	schemaBytes, err := os.ReadFile(filepath.Join(outDir, Neo4jCSVSchemaFileName))
	require.NoError(t, err)
	require.Contains(t, string(schemaBytes), "CREATE CONSTRAINT `person_name_unique` IF NOT EXISTS")
	require.True(t, strings.Contains(string(schemaBytes), "CREATE INDEX `person_age_idx` IF NOT EXISTS") || strings.Contains(string(schemaBytes), "CREATE INDEX person_age_idx IF NOT EXISTS"))
	_, err = os.Stat(filepath.Join(outDir, Neo4jCSVNornicSchemaFileName))
	require.NoError(t, err)

	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)

	destBase := storage.NewMemoryEngine()
	report, err := ImportFull(context.Background(), destBase, Options{
		DatabaseName: "dest",
		NodeSources:  nodeSources,
		RelSources:   relSources,
		SchemaFile:   filepath.Join(outDir, Neo4jCSVSchemaFileName),
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), report.NodesImported)
	require.Equal(t, int64(1), report.RelationshipsImported)

	dest := storage.NewNamespacedEngine(destBase, "dest")
	alice, err := dest.GetNode("u1")
	require.NoError(t, err)
	require.Equal(t, "Alice", alice.Properties["name"])
	require.Equal(t, int64(34), alice.Properties["age"])
	require.Equal(t, []any{"agent", "graph"}, alice.Properties["tags"])
	require.Equal(t, []float32{0.1, 0.2, 0.3}, alice.Properties["vec"])
	require.Equal(t, []float32{0.4, 0.5, 0.6}, alice.NamedEmbeddings["default"])
	require.ElementsMatch(t, []string{"Person", "User"}, alice.Labels)

	edges, err := dest.GetOutgoingEdges("u1")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, "KNOWS", edges[0].Type)
	require.Equal(t, int64(2024), edges[0].Properties["since"])
	constraints := dest.GetSchema().GetConstraintsForLabels([]string{"Person"})
	require.Len(t, constraints, 1)
	require.Equal(t, "person_name_unique", constraints[0].Name)
	indexes := dest.GetSchema().GetIndexes()
	require.NotEmpty(t, indexes)
}

func TestNeo4jCSVExportRoundTripsFullSchemaDefinition(t *testing.T) {
	base := storage.NewMemoryEngine()
	source := storage.NewNamespacedEngine(base, "source")
	_, err := source.CreateNode(&storage.Node{
		ID:     "u1",
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name":      "Alice",
			"age":       int64(34),
			"email":     "alice@example.com",
			"country":   "US",
			"city":      "NYC",
			"status":    "active",
			"embedding": []float32{0.1, 0.2, 0.3},
		},
	})
	require.NoError(t, err)
	_, err = source.CreateNode(&storage.Node{
		ID:     "u2",
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name":    "Bob",
			"age":     int64(29),
			"email":   "bob@example.com",
			"country": "US",
			"city":    "LA",
			"status":  "active",
		},
	})
	require.NoError(t, err)
	require.NoError(t, source.CreateEdge(&storage.Edge{
		ID:        "r1",
		StartNode: "u1",
		EndNode:   "u2",
		Type:      "KNOWS",
		Properties: map[string]any{
			"note":  "met at graph conf",
			"since": int64(2024),
		},
	}))

	schema := source.GetSchema()
	require.NoError(t, schema.AddUniqueConstraint("person_email_unique", "Person", "email"))
	require.NoError(t, schema.AddPropertyTypeConstraint("person_age_type", "Person", "age", storage.PropertyTypeInteger))
	require.NoError(t, schema.AddPropertyIndex("person_name_idx", "Person", []string{"name"}))
	require.NoError(t, schema.AddCompositeIndex("person_location_idx", "Person", []string{"country", "city"}))
	require.NoError(t, schema.AddFulltextIndex("person_search_idx", []string{"Person"}, []string{"name"}))
	require.NoError(t, schema.AddFulltextRelationshipIndex("knows_note_idx", []string{"KNOWS"}, []string{"note"}))
	require.NoError(t, schema.AddVectorIndex("person_embedding_idx", "Person", "embedding", 3, "cosine"))
	require.NoError(t, schema.AddRangeIndex("person_age_range", "Person", "age"))
	require.NoError(t, schema.AddConstraintContractBundle(storage.ConstraintContract{
		Name:              "person_name_contract",
		TargetEntityType:  string(storage.ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Definition:        "CREATE CONSTRAINT CONTRACT person_name_contract FOR (n:Person) REQUIRE n.name IS NOT NULL",
		Entries: []storage.ConstraintContractEntry{{
			Kind:       storage.ConstraintContractKindBooleanNode,
			Expression: "n.status IN ['active']",
		}},
	}, nil, nil, false))
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "person_decay_profile",
		HalfLifeSeconds:     86400,
		VisibilityThreshold: 0.25,
		ScoreFloor:          0.10,
		Function:            knowledgepolicy.DecayFunctionExponential,
		Scope:               knowledgepolicy.ScopeNode,
		DecayEnabled:        true,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	}))
	threshold := 0.25
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:                "person_decay_binding",
		TargetLabels:        []string{"Person"},
		ProfileRef:          "person_decay_profile",
		VisibilityThreshold: &threshold,
		Order:               1,
	}))
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
			Predicate:  "n.age >= 30",
			ProfileRef: "person_boost_profile",
			Order:      1,
		}},
	}))

	outDir := filepath.Join(t.TempDir(), "neo4j-csv-full-schema")
	require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: outDir}))
	_, err = os.Stat(filepath.Join(outDir, Neo4jCSVSchemaFileName))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(outDir, Neo4jCSVNornicSchemaFileName))
	require.NoError(t, err)

	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)

	destBase := storage.NewMemoryEngine()
	_, err = ImportFull(context.Background(), destBase, Options{
		DatabaseName: "dest",
		NodeSources:  nodeSources,
		RelSources:   relSources,
		SchemaFile:   filepath.Join(outDir, Neo4jCSVNornicSchemaFileName),
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)

	dest := storage.NewNamespacedEngine(destBase, "dest")
	require.Equal(t, source.GetSchema().ExportDefinition(), dest.GetSchema().ExportDefinition())
}

func TestImporterReportsDuplicateIDs(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(nodesPath, []byte(`:ID,name
n1,Alice
n1,Bob
`), 0o600))

	_, err := ImportFull(context.Background(), storage.NewMemoryEngine(), Options{
		DatabaseName: "mydb",
		NodeSources:  []string{nodesPath},
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.Error(t, err)
	var importErr *Error
	require.ErrorAs(t, err, &importErr)
	require.Equal(t, ExitDuplicateID, importErr.ExitCode)
}

func TestImporterCompositeIDRelationships(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	relsPath := filepath.Join(dir, "rels.csv")
	require.NoError(t, os.WriteFile(nodesPath, []byte(`tenant:ID(Scope),local:ID(Scope),name
a,1,Alice
a,2,Bob
`), 0o600))
	require.NoError(t, os.WriteFile(relsPath, []byte(`:START_ID(Scope),:START_ID(Scope),:END_ID(Scope),:END_ID(Scope),:TYPE
a,1,a,2,KNOWS
`), 0o600))

	base := storage.NewMemoryEngine()
	_, err := ImportFull(context.Background(), base, Options{
		DatabaseName: "mydb",
		NodeSources:  []string{nodesPath},
		RelSources:   []string{relsPath},
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)

	engine := storage.NewNamespacedEngine(base, "mydb")
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, storage.NodeID("a|1"), edges[0].StartNode)
	require.Equal(t, storage.NodeID("a|2"), edges[0].EndNode)
}

func TestImporterBadRelationshipReferenceHonorsSkipFlag(t *testing.T) {
	dir := t.TempDir()
	nodesPath := filepath.Join(dir, "nodes.csv")
	relsPath := filepath.Join(dir, "rels.csv")
	require.NoError(t, os.WriteFile(nodesPath, []byte(`:ID,name
n1,Alice
`), 0o600))
	require.NoError(t, os.WriteFile(relsPath, []byte(`:START_ID,:END_ID,:TYPE
n1,missing,KNOWS
`), 0o600))

	_, err := ImportFull(context.Background(), storage.NewMemoryEngine(), Options{
		DatabaseName:         "mydb",
		NodeSources:          []string{nodesPath},
		RelSources:           []string{relsPath},
		BuildIndexes:         false,
		Now:                  fixedImportTime,
		SkipBadRelationships: false,
	})
	require.Error(t, err)
	var importErr *Error
	require.ErrorAs(t, err, &importErr)
	require.Equal(t, ExitBadRelationship, importErr.ExitCode)

	report, err := ImportFull(context.Background(), storage.NewMemoryEngine(), Options{
		DatabaseName:         "mydb",
		NodeSources:          []string{nodesPath},
		RelSources:           []string{relsPath},
		BuildIndexes:         false,
		Now:                  fixedImportTime,
		SkipBadRelationships: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), report.BadRelationships)
	require.Equal(t, int64(0), report.RelationshipsImported)
}

func TestImporterScenario_MissingSourceFolderFails(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	missingPath := filepath.Join(t.TempDir(), "missing", "nodes.csv")
	_, err := ImportFull(context.Background(), base, Options{
		DatabaseName: "missingdb",
		NodeSources:  []string{missingPath},
		BuildIndexes: false,
		Now:          fixedImportTime,
	})
	require.Error(t, err)

	var importErr *Error
	require.ErrorAs(t, err, &importErr)
	require.Equal(t, ExitCSV, importErr.ExitCode)
	require.ErrorIs(t, importErr.Err, os.ErrNotExist)
}

func TestImporterScenario_TargetingAndExistingDataIsolation(t *testing.T) {
	dir := t.TempDir()

	alphaNodesHeader := filepath.Join(dir, "alpha_nodes_header.csv")
	alphaNodesRows := filepath.Join(dir, "alpha_nodes_rows.csv")
	alphaRels := filepath.Join(dir, "alpha_rels.csv")
	require.NoError(t, os.WriteFile(alphaNodesHeader, []byte(`id:ID(User),name:string,role:string,:LABEL
`), 0o600))
	require.NoError(t, os.WriteFile(alphaNodesRows, []byte(`u1,Alice,admin,Person;User
u2,Bob,editor,Person
`), 0o600))
	require.NoError(t, os.WriteFile(alphaRels, []byte(`:START_ID(User),:END_ID(User),:TYPE,since:int
u1,u2,KNOWS,2024
`), 0o600))

	betaNodesHeader := filepath.Join(dir, "beta_nodes_header.csv")
	betaNodesRows := filepath.Join(dir, "beta_nodes_rows.csv")
	betaRels := filepath.Join(dir, "beta_rels.csv")
	require.NoError(t, os.WriteFile(betaNodesHeader, []byte(`id:ID(User),name:string,role:string,:LABEL
`), 0o600))
	require.NoError(t, os.WriteFile(betaNodesRows, []byte(`u1,Carol,owner,Person;User
u2,Dan,reader,Person
`), 0o600))
	require.NoError(t, os.WriteFile(betaRels, []byte(`:START_ID(User),:END_ID(User),:TYPE,since:int
u1,u2,WORKS_WITH,2025
`), 0o600))

	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	alphaReport, err := ImportFull(context.Background(), base, Options{
		DatabaseName: "alpha",
		NodeSources:  []string{"Imported=" + alphaNodesHeader + "," + alphaNodesRows},
		RelSources:   []string{alphaRels},
		BuildIndexes: false,
		ChunkSize:    1,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), alphaReport.NodesImported)
	require.Equal(t, int64(1), alphaReport.RelationshipsImported)

	alpha := storage.NewNamespacedEngine(base, "alpha")
	alphaAlice, err := alpha.GetNode("u1")
	require.NoError(t, err)
	require.Equal(t, "Alice", alphaAlice.Properties["name"])
	require.Equal(t, "admin", alphaAlice.Properties["role"])
	require.ElementsMatch(t, []string{"Imported", "Person", "User"}, alphaAlice.Labels)

	alphaEdges, err := alpha.GetOutgoingEdges("u1")
	require.NoError(t, err)
	require.Len(t, alphaEdges, 1)
	require.Equal(t, storage.NodeID("u1"), alphaEdges[0].StartNode)
	require.Equal(t, storage.NodeID("u2"), alphaEdges[0].EndNode)
	require.Equal(t, "KNOWS", alphaEdges[0].Type)
	require.Equal(t, int64(2024), alphaEdges[0].Properties["since"])

	_, err = ImportFull(context.Background(), base, Options{
		DatabaseName: "alpha",
		NodeSources:  []string{"Imported=" + alphaNodesHeader + "," + alphaNodesRows},
		RelSources:   []string{alphaRels},
		BuildIndexes: false,
		ChunkSize:    1,
		Now:          fixedImportTime,
	})
	require.Error(t, err)
	var targetErr *Error
	require.ErrorAs(t, err, &targetErr)
	require.Equal(t, ExitUnsupported, targetErr.ExitCode)
	require.Contains(t, targetErr.Message, "empty target database")

	betaReport, err := ImportFull(context.Background(), base, Options{
		DatabaseName: "beta",
		NodeSources:  []string{"Imported=" + betaNodesHeader + "," + betaNodesRows},
		RelSources:   []string{betaRels},
		BuildIndexes: false,
		ChunkSize:    1,
		Now:          fixedImportTime,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), betaReport.NodesImported)
	require.Equal(t, int64(1), betaReport.RelationshipsImported)

	beta := storage.NewNamespacedEngine(base, "beta")
	betaCarol, err := beta.GetNode("u1")
	require.NoError(t, err)
	require.Equal(t, "Carol", betaCarol.Properties["name"])
	require.Equal(t, "owner", betaCarol.Properties["role"])
	require.ElementsMatch(t, []string{"Imported", "Person", "User"}, betaCarol.Labels)

	betaEdges, err := beta.GetOutgoingEdges("u1")
	require.NoError(t, err)
	require.Len(t, betaEdges, 1)
	require.Equal(t, storage.NodeID("u1"), betaEdges[0].StartNode)
	require.Equal(t, storage.NodeID("u2"), betaEdges[0].EndNode)
	require.Equal(t, "WORKS_WITH", betaEdges[0].Type)
	require.Equal(t, int64(2025), betaEdges[0].Properties["since"])

	alphaAlice, err = alpha.GetNode("u1")
	require.NoError(t, err)
	require.Equal(t, "Alice", alphaAlice.Properties["name"])
	require.Equal(t, "admin", alphaAlice.Properties["role"])

	alphaCount, err := alpha.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), alphaCount)
	betaCount, err := beta.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), betaCount)

	_, err = beta.GetNode("missing")
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrNotFound))
}
