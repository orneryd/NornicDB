package adminimport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
