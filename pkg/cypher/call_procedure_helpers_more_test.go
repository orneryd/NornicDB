package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type allNodesErrEngine struct {
	*storage.MemoryEngine
	err error
}

func (e *allNodesErrEngine) AllNodes() ([]*storage.Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.MemoryEngine.AllNodes()
}

type allEdgesErrEngine struct {
	*storage.MemoryEngine
	err error
}

func (e *allEdgesErrEngine) AllEdges() ([]*storage.Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.MemoryEngine.AllEdges()
}

type callNilSchemaEngine struct {
	*storage.MemoryEngine
}

func (e *callNilSchemaEngine) GetSchema() *storage.SchemaManager {
	return nil
}

func TestCallDbHelpers_ProceduresAndCounts(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "call_db_helpers_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {id:'p1'})-[:KNOWS]->(:Person {id:'p2'})", nil)
	require.NoError(t, err)
	// An index and a constraint on one property are exclusive, as in Neo4j (#884).
	_, err = exec.Execute(ctx, "CREATE INDEX idx_person_name_cov IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT c_person_id_cov IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)

	labels, err := exec.callDbLabels()
	require.NoError(t, err)
	require.Equal(t, []string{"label"}, labels.Columns)
	require.NotEmpty(t, labels.Rows)

	relTypes, err := exec.callDbRelationshipTypes()
	require.NoError(t, err)
	require.Equal(t, []string{"relationshipType"}, relTypes.Columns)
	require.NotEmpty(t, relTypes.Rows)

	indexes, err := exec.callDbIndexes()
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "state"}, indexes.Columns)

	constraints, err := exec.callDbConstraints()
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "propertyType"}, constraints.Columns)
	require.NotEmpty(t, constraints.Rows)

	components, err := exec.callDbmsComponents()
	require.NoError(t, err)
	require.Equal(t, []string{"name", "versions", "edition"}, components.Columns)
	require.Len(t, components.Rows, 1)

	version, err := exec.callNornicDbVersion()
	require.NoError(t, err)
	require.Equal(t, []string{"version", "build", "edition"}, version.Columns)
	require.Len(t, version.Rows, 1)

	stats, err := exec.callNornicDbStats()
	require.NoError(t, err)
	require.Equal(t, []string{"nodes", "relationships", "labels", "relationshipTypes"}, stats.Columns)
	require.Len(t, stats.Rows, 1)

	decay, err := exec.callNornicDbDecayInfo()
	require.NoError(t, err)
	require.Equal(t, []string{"enabled", "system", "configuredVia"}, decay.Columns)
	require.Len(t, decay.Rows, 1)
}

func TestCallDbHelpers_ErrorAndNilSchemaBranches(t *testing.T) {
	nodesErr := errors.New("all nodes failed")
	edgesErr := errors.New("all edges failed")

	execNodesErr := NewStorageExecutor(&allNodesErrEngine{MemoryEngine: storage.NewMemoryEngine(), err: nodesErr})
	_, err := execNodesErr.callDbLabels()
	require.ErrorIs(t, err, nodesErr)
	require.Equal(t, 0, execNodesErr.countLabels())

	execEdgesErr := NewStorageExecutor(&allEdgesErrEngine{MemoryEngine: storage.NewMemoryEngine(), err: edgesErr})
	_, err = execEdgesErr.callDbRelationshipTypes()
	require.ErrorIs(t, err, edgesErr)
	require.Equal(t, 0, execEdgesErr.countRelTypes())

	execNilSchema := NewStorageExecutor(&callNilSchemaEngine{MemoryEngine: storage.NewMemoryEngine()})
	res, err := execNilSchema.callDbConstraints()
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "propertyType"}, res.Columns)
	require.Empty(t, res.Rows)
}
