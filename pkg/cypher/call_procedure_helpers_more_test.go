package cypher

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type allNodesErrEngine struct {
	*storage.MemoryEngine
	err error
}

func (e *allNodesErrEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	return fixtureowner.ReadWithFailure(ctx, e.err, func(ctx context.Context) ([]*storage.Node, error) {
		return e.MemoryEngine.AllNodes(ctx)
	})
}

type allEdgesErrEngine struct {
	*storage.MemoryEngine
	err error
}

func (e *allEdgesErrEngine) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.err, func(ctx context.Context) ([]*storage.Edge, error) {
		return e.MemoryEngine.AllEdges(ctx)
	})
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

	labels, err := exec.callDbLabels(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"label"}, labels.Columns)
	require.NotEmpty(t, labels.Rows)

	relTypes, err := exec.callDbRelationshipTypes(ctx)
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

	stats, err := exec.callNornicDbStats(ctx)
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

	execNodesErr := NewStorageExecutor(&allNodesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: nodesErr})
	_, err := execNodesErr.callDbLabels(context.Background())
	require.ErrorIs(t, err, nodesErr)
	require.Equal(t, 0, execNodesErr.countLabels(context.Background()))

	execEdgesErr := NewStorageExecutor(&allEdgesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: edgesErr})
	_, err = execEdgesErr.callDbRelationshipTypes(context.Background())
	require.ErrorIs(t, err, edgesErr)
	require.Equal(t, 0, execEdgesErr.countRelTypes(context.Background()))

	execNilSchema := NewStorageExecutor(&callNilSchemaEngine{MemoryEngine: newTestMemoryEngine(t)})
	res, err := execNilSchema.callDbConstraints()
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "propertyType"}, res.Columns)
	require.Empty(t, res.Rows)
}
