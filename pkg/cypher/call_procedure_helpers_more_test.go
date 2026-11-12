package cypher

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
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

	indexes, err := exec.callDbIndexes(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "state"}, indexes.Columns)

	constraints, err := exec.callDbConstraints(ctx)
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
	// nornicdb.stats reads the same label population, so the call fails with
	// that population error instead of reporting zero labels.
	_, err = execNodesErr.callNornicDbStats(context.Background())
	require.ErrorIs(t, err, nodesErr)

	execEdgesErr := NewStorageExecutor(&allEdgesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: edgesErr})
	_, err = execEdgesErr.callDbRelationshipTypes(context.Background())
	require.ErrorIs(t, err, edgesErr)
	// The relationship type population fails the same statistics call.
	_, err = execEdgesErr.callNornicDbStats(context.Background())
	require.ErrorIs(t, err, edgesErr)

	execNilSchema := NewStorageExecutor(&callNilSchemaEngine{MemoryEngine: newTestMemoryEngine(t)})
	res, err := execNilSchema.callDbConstraints(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"name", "type", "labelsOrTypes", "properties", "propertyType"}, res.Columns)
	require.Empty(t, res.Rows)
}

// TestCallDbPropertyKeysReportsScanFailure is the public regression for the two
// population scans db.propertyKeys used to discard: each scan's own failure is
// the call's failure, instead of the call answering from the half of the graph
// the other scan returned.
func TestCallDbPropertyKeysReportsScanFailure(t *testing.T) {
	nodesErr := errors.New("all nodes failed")
	edgesErr := errors.New("all edges failed")
	for _, testCase := range []struct {
		name  string
		inner storage.Engine
		cause error
	}{
		{"node population", &allNodesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: nodesErr}, nodesErr},
		{"edge population", &allEdgesErrEngine{MemoryEngine: newTestMemoryEngine(t), err: edgesErr}, edgesErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(testCase.inner, "property_keys_failure"))
			_, err := exec.Execute(t.Context(), "CALL db.propertyKeys()", nil)
			require.ErrorContains(t, err, testCase.cause.Error(), "the scan's own error reaches the caller")
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code)
		})
	}

	// Control: with both populations readable, the same call lists the keys of
	// both of them.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "property_keys_populations"))
	_, err := exec.Execute(t.Context(), "CREATE (:Keyed {fromNode: 1})-[:HAS]->(:Keyed)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(t.Context(), "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"fromNode"}}, result.Rows)
}
