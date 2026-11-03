package adminimport_test

// Public-contract reproduction for JSON schema application after cancellation.
//
// ImportFull checks ctx at the top of each data-row iteration, but the final
// partial chunk is written after the reader reports EOF. A JSON SchemaFile is
// then handed to the ctx-less applySchemaDefinition, so cancellation that
// happens immediately after the final accepted data write can still publish a
// replacement schema and let ImportFull report success.
//
// The fixture below cancels through the real engine write path, not through a
// fake persister: BulkCreateNodes reaches the underlying Memory/Badger engine
// and the context is canceled only after that write returned successfully.
// The expected public contract is a canceled error and no schema replacement
// publication. Prior accepted data writes are explicitly not rolled back, so
// this is not an all-or-nothing import contract.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const schemaCancellationNamespace = "schemacancel"

const (
	schemaCancellationProbeConstraint    = "person_name_unique"
	schemaCancellationBaselineConstraint = "baseline_person_id_unique"
)

// cancelAfterFinalDataWriteEngine is a real storage.Engine delegate whose only
// additional behavior is to cancel the import context immediately after the
// single successful bulk node write of the fixture. Every write reaches the
// underlying engine; nothing is faked or replayed.
type cancelAfterFinalDataWriteEngine struct {
	storage.Engine
	cancel           context.CancelFunc
	cancelAfterWrite int64
	persisterCalls   *atomic.Int64

	dataWrites        atomic.Int64
	canceledAtWrite   atomic.Int64
	persisterAtCancel atomic.Int64
}

func (e *cancelAfterFinalDataWriteEngine) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	if err := e.Engine.BulkCreateNodes(ctx, nodes); err != nil {
		return err
	}
	write := e.dataWrites.Add(1)
	if write != e.cancelAfterWrite {
		return nil
	}
	if e.persisterCalls != nil {
		e.persisterAtCancel.Store(e.persisterCalls.Load())
	}
	e.canceledAtWrite.Store(write)
	e.cancel()
	return nil
}

// GetSchemaForNamespace keeps the namespace-schema behavior of the wrapped
// engine. ImportFull wraps its engine in NamespacedEngine, which uses this
// optional interface when it is present.
func (e *cancelAfterFinalDataWriteEngine) GetSchemaForNamespace(namespace string) *storage.SchemaManager {
	if provider, ok := e.Engine.(storage.NamespaceSchemaProvider); ok {
		return provider.GetSchemaForNamespace(namespace)
	}
	return e.Engine.GetSchema()
}

// writeSchemaCancellationFixture writes two accepted CSV data rows and a JSON
// schema whose only constraint is a replacement probe. With the default chunk
// size both rows are flushed in one trailing write after EOF, which is where
// cancellation is injected.
func writeSchemaCancellationFixture(t *testing.T) (nodesPath, schemaPath string) {
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
		Constraints: []storage.Constraint{{
			Name:       schemaCancellationProbeConstraint,
			Label:      "Person",
			Type:       storage.ConstraintUnique,
			Properties: []string{"name"},
		}},
	}
	raw, err := json.Marshal(definition)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(schemaPath, raw, 0o600))
	return nodesPath, schemaPath
}

func hasSchemaConstraint(schema *storage.SchemaManager, name string) bool {
	for _, constraint := range schema.GetAllConstraints() {
		if constraint.Name == name {
			return true
		}
	}
	return false
}

func schemaConstraintNames(schema *storage.SchemaManager) []string {
	constraints := schema.GetAllConstraints()
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

// requireCancellationPhase proves the scenario before the contract assertions:
// one real bulk write happened, cancellation was issued immediately after it,
// and the import context is actually canceled.
func requireCancellationPhase(t *testing.T, phase *cancelAfterFinalDataWriteEngine, ctx context.Context) {
	t.Helper()
	require.Equal(t, int64(1), phase.dataWrites.Load(),
		"fixture must flush the accepted rows in exactly one real bulk write")
	require.Equal(t, int64(1), phase.canceledAtWrite.Load(),
		"cancellation must be issued immediately after the final accepted data write")
	require.ErrorIs(t, ctx.Err(), context.Canceled,
		"the import context must be observably canceled when the write returns")
	t.Logf("cancellation phase: bulkWrites=%d canceledAtWrite=%d contextErr=%v",
		phase.dataWrites.Load(), phase.canceledAtWrite.Load(), ctx.Err())
}

// assertAcceptedDataSurvives records that data written before cancellation is
// allowed to remain. The missing schema replacement is the only publication
// this test forbids.
func assertAcceptedDataSurvives(t *testing.T, engine storage.Engine, report adminimport.Report) {
	t.Helper()
	namespaced := storage.NewNamespacedEngine(engine, schemaCancellationNamespace)
	count, err := namespaced.NodeCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), report.NodesImported,
		"both accepted rows must have been counted as imported")
	assert.Equal(t, int64(2), count,
		"accepted data writes need not be rolled back by cancellation")
}

func TestImportFullJSONSchemaCancellationDoesNotPublishSchema(t *testing.T) {
	t.Run("memory live definition", func(t *testing.T) {
		nodesPath, schemaPath := writeSchemaCancellationFixture(t)

		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		schema := base.GetSchemaForNamespace(schemaCancellationNamespace)
		require.NoError(t, schema.AddUniqueConstraint(
			schemaCancellationBaselineConstraint, "Person", "id"))

		var persisterCalls atomic.Int64
		schema.SetPersister(func(*storage.SchemaDefinition) error {
			persisterCalls.Add(1)
			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		phase := &cancelAfterFinalDataWriteEngine{
			Engine:           base,
			cancel:           cancel,
			cancelAfterWrite: 1,
			persisterCalls:   &persisterCalls,
		}

		report, importErr := adminimport.ImportFull(ctx, phase, adminimport.Options{
			DatabaseName: schemaCancellationNamespace,
			NodeSources:  []string{nodesPath},
			SchemaFile:   schemaPath,
		})

		requireCancellationPhase(t, phase, ctx)

		assert.ErrorIs(t, importErr, context.Canceled,
			"ImportFull must surface cancellation that happens after the final accepted data write")

		live := base.GetSchemaForNamespace(schemaCancellationNamespace)
		t.Logf("live constraints after cancellation: %v; schema persister calls at cancel=%d after=%d",
			schemaConstraintNames(live), phase.persisterAtCancel.Load(), persisterCalls.Load())
		assert.False(t, hasSchemaConstraint(live, schemaCancellationProbeConstraint),
			"the JSON schema replacement must not be published after cancellation")
		assert.True(t, hasSchemaConstraint(live, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain published after cancellation")

		assertAcceptedDataSurvives(t, base, report)

		assert.Equal(t, phase.persisterAtCancel.Load(), persisterCalls.Load(),
			"no schema persistence may occur after the cancellation phase")
	})

	t.Run("badger live definition and native reopen", func(t *testing.T) {
		nodesPath, schemaPath := writeSchemaCancellationFixture(t)
		dir := filepath.Join(t.TempDir(), "graph")

		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
			storage.NewBadgerEngine(dir))
		require.NoError(t, err)

		schema := base.GetSchemaForNamespace(schemaCancellationNamespace)
		require.NoError(t, schema.AddUniqueConstraint(
			schemaCancellationBaselineConstraint, "Person", "id"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		phase := &cancelAfterFinalDataWriteEngine{
			Engine:           base,
			cancel:           cancel,
			cancelAfterWrite: 1,
		}

		report, importErr := adminimport.ImportFull(ctx, phase, adminimport.Options{
			DatabaseName: schemaCancellationNamespace,
			NodeSources:  []string{nodesPath},
			SchemaFile:   schemaPath,
		})

		requireCancellationPhase(t, phase, ctx)

		assert.ErrorIs(t, importErr, context.Canceled,
			"ImportFull must surface cancellation that happens after the final accepted data write")

		live := base.GetSchemaForNamespace(schemaCancellationNamespace)
		t.Logf("live constraints after cancellation: %v", schemaConstraintNames(live))
		assert.False(t, hasSchemaConstraint(live, schemaCancellationProbeConstraint),
			"the JSON schema replacement must not be published after cancellation")
		assert.True(t, hasSchemaConstraint(live, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain published after cancellation")

		assertAcceptedDataSurvives(t, base, report)

		// Read the durable definition through a native reopen. This separates
		// "applied in memory" from "persisted", so a later correction cannot
		// pass by only hiding the live definition.
		require.NoError(t, base.Close())
		reopened, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
			storage.NewBadgerEngine(dir))
		require.NoError(t, err)

		durable := reopened.GetSchemaForNamespace(schemaCancellationNamespace)
		t.Logf("durable constraints after native reopen: %v", schemaConstraintNames(durable))
		assert.False(t, hasSchemaConstraint(durable, schemaCancellationProbeConstraint),
			"the JSON schema replacement must not be persisted after cancellation")
		assert.True(t, hasSchemaConstraint(durable, schemaCancellationBaselineConstraint),
			"the pre-import schema must remain the durable definition after cancellation")

		namespaced := storage.NewNamespacedEngine(reopened, schemaCancellationNamespace)
		count, err := namespaced.NodeCount(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(2), count,
			"accepted data writes need not be rolled back across a native reopen")
	})
}
