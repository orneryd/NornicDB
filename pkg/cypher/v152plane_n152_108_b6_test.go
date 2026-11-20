package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-111 / FIX-SCHEMA-START: repeating an already-installed equivalent UNIQUE
// declaration with IF NOT EXISTS must not re-validate stored values or rebuild
// the unique-value caches. The reported cost was whole-label revalidation on
// every startup replay of an unchanged schema.
//
// Public schema DDL owns a native transaction, whose stored reads bypass an
// outer engine decorator. Observe the validation and unique-refresh owners at
// their context checks as well as direct stored-read calls. Positive first-
// install and explicit-refresh controls prove these observations are live;
// zero replay observations mean neither owner was re-entered. This is an
// entry-count correctness/control test, not a latency measurement.
type nor111ScanCountEngine struct {
	*storage.MemoryEngine

	mu                 sync.Mutex
	labelScans         int
	labelScanLabels    []string
	uniqueRebuildScans int
	rebuildLabels      []string
}

func (e *nor111ScanCountEngine) resetScans() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.labelScans, e.uniqueRebuildScans = 0, 0
	e.labelScanLabels, e.rebuildLabels = nil, nil
}

func (e *nor111ScanCountEngine) recordLabelScan(label string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.labelScans++
	e.labelScanLabels = append(e.labelScanLabels, label)
}

func (e *nor111ScanCountEngine) recordUniqueRebuild(label string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.uniqueRebuildScans++
	e.rebuildLabels = append(e.rebuildLabels, label)
}

// The database wrapper reads labels through the scoped stored forms, so both
// the scoped and unscoped forms count; the unscoped forms keep the counter
// honest for a caller that passes this engine directly to storage.
func (e *nor111ScanCountEngine) GetStoredNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	e.recordLabelScan(label)
	return e.MemoryEngine.GetStoredNodesByLabel(ctx, label)
}

func (e *nor111ScanCountEngine) GetStoredNodesByLabelInScope(ctx context.Context, scope, label string) ([]*storage.Node, error) {
	e.recordLabelScan(label)
	return e.MemoryEngine.GetStoredNodesByLabelInScope(ctx, scope, label)
}

func (e *nor111ScanCountEngine) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	e.recordUniqueRebuild(label)
	return e.MemoryEngine.StreamStoredNodesByLabelProjected(ctx, label, properties, visit)
}

func (e *nor111ScanCountEngine) StreamStoredNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*storage.Node) error) error {
	e.recordUniqueRebuild(label)
	return e.MemoryEngine.StreamStoredNodesByLabelProjectedInScope(ctx, scope, label, properties, visit)
}

// The observer preserves its parent's cancellation contract and records only
// named validation/refresh frames. Fixture work is synchronous and bounded.
type nor111ObservedContext struct {
	context.Context
	scans *nor111ScanCountEngine
}

func (c *nor111ObservedContext) Err() error {
	var pcs [64]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, "/storage.ValidateConstraintOnCreationForEngine") {
			c.scans.recordLabelScan("<creation-validation-owner>")
		}
		if strings.HasSuffix(frame.Function, "/storage.RefreshUniqueConstraintValuesForEngine") {
			c.scans.recordUniqueRebuild("<unique-refresh-owner>")
		}
		if !more {
			break
		}
	}
	return c.Context.Err()
}

type nor111Observation struct {
	labelScans, uniqueRebuildScans int
	labelScanLabels, rebuildLabels []string
}

func (e *nor111ScanCountEngine) observations() nor111Observation {
	e.mu.Lock()
	defer e.mu.Unlock()
	return nor111Observation{
		labelScans: e.labelScans, uniqueRebuildScans: e.uniqueRebuildScans,
		labelScanLabels: append([]string(nil), e.labelScanLabels...),
		rebuildLabels:   append([]string(nil), e.rebuildLabels...),
	}
}

func newNOR111ScanFixture(t *testing.T) (*StorageExecutor, storage.Engine, *nor111ScanCountEngine) {
	t.Helper()
	counting := &nor111ScanCountEngine{MemoryEngine: newTestMemoryEngine(t)}
	store := storage.NewNamespacedEngine(counting, "nor111_constraint_replay")
	return NewStorageExecutor(store), store, counting
}

// nor111SeedNodes creates count nodes of label with sequential integer ids
// unless duplicate is set, in which case every node carries the same id value.
func nor111SeedNodes(t *testing.T, store storage.Engine, label string, count int, duplicate bool) {
	t.Helper()
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%s-%d", label, i)
		value := any(int64(i))
		if duplicate {
			value = "duplicate"
		}
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{label},
			Properties: map[string]any{"id": value},
		})
		require.NoError(t, err)
	}
}

// TestNOR111ExistingUniqueIfNotExistsDoesNotRevalidateStore reproduces the
// separate idempotent-DDL defect owned by NOR-111: an equivalent existing
// UNIQUE declaration must return without stored-data validation or a
// unique-value cache rebuild, on both parsers, while first installation,
// conflicting declarations and duplicate stored data keep their behavior.
func TestNOR111ExistingUniqueIfNotExistsDoesNotRevalidateStore(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("first install validates and rebuilds then replays are free", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "ReplayDoc", 8, false)

			const ddl = "CREATE CONSTRAINT replay_doc_id IF NOT EXISTS FOR (n:ReplayDoc) REQUIRE n.id IS UNIQUE"
			schema := store.GetSchema()

			scans.resetScans()
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err)
			require.True(t, schema.HasConstraintNamed("replay_doc_id"))
			index, owned := schema.ConstraintPropertyIndex("replay_doc_id")
			require.True(t, owned, "the new constraint owns its equality index")
			require.Positive(t, scans.observations().labelScans, "installing a constraint must validate stored values")
			require.Positive(t, scans.observations().uniqueRebuildScans, "installing a constraint must build its unique-value cache")
			t.Logf("first install: labelScans=%d %v uniqueRebuildScans=%d %v",
				scans.observations().labelScans, scans.observations().labelScanLabels, scans.observations().uniqueRebuildScans, scans.observations().rebuildLabels)

			for replay := 0; replay < 2; replay++ {
				scans.resetScans()
				_, err = exec.Execute(ctx, ddl, nil)
				require.NoError(t, err, "replay %d", replay)
				require.Zero(t, scans.observations().labelScans,
					"replay %d re-validated stored nodes on %v", replay, scans.observations().labelScanLabels)
				require.Zero(t, scans.observations().uniqueRebuildScans,
					"replay %d rebuilt unique values for %v", replay, scans.observations().rebuildLabels)
				after, owned := schema.ConstraintPropertyIndex("replay_doc_id")
				require.True(t, owned)
				require.Same(t, index, after, "replay %d replaced the constraint's index", replay)
			}
		})

		t.Run("existing unnamed and equivalently named declarations are free", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "ReplayUnnamed", 4, false)
			schema := store.GetSchema()

			_, err := exec.Execute(ctx,
				"CREATE CONSTRAINT IF NOT EXISTS FOR (n:ReplayUnnamed) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)
			index, owned := schema.ConstraintPropertyIndex("constraint_ReplayUnnamed_id")
			require.True(t, owned)

			for _, replay := range []string{
				"CREATE CONSTRAINT IF NOT EXISTS FOR (n:ReplayUnnamed) REQUIRE n.id IS UNIQUE",
				"CREATE CONSTRAINT replay_unnamed_equivalent IF NOT EXISTS FOR (n:ReplayUnnamed) REQUIRE n.id IS UNIQUE",
			} {
				scans.resetScans()
				_, err = exec.Execute(ctx, replay, nil)
				require.NoError(t, err, replay)
				require.Zero(t, scans.observations().labelScans, "%s re-validated stored nodes on %v", replay, scans.observations().labelScanLabels)
				require.Zero(t, scans.observations().uniqueRebuildScans, "%s rebuilt unique values for %v", replay, scans.observations().rebuildLabels)
				after, owned := schema.ConstraintPropertyIndex("constraint_ReplayUnnamed_id")
				require.True(t, owned, replay)
				require.Same(t, index, after, "%s replaced the constraint's index", replay)
			}
		})

		t.Run("existing empty-label declaration beside an unrelated population is free", func(t *testing.T) {
			exec, _, scans := newNOR111ScanCountFixtureWithPopulation(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}

			const ddl = "CREATE CONSTRAINT schema_start_probe IF NOT EXISTS FOR (n:UnusedSchemaLabel) REQUIRE n.id IS UNIQUE"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err)

			for replay := 0; replay < 2; replay++ {
				scans.resetScans()
				_, err = exec.Execute(ctx, ddl, nil)
				require.NoError(t, err, "replay %d", replay)
				require.Zero(t, scans.observations().labelScans,
					"replay %d scanned stored nodes on %v (the reported defect fixture)", replay, scans.observations().labelScanLabels)
				require.Zero(t, scans.observations().uniqueRebuildScans,
					"replay %d rebuilt unique values for %v (the reported defect fixture)", replay, scans.observations().rebuildLabels)
			}
		})

		t.Run("equivalent declaration without IF NOT EXISTS still conflicts without scanning", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "ReplayConflict", 4, false)

			const ddl = "CREATE CONSTRAINT replay_conflict_id IF NOT EXISTS FOR (n:ReplayConflict) REQUIRE n.id IS UNIQUE"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err)

			scans.resetScans()
			_, err = exec.Execute(ctx, "CREATE CONSTRAINT replay_conflict_id FOR (n:ReplayConflict) REQUIRE n.id IS UNIQUE", nil)
			requireErrorStatus(t, err, "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists", ddl)
			require.Zero(t, scans.observations().labelScans, "a conflict must be admitted before stored-data validation")
			require.Zero(t, scans.observations().uniqueRebuildScans, "a conflict must not rebuild unique values")

			scans.resetScans()
			_, err = exec.Execute(ctx, "CREATE CONSTRAINT replay_conflict_other FOR (n:ReplayConflict) REQUIRE n.id IS UNIQUE", nil)
			requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintAlreadyExists", ddl)
			require.Zero(t, scans.observations().labelScans)
			require.Zero(t, scans.observations().uniqueRebuildScans)
		})

		t.Run("different declaration of an existing name conflicts without scanning", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "ReplayDifferent", 4, false)
			schema := store.GetSchema()

			const ddl = "CREATE CONSTRAINT replay_different_id IF NOT EXISTS FOR (n:ReplayDifferent) REQUIRE n.id IS UNIQUE"
			_, err := exec.Execute(ctx, ddl, nil)
			require.NoError(t, err)
			index, owned := schema.ConstraintPropertyIndex("replay_different_id")
			require.True(t, owned)
			installed, err := schema.GetConstraints(ctx)
			require.NoError(t, err)
			require.Len(t, installed, 1)
			require.Equal(t, "id", installed[0].Property)

			scans.resetScans()
			_, err = exec.Execute(ctx,
				"CREATE CONSTRAINT replay_different_id IF NOT EXISTS FOR (n:ReplayDifferent) REQUIRE n.other IS UNIQUE", nil)
			requireErrorStatus(t, err, "Neo.ClientError.Schema.ConstraintWithNameAlreadyExists", ddl)
			require.Zero(t, scans.observations().labelScans, "a conflicting declaration must not validate the store")
			require.Zero(t, scans.observations().uniqueRebuildScans, "a conflicting declaration must not rebuild unique values")

			after, owned := schema.ConstraintPropertyIndex("replay_different_id")
			require.True(t, owned)
			require.Same(t, index, after, "a rejected declaration replaced the installed index")
			remaining, err := schema.GetConstraints(ctx)
			require.NoError(t, err)
			require.Len(t, remaining, 1)
			require.Equal(t, "id", remaining[0].Property)
		})

		t.Run("duplicate stored data rejects first install and never rebuilds", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "DuplicateData", 2, true)
			schema := store.GetSchema()

			const ddl = "CREATE CONSTRAINT duplicate_data_id IF NOT EXISTS FOR (n:DuplicateData) REQUIRE n.id IS UNIQUE"
			scans.resetScans()
			_, err := exec.Execute(ctx, ddl, nil)
			var violation *storage.ConstraintViolationError
			require.ErrorAs(t, err, &violation)
			require.Equal(t, storage.ConstraintUnique, violation.Type)
			require.Positive(t, scans.observations().labelScans, "first installation must read the stored values it rejects")
			require.Zero(t, scans.observations().uniqueRebuildScans, "a rejected installation must not rebuild the unique-value cache")
			require.False(t, schema.HasConstraintNamed("duplicate_data_id"), "rejected constraint must not be registered")
			_, owned := schema.ConstraintPropertyIndex("duplicate_data_id")
			require.False(t, owned, "rejected constraint must not leave an owned index behind")
			t.Logf("duplicate data install: labelScans=%d %v uniqueRebuildScans=%d",
				scans.observations().labelScans, scans.observations().labelScanLabels, scans.observations().uniqueRebuildScans)
		})

		t.Run("counter control observes a genuine unique-value rebuild", func(t *testing.T) {
			exec, store, scans := newNOR111ScanFixture(t)
			ctx := &nor111ObservedContext{Context: t.Context(), scans: scans}
			nor111SeedNodes(t, store, "CounterControl", 3, false)

			_, err := exec.Execute(ctx,
				"CREATE CONSTRAINT counter_control_id IF NOT EXISTS FOR (n:CounterControl) REQUIRE n.id IS UNIQUE", nil)
			require.NoError(t, err)

			scans.resetScans()
			require.NoError(t, storage.RefreshUniqueConstraintValuesForEngine(ctx, store, store.GetSchema()))
			require.Positive(t, scans.observations().uniqueRebuildScans,
				"the rebuild counter must observe the storage read a unique-value rebuild performs")
			require.Contains(t, scans.observations().rebuildLabels, "CounterControl")
		})
	})
}

// newNOR111ScanCountFixtureWithPopulation seeds the reported fixture's shape:
// an unrelated populated label beside the label the replayed declaration names.
func newNOR111ScanCountFixtureWithPopulation(t *testing.T) (*StorageExecutor, storage.Engine, *nor111ScanCountEngine) {
	t.Helper()
	exec, store, scans := newNOR111ScanFixture(t)
	nor111SeedNodes(t, store, "SchemaLoad", 128, false)
	return exec, store, scans
}
