package nornicdb

// NOR-287 reproduction (release head d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5).
//
// Scope of this file: the exported DB.Stats contract (pkg/nornicdb/db_admin.go)
// and a controlled DB.GetIndexes composition over CompositeEngine.GetSchema.
// Assigning DB.storage below is a component seam, not proof that public Open
// installs a statement-owned composite router in a production DB instance.
//
// DB.Stats already owns an error result and returns it: this file pins that
// corrected contract for both counts, the closed-storage outcome, cancellation
// and the healthy empty/populated semantics. The composite witness at the end
// shows that the composite engine beneath DB.Stats still publishes a partial
// total as success; that fan-out skip is the root NOR-312 records, and the
// witness is kept here only as NOR-287 reproduction evidence.

import (
	"context"
	"errors"
	"sort"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var errNOR287StatsInjected = errors.New("nor287: injected count read failure")

// nor287StatsFaultEngine fails one count read without ending the fixture's
// lifecycle.
type nor287StatsFaultEngine struct {
	storage.Engine
	nodeErr error
	edgeErr error
}

func (e nor287StatsFaultEngine) NodeCount(ctx context.Context) (int64, error) {
	if e.nodeErr != nil {
		return 0, e.nodeErr
	}
	return e.Engine.NodeCount(ctx)
}

func (e nor287StatsFaultEngine) EdgeCount(ctx context.Context) (int64, error) {
	if e.edgeErr != nil {
		return 0, e.edgeErr
	}
	return e.Engine.EdgeCount(ctx)
}

func nor287StatsEngine(t *testing.T) *storage.MemoryEngine {
	t.Helper()
	return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
}

func nor287SeedStatsGraph(t *testing.T, engine storage.Engine, nodes, edges int) {
	t.Helper()
	ids := make([]storage.NodeID, 0, nodes)
	for i := 0; i < nodes; i++ {
		id := storage.NodeID("nor287:node-" + string(rune('a'+i)))
		ids = append(ids, id)
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	for i := 0; i < edges; i++ {
		require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID("nor287:edge-" + string(rune('a'+i))),
			StartNode: ids[i],
			EndNode:   ids[i+1],
			Type:      "RELATES_TO",
		}))
	}
}

// TestNOR287DBStatsErrorContract pins the error-aware behavior DB.Stats already
// has: a failed count is reported with the zero DBStats value, never as a
// successful zero, and healthy empty and populated stores keep their counts.
func TestNOR287DBStatsErrorContract(t *testing.T) {
	t.Run("healthy empty store", func(t *testing.T) {
		db := &DB{storage: nor287StatsEngine(t)}
		stats, err := db.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, DBStats{}, stats, "a healthy empty store is a valid zero, not a failure")
	})

	t.Run("healthy populated store", func(t *testing.T) {
		engine := nor287StatsEngine(t)
		nor287SeedStatsGraph(t, engine, 2, 1)
		db := &DB{storage: engine}

		stats, err := db.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, DBStats{NodeCount: 2, EdgeCount: 1}, stats)
	})

	t.Run("failed node count is reported", func(t *testing.T) {
		engine := nor287StatsEngine(t)
		nor287SeedStatsGraph(t, engine, 2, 1)
		db := &DB{storage: nor287StatsFaultEngine{Engine: engine, nodeErr: errNOR287StatsInjected}}

		stats, err := db.Stats(t.Context())
		require.ErrorIs(t, err, errNOR287StatsInjected)
		require.Equal(t, DBStats{}, stats, "a failed node count must not be published as a count")
	})

	t.Run("failed relationship count is reported", func(t *testing.T) {
		engine := nor287StatsEngine(t)
		nor287SeedStatsGraph(t, engine, 2, 1)
		db := &DB{storage: nor287StatsFaultEngine{Engine: engine, edgeErr: errNOR287StatsInjected}}

		stats, err := db.Stats(t.Context())
		require.ErrorIs(t, err, errNOR287StatsInjected)
		require.Equal(t, DBStats{}, stats, "the healthy node count is not published beside a failed relationship count")
	})

	t.Run("closed storage is reported", func(t *testing.T) {
		engine := nor287StatsEngine(t)
		nor287SeedStatsGraph(t, engine, 1, 0)
		db := &DB{storage: engine}
		require.NoError(t, engine.Close())

		stats, err := db.Stats(t.Context())
		require.ErrorIs(t, err, storage.ErrStorageClosed)
		require.Equal(t, DBStats{}, stats)
	})

	t.Run("cancelled request is reported", func(t *testing.T) {
		// TestStatsDoesNotReportCanceledReadsAsZeroSuccess owns the end-to-end
		// canceled read; this control pins the same outcome for the injected
		// storage seam this file uses.
		engine := nor287StatsEngine(t)
		nor287SeedStatsGraph(t, engine, 1, 0)
		db := &DB{storage: engine}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		stats, err := db.Stats(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, DBStats{}, stats)
	})

	t.Run("uninitialized database", func(t *testing.T) {
		// The nil-storage branch of DB.Stats returns a zero value and no error.
		// Open either publishes a fully initialized DB or fails, so this branch
		// is a zero-value guard, not a supported operating state.
		stats, err := (&DB{}).Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, DBStats{}, stats)
	})
}

// nor287CompositeStatsView builds a two-constituent composite holding one node
// in constituent "a", whose second constituent's participant cannot be admitted
// when denySecond is set: the statement view pkg/cypher builds in
// compositeStatement.router.
func nor287CompositeStatsView(t *testing.T, denySecond bool) storage.Engine {
	t.Helper()
	inner := nor287StatsEngine(t)
	constituents := map[string]storage.Engine{
		"a": storage.NewNamespacedEngine(inner, "a"),
		"b": storage.NewNamespacedEngine(inner, "b"),
	}
	_, err := constituents["a"].CreateNode(t.Context(), &storage.Node{ID: "n-0", Labels: []string{"Doc"}})
	require.NoError(t, err)
	composite := storage.NewCompositeEngine(
		constituents,
		map[string]string{"a": "a", "b": "b"},
		map[string]string{"a": "read_write", "b": "read_write"},
	)
	if !denySecond {
		return composite
	}
	return composite.WithConstituentAccess(nil, func(alias string, engine storage.Engine, _ bool) (storage.Engine, error) {
		if alias == "b" {
			return nil, errNOR287StatsInjected
		}
		return engine, nil
	})
}

// TestNOR287DBStatsCompositeAdmissionFailure is the fail-first witness for the
// composite count path behind DB.Stats. A direct storage count keeps the
// admitted subset; the witness states that the exported error result must carry
// the constituent admission failure instead.
func TestNOR287DBStatsCompositeAdmissionFailure(t *testing.T) {
	t.Run("healthy composite control", func(t *testing.T) {
		stats, err := (&DB{storage: nor287CompositeStatsView(t, false)}).Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, DBStats{NodeCount: 1}, stats)
	})

	t.Run("current stats publish the admitted subset", func(t *testing.T) {
		stats, err := (&DB{storage: nor287CompositeStatsView(t, true)}).Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, DBStats{NodeCount: 1}, stats)
	})

	t.Run("stats must not answer a failed constituent with a successful total", func(t *testing.T) {
		stats, err := (&DB{storage: nor287CompositeStatsView(t, true)}).Stats(t.Context())
		require.ErrorIs(t, err, errNOR287StatsInjected,
			"DB.Stats returned %+v and no error after a constituent admission failure", stats)
	})
}

// TestNOR287DBGetIndexesCompositeSchemaAdmissionFailure records how the one
// exported error-returning consumer of the merged composite schema behaves: the
// error channel cannot carry the constituent admission failure because
// CompositeEngine.GetSchema has none.
func TestNOR287DBGetIndexesCompositeSchemaAdmissionFailure(t *testing.T) {
	build := func(t *testing.T, denySecond bool) *DB {
		t.Helper()
		a := nor287StatsEngine(t)
		b := nor287StatsEngine(t)
		require.NoError(t, a.GetSchema().AddPropertyIndex("idx_a_person_name", "Person", []string{"name"}))
		require.NoError(t, b.GetSchema().AddPropertyIndex("idx_b_company_age", "Company", []string{"age"}))
		composite := storage.NewCompositeEngine(
			map[string]storage.Engine{"a": a, "b": b},
			map[string]string{"a": "a", "b": "b"},
			map[string]string{"a": "read_write", "b": "read_write"},
		)
		if !denySecond {
			return &DB{storage: composite}
		}
		view := composite.WithConstituentAccess(nil, func(alias string, engine storage.Engine, _ bool) (storage.Engine, error) {
			if alias == "b" {
				return nil, errNOR287StatsInjected
			}
			return engine, nil
		})
		return &DB{storage: view}
	}

	// declaredIndexes drops the token lookup indexes every schema manager owns.
	declaredIndexes := func(indexes []*IndexInfo) []string {
		out := make([]string, 0, len(indexes))
		for _, index := range indexes {
			if index.Type == "lookup" {
				continue
			}
			out = append(out, index.Name)
		}
		sort.Strings(out)
		return out
	}

	t.Run("healthy composite lists every constituent index", func(t *testing.T) {
		indexes, err := build(t, false).GetIndexes(t.Context())
		require.NoError(t, err)
		require.Equal(t, []string{"idx_a_person_name", "idx_b_company_age"}, declaredIndexes(indexes))
	})

	t.Run("admission failure is invisible to the error channel", func(t *testing.T) {
		indexes, err := build(t, true).GetIndexes(t.Context())
		require.NoError(t, err, "GetIndexes has no way to report a composite schema admission failure")
		require.Equal(t, []string{"idx_a_person_name"}, declaredIndexes(indexes),
			"the failed constituent's index is missing from a successful result")
	})
}
