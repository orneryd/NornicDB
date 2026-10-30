package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH475_StoredListSubscriptAndHeadInMatch(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh475")
			for index := int64(1); index <= 4; index++ {
				tag := "t0"
				if index%2 == 1 {
					tag = "t1"
				}
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + index))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": index, "name": "n" + string(rune('0'+index)), "tags": []string{tag, "all"}},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			projected, err := exec.Execute(ctx, "MATCH (a:A) RETURN a.id AS id, a.tags[0] AS first ORDER BY id LIMIT 2", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), "t1"}, {int64(2), "t0"}}, projected.Rows)

			filtered, err := exec.Execute(ctx, "MATCH (a:A) WHERE a.tags[0] = 't0' RETURN a.id AS id ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, filtered.Rows)

			headed, err := exec.Execute(ctx, "MATCH (a:A) WHERE head(a.tags) = 't0' RETURN count(a) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, headed.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}

func TestGH475_StoredMixedListAcrossBadgerReopenAndSnapshot(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badger")
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dataDir}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})
	store := storage.NewNamespacedEngine(engine, "gh475_disk")
	_, err = store.CreateNode(&storage.Node{
		ID: "one", Labels: []string{"A"},
		Properties: map[string]interface{}{"tags": []interface{}{int64(7), "t0", true}},
	})
	require.NoError(t, err)
	ctx := context.Background()

	check := func(t *testing.T, explicit bool) {
		view := storage.NewNamespacedEngine(engine, "gh475_disk")
		projected, err := view.GetNodeProjected("one", []string{"tags"})
		require.NoError(t, err)
		require.Equal(t, []interface{}{int64(7), "t0", true}, projected.Properties["tags"])
		exec := NewStorageExecutor(view)
		if explicit {
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
		}
		result, err := exec.Execute(ctx, "MATCH (a:A) RETURN a.tags[0] AS first, head(a.tags) AS h, a.tags[1] AS second, a.tags[2] AS third", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7), int64(7), "t0", true}}, result.Rows)
		filtered, err := exec.Execute(ctx, "MATCH (a:A) WHERE a.tags[0] = $first RETURN a.tags[1] AS tag", map[string]interface{}{"first": int64(7)})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"t0"}}, filtered.Rows)
		forms, err := exec.Execute(ctx, "MATCH (a:A) RETURN [7, 't0'][0] AS literal, head($items) AS parameter, collect(a.tags[0]) AS collected", map[string]interface{}{"items": []interface{}{int64(7), "t0"}})
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(7), int64(7), []interface{}{int64(7)}}}, forms.Rows)
		if explicit {
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
		}
	}

	t.Run("live", func(t *testing.T) { check(t, false) })
	require.NoError(t, engine.Close())
	engine = nil
	engine, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dataDir}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	t.Run("reopened snapshot", func(t *testing.T) { check(t, true) })
}
