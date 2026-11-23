package nornicdb

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDBGetIndexesRequiresEveryReadableConstituent(t *testing.T) {
	left := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	right := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	require.NoError(t, left.GetSchema().AddPropertyIndex("left_index", "Left", []string{"name"}))
	require.NoError(t, right.GetSchema().AddPropertyIndex("right_index", "Right", []string{"title"}))
	composite := storage.NewCompositeEngine(
		map[string]storage.Engine{"left": left, "right": right},
		map[string]string{"left": "left", "right": "right"},
		map[string]string{"left": "read_write", "right": "read"},
	)
	t.Run("complete", func(t *testing.T) {
		db := &DB{storage: composite}
		indexes, err := db.GetIndexes(t.Context())
		require.NoError(t, err)
		var names []string
		for _, index := range indexes {
			if index.Type != "lookup" {
				names = append(names, index.Name)
			}
		}
		require.ElementsMatch(t, []string{"left_index", "right_index"}, names)
	})
	t.Run("unavailable participant", func(t *testing.T) {
		admissionErr := errors.New("participant admission failed")
		view := composite.WithConstituentAccess(nil, func(alias string, engine storage.Engine, _ bool) (storage.Engine, error) {
			if alias == "right" {
				return nil, admissionErr
			}
			return engine, nil
		})
		db := &DB{storage: view}
		indexes, err := db.GetIndexes(t.Context())
		require.ErrorIs(t, err, admissionErr)
		require.Nil(t, indexes)
		require.Nil(t, view.GetSchema(), "errorless callers receive an unavailable schema")
	})
	t.Run("missing configured participant", func(t *testing.T) {
		view := storage.NewCompositeEngine(
			map[string]storage.Engine{"left": left},
			map[string]string{"left": "left", "right": "right"},
			map[string]string{"left": "read", "right": "read"},
		)
		db := &DB{storage: view}
		indexes, err := db.GetIndexes(t.Context())
		require.ErrorContains(t, err, "constituent 'right' not found")
		require.Nil(t, indexes)
		require.Nil(t, view.GetSchema())
		count, err := view.NodeCount(t.Context())
		require.ErrorContains(t, err, "constituent 'right' not found")
		require.Zero(t, count)
		count, err = view.EdgeCount(t.Context())
		require.ErrorContains(t, err, "constituent 'right' not found")
		require.Zero(t, count)
	})
	t.Run("empty composite", func(t *testing.T) {
		view := storage.NewCompositeEngine(nil, nil, nil)
		db := &DB{storage: view}
		_, err := db.GetIndexes(t.Context())
		require.NoError(t, err)
		require.NotNil(t, view.GetSchema())
		count, err := view.NodeCount(t.Context())
		require.NoError(t, err)
		require.Zero(t, count)
	})
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		db := &DB{storage: composite}
		indexes, err := db.GetIndexes(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, indexes)
	})
	t.Run("closed constituent", func(t *testing.T) {
		closed := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		require.NoError(t, closed.Close())
		view := storage.NewCompositeEngine(
			map[string]storage.Engine{"left": left, "right": closed},
			map[string]string{"left": "left", "right": "right"},
			map[string]string{"left": "read", "right": "read"},
		)
		db := &DB{storage: view}
		indexes, err := db.GetIndexes(t.Context())
		require.ErrorIs(t, err, storage.ErrStorageClosed)
		require.Nil(t, indexes)
		require.Nil(t, view.GetSchema())
	})
}
