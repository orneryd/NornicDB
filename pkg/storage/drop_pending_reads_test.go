package storage_test

import (
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFailedDropRejectsWarmGraphReads(t *testing.T) {
	b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(&storage.Node{ID: "tenant:warm"})
	require.NoError(t, err)
	_, err = b.CreateNode(&storage.Node{ID: "peer:keep"})
	require.NoError(t, err)
	_, err = b.GetNode("tenant:warm")
	require.NoError(t, err)
	count, err := b.NodeCountByPrefix("tenant:")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	storage.ReviewSetDropTailFailure(b, func(string) error { return errors.New("intrinsic cleanup unavailable") })
	_, _, err = b.DeleteByPrefix("tenant:")
	require.ErrorContains(t, err, "intrinsic cleanup unavailable")
	_, err = b.GetNode("tenant:warm")
	assert.ErrorIs(t, err, storage.ErrConflict, "warm body must not survive as a successful graph read")
	_, err = b.NodeCountByPrefix("tenant:")
	assert.ErrorIs(t, err, storage.ErrConflict, "warm count must not report deleted rows")
	_, err = b.NodeCount()
	assert.ErrorIs(t, err, storage.ErrConflict, "global count includes incomplete namespace cleanup")
	_, err = b.GetNode("peer:keep")
	require.NoError(t, err)
	count, err = b.NodeCountByPrefix("peer:")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}
