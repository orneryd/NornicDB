package nornicdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteCypherCacheReflectsPublicNodeDelete(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.DecayEnabled = false
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	const query = `MATCH (n:CacheDelete) RETURN count(n)`

	_, err = db.CreateNodeWithID(ctx, "node", []string{"CacheDelete"}, nil)
	require.NoError(t, err)
	result, err := db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	result, err = db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	require.NoError(t, db.DeleteNode(ctx, "node"))

	result, err = db.ExecuteCypher(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
