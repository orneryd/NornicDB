package resolvers

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGraphQLPathBoundsAndEnumeration(t *testing.T) {
	db := testDB(t)
	resolver := NewResolver(db, testDBManager(t, db))
	qr := &queryResolver{resolver}
	ctx := context.Background()
	a := createNodeViaCypher(t, resolver, []string{"PathNode"}, map[string]interface{}{"name": "a"})
	b := createNodeViaCypher(t, resolver, []string{"PathNode"}, map[string]interface{}{"name": "b"})
	c := createNodeViaCypher(t, resolver, []string{"PathNode"}, map[string]interface{}{"name": "c"})
	d := createNodeViaCypher(t, resolver, []string{"PathNode"}, map[string]interface{}{"name": "d"})
	createEdgeViaCypher(t, resolver, a.ID, b.ID, "R", nil)
	createEdgeViaCypher(t, resolver, b.ID, d.ID, "R", nil)
	createEdgeViaCypher(t, resolver, a.ID, c.ID, "R", nil)
	createEdgeViaCypher(t, resolver, c.ID, d.ID, "R", nil)
	ids := func(path []models.Node) []string {
		out := make([]string, len(path))
		for i, n := range path {
			out[i] = n.ID
		}
		return out
	}
	one, two, three, zero := 1, 2, 3, 0
	t.Run("direct neighbor at inclusive depth", func(t *testing.T) {
		path, err := qr.queryShortestPath(ctx, a.ID, b.ID, &one, []string{"R"})
		require.NoError(t, err)
		require.Equal(t, []string{a.ID, b.ID}, ids(path))
	})
	t.Run("two hops at inclusive depth", func(t *testing.T) {
		path, err := qr.queryShortestPath(ctx, a.ID, d.ID, &two, nil)
		require.NoError(t, err)
		require.Len(t, path, 3)
		require.Equal(t, a.ID, path[0].ID)
		require.Equal(t, d.ID, path[2].ID)
	})
	t.Run("all diamond paths", func(t *testing.T) {
		paths, err := qr.queryAllPaths(ctx, a.ID, d.ID, &three, nil)
		require.NoError(t, err)
		got := make([][]string, len(paths))
		for i, p := range paths {
			got[i] = ids(p)
		}
		require.ElementsMatch(t, [][]string{{a.ID, b.ID, d.ID}, {a.ID, c.ID, d.ID}}, got)
	})
	t.Run("limit zero", func(t *testing.T) {
		paths, err := qr.queryAllPaths(ctx, a.ID, d.ID, &three, &zero)
		require.NoError(t, err)
		require.Empty(t, paths)
	})
	t.Run("limit one", func(t *testing.T) {
		paths, err := qr.queryAllPaths(ctx, a.ID, d.ID, &three, &one)
		require.NoError(t, err)
		require.Len(t, paths, 1)
	})
	t.Run("too shallow", func(t *testing.T) {
		paths, err := qr.queryAllPaths(ctx, a.ID, d.ID, &one, nil)
		require.NoError(t, err)
		require.Empty(t, paths)
	})
	t.Run("type filter", func(t *testing.T) {
		path, err := qr.queryShortestPath(ctx, a.ID, d.ID, &two, []string{"MISSING"})
		require.NoError(t, err)
		require.Empty(t, path)
	})
}

func TestGraphQLPathOwnerControls(t *testing.T) {
	db := testDB(t)
	manager := testDBManager(t, db)
	resolver := NewResolver(db, manager)
	qr := &queryResolver{resolver}
	ctx := context.Background()
	a := createNodeViaCypher(t, resolver, []string{"PathNode"}, map[string]interface{}{"id": "business-id"})
	b := createNodeViaCypher(t, resolver, []string{"PathNode"}, nil)
	c := createNodeViaCypher(t, resolver, []string{"PathNode"}, nil)
	createEdgeViaCypher(t, resolver, a.ID, b.ID, "R", nil)
	createEdgeViaCypher(t, resolver, b.ID, c.ID, "R", nil)
	one, two, zero, negative := 1, 2, 0, -1
	t.Run("reverse direction", func(t *testing.T) {
		p, err := qr.queryShortestPath(ctx, c.ID, a.ID, &two, []string{"R"})
		require.NoError(t, err)
		require.Len(t, p, 3)
		require.Equal(t, c.ID, p[0].ID)
		require.Equal(t, a.ID, p[2].ID)
	})
	t.Run("property ID fallback", func(t *testing.T) {
		p, err := qr.queryShortestPath(ctx, "business-id", "business-id", &one, nil)
		require.NoError(t, err)
		require.Len(t, p, 1)
		require.Equal(t, a.ID, p[0].ID)
	})
	t.Run("same endpoint", func(t *testing.T) {
		p, err := qr.queryShortestPath(ctx, a.ID, a.ID, &one, nil)
		require.NoError(t, err)
		require.Len(t, p, 1)
		require.Equal(t, a.ID, p[0].ID)
	})
	t.Run("missing endpoints", func(t *testing.T) {
		p, err := qr.queryShortestPath(ctx, "missing", a.ID, nil, nil)
		require.NoError(t, err)
		require.Empty(t, p)
		ps, err := qr.queryAllPaths(ctx, a.ID, "missing", nil, nil)
		require.NoError(t, err)
		require.Empty(t, ps)
	})
	t.Run("nonpositive bounds", func(t *testing.T) {
		for _, v := range []*int{&zero, &negative} {
			p, err := qr.queryShortestPath(ctx, a.ID, b.ID, v, nil)
			require.NoError(t, err)
			require.Empty(t, p)
			ps, err := qr.queryAllPaths(ctx, a.ID, b.ID, &two, v)
			require.NoError(t, err)
			require.Empty(t, ps)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := qr.queryShortestPath(cctx, a.ID, b.ID, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
		_, err = qr.queryAllPaths(cctx, a.ID, b.ID, nil, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("quoted relationship types", func(t *testing.T) {
		engine, err := manager.GetDefaultStorage()
		require.NoError(t, err)
		typ := "R`|other"
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "quoted-type-edge", StartNode: storage.NodeID(a.ID), EndNode: storage.NodeID(c.ID), Type: typ}))
		p, err := qr.queryShortestPath(ctx, a.ID, c.ID, &one, []string{typ})
		require.NoError(t, err)
		require.Len(t, p, 2)
		p, err = qr.queryShortestPath(ctx, a.ID, c.ID, &one, []string{"R"})
		require.NoError(t, err)
		require.Empty(t, p)
	})
}
