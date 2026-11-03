package apoc

import (
	"context"
	"errors"
	"testing"

	apocpath "github.com/orneryd/nornicdb/apoc/path"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

type subgraphAllBackend struct {
	*apocstorage.InMemoryStorage
	cancel  context.CancelFunc
	failure error
	stale   []*apocstorage.Relationship
	reads   int
}

func (s *subgraphAllBackend) GetNodeRelationships(ctx context.Context, id int64, kind string, direction apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	s.reads++
	if s.failure != nil {
		if cancel := s.cancel; cancel != nil {
			s.cancel = nil
			cancel()
		}
		return s.stale, s.failure
	}
	rels, err := s.InMemoryStorage.GetNodeRelationships(ctx, id, kind, direction)
	if cancel := s.cancel; cancel != nil {
		s.cancel = nil
		cancel()
	}
	return rels, err
}
func TestSubgraphAllRegisteredMapErrorsAndRetry(t *testing.T) {
	backend := &subgraphAllBackend{InMemoryStorage: apocstorage.NewInMemoryStorage()}
	first, err := backend.CreateNode(t.Context(), []string{"First"}, nil)
	require.NoError(t, err)
	second, err := backend.CreateNode(t.Context(), []string{"Second"}, nil)
	require.NoError(t, err)
	rel, err := backend.CreateRelationship(t.Context(), first.ID, second.ID, "R", nil)
	require.NoError(t, err)
	installPointBackend(t, backend)
	cfg := apocpath.Config{MaxLevel: 2, RelationshipFilter: "R"}
	call := func(ctx context.Context) (interface{}, error) {
		return apocCallStatementHandler(t, "apoc.path.subgraphAll", ctx, first, cfg)
	}
	value, err := call(t.Context())
	require.NoError(t, err)
	result, ok := value.(map[string]interface{})
	require.True(t, ok)
	require.Len(t, result, 2)
	require.Equal(t, []*apocstorage.Node{first, second}, result["nodes"])
	require.Equal(t, []*apocstorage.Relationship{rel, rel}, result["rels"], "existing both-endpoint traversal multiplicity is preserved")
	request, cancel := context.WithCancel(t.Context())
	cancel()
	backend.reads = 0
	value, err = call(request)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value)
	require.Zero(t, backend.reads)
	request, cancel = context.WithCancel(t.Context())
	defer cancel()
	backend.cancel = cancel
	value, err = call(request)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value)
	require.Equal(t, 1, backend.reads)
	failure := errors.New("subgraph backend unavailable")
	backend.failure = failure
	value, err = call(t.Context())
	require.ErrorIs(t, err, failure)
	require.Same(t, failure, err)
	require.Nil(t, value)
	backend.failure = nil
	value, err = call(t.Context())
	require.NoError(t, err)
	require.Equal(t, result, value)
	missing := &apocstorage.Node{ID: 999}
	value, err = apocCallStatementHandler(t, "apoc.path.subgraphAll", t.Context(), missing, cfg)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"nodes": []*apocstorage.Node{missing}, "rels": []*apocstorage.Relationship{}}, value)
}

func TestSubgraphAllDiscardsDataReturnedWithOrdinaryMiss(t *testing.T) {
	backend := &subgraphAllBackend{InMemoryStorage: apocstorage.NewInMemoryStorage()}
	first, err := backend.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	second, err := backend.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	rel, err := backend.CreateRelationship(t.Context(), first.ID, second.ID, "R", nil)
	require.NoError(t, err)
	installPointBackend(t, backend)
	cfg := apocpath.Config{MaxLevel: 1}
	call := func(ctx context.Context) (interface{}, error) {
		return apocCallStatementHandler(t, "apoc.path.subgraphAll", ctx, first, cfg)
	}
	backend.failure = apocstorage.ErrNodeNotFound
	backend.stale = []*apocstorage.Relationship{rel}
	value, err := call(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"nodes": []*apocstorage.Node{first}, "rels": []*apocstorage.Relationship{}}, value, "a failed read's data is not traversed")
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend.cancel = cancel
	value, err = call(request)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value, "cancellation still wins after an ordinary miss")
	failure := errors.New("original backend failure")
	backend.failure = failure
	value, err = call(t.Context())
	require.ErrorIs(t, err, failure)
	require.Same(t, failure, err)
	require.Nil(t, value)
	backend.failure = nil
	value, err = call(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"nodes": []*apocstorage.Node{first, second}, "rels": []*apocstorage.Relationship{rel}}, value)
}
