package apoc

import (
	"context"
	"errors"
	"testing"

	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

func installPointBackend(t *testing.T, backend apocstorage.Storage) {
	t.Helper()
	previous := GetStorage()
	require.NoError(t, SetStorage(backend))
	t.Cleanup(func() { require.NoError(t, SetStorage(previous)) })
}

func TestAPOCPointHelpersPropagateCancellation(t *testing.T) {
	backend := apocstorage.NewInMemoryStorage()
	node, err := backend.CreateNode(t.Context(), []string{"Point"}, nil)
	require.NoError(t, err)
	installPointBackend(t, backend)
	request, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name string
		args []interface{}
	}{
		{"apoc.node.degree", []interface{}{node, ""}},
		{"apoc.rel.startNode", []interface{}{&apocstorage.Relationship{StartNode: node.ID}}},
		{"apoc.neighbors.atHop", []interface{}{node, "", 1}},
		{"apoc.nodes.get", []interface{}{[]int64{node.ID}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := apocCallStatementHandler(t, tc.name, request, tc.args...)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, value, "cancellation publishes no zero-success or partial collection")
		})
	}
}

type cancelingPointBackend struct {
	*apocstorage.InMemoryStorage
	cancel context.CancelFunc
	reads  int
}

func (b *cancelingPointBackend) GetNode(ctx context.Context, id int64) (*apocstorage.Node, error) {
	b.reads++
	node, err := b.InMemoryStorage.GetNode(ctx, id)
	if cancel := b.cancel; cancel != nil {
		b.cancel = nil
		cancel()
	}
	return node, err
}

func TestAPOCPointHelperStopsAfterActualCallerCancellationAndRetries(t *testing.T) {
	backend := &cancelingPointBackend{InMemoryStorage: apocstorage.NewInMemoryStorage()}
	ids := []int64{}
	for range 3 {
		node, err := backend.CreateNode(t.Context(), nil, nil)
		require.NoError(t, err)
		ids = append(ids, node.ID)
	}
	installPointBackend(t, backend)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend.cancel = cancel
	value, err := apocCallStatementHandler(t, "apoc.nodes.get", request, ids)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value)
	require.Equal(t, 1, backend.reads, "no second backend request after cancellation")
	value, err = apocCallStatementHandler(t, "apoc.nodes.get", t.Context(), ids)
	require.NoError(t, err)
	require.Len(t, value, 3)
	require.Equal(t, 4, backend.reads)
	value, err = apocCallStatementHandler(t, "apoc.rel.startNode", t.Context(), &apocstorage.Relationship{StartNode: 999})
	require.NoError(t, err, "an ordinary missing endpoint retains its null result")
	require.Nil(t, value)
}

type failingDegreeBackend struct {
	*apocstorage.InMemoryStorage
	failure error
}

func (b *failingDegreeBackend) GetNodeDegree(context.Context, int64, string, apocstorage.Direction) (int, error) {
	return 0, b.failure
}
func TestAPOCPointHelperPreservesBackendFailure(t *testing.T) {
	failure := errors.New("point backend unavailable")
	backend := &failingDegreeBackend{InMemoryStorage: apocstorage.NewInMemoryStorage(), failure: failure}
	node, err := backend.CreateNode(t.Context(), nil, nil)
	require.NoError(t, err)
	installPointBackend(t, backend)
	value, err := apocCallStatementHandler(t, "apoc.node.degree", t.Context(), node, "")
	require.ErrorIs(t, err, failure)
	require.Nil(t, value)
}

type cancelingCreateBackend struct {
	*apocstorage.InMemoryStorage
	cancel context.CancelFunc
	writes int
}

func (b *cancelingCreateBackend) CreateNode(ctx context.Context, labels []string, properties map[string]interface{}) (*apocstorage.Node, error) {
	node, err := b.InMemoryStorage.CreateNode(ctx, labels, properties)
	if err == nil {
		b.writes++
		if cancel := b.cancel; cancel != nil {
			b.cancel = nil
			cancel()
		}
	}
	return node, err
}
func TestAPOCMultiwriteCancellationPreservesAlreadyAcceptedWrite(t *testing.T) {
	backend := &cancelingCreateBackend{InMemoryStorage: apocstorage.NewInMemoryStorage()}
	input := []*apocstorage.Node{}
	for range 3 {
		node, err := backend.InMemoryStorage.CreateNode(t.Context(), []string{"Clone"}, nil)
		require.NoError(t, err)
		input = append(input, node)
	}
	installPointBackend(t, backend)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	backend.cancel = cancel
	value, err := apocCallStatementHandler(t, "apoc.refactor.cloneNodes", request, input)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, value)
	require.Equal(t, 1, backend.writes, "cancellation stops the next write, not the already accepted write")
	nodes, err := backend.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 4)
	value, err = apocCallStatementHandler(t, "apoc.refactor.cloneNodes", t.Context(), input)
	require.NoError(t, err)
	require.Len(t, value, 3)
	nodes, err = backend.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 7)
	require.Equal(t, 4, backend.writes)
}

func TestAPOCAliasedLabelBackendHelpersKeepValuesAndErrors(t *testing.T) {
	backend := apocstorage.NewInMemoryStorage()
	for _, labels := range [][]string{{"Person"}, {"Person", "Company"}} {
		_, err := backend.CreateNode(t.Context(), labels, nil)
		require.NoError(t, err)
	}
	installPointBackend(t, backend)
	value, err := apocCallStatementHandler(t, "apoc.label.exists", t.Context(), "Person")
	require.NoError(t, err)
	require.Equal(t, true, value)
	value, err = apocCallStatementHandler(t, "apoc.label.count", t.Context(), "Person")
	require.NoError(t, err)
	require.Equal(t, 2, value)
	value, err = apocCallStatementHandler(t, "apoc.label.nodes", t.Context(), "Person")
	require.NoError(t, err)
	require.Len(t, value, 2)
	value, err = apocCallStatementHandler(t, "apoc.label.list", t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person", "Company"}, value)
	value, err = apocCallStatementHandler(t, "apoc.label.search", t.Context(), "Per*")
	require.NoError(t, err)
	require.Equal(t, []string{"Person"}, value)
	value, err = apocCallStatementHandler(t, "apoc.label.stats", t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"total": 2, "counts": map[string]int{"Person": 2, "Company": 1}}, value)
	request, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name string
		args []interface{}
	}{
		{"apoc.label.exists", []interface{}{"Person"}}, {"apoc.label.count", []interface{}{"Person"}},
		{"apoc.label.nodes", []interface{}{"Person"}}, {"apoc.label.list", nil},
		{"apoc.label.search", []interface{}{"Per*"}}, {"apoc.label.stats", nil},
	} {
		value, err = apocCallStatementHandler(t, tc.name, request, tc.args...)
		require.ErrorIs(t, err, context.Canceled, tc.name)
		require.Nil(t, value, tc.name)
	}
}
