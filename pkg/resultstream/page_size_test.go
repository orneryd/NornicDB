package resultstream

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestRegistryPageSize(t *testing.T) {
	registry, err := NewRegistry(Config{MaxPageSize: 2})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	newStream := func() Stream {
		stream, err := NewProgressive([][]any{{1}, {2}, {3}, {4}}, true, 4, nil)
		require.NoError(t, err)
		return stream
	}
	scope := Scope{Owner: "alice", Database: "nornic"}
	_, err = registry.Start(context.Background(), scope, newStream(), 3)
	var oversized *PageSizeExceededError
	require.ErrorAs(t, err, &oversized)
	require.ErrorIs(t, err, ErrInvalidPageSize)
	require.Equal(t, 2, oversized.MaxPageSize)
	var localized *localization.LocalizedError
	require.ErrorAs(t, err, &localized)
	require.Equal(t, localization.MessageID("search.page_size_exceeded"), localized.Message.ID)
	_, err = registry.Start(context.Background(), scope, newStream(), 0)
	require.ErrorIs(t, err, ErrInvalidPageSize)
	require.NotErrorAs(t, err, &oversized)
	first, err := registry.Start(context.Background(), scope, newStream(), 2)
	require.NoError(t, err)
	_, err = registry.Pull(context.Background(), scope, first.QID, 3)
	require.ErrorAs(t, err, &oversized)
	last, err := registry.Pull(context.Background(), scope, first.QID, 2)
	require.NoError(t, err)
	require.Len(t, last.Rows, 2)
}
