package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteFailureRollbackContextPreservesRequestOwnership(t *testing.T) {
	type requestValueKey struct{}
	request, cancelRequest := context.WithTimeout(
		context.WithValue(context.Background(), requestValueKey{}, "request-trace"),
		storage.RemoteRequestTimeout/2,
	)
	defer cancelRequest()
	requestDeadline, ok := request.Deadline()
	require.True(t, ok)

	rollback, cancelRollback := remoteFailureRollbackContext(request)
	defer cancelRollback()
	rollbackDeadline, ok := rollback.Deadline()
	require.True(t, ok)
	require.True(t, rollbackDeadline.Equal(requestDeadline))
	require.Equal(t, "request-trace", rollback.Value(requestValueKey{}))

	cancelRequest()
	require.ErrorIs(t, request.Err(), context.Canceled)
	require.NoError(t, rollback.Err(), "request cancellation must not cancel accepted rollback cleanup")

	cancelRollback()
	require.ErrorIs(t, rollback.Err(), context.Canceled)
}
