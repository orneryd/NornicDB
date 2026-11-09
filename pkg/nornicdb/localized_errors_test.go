package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func requireNornicDBLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestNornicDBLocalizedErrorsPreserveIdentityAndArguments(t *testing.T) {
	t.Run("sentinel has typed identity", func(t *testing.T) {
		requireNornicDBLocalizedError(t, ErrClosed, localization.MessageNornicDBCoreDatabaseClosed, "database is closed")
		db := &DB{}
		db.work.CloseAdmission()
		_, err := db.GetOrCreateSearchService("tenant-a", nil)
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("validation error has typed identity", func(t *testing.T) {
		db := &DB{}
		_, err := db.CreateNodeWithID(context.Background(), "", nil, nil)
		requireNornicDBLocalizedError(t, err, localization.MessageNornicDBCoreNodeIDRequired, "node ID must not be empty")
	})

	t.Run("dimension wrapper preserves sentinel and named arguments", func(t *testing.T) {
		db := &DB{}
		db.SetDbConfigResolver(func(string) (int, float64) { return 3, 0 })

		err := db.validateQueryEmbeddingDimensions("tenant-a", []float32{1, 2})
		localizedErr := requireNornicDBLocalizedError(t, err, localization.MessageNornicDBCoreQueryDimensionMismatchForDatabase, "database \"tenant-a\": query embedding dimension mismatch (index dims 3, query dims 2)")
		require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
		require.Equal(t, "tenant-a", localizedErr.Message.Data["Database"])
		require.Equal(t, 3, localizedErr.Message.Data["IndexDimensions"])
		require.Equal(t, 2, localizedErr.Message.Data["QueryDimensions"])
	})
}

func TestNornicDBLocalizedErrorPreservesWrappedCause(t *testing.T) {
	cause := errors.New("storage unavailable")
	err := localizedError(localization.Message{
		ID:       localization.MessageID("nornicdbcore.test_wrapper"),
		Fallback: "operation failed: " + cause.Error(),
	}, cause)

	require.EqualError(t, err, "operation failed: storage unavailable")
	require.ErrorIs(t, err, cause)
}
