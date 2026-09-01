package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
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
		db := &DB{closed: true}
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
		db.SetDbConfigResolver(func(string) (int, float64, string) { return 3, 0, "" })

		err := db.validateQueryEmbeddingDimensions("tenant-a", []float32{1, 2})
		localizedErr := requireNornicDBLocalizedError(t, err, localization.MessageNornicDBCoreQueryDimensionMismatchForDatabase, "database \"tenant-a\": query embedding dimension mismatch (index dims 3, query dims 2)")
		require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
		require.Equal(t, "tenant-a", localizedErr.Message.Data["Database"])
		require.Equal(t, 3, localizedErr.Message.Data["IndexDimensions"])
		require.Equal(t, 2, localizedErr.Message.Data["QueryDimensions"])
	})
}

func TestNornicDBLocalizedErrorRendersAtBoundary(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.NornicDBCoreQueryDimensionMismatchForDatabase("tenant-a", 3, 2)

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "base de datos \"tenant-a\": las dimensiones del embedding de consulta no coinciden (dimensiones del índice 3, dimensiones de la consulta 2)", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! database \"tenant-a\": query embedding dimension mismatch (index dims 3, query dims 2) !!]", pseudo)
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
