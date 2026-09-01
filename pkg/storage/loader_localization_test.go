package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

type loaderUnsupportedEngine struct{ Engine }

func requireStorageLoaderLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestStorageLoaderLocalizedPublicErrors(t *testing.T) {
	t.Run("load preserves nested cause and path data", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")
		err := LoadFromNeo4jExport(loaderUnsupportedEngine{}, path)

		localizedErr := requireStorageLoaderLocalizedError(t, err, localization.MessageStorageLoaderOpeningFile, "opening file: open "+path+": no such file or directory")
		require.Equal(t, path, localizedErr.Message.Data["Path"])
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
	})

	t.Run("invalid node ID preserves sentinel through import wrappers", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, writeLoaderFixture(filepath.Join(dir, "nodes.json"), `{"id":""}`+"\n"))

		err := LoadFromNeo4jJSON(loaderUnsupportedEngine{}, dir)
		requireStorageLoaderLocalizedError(t, err, localization.MessageStorageLoaderLoadingNodes, "loading nodes: converting node: invalid id")
		require.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("unsupported export engine retains type as data", func(t *testing.T) {
		err := SaveToNeo4jExport(nil, filepath.Join(t.TempDir(), "export.json"))

		localizedErr := requireStorageLoaderLocalizedError(t, err, localization.MessageStorageLoaderExportEngineUnsupported, "SaveToNeo4jExport: engine type <nil> does not support full export")
		require.Equal(t, "<nil>", localizedErr.Message.Data["EngineType"])
	})
}

func TestStorageLoaderCatalogsRenderNamedArguments(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.StorageLoaderCreatingFile("/tmp/export.json", errors.New("permission denied"))

	spanish, _, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, "al crear el archivo: permission denied", spanish)

	pseudo, _, err := manager.Render(localization.WithPreferences(context.Background(), language.MustParse("en-XA")), message)
	require.NoError(t, err)
	require.Equal(t, "[!! creating file: permission denied !!]", pseudo)
}

func writeLoaderFixture(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
