package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// A plugin that fails to load is reported with its file and cause in every
// catalog locale (#867).
func TestNornicDBCorePluginLoadFailedRenders(t *testing.T) {
	message := NornicDBCorePluginLoadFailed("bad.so", errors.New("open: not a plugin"))
	require.Equal(t, MessageNornicDBCorePluginLoadFailed, message.ID)
	require.Equal(t, "plugin bad.so was not loaded: open: not a plugin", message.Fallback)

	for _, tc := range []struct {
		tag  language.Tag
		text string
	}{
		{language.AmericanEnglish, "plugin bad.so was not loaded: open: not a plugin"},
		{language.MustParse("es-ES"), "el plugin bad.so no se cargó: open: not a plugin"},
	} {
		manager, err := NewManager([]language.Tag{tc.tag}, nil)
		require.NoError(t, err)
		rendered, _, err := manager.Render(WithPreferences(context.Background(), tc.tag), message)
		require.NoError(t, err)
		require.Equal(t, tc.text, rendered, tc.tag.String())
	}
}
