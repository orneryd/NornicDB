package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestStorageTransactionSnapshotReadFailedLocalization(t *testing.T) {
	manager, err := NewManager(nil, nil)
	require.NoError(t, err)
	message := StorageTransactionSnapshotReadFailed("test:doc", errors.New("discarded reader"))
	require.Equal(t, MessageStorageTransactionSnapshotReadFailed, message.ID)
	require.Equal(t, "failed to read transaction snapshot for node test:doc: discarded reader", message.Fallback)
	for _, tc := range []struct {
		locale string
		want   string
	}{
		{"en-US", message.Fallback},
		{"en-XA", "[!! " + message.Fallback + " !!]"},
		{"es-ES", "no se pudo leer la instantánea de la transacción para el nodo test:doc: discarded reader"},
	} {
		t.Run(tc.locale, func(t *testing.T) {
			locale := language.MustParse(tc.locale)
			got, tag, err := manager.Render(WithPreferences(context.Background(), locale), message)
			require.NoError(t, err)
			require.Equal(t, locale, tag)
			require.Equal(t, tc.want, got)
		})
	}
}
