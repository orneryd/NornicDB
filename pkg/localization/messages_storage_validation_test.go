package localization

import (
	"errors"
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

func TestStorageTransactionLargeCommitUnrecoverableRendering(t *testing.T) {
	paths := []string{
		"catalog/active.storagevalidation.en-US.yaml",
		"catalog/active.storagevalidation.es-ES.yaml",
		"catalog/active.storagevalidation.en-XA.yaml",
	}
	require.NoError(t, validateCatalogFiles(catalogFS, paths))
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range paths {
		_, err := bundle.LoadMessageFileFS(catalogFS, path)
		require.NoError(t, err)
	}

	message := StorageTransactionLargeCommitUnrecoverable(errors.New("disk gone"))
	require.Equal(t, MessageStorageTransactionLargeCommitUnrecoverable, message.ID)
	require.Equal(t, "storage refuses writes until restart: a failed large commit could not be rolled back: disk gone", message.Fallback)
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}

	english, err := i18n.NewLocalizer(bundle, "en-US").Localize(config)
	require.NoError(t, err)
	require.Equal(t, message.Fallback, english)
	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "el almacenamiento rechaza escrituras hasta reiniciar: no se pudo revertir una confirmación grande fallida: disk gone", spanish)
}

func TestStorageRemoteGraphValidationCatalogRendering(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
		_, err := bundle.LoadMessageFileFS(catalogFS, "catalog/active.storagevalidation."+locale+".yaml")
		require.NoError(t, err)
	}
	messages := []Message{StorageRemoteRESTInvalidJSON(), StorageRemotePathEntitySequenceMissing(), StorageRemotePathNonEntity(), StorageRemotePathEntitiesInconsistent(), StorageRemoteRESTMapKeysMismatch(), StorageRemoteRESTListLengthMismatch(), StorageRemoteRESTRowLengthMismatch()}
	for _, message := range messages {
		t.Run(string(message.ID), func(t *testing.T) {
			config := &i18n.LocalizeConfig{MessageID: string(message.ID)}
			english, err := i18n.NewLocalizer(bundle, "en-US").Localize(config)
			require.NoError(t, err)
			require.Equal(t, message.Fallback, english)
			spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
			require.NoError(t, err)
			require.NotEqual(t, english, spanish)
			pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
			require.NoError(t, err)
			require.Contains(t, pseudo, "[!! "+english+" !!]")
		})
	}
}

func TestStorageTransactionHiddenEdgeIdentityConflictRendering(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, locale := range []string{"en-US", "es-ES", "en-XA"} {
		_, err := bundle.LoadMessageFileFS(catalogFS, "catalog/active.storagevalidation."+locale+".yaml")
		require.NoError(t, err)
	}
	message := StorageTransactionHiddenEdgeIdentityConflict("r")
	require.Equal(t, MessageStorageTransactionHiddenEdgeIdentityConflict, message.ID)
	require.Equal(t, "stored hidden relationship r has a different final image", message.Fallback)
	config := &i18n.LocalizeConfig{MessageID: string(message.ID), TemplateData: message.Data}
	english, err := i18n.NewLocalizer(bundle, "en-US").Localize(config)
	require.NoError(t, err)
	require.Equal(t, message.Fallback, english)
	spanish, err := i18n.NewLocalizer(bundle, "es-ES").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "la relación oculta almacenada r tiene una imagen final diferente", spanish)
	pseudo, err := i18n.NewLocalizer(bundle, "en-XA").Localize(config)
	require.NoError(t, err)
	require.Equal(t, "[!! "+english+" !!]", pseudo)
}
