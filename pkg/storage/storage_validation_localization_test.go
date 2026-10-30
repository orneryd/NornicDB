package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestStorageCatalogNamedArguments(t *testing.T) {
	cases := []struct {
		name            string
		message         localization.Message
		expectedSpanish string
		expectedPseudo  string
	}{
		{
			name:            "TestStorageLoaderCatalogsRenderNamedArguments",
			message:         localization.StorageLoaderCreatingFile("/tmp/export.json", errors.New("permission denied")),
			expectedSpanish: "al crear el archivo: permission denied",
			expectedPseudo:  "[!! creating file: permission denied !!]",
		},
		{
			name:            "TestStorageValidationCatalogsRenderNamedArguments",
			message:         localization.StorageTransactionCrossNamespace("tenant-a", "tenant-b"),
			expectedSpanish: "la transacción abarca varios espacios de nombres: fijada en \"tenant-a\", se intentó \"tenant-b\"",
			expectedPseudo:  "[!! transaction spans multiple namespaces: pinned to \"tenant-a\", attempted \"tenant-b\" !!]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
			require.NoError(t, err)

			spanish, _, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), localization.MessageRequest(tc.message))
			require.NoError(t, err)
			require.Equal(t, tc.expectedSpanish, spanish)

			pseudo, _, err := manager.Render(localization.WithPreferences(context.Background(), language.MustParse("en-XA")), localization.MessageRequest(tc.message))
			require.NoError(t, err)
			require.Equal(t, tc.expectedPseudo, pseudo)
		})
	}
}

func requireStorageValidationLocalizedError(t *testing.T, err error, messageID localization.MessageID, text string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, text)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, messageID, localizedErr.Message.ID)
	require.Equal(t, string(messageID), localizedErr.Code)
	return localizedErr
}

func TestStorageValidationLocalizedConfigurationAndPropertyErrors(t *testing.T) {
	t.Run("remote URI required", func(t *testing.T) {
		_, err := NewRemoteEngine(RemoteEngineConfig{Database: "neo4j"})
		requireStorageValidationLocalizedError(t, err, localization.MessageStorageRemoteURIRequired, "remote engine URI cannot be empty")
	})

	t.Run("remote URI is an untranslated argument", func(t *testing.T) {
		_, err := NewRemoteEngine(RemoteEngineConfig{URI: "ftp://db.example", Database: "neo4j"})
		localizedErr := requireStorageValidationLocalizedError(t, err, localization.MessageStorageRemoteURISchemeUnsupported, "unsupported remote engine URI scheme: ftp://db.example (expected bolt://, neo4j://, http://, or https://)")
		require.Equal(t, "ftp://db.example", localizedErr.Message.Data["URI"])
	})

	t.Run("nested property path remains typed", func(t *testing.T) {
		err := validatePropertiesForStorage(map[string]interface{}{"profile": []interface{}{make(chan int)}})
		localizedErr := requireStorageValidationLocalizedError(t, err, localization.MessageStoragePropertyInvalidValue, "invalid property value for key \"profile\": index 0: unsupported property value type chan int")
		require.Equal(t, "profile", localizedErr.Message.Data["Key"])

		indexErr := errors.Unwrap(localizedErr)
		requireStorageValidationLocalizedError(t, indexErr, localization.MessageStoragePropertyInvalidIndex, "index 0: unsupported property value type chan int")
	})

	t.Run("property type is an untranslated argument", func(t *testing.T) {
		err := ValidatePropertyType("7", PropertyTypeInteger)
		localizedErr := requireStorageValidationLocalizedError(t, err, localization.MessageStorageValidationExpectedType, "expected INTEGER, got string")
		require.Equal(t, "string", localizedErr.Message.Data["ActualType"])
	})
}

func TestStorageValidationLocalizedConstraintViolation(t *testing.T) {
	err := validateRelExistenceOnEdges([]*Edge{{ID: "tenant:edge-1", Type: "OWNS"}}, Constraint{
		Type:       ConstraintExists,
		Label:      "OWNS",
		Properties: []string{"since"},
	})

	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
	require.EqualError(t, err, "Constraint violation (EXISTS on OWNS.[since]): Cannot create constraint on relationship: edge tenant:edge-1 is missing required property since")
	localizedErr := requireStorageValidationLocalizedError(t, errors.Unwrap(violation), localization.MessageStorageValidationRelationshipPropertyMissing, "Cannot create constraint on relationship: edge tenant:edge-1 is missing required property since")
	require.Equal(t, "tenant:edge-1", localizedErr.Message.Data["EdgeID"])
	require.Equal(t, "since", localizedErr.Message.Data["Property"])
}

func TestStorageValidationLocalizedTransactionErrorsPreserveSentinels(t *testing.T) {
	t.Run("closed engine", func(t *testing.T) {
		engine := &BadgerEngine{}
		engine.work.CloseAdmission()
		_, err := engine.BeginTransaction()
		requireStorageValidationLocalizedError(t, err, localization.MessageStorageTransactionEngineClosed, "engine is closed")
		require.ErrorIs(t, err, ErrStorageClosed)
	})

	t.Run("cross namespace", func(t *testing.T) {
		tx := &BadgerTransaction{Status: TxStatusActive, namespace: "tenant-a"}
		err := tx.SetNamespace("tenant-b")
		localizedErr := requireStorageValidationLocalizedError(t, err, localization.MessageStorageTransactionCrossNamespace, "transaction spans multiple namespaces: pinned to \"tenant-a\", attempted \"tenant-b\"")
		require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
		require.Equal(t, "tenant-a", localizedErr.Message.Data["PinnedNamespace"])
		require.Equal(t, "tenant-b", localizedErr.Message.Data["AttemptedNamespace"])
	})

	t.Run("Badger conflict", func(t *testing.T) {
		err := commitConflictError(badger.ErrConflict)
		requireStorageValidationLocalizedError(t, err, localization.MessageStorageTransactionCommitConflict, "conflict detected: concurrent transaction modified data before commit: Transaction Conflict. Please retry")
		require.ErrorIs(t, err, ErrConflict)
		require.ErrorIs(t, err, badger.ErrConflict)
	})

	t.Run("metadata size", func(t *testing.T) {
		tx := &BadgerTransaction{Status: TxStatusActive}
		err := tx.SetMetadata(map[string]interface{}{"key": strings.Repeat("x", 2046)})
		requireStorageValidationLocalizedError(t, err, localization.MessageStorageTransactionMetadataTooLarge, "transaction metadata too large: 2049 chars (max 2048)")
	})
}
