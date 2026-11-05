package observability

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestCredentialStructuralErrorProjection(t *testing.T) {
	const source = "credential projection fixture 841276"
	cause := errors.New(source)
	message := localization.CypherAdminInvalidIdentifier("`" + source + "`")
	original := localization.NewLocalizedError(string(message.ID), message, cause)
	projected := RedactErrorDetails(original)
	require.ErrorIs(t, projected, cause)
	var localized *localization.LocalizedError
	require.ErrorAs(t, projected, &localized)
	require.Equal(t, message.ID, localized.Message.ID)
	for _, output := range []string{projected.Error(), localized.Message.Fallback, fmt.Sprint(localized.Message.Data)} {
		require.False(t, strings.Contains(output, source), "source details withheld")
	}
	require.Contains(t, original.Error(), source, "projection must not mutate the causal error")
}

func TestCredentialStructuralProjectionUsesCatalogText(t *testing.T) {
	const secret = "opaque-password-fixture-29471"
	message := localization.CypherCompositeConstituentUserPasswordRequired()
	want := message.Fallback
	message.Fallback = secret
	cause := errors.New(secret)
	original := localization.NewLocalizedError(string(message.ID), message, cause)
	projected := RedactErrorDetails(fmt.Errorf("%s: %w", secret, original))
	require.Equal(t, want, projected.Error())
	require.ErrorIs(t, projected, cause)
	var localized *localization.LocalizedError
	require.ErrorAs(t, projected, &localized)
	require.Equal(t, want, localized.Message.Fallback)
	require.Equal(t, secret, original.Message.Fallback, "source must remain unchanged")

	unknown := localization.NewLocalizedError("unknown", localization.Message{ID: "unknown", Fallback: secret}, nil)
	require.Equal(t, "<REDACTED>", RedactErrorDetails(unknown).Error())
}

func TestCredentialStructuralProjectionSuppressesStructuredData(t *testing.T) {
	const secret = "structured-password-fixture-29573"
	message := localization.Message{
		ID:          localization.MessageCypherCompositeConstituentUserPasswordRequired,
		Fallback:    secret,
		Data:        map[string]any{"Nested": map[string]any{"Value": secret}},
		PluralCount: secret,
	}
	original := localization.NewLocalizedError(string(message.ID), message, nil)
	projected := RedactErrorDetails(original)
	var localized *localization.LocalizedError
	require.ErrorAs(t, projected, &localized)
	for _, text := range []string{projected.Error(), fmt.Sprint(localized.Message)} {
		require.NotContains(t, text, secret)
	}
	require.Contains(t, fmt.Sprint(original.Message), secret)
}
