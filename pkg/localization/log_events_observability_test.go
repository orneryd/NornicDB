package localization

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestObservabilityTenantLabelsResolvedEvent(t *testing.T) {
	event := ObservabilityTenantLabelsResolvedEvent(true, "k8s_detected", true, true)
	require.Equal(t, EventObservabilityTenantLabelsResolved, event.ID)
	require.Equal(t, MessageObservabilityLogTenantLabelsResolved, event.Message.ID)
	require.Equal(t, "observability", event.Attrs[0].Value.String())
	require.True(t, event.Attrs[1].Value.Bool())
	require.Equal(t, "k8s_detected", event.Attrs[2].Value.String())
	require.True(t, event.Attrs[3].Value.Bool())
	require.True(t, event.Attrs[4].Value.Bool())

	manager, err := NewManager(nil, slog.Default())
	require.NoError(t, err)
	for _, locale := range []struct {
		tag  language.Tag
		want string
	}{
		{language.AmericanEnglish, "resolved tenant labels enabled"},
		{language.EuropeanSpanish, "se resolvió la activación de etiquetas de inquilino"},
		{language.MustParse("en-XA"), "[!! resolved tenant labels enabled !!]"},
	} {
		got, _, renderErr := manager.Render(WithPreferences(context.Background(), locale.tag), event.Message)
		require.NoError(t, renderErr)
		require.Equal(t, locale.want, got)
	}
}
