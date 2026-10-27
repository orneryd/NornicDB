package localization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestRenderRequestPreservesNilBoundaryPolicies(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	source := Message{ID: "synthetic.id", Fallback: "source fallback", Data: map[string]any{"unused": "must not interpolate"}}
	idOnly := Message{ID: "synthetic.id"}
	for _, tc := range []struct {
		name    string
		manager *Manager
		request RenderRequest
		text    string
		failure bool
	}{
		{"nil error nil manager", nil, ErrorRequest(nil), "", false},
		{"nil error present manager", manager, ErrorRequest(nil), "", false},
		{"message fallback nil manager", nil, MessageRequest(source), source.Fallback, false},
		{"message ID nil manager", nil, MessageRequest(idOnly), string(idOnly.ID), true},
		{"error fallback nil manager", nil, ErrorRequest(NewLocalizedError("code", source, nil)), source.Fallback, false},
		{"error ID nil manager", nil, ErrorRequest(NewLocalizedError("code", idOnly, nil)), string(idOnly.ID), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, tag, err := tc.manager.Render(context.Background(), tc.request)
			require.Equal(t, tc.text, text)
			require.Equal(t, language.AmericanEnglish, tag)
			if tc.failure {
				require.EqualError(t, err, "localization manager is nil")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRenderRequestSharesCatalogWithoutChangingErrorIdentity(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	cause := errors.New("synthetic underlying cause")
	message := DatabaseNotFound("analytics")
	localized := NewLocalizedError("code", message, cause)
	for _, tag := range []language.Tag{language.AmericanEnglish, language.EuropeanSpanish, language.MustParse("en-XA")} {
		ctx := WithPreferences(context.Background(), tag)
		text, wantTag, err := manager.Render(ctx, MessageRequest(message))
		require.NoError(t, err)
		actual, actualTag, err := manager.Render(ctx, ErrorRequest(localized))
		require.NoError(t, err)
		require.Equal(t, text, actual)
		require.Equal(t, wantTag, actualTag)
	}
	require.ErrorIs(t, localized, cause)
	require.Equal(t, "Database 'analytics' does not exist", localized.Error())
	require.Equal(t, message, localized.Message)
}

func TestRenderRequestKeepsSharedDiagnosticsImmutable(t *testing.T) {
	manager, err := NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := CypherANTLRParseSyntaxError("source detail", CypherANTLRUnexpectedInput(2, 7), CypherANTLRUnexpectedInput(3, 8))
	carrier := NewLocalizedError("syntax", message, nil)
	original := MessageSequence{Messages: []Message{CypherANTLRUnexpectedInput(2, 7), CypherANTLRUnexpectedInput(3, 8)}, Separator: "; "}
	t.Cleanup(func() { require.Equal(t, original, message.Data["Detail"]); require.Equal(t, message, carrier.Message) })
	for _, test := range []struct{ locale, diagnostic string }{{"en-US", "line 2:7"}, {"es-ES", "línea 2:7"}, {"en-XA", "line 2:7"}} {
		t.Run(test.locale, func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 8; i++ {
				text, _, err := manager.Render(WithPreferences(t.Context(), language.MustParse(test.locale)), ErrorRequest(carrier))
				require.NoError(t, err)
				require.Contains(t, text, test.diagnostic)
			}
		})
	}
}

func TestRenderRequestRetainsCaptureAndDirectSourceContracts(t *testing.T) {
	var manager *Manager
	source := Message{ID: "capture.id", Fallback: "original"}
	captured := MessageRequest(source)
	source.Fallback = "changed caller variable"
	text, _, err := manager.Render(t.Context(), captured)
	require.NoError(t, err)
	require.Equal(t, "original", text)
	carrier := NewLocalizedError("identity", Message{ID: "error.id", Fallback: "before"}, nil)
	errorRequest := ErrorRequest(carrier)
	carrier.Message.Fallback = "after"
	text, _, err = manager.Render(t.Context(), errorRequest)
	require.NoError(t, err)
	require.Equal(t, "after", text)
	text, tag, err := manager.Render(t.Context(), RenderRequest{})
	require.Equal(t, "", text)
	require.Equal(t, language.AmericanEnglish, tag)
	require.EqualError(t, err, "localization manager is nil")
	text, tag, err = manager.Render(t.Context(), RenderRequest{Purpose: RenderPurpose(255)})
	require.Equal(t, "", text)
	require.Equal(t, language.AmericanEnglish, tag)
	require.EqualError(t, err, "invalid localization render purpose 255")
}
