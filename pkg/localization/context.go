package localization

import (
	"context"

	"golang.org/x/text/language"
)

type preferencesContextKey struct{}

// WithPreferences returns a context carrying ordered language preferences.
func WithPreferences(ctx context.Context, preferences ...language.Tag) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	copyOfPreferences := append([]language.Tag(nil), preferences...)
	return context.WithValue(ctx, preferencesContextKey{}, copyOfPreferences)
}

// PreferencesFromContext returns ordered language preferences from ctx.
func PreferencesFromContext(ctx context.Context) []language.Tag {
	if ctx == nil {
		return nil
	}
	preferences, _ := ctx.Value(preferencesContextKey{}).([]language.Tag)
	return append([]language.Tag(nil), preferences...)
}
