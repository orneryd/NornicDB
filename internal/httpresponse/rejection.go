package httpresponse

import (
	"encoding/json"
	"net/http"

	"github.com/orneryd/nornicdb/internal/localizationboundary"
	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

// RejectionEnvelope selects the existing text/plain rejection body format.
type RejectionEnvelope uint8

const (
	PlainText RejectionEnvelope = iota
	JSONErrorText
)

// WriteLocalizedRejection writes an already-decided rejection. Validation and
// authentication remain with the calling middleware. JSONErrorText retains the
// auth boundary's JSON text passed through http.Error, including its newline,
// content type and nosniff behavior; it is not a normal JSON response.
func WriteLocalizedRejection(w http.ResponseWriter, r *http.Request, manager *localization.Manager, message localization.Message, status int, envelope RejectionEnvelope) {
	ctx := r.Context()
	if manager != nil {
		preferences, _, err := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
		if err == nil && len(preferences) > 0 {
			match := manager.Resolve("http", preferences...)
			ctx = localization.WithPreferences(ctx, match.Tag)
		}
	}
	text, tag := localizationboundary.Render(ctx, manager, message, localizationboundary.FallbackOnly, localizationboundary.LiteralFallback)
	if envelope == JSONErrorText {
		body, _ := json.Marshal(map[string]string{"error": text})
		text = string(body)
	}
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	http.Error(w, text, status)
}
