// Package localizationboundary preserves the fallback policies of protocol
// boundaries while catalog lookup and language matching remain in localization.
package localizationboundary

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

// NilFallback selects the existing text contract when no manager is configured.
type NilFallback uint8

const (
	FallbackOnly NilFallback = iota
	FallbackOrID
)

// RenderFailure selects the existing boundary's response to a catalog error.
type RenderFailure uint8

const (
	LiteralFallback RenderFailure = iota
	SourceEnglish
)

// Render uses the configured immutable manager and the caller's preferences.
// The two policies preserve boundary-specific nil-manager and error behavior;
// they do not alter catalog resolution or the message's diagnostic data.
func Render(ctx context.Context, manager *localization.Manager, message localization.Message, absent NilFallback, failure RenderFailure) (string, language.Tag) {
	if manager == nil {
		text := message.Fallback
		if text == "" && absent == FallbackOrID {
			text = string(message.ID)
		}
		return text, language.AmericanEnglish
	}
	text, tag, err := manager.Render(ctx, localization.MessageRequest(message))
	if err == nil {
		return text, tag
	}
	if failure == SourceEnglish {
		text = manager.MustRenderEnglish(message)
	} else {
		text = message.Fallback
	}
	return text, language.AmericanEnglish
}
