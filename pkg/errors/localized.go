package errors

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

// Localized carries stable error identity and a localizable message separately.
type Localized struct {
	Code    string
	Message localization.Message
	Cause   error
}

// NewLocalized constructs an error that preserves message identity and wrapping.
func NewLocalized(code string, message localization.Message, cause error) *Localized {
	return &Localized{Code: code, Message: message, Cause: cause}
}

// Error returns the source English fallback for backward compatibility.
func (e *Localized) Error() string {
	if e == nil {
		return ""
	}
	if e.Message.Fallback != "" {
		return e.Message.Fallback
	}
	return string(e.Message.ID)
}

// Unwrap preserves errors.Is and errors.As behavior for the underlying cause.
func (e *Localized) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Render localizes the human-readable message at a boundary.
func (e *Localized) Render(ctx context.Context, manager *localization.Manager) (string, language.Tag, error) {
	if e == nil {
		return "", language.AmericanEnglish, nil
	}
	if manager == nil {
		return e.Error(), language.AmericanEnglish, nil
	}
	return manager.Render(ctx, e.Message)
}
