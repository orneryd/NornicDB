package localization

import (
	"context"

	"golang.org/x/text/language"
)

// LocalizedError carries stable error identity and a localizable message separately.
type LocalizedError struct {
	Code    string
	Message Message
	Cause   error
}

// NewLocalizedError constructs an error that preserves message identity and wrapping.
func NewLocalizedError(code string, message Message, cause error) *LocalizedError {
	return &LocalizedError{Code: code, Message: message, Cause: cause}
}

// Error returns the source English fallback for backward compatibility.
func (e *LocalizedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message.Fallback != "" {
		return e.Message.Fallback
	}
	return string(e.Message.ID)
}

// Unwrap preserves errors.Is and errors.As behavior for the underlying cause.
func (e *LocalizedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Render localizes the human-readable message at a boundary.
func (e *LocalizedError) Render(ctx context.Context, manager *Manager) (string, language.Tag, error) {
	if e == nil {
		return "", language.AmericanEnglish, nil
	}
	if manager == nil {
		return e.Error(), language.AmericanEnglish, nil
	}
	return manager.Render(ctx, e.Message)
}
