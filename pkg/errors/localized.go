package errors

import "github.com/orneryd/nornicdb/pkg/localization"

// Localized aliases the dependency-neutral localization error carrier.
type Localized = localization.LocalizedError

// NewLocalized constructs an error that preserves message identity and wrapping.
func NewLocalized(code string, message localization.Message, cause error) *Localized {
	return localization.NewLocalizedError(code, message, cause)
}
