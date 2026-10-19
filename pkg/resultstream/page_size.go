package resultstream

import "github.com/orneryd/nornicdb/pkg/localization"

// DefaultMaxPageSize is the maximum continuation page size unless configured.
const DefaultMaxPageSize = 500

// PageSizeExceededError distinguishes an oversized page from malformed input.
// It retains ErrInvalidPageSize identity for existing embedded callers.
type PageSizeExceededError struct {
	*localization.LocalizedError
	MaxPageSize int
}

// Unwrap preserves the localized status as well as ErrInvalidPageSize.
func (e *PageSizeExceededError) Unwrap() error { return e.LocalizedError }

// BoltErrorCode exposes the argument status through the shared status mapper.
func (e *PageSizeExceededError) BoltErrorCode() string { return e.Code }

// MaxPageSize reports the effective limit enforced by this registry.
func (r *Registry) MaxPageSize() int { return r.config.MaxPageSize }

func (r *Registry) pageSizeExceeded() error {
	return &PageSizeExceededError{
		LocalizedError: localization.NewLocalizedError("Neo.ClientError.Statement.ArgumentError",
			localization.SearchPageSizeExceeded(r.MaxPageSize()), ErrInvalidPageSize),
		MaxPageSize: r.MaxPageSize(),
	}
}
