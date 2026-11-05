// Package resultstream provides durable, position-addressed result streams.
package resultstream

import (
	"context"
	"errors"
	"time"
)

var (
	ErrClosed          = errors.New("result stream is closed")
	ErrInvalidPageSize = errors.New("invalid result stream page size")
	ErrInvalidPosition = errors.New("invalid result stream position")
	ErrInvalidated     = errors.New("result stream was invalidated by a data or policy change")
)

// Page is an immutable view of one stream position. Rows must be treated as
// read-only by adapters.
type Page struct {
	Rows      [][]any
	QID       string
	Position  uint64
	Next      uint64
	HasMore   bool
	Total     *uint64
	ExpiresAt time.Time
	Metadata  map[string]any
}

// Stream returns deterministic pages for explicit positions. Implementations
// must permit concurrent and repeated Pull calls.
type Stream interface {
	Pull(ctx context.Context, position uint64, n int) (*Page, error)
	Close() error
}

// RetainedBytesReporter optionally reports memory retained for future pulls.
type RetainedBytesReporter interface {
	RetainedBytes() int64
}

// RetainedBytesGrowthGuard allows a registry to approve retained growth before
// an expanding stream publishes its new replayable population.
type RetainedBytesGrowthGuard interface {
	SetRetainedBytesGrowthGuard(func() bool)
}

// ValidityChecker lets the registry retire invalidated, abandoned streams
// during periodic cleanup or before rejecting a new admission for capacity.
type ValidityChecker interface {
	CheckValid() error
}
