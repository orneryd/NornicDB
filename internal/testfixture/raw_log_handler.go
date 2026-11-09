package testfixture

import (
	"context"
	"log/slog"
)

// RawLogHandler adapts an observer callback to slog.Handler for the test
// probes that assert on the raw records an owner emits.
//
// It is a test probe, not a general-purpose production logger. Handle hands the
// emitted record and its context to the callback unchanged, Enabled accepts
// every level, and WithAttrs and WithGroup return the same raw observer:
// attributes and group names bound on a logger are intentionally discarded
// instead of being stored or applied. That preserves the existing fixtures'
// contract — every record the owner emits reaches their callback, and no
// logger-bound attribute or open group can hide or reshape it — so a probe that
// needs bound attributes or nested groups needs a different handler.
type RawLogHandler func(context.Context, slog.Record) error

// Enabled reports true for every record: the probe observes what the owner
// emits rather than filtering by level.
func (RawLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle invokes the observer callback with the emitted record.
func (handler RawLogHandler) Handle(ctx context.Context, record slog.Record) error {
	return handler(ctx, record)
}

// WithAttrs returns the same raw observer, discarding the supplied attributes.
func (handler RawLogHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }

// WithGroup returns the same raw observer, discarding the group name.
func (handler RawLogHandler) WithGroup(string) slog.Handler { return handler }
