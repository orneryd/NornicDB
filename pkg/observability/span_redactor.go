package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SensitiveKeys is a compatibility view of the canonical built-in names.
// Deprecated: use IsSensitiveFieldName for aliases and compound field names.
var SensitiveKeys = func() map[string]bool {
	keys := make(map[string]bool, len(DefaultRedactKeys))
	for _, key := range DefaultRedactKeys {
		keys[key] = true
	}
	return keys
}()

// redactingSpanProcessor wraps an inner SpanProcessor and strips sensitive
// attributes from spans before forwarding to the inner processor.
type redactingSpanProcessor struct {
	inner sdktrace.SpanProcessor
}

// NewRedactingSpanProcessor wraps inner with attribute redaction (SEC-01).
func NewRedactingSpanProcessor(inner sdktrace.SpanProcessor) sdktrace.SpanProcessor {
	return &redactingSpanProcessor{inner: inner}
}

func (r *redactingSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	r.inner.OnStart(parent, s)
}

func (r *redactingSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if needsRedaction(s) {
		r.inner.OnEnd(newRedactedSpan(s))
	} else {
		r.inner.OnEnd(s)
	}
}

func (r *redactingSpanProcessor) Shutdown(ctx context.Context) error {
	return r.inner.Shutdown(ctx)
}

func (r *redactingSpanProcessor) ForceFlush(ctx context.Context) error {
	return r.inner.ForceFlush(ctx)
}

// isSensitiveKey checks whether a key should be redacted. Case-insensitive
// and also catches keys that contain a sensitive substring.
func isSensitiveKey(key string) bool {
	return IsSensitiveFieldName(key)
}

// needsRedaction scans attributes without allocating. Returns true only if
// at least one key is sensitive — the common case (no PII) exits early with
// zero allocations.
func needsRedaction(s sdktrace.ReadOnlySpan) bool {
	for _, a := range s.Attributes() {
		if isSensitiveKey(string(a.Key)) || (a.Value.Type() == attribute.STRING && RedactText(a.Value.AsString()) != a.Value.AsString()) {
			return true
		}
	}
	return false
}

// redactedSpan wraps a ReadOnlySpan and filters its attributes.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
	attrs []attribute.KeyValue
}

func newRedactedSpan(s sdktrace.ReadOnlySpan) *redactedSpan {
	orig := s.Attributes()
	filtered := make([]attribute.KeyValue, 0, len(orig))
	for _, a := range orig {
		if isSensitiveKey(string(a.Key)) {
			filtered = append(filtered, attribute.String(string(a.Key), "REDACTED"))
		} else if a.Value.Type() == attribute.STRING {
			filtered = append(filtered, attribute.String(string(a.Key), RedactText(a.Value.AsString())))
		} else {
			filtered = append(filtered, a)
		}
	}
	return &redactedSpan{ReadOnlySpan: s, attrs: filtered}
}

func (r *redactedSpan) Attributes() []attribute.KeyValue {
	return r.attrs
}
