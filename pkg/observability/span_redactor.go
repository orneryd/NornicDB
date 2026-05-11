package observability

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SensitiveKeys is the set of span attribute keys that must be redacted before
// export (SEC-01). Shared with pkg/audit via this exported variable so the
// redaction list has a single source of truth.
var SensitiveKeys = map[string]bool{
	"auth.token":         true,
	"auth.password":      true,
	"auth.credentials":   true,
	"db.password":        true,
	"bolt.auth_token":    true,
	"http.authorization": true,
	"user.password":      true,
	"user.token":         true,
	"credentials":        true,
	"password":           true,
	"secret":             true,
	"api_key":            true,
	"access_token":       true,
	"refresh_token":      true,
	"session_token":      true,
	"private_key":        true,
}

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
	r.inner.OnEnd(newRedactedSpan(s))
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
	lower := strings.ToLower(key)
	if SensitiveKeys[lower] {
		return true
	}
	for sensitive := range SensitiveKeys {
		if strings.Contains(lower, sensitive) {
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
		} else {
			filtered = append(filtered, a)
		}
	}
	return &redactedSpan{ReadOnlySpan: s, attrs: filtered}
}

func (r *redactedSpan) Attributes() []attribute.KeyValue {
	return r.attrs
}
