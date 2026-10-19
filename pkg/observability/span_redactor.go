package observability

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SensitiveKeys is a compatibility view of the canonical built-in names.
// Register custom span keys before constructing processors; each processor
// snapshots the registrations using the canonical name/alias policy.
// Deprecated: use IsSensitiveFieldName for aliases and compound field names.
var SensitiveKeys = func() map[string]bool {
	keys := make(map[string]bool, len(DefaultRedactKeys))
	for _, key := range DefaultRedactKeys {
		keys[key] = true
	}
	return keys
}()

// redactingSpanProcessor projects attributes, status and events before
// forwarding spans to the inner processor. The SDK snapshot is never mutated.
type redactingSpanProcessor struct {
	inner sdktrace.SpanProcessor
	names *sensitiveNames
}

// NewRedactingSpanProcessor wraps inner with diagnostic redaction (SEC-01).
func NewRedactingSpanProcessor(inner sdktrace.SpanProcessor) sdktrace.SpanProcessor {
	return &redactingSpanProcessor{inner: inner, names: spanSensitiveNames()}
}

func spanSensitiveNames() *sensitiveNames {
	keys := defaultRedactSet()
	for key, sensitive := range SensitiveKeys {
		if sensitive {
			keys[key] = struct{}{}
		}
	}
	return newSensitiveNames(keys)
}

func (r *redactingSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	r.inner.OnStart(parent, s)
}

func (r *redactingSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if needsRedaction(s, r.names) {
		r.inner.OnEnd(newRedactedSpan(s, r.names))
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
	return isSensitiveKeyName(key, spanSensitiveNames())
}

// needsRedaction leaves clean snapshots unchanged without allocating.
func needsRedaction(s sdktrace.ReadOnlySpan, names *sensitiveNames) bool {
	for _, a := range s.Attributes() {
		if spanAttributeNeedsRedaction(a, names) {
			return true
		}
	}
	if text := s.Status().Description; RedactText(text) != text {
		return true
	}
	for _, event := range s.Events() {
		if RedactText(event.Name) != event.Name {
			return true
		}
		for _, a := range event.Attributes {
			if spanAttributeNeedsRedaction(a, names) {
				return true
			}
		}
	}
	return false
}

func spanAttributeNeedsRedaction(a attribute.KeyValue, names *sensitiveNames) bool {
	if isSensitiveKeyName(string(a.Key), names) {
		return true
	}
	switch a.Value.Type() {
	case attribute.STRING:
		return RedactText(a.Value.AsString()) != a.Value.AsString()
	case attribute.STRINGSLICE:
		for _, text := range a.Value.AsStringSlice() {
			if RedactText(text) != text {
				return true
			}
		}
	}
	return false
}

// redactedSpan changes only display fields; codes, event identity/timestamps,
// exception types and the rest of the SDK snapshot retain their contracts.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
	attrs  []attribute.KeyValue
	status sdktrace.Status
	events []sdktrace.Event
}

func newRedactedSpan(s sdktrace.ReadOnlySpan, names *sensitiveNames) *redactedSpan {
	status := s.Status()
	status.Description = RedactText(status.Description)
	events := append([]sdktrace.Event(nil), s.Events()...)
	for i := range events {
		events[i].Name = RedactText(events[i].Name)
		events[i].Attributes = redactSpanAttributes(events[i].Attributes, names)
	}
	return &redactedSpan{ReadOnlySpan: s, attrs: redactSpanAttributes(s.Attributes(), names), status: status, events: events}
}

func redactSpanAttributes(orig []attribute.KeyValue, names *sensitiveNames) []attribute.KeyValue {
	filtered := make([]attribute.KeyValue, 0, len(orig))
	for _, a := range orig {
		if isSensitiveKeyName(string(a.Key), names) {
			filtered = append(filtered, attribute.String(string(a.Key), "REDACTED"))
		} else if a.Value.Type() == attribute.STRING {
			filtered = append(filtered, attribute.String(string(a.Key), RedactText(a.Value.AsString())))
		} else if a.Value.Type() == attribute.STRINGSLICE {
			values := append([]string(nil), a.Value.AsStringSlice()...)
			for i, text := range values {
				values[i] = RedactText(text)
			}
			filtered = append(filtered, attribute.StringSlice(string(a.Key), values))
		} else {
			filtered = append(filtered, a)
		}
	}
	return filtered
}

func (r *redactedSpan) Attributes() []attribute.KeyValue {
	return r.attrs
}

func (r *redactedSpan) Status() sdktrace.Status { return r.status }

func (r *redactedSpan) Events() []sdktrace.Event { return r.events }
