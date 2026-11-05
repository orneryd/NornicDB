package testfixture

import "go.opentelemetry.io/otel/attribute"

// SpanAttrs projects recorded attributes to the canonical string values used by
// span contract assertions. Callers select their recorder's attribute slice.
func SpanAttrs(attributes []attribute.KeyValue) map[string]string {
	attrs := make(map[string]string, len(attributes))
	for _, a := range attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	return attrs
}
