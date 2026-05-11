package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// piiCorpus is the SEC-04 / TEST-07 PII test corpus: a comprehensive set of
// attribute keys that MUST be redacted before export.
var piiCorpus = []struct {
	key   string
	value string
}{
	// Direct matches in SensitiveKeys
	{"auth.token", "eyJhbGciOiJIUzI1NiJ9.fake"},
	{"auth.password", "hunter2"},
	{"auth.credentials", "Basic dXNlcjpwYXNz"},
	{"db.password", "dbpass123"},
	{"bolt.auth_token", "bolt-token-xyz"},
	{"http.authorization", "Bearer secret-jwt"},
	{"user.password", "p@ssw0rd"},
	{"user.token", "user-tok-abc"},
	{"credentials", "cred-value"},
	{"password", "pass-value"},
	{"secret", "secret-value"},
	{"api_key", "sk-12345"},
	{"access_token", "at-67890"},
	{"refresh_token", "rt-abcde"},
	{"session_token", "st-fghij"},
	{"private_key", "-----BEGIN RSA PRIVATE KEY-----"},

	// Case variations
	{"AUTH.TOKEN", "upper-case-token"},
	{"Password", "title-case-pass"},
	{"API_KEY", "upper-api-key"},
	{"Secret", "title-secret"},

	// Substring matches (keys containing sensitive words)
	{"my.custom.password.field", "nested-pass"},
	{"x-api_key-header", "header-api-key"},
	{"service.access_token.v2", "service-token"},
	{"db.connection.credentials.encrypted", "enc-creds"},
}

// TestPIICorpus_AllRedacted (SEC-04/TEST-07) verifies that the full PII corpus
// is redacted by the RedactingSpanProcessor before export.
func TestPIICorpus_AllRedacted(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test-pii")

	_, span := tracer.Start(context.Background(), "pii-test")
	for _, tc := range piiCorpus {
		span.SetAttributes(attribute.String(tc.key, tc.value))
	}
	span.SetAttributes(attribute.String("safe.operation", "CREATE"))
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)
	attrs := spans[0].Attributes()

	attrMap := make(map[string]string)
	for _, a := range attrs {
		attrMap[string(a.Key)] = a.Value.AsString()
	}

	for _, tc := range piiCorpus {
		val, exists := attrMap[tc.key]
		if !exists {
			t.Errorf("PII key %q missing from span attributes", tc.key)
			continue
		}
		assert.Equal(t, "REDACTED", val,
			"PII key %q must be redacted, got %q", tc.key, val)
		assert.NotContains(t, val, tc.value,
			"original PII value for key %q must not appear in output", tc.key)
	}

	assert.Equal(t, "CREATE", attrMap["safe.operation"],
		"non-sensitive key must be preserved")
}

// TestPIICorpus_NoLeakInRawOutput (SEC-04) verifies none of the PII values
// appear anywhere in the exported span attributes.
func TestPIICorpus_NoLeakInRawOutput(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	redactor := NewRedactingSpanProcessor(inner)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(redactor))
	tracer := tp.Tracer("test-pii-noleak")

	_, span := tracer.Start(context.Background(), "pii-noleak-test")
	for _, tc := range piiCorpus {
		span.SetAttributes(attribute.String(tc.key, tc.value))
	}
	span.End()

	spans := inner.Ended()
	require.Len(t, spans, 1)

	for _, a := range spans[0].Attributes() {
		for _, tc := range piiCorpus {
			assert.NotEqual(t, tc.value, a.Value.AsString(),
				"PII value %q for key %q leaked through to attribute %q",
				tc.value, tc.key, string(a.Key))
		}
	}
}

// TestPIICorpus_BoltHelloCredentialNeverOnSpan (SEC-03) verifies the specific
// Bolt HELLO auth fields are covered by the redaction list.
func TestPIICorpus_BoltHelloCredentialNeverOnSpan(t *testing.T) {
	boltAuthKeys := []string{
		"bolt.auth_token",
		"credentials",
		"password",
		"auth.token",
		"auth.password",
		"auth.credentials",
	}

	for _, key := range boltAuthKeys {
		assert.True(t, isSensitiveKey(key),
			"Bolt auth key %q must be detected as sensitive", key)
	}
}
