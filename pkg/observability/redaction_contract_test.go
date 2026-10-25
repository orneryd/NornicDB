package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSensitiveKeysCompatibility(t *testing.T) {
	for _, name := range []string{"auth.token", "auth.password", "auth.credentials", "db.password", "bolt.auth_token", "http.authorization", "user.password", "user.token", "credentials", "password", "secret", "api_key", "access_token", "refresh_token", "session_token", "private_key"} {
		if !SensitiveKeys[name] || !IsSensitiveFieldName(name) {
			t.Errorf("compatibility entry %s is missing from the canonical policy view", name)
		}
	}
}

func TestDisplayPreservesUnsetSecrets(t *testing.T) {
	for _, name := range []string{"api_key", "db.nornic.embedding.api.key", "auth.password", "encryption_audit_sign_key"} {
		if DisplayValue(name, "") != "" {
			t.Error("unset configuration secret became set in its display")
		}
	}
}

func TestAuditSigningKeyDisplay(t *testing.T) {
	const credential = "synthetic-audit-sign-credential-93182746"
	names := []string{"sign_key", "audit_sign_key", "encryption_audit_sign_key", "EncryptionAuditSignKey", "AuditSigningKey", "NORNICDB_ENCRYPTION_AUDIT_SIGN_KEY"}
	var output bytes.Buffer
	logger := slog.New(newRedactingHandler(slog.NewJSONHandler(&output, nil), defaultRedactSet()))
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(recorder)))
	_, span := provider.Tracer("audit-display").Start(context.Background(), "probe")
	for _, name := range names {
		if !IsSensitiveFieldName(name) || DisplayValue(name, credential) == credential {
			t.Error("audit signing key escaped the display classifier")
		}
		logger.Info("probe", name, credential)
		span.SetAttributes(attribute.String(name, credential))
	}
	span.End()
	_ = provider.Shutdown(context.Background())
	if strings.Contains(output.String(), credential) {
		t.Error("audit signing key reached a log; output withheld")
	}
	for _, ended := range recorder.Ended() {
		for _, attr := range ended.Attributes() {
			if strings.Contains(attr.Value.Emit(), credential) {
				t.Error("audit signing key reached a span; output withheld")
			}
		}
	}
	for _, name := range []string{"signature", "audit_signature", "audit_sign_events", "signing_key_id", "sign_key_path", "signing_key_ref"} {
		if IsSensitiveFieldName(name) || DisplayValue(name, "public-metadata") != "public-metadata" {
			t.Error("signing metadata was over-redacted")
		}
	}
}

func TestDisplayFlagDefaults(t *testing.T) {
	for _, value := range []string{"", "synthetic-flag-default-credential"} {
		if DisplayFlagDefault("api-key", value) != RedactedPlaceholder() {
			t.Error("secret flag default was not hidden")
		}
	}
	for _, value := range []string{"", "7474"} {
		if DisplayFlagDefault("http-port", value) != value {
			t.Error("ordinary flag default changed")
		}
	}
}
