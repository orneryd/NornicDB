package observability

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDisplaySecretFamily(t *testing.T) {
	for _, name := range []string{"NORNICDB_EMBEDDING_API_KEY", "EmbeddingAPIKey", "db.nornic.embedding.api.key", "embedding-key", "JWTSecret", "HTTPTLSKey", "private_key", "EncryptionMasterKey", "replication-secret"} {
		if !IsSensitiveFieldName(name) {
			t.Errorf("secret alias missed: %s", name)
		}
	}
	for _, name := range []string{"max_tokens", "token_count", "token_budget", "key_id", "private_key_path", "credentials_file", "secret_ref", "embedding-enabled", "tokenizer", "http-port"} {
		if IsSensitiveFieldName(name) {
			t.Errorf("non-secret metadata hidden: %s", name)
		}
	}
	credential := "synthetic-credential/+&314159"
	raw := "https://user:" + url.PathEscape(credential) + "@example.test/api?model=probe&api_key=" + url.QueryEscape(credential)
	safe := RedactURL(raw)
	if strings.Contains(safe, "314159") || !strings.Contains(safe, "example.test/api?model=probe&api_key=") {
		t.Fatal("credential URL projection failed (value withheld)")
	}
	ordinary := "https://example.test/api?model=probe%20model&limit=3"
	if RedactURL(ordinary) != ordinary {
		t.Error("ordinary URL changed")
	}
	args := []string{"nornicdb", "serve", "--embedding-key=" + credential, "--admin-password", credential, "--embedding-url=" + raw, "--http-port", "7474"}
	before := append([]string(nil), args...)
	projected := DisplayArgs(args)
	if !reflect.DeepEqual(args, before) || projected[6] != "--http-port" || projected[7] != "7474" {
		t.Error("argv projection changed runtime or safe flags")
	}
	if strings.Contains(strings.Join(projected, "\x00"), "314159") {
		t.Error("argv disclosed synthetic credential")
	}
	for _, encoded := range []string{credential, url.QueryEscape(credential), url.PathEscape(credential), base64.StdEncoding.EncodeToString([]byte(credential))} {
		if strings.Contains(RedactText("provider: "+encoded, credential), "314159") || RedactText(encoded, credential) != RedactedPlaceholder() {
			t.Error("owned credential encoding disclosed")
		}
	}
}

func TestDisplaySecretStructuredBodiesAndURLTokens(t *testing.T) {
	const credential = "synthetic-issued-credential-314159"
	body := `{"error":{"access_token":"` + credential + `","code":401,"request_id":"probe"}}`
	safe := RedactText(body)
	if strings.Contains(safe, credential) || !strings.Contains(safe, `"code":401`) || !strings.Contains(safe, `"request_id":"probe"`) {
		t.Error("structured error projection changed metadata or disclosed a credential")
	}
	for _, raw := range []string{"https://example.test?X-Amz-Credential=" + credential, "https://example.test?AWSAccessKeyId=" + credential, "https://example.test?code=" + credential, "https://example.test#access_token=" + credential, "https://example.test?sig=" + credential} {
		if strings.Contains(RedactURL(raw), credential) {
			t.Error("URL credential alias disclosed (value withheld)")
		}
	}
}

func TestDisplaySecretErrorPreservesContracts(t *testing.T) {
	credential := "synthetic-localized-credential"
	cause := fmt.Errorf("transport: %s", credential)
	message := localization.StorageRemoteError("Neo.ClientError.Security.Unauthorized", credential)
	original := localization.NewLocalizedError("probe", message, cause)
	err := RedactError(original, credential)
	if !errors.Is(err, cause) || !errors.Is(err, original) || errors.Unwrap(err) != original {
		t.Error("cause identity changed")
	}
	var localized *localization.LocalizedError
	if !errors.As(err, &localized) || localized.Code != original.Code || localized.Message.ID != message.ID {
		t.Fatal("localized identity lost")
	}
	if strings.Contains(fmt.Sprint(localized.Message.Data), credential) || strings.Contains(localized.Error(), credential) || strings.Contains(err.Error(), credential) {
		t.Error("localized display disclosed credential")
	}
	if original.Message.Fallback != message.Fallback || original.Message.Data["Message"] != message.Data["Message"] {
		t.Error("source error mutated")
	}
}

func TestDisplaySecretSpanAliasesAndURL(t *testing.T) {
	inner := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(inner)))
	defer tp.Shutdown(context.Background())
	_, span := tp.Tracer("display").Start(context.Background(), "probe")
	span.SetAttributes(attribute.String("EmbeddingAPIKey", "synthetic-span-credential"), attribute.String("endpoint", "https://u:synthetic-span-credential@example.test/api"), attribute.Int("max_tokens", 12))
	span.End()
	for _, attr := range inner.Ended()[0].Attributes() {
		if strings.Contains(attr.Value.Emit(), "synthetic-span-credential") {
			t.Error("exported span disclosed credential")
		}
		if attr.Key == "max_tokens" && attr.Value.AsInt64() != 12 {
			t.Error("non-secret span changed")
		}
		if attr.Key == "endpoint" && !strings.Contains(attr.Value.AsString(), "example.test/api") {
			t.Error("safe URL components lost")
		}
	}
}
