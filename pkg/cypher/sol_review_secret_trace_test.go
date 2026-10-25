package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSolReviewCompositePasswordTrace(t *testing.T) {
	const credential = "synthetic-sol-composite-password-2394876251"
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(observability.NewRedactingSpanProcessor(recorder)))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) }()
	base := newTestMemoryEngine(t)
	cfg := multidb.DefaultConfig()
	cfg.RemoteCredentialEncryptionKey = "synthetic-sol-metadata-encryption-781624"
	manager, err := multidb.NewDatabaseManager(base, cfg)
	if err != nil {
		t.Fatal("manager init failed")
	}
	executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "system"))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	query := "CREATE COMPOSITE DATABASE sol_trace ALIAS remote FOR DATABASE nornic AT 'http://127.0.0.1:1' USER 'probe' PASSWORD '" + credential + "' TYPE remote ACCESS read"
	_, err = executor.Execute(context.Background(), query, nil)
	if config.IsANTLRParser() {
		// The strict grammar accepts local composite aliases but not the
		// remote AT/USER/PASSWORD extension. Rejection must still export a
		// redacted query and must not create the composite database.
		var syntax *SemanticError
		if !errors.As(err, &syntax) || syntax.Code != "Neo.ClientError.Statement.SyntaxError" || syntax.Detail != "UnexpectedSyntax" {
			t.Fatal("remote composite did not return the expected ANTLR syntax error; details withheld")
		}
		if manager.IsCompositeDatabase("sol_trace") {
			t.Fatal("ANTLR rejected the statement after creating the composite database")
		}
	} else if err != nil {
		t.Fatal("composite configuration did not execute; details withheld")
	} else if !manager.IsCompositeDatabase("sol_trace") {
		t.Fatal("successful statement did not create the composite database")
	}
	count := 0
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if string(attr.Key) == "cypher.query" {
				count++
			}
			if strings.Contains(attr.Value.Emit(), credential) {
				t.Error("exported span disclosed the configured remote password; value withheld")
			}
		}
	}
	if count == 0 {
		t.Error("execute span was not exported")
	}
}
