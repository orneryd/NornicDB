package observability

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRedactingSpanProcessor_StatusAndEvents(t *testing.T) {
	for _, sink := range []string{"status-only", "events-only", "attributes-and-errors"} {
		t.Run(sink, func(t *testing.T) {
			const canary = "synthetic-r2-span-credential-91632748"
			url := "https://probe:" + canary + "@example.test/path?auth=" + canary + "&mode=read"
			projected, original := tracetest.NewSpanRecorder(), tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(
				sdktrace.WithSpanProcessor(NewRedactingSpanProcessor(projected)),
				sdktrace.WithSpanProcessor(original),
			)
			defer func() { _ = provider.Shutdown(context.Background()) }()
			_, span := provider.Tracer("test").Start(context.Background(), "diagnostic")
			if sink != "events-only" {
				span.SetStatus(codes.Error, "upstream rejected "+url)
			}
			if sink != "status-only" {
				span.RecordError(errors.New("upstream rejected " + url))
				span.AddEvent("upstream", trace.WithTimestamp(time.Unix(42, 0)), trace.WithAttributes(
					attribute.String("auth.token", canary),
					attribute.StringSlice("upstream.urls", []string{url, "https://example.test/public?mode=read"}),
					attribute.Int("http.status_code", 401),
				))
			}
			if sink == "attributes-and-errors" {
				span.SetAttributes(attribute.String("db.password", canary))
			}
			span.End()
			if len(projected.Ended()) != 1 || len(original.Ended()) != 1 {
				t.Fatal("span was not forwarded to both processors")
			}
			safe, raw := projected.Ended()[0], original.Ended()[0]
			if safe.Status().Code != raw.Status().Code || strings.Contains(safe.Status().Description, canary) {
				t.Error("status projection lost its code or disclosed a credential")
			}
			if sink != "events-only" && !strings.Contains(raw.Status().Description, canary) {
				t.Error("projection mutated the original SDK status")
			}
			if len(safe.Events()) != len(raw.Events()) {
				t.Fatal("event projection changed the event count")
			}
			for i, event := range safe.Events() {
				before := raw.Events()[i]
				if event.Name != before.Name || event.Time != before.Time || event.DroppedAttributeCount != before.DroppedAttributeCount {
					t.Error("event identity or timing changed")
				}
				for j, attr := range event.Attributes {
					if strings.Contains(attr.Value.Emit(), canary) {
						t.Error("event disclosed a credential; capture withheld")
					}
					if string(attr.Key) == "exception.type" && attr != before.Attributes[j] {
						t.Error("exception type changed")
					}
					if string(attr.Key) == "upstream.urls" {
						urls := attr.Value.AsStringSlice()
						if len(urls) != 2 || urls[1] != "https://example.test/public?mode=read" || !strings.Contains(before.Attributes[j].Value.Emit(), canary) {
							t.Error("array projection changed public data or mutated the SDK event")
						}
					}
					if string(attr.Key) == "http.status_code" && attr.Value.AsInt64() != 401 {
						t.Error("event status code changed")
					}
				}
			}
		})
	}
}
