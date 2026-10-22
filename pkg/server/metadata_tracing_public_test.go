package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type admissionOrderPropagator struct {
	policy    *memory.Policy
	calls     atomic.Int64
	uncharged atomic.Bool
}

func (p *admissionOrderPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	p.calls.Add(1)
	if memory.WireRequestFromContext(ctx) == nil || p.policy.Reserved() == 0 {
		p.uncharged.Store(true)
	}
	return ctx
}

func (*admissionOrderPropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (*admissionOrderPropagator) Fields() []string { return nil }

func TestHTTPRequestMetadataPrecedesProductionTracing(t *testing.T) {
	server, _ := setupTestServer(t)
	server.config.Address, server.config.Port = "127.0.0.1", 0
	policy := memory.NewPolicy(128<<10, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: 1}, nil }, time.Now)
	require.NoError(t, policy.Refresh())
	server.memoryPolicy = policy
	probe := &admissionOrderPropagator{policy: policy}
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(probe)
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	server.SetHTTPMetrics(observability.NewHTTPMetrics(prometheus.NewRegistry(), false))
	startTestHTTPServer(t, server)
	client := &http.Client{Timeout: 3 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	for _, surface := range []string{"query", "headers"} {
		t.Run(surface, func(t *testing.T) {
			path := "http://" + server.Addr() + "/graphql"
			if surface == "query" {
				path += "?token=%3C&variables=" + strings.Repeat("%78", 64<<10)
			}
			request, err := http.NewRequest(http.MethodGet, path, nil)
			require.NoError(t, err)
			if surface == "headers" {
				request.Header.Set("Baggage", strings.Repeat("x", 40<<10))
			}
			response, err := client.Do(request)
			require.NoError(t, err)
			data, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode, string(data))
			require.Zero(t, probe.calls.Load(), "production tracing must not parse rejected metadata")
			require.Zero(t, policy.Reserved())
		})
	}
	response, err := client.Get("http://" + server.Addr() + "/")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Positive(t, probe.calls.Load(), "admitted traffic still reaches the real tracing wrapper")
	require.False(t, probe.uncharged.Load())
	require.Zero(t, policy.Reserved())
}
