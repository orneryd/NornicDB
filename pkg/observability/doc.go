// Package observability is NornicDB's telemetry seam: metrics (Prometheus +
// OTel), traces (OTLP), and the building blocks for /metrics, /livez,
// /readyz, /version, and opt-in pprof endpoints.
//
// # Architecture
//
// observability is a leaf package. It depends on stdlib, OpenTelemetry SDK,
// prometheus/client_golang, and pkg/lifecycle (one-way; pkg/lifecycle has zero
// observability deps). Business packages (cypher, storage, bolt, server) MUST
// NOT import this package directly — they receive instrumentation via accessors
// passed through dependency injection from cmd/nornicdb/main.go.
//
// # Wiring
//
// Construct a *Provider via:
//
//	prov, err := observability.New(ctx, cfg.Observability, observability.ServiceInfo{
//	    Name:    "nornicdb",
//	    Version: buildinfo.Version(),
//	    NodeID:  cfg.NodeID,
//	})
//
// New always succeeds at the Provider-construction level: a misconfigured or
// unreachable OTLP collector is reported via WARN log and a noop tracer
// provider is installed (OBS-11). Process startup never fails because of
// observability init failure.
//
// # Init order (OBS-03)
//
// New constructs in this order, mandated by the ADR-0001 §2.5 contract:
//
//  1. logger (deferred to Phase 2 slog migration; Phase 1 uses stdlib log).
//  2. resource attributes (semconv.ServiceName/Version/InstanceID).
//  3. meter + tracer providers (real SDK or noop fallback).
//  4. registries (the Prometheus registry and the OTel→Prom bridge).
//  5. middleware + listeners (Plan 03 — listener.go, health.go, pprof.go).
//
// # Endpoint precedence (OBS-12)
//
// OTLP endpoint resolution honors:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT > YAML tracing.endpoint > built-in default
//
// See TracingConfig.OTLPEndpoint().
//
// # service.instance.id resolution (OBS-10)
//
// resolveInstanceID resolves through this chain:
//
//	cfg.NodeID → POD_NAME env → os.Hostname() → "standalone"
//
// The resolved value and its source are logged once at startup.
//
// # Compliance boundary
//
// pkg/audit (Phase-2 untouched) is NOT migrated to OTel logs. The compliance
// audit trail keeps its existing retention/signing/serialization. This package
// only owns observability — operator-facing telemetry — not compliance.
//
// # Test isolation
//
// Each test owns a fresh *prometheus.Registry, an in-memory span exporter, and
// a discard logger. Never touch prometheus.DefaultRegisterer or slog.SetDefault
// in tests. The NewTestEnv helper that codifies this ships in Plan 03.
package observability
