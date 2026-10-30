// Package lifecycle provides NornicDB's canonical process supervisor.
//
// The package is deliberately a leaf in the import graph — it depends only
// on the Go standard library and golang.org/x/sync/errgroup. It never
// imports observability, storage, Cypher, Bolt, server, or any other
// NornicDB business package. This separation lets every current and future
// NornicDB binary (cmd/nornicdb/, planned cmd/metrics-doc-gen/, future CLI
// utilities) reuse the same supervision discipline without pulling in the
// OpenTelemetry SDK or Prometheus registry.
//
// # Architecture
//
// A Component is anything that can be started and shut down. Run takes
// the parent context plus a forward-ordered slice of components and:
//
//  1. Wraps the parent in signal.NotifyContext(SIGINT, SIGTERM) so any
//     OS signal cleanly cancels the supervisor (replaces the legacy
//     channel-leak-prone signal.Notify(chan)+<-chan idiom).
//  2. Drives every Component.Start on a single golang.org/x/sync/errgroup,
//     so the FIRST non-nil Start error cancels the whole group.
//  3. After every Start has returned, drains components in REVERSE
//     registration order (the OBS-08 contract — last to start, first to
//     stop).
//  4. Drives every Shutdown on a FRESH context.WithTimeout(
//     context.Background(), ShutdownTimeout). This is the OBS-09 keystone:
//     deriving from the cancelled errgroup ctx would return immediately
//     and break the 30s flush budget.
//  5. Returns errors.Join(runErr, shutdownErr) so callers see both the
//     trigger and any cleanup failures.
//
// # Wiring contract
//
// Callers register components in startup order. Reverse-order shutdown is
// the OBS-08 mechanism — components like the telemetry/metrics listener
// (started first) drain LAST so kubelet keeps scraping during graceful
// shutdown, while the application HTTP edge (started last) drains FIRST.
//
// See ADR-0001 §2.8.1 (amendments A10a + A10c) for the architectural
// contract this package implements.
package lifecycle
