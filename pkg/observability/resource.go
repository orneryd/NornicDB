package observability

import (
	"log"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// hostnameFn is overridable for tests. Default: os.Hostname.
var hostnameFn = os.Hostname

// ServiceInfo identifies the running binary for telemetry resource attrs.
//
// Multi-binary forward-compat (D-02b): future binaries (cmd/metrics-doc-gen,
// per-service binaries) override Name and Component without forking the
// resource construction code. Phase-1 callers pass Name="nornicdb".
//
// ExtraResourceAttrs are merged AFTER semconv keys via resource.Merge, which
// is last-wins. A caller setting ExtraResourceAttrs={service.name: "x"} will
// override the semconv default.
type ServiceInfo struct {
	// Name is the OTel service.name. REQUIRED.
	Name string
	// Version is the OTel service.version. REQUIRED.
	Version string
	// Component is an optional sub-component label.
	Component string
	// NodeID feeds the service.instance.id resolution chain (OBS-10).
	NodeID string
	// ClusterMode is the deployment topology (e.g. "standalone", "cluster").
	// TRC-10: emitted as nornicdb.cluster.mode resource attribute.
	ClusterMode string
	// ReplicationRole is the node's role (e.g. "primary", "replica", "standalone").
	// TRC-10: emitted as nornicdb.replication.role resource attribute.
	ReplicationRole string
	// ExtraResourceAttrs are merged after semconv defaults; duplicate keys win.
	ExtraResourceAttrs []attribute.KeyValue
}

// resolveInstanceID returns the resolved service.instance.id and the source
// of the resolution. The chain is: cfg.NodeID → POD_NAME → os.Hostname() →
// "standalone" (OBS-10).
//
// The hostname leg uses package-private hostnameFn so tests can simulate
// stripped-container environments (Pitfall 4).
func resolveInstanceID(nodeID string) (id, source string) {
	if nodeID != "" {
		return nodeID, "config"
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod, "POD_NAME"
	}
	if host, err := hostnameFn(); err == nil && host != "" {
		return host, "hostname"
	}
	return "standalone", "fallback"
}

// buildResource constructs the OTel Resource with semconv keys plus the
// caller-supplied ExtraResourceAttrs. The merge is last-wins — see ServiceInfo
// godoc.
//
// The OBS-10 startup log line is emitted here, exactly once per Provider
// construction.
func buildResource(info ServiceInfo) *resource.Resource {
	instanceID, source := resolveInstanceID(info.NodeID)
	log.Printf("INFO observability: service.instance.id=%q (resolved from %s)", instanceID, source)

	attrs := []attribute.KeyValue{
		semconv.ServiceName(info.Name),
		semconv.ServiceVersion(info.Version),
		attribute.String("service.instance.id", instanceID),
	}
	if info.Component != "" {
		attrs = append(attrs, attribute.String("service.component", info.Component))
	}
	if info.ClusterMode != "" {
		attrs = append(attrs, attribute.String("nornicdb.cluster.mode", info.ClusterMode))
	}
	if info.ReplicationRole != "" {
		attrs = append(attrs, attribute.String("nornicdb.replication.role", info.ReplicationRole))
	}

	base, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	)

	if len(info.ExtraResourceAttrs) > 0 {
		extra := resource.NewWithAttributes(semconv.SchemaURL, info.ExtraResourceAttrs...)
		merged, _ := resource.Merge(base, extra)
		return merged
	}
	return base
}
