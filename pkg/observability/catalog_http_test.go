package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: this file references NewHTTPMetrics which Plan 04-02
// will deliver. Until then, `go test ./pkg/observability/...` compile-fails
// citing `undefined: NewHTTPMetrics`. That failure IS the artifact, mirroring
// the Phase 1/2/3 RED-first cadence (CONTEXT D-01a).

// TestHTTPMetrics_RegistersFiveFamilies asserts the five HTTP families per
// MET-06: requests_total, request_duration_seconds, in_flight_requests,
// request_body_bytes, response_body_bytes. Closed enum + label discipline
// per Phase 3 D-03 (forbidden labels: path, query — only path_template).
func TestHTTPMetrics_RegistersFiveFamilies(t *testing.T) {
	t.Skip("RED: pending Plan 04-02 (HTTP bag delivery)")

	te := NewTestEnv(t)
	bag := NewHTTPMetrics(te.Registry, false /* tenantLabelsEnabled */)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_http_requests_total",
		"nornicdb_http_request_duration_seconds",
		"nornicdb_http_in_flight_requests",
		"nornicdb_http_request_body_bytes",
		"nornicdb_http_response_body_bytes",
	} {
		assert.Contains(t, names, want, "MET-06: HTTP family %q must register", want)
	}
}
