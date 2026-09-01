package observability

import (
	"bytes"
	"errors"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapLogEventPreservesEnglishAndAddsCanonicalAttrs(t *testing.T) {
	var output bytes.Buffer
	logger := log.New(&output, "", 0)

	logBootstrapEvent(logger, bootstrapSpanExporterInitFailed, errors.New("dial failed"))

	require.Equal(t,
		"WARN observability: span exporter init failed: dial failed; installing noop tracer provider — process continues event_id=\"observability.tracing.span_exporter_init_failed\" component=\"observability\" error=\"dial failed\"\n",
		output.String(),
	)
}

func TestBootstrapLogEventsHaveStableDescriptors(t *testing.T) {
	tests := []struct {
		event bootstrapLogEvent
		id    string
		level string
	}{
		{bootstrapInstanceIDResolved, "observability.resource.instance_id_resolved", "INFO"},
		{bootstrapOTLPPlaintextRejected, "observability.tracing.otlp_plaintext_rejected", "WARN"},
		{bootstrapSpanExporterInitFailed, "observability.tracing.span_exporter_init_failed", "WARN"},
		{bootstrapSamplerModeInvalid, "observability.tracing.sampler_mode_invalid", "WARN"},
		{bootstrapParentStrictUnbounded, "observability.tracing.parent_strict_unbounded", "WARN"},
	}
	for _, test := range tests {
		require.Equal(t, test.id, test.event.id)
		require.Equal(t, test.level, test.event.level)
		require.NotEmpty(t, test.event.format)
		require.NotEmpty(t, test.event.attrs)
	}
}
