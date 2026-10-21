package observability

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlogBootstrapEventOmitsLevelPrefix(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))

	logSlogBootstrapEvent(logger, bootstrapSpanExporterInitFailed, errors.New("dial failed"))

	line := output.String()
	require.Contains(t, line, `msg="observability: span exporter init failed: dial failed; installing noop tracer provider — process continues"`)
	require.Contains(t, line, `event_id=observability.tracing.span_exporter_init_failed`)
	require.Contains(t, line, `component=observability`)
	require.Contains(t, line, `error="dial failed"`)
	require.NotContains(t, line, `msg="WARN observability`)
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
		require.NotNil(t, test.event.message)
		require.NotEmpty(t, test.event.attrs)
	}
}
