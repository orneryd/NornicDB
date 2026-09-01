package localization

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeimdallLogEventsPreserveStableIdentityAndOpaqueFields(t *testing.T) {
	params := map[string]interface{}{"prompt": "no traducir", "limit": 3}
	event := HeimdallToolCallEvent("request-7", "search", params)

	require.Equal(t, EventID("heimdall.tool.call"), event.ID)
	require.Equal(t, "[Heimdall] Tool call: request=request-7 action=search params=map[limit:3 prompt:no traducir]", event.Message.Fallback)
	require.Equal(t, params, event.Attrs[2].Value.Resolve().Any())

	operatorEvent := HeimdallOperatorEvent("[Bifrost] Request cancelled by %s: %s", "plugin-a", "policy")
	require.Equal(t, EventHeimdallOperator, operatorEvent.ID)
	require.Equal(t, "[Bifrost] Request cancelled by plugin-a: policy", operatorEvent.Message.Fallback)
	require.Equal(t, "plugin-a", operatorEvent.Attrs[2].Value.Resolve().Any())
}

func TestInferenceLogEventsPreserveStableIdentityAndExactEnglish(t *testing.T) {
	tests := []struct {
		event LogEvent
		id    EventID
		text  string
	}{
		{InferenceEdgeDecayStartedEvent(0.95, 0.3, time.Hour, 7*24*time.Hour), "inference.edge_decay.started", "[EDGE-DECAY] Started | decay_rate=0.95 min_conf=0.30 scan_interval=1h0m0s grace=168h0m0s"},
		{InferenceHeimdallBatchErrorEvent(errors.New("backend unavailable")), "inference.heimdall.batch_error", "[HEIMDALL] ⚠️ Batch error, fail-open: backend unavailable"},
	}

	for _, test := range tests {
		require.Equal(t, test.id, test.event.ID)
		require.Equal(t, test.text, test.event.Message.Fallback)
	}

	operatorEvent := InferenceOperatorEvent("[EDGE-DECAY] DRY-RUN: Would delete %d edges", 4)
	require.Equal(t, EventInferenceOperator, operatorEvent.ID)
	require.Equal(t, "[EDGE-DECAY] DRY-RUN: Would delete 4 edges", operatorEvent.Message.Fallback)
	require.Equal(t, int64(4), operatorEvent.Attrs[2].Value.Resolve().Any())
}

func TestObservabilityLogEventsPreserveStableIdentityAndMachineFields(t *testing.T) {
	event := ObservabilityInstanceIDResolvedEvent("node-7", "config")
	require.Equal(t, EventObservabilityInstanceIDResolved, event.ID)
	require.Equal(t, `INFO observability: service.instance.id="node-7" (resolved from config)`, event.Message.Fallback)
	require.Equal(t, "node-7", event.Attrs[1].Value.String())
	require.Equal(t, "config", event.Attrs[2].Value.String())
}
