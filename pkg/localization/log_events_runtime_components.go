package localization

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	EventHeimdallToolCall                         EventID   = "heimdall.tool.call"
	EventHeimdallOperator                         EventID   = "heimdall.operator"
	EventInferenceEdgeDecayStarted                EventID   = "inference.edge_decay.started"
	EventInferenceHeimdallBatchError              EventID   = "inference.heimdall.batch_error"
	EventInferenceOperator                        EventID   = "inference.operator"
	EventObservabilityTenantLabelsResolved        EventID   = "observability.tenant_labels.resolved"
	EventObservabilityInstanceIDResolved          EventID   = "observability.instance_id.resolved"
	EventObservabilityOperator                    EventID   = "observability.operator"
	EventReplicationRaftBecameLeader              EventID   = "replication.raft.became_leader"
	EventReplicationOperator                      EventID   = "replication.operator"
	MessageHeimdallLogToolCall                    MessageID = "runtime.log.heimdall_tool_call"
	MessageHeimdallLogOperator                    MessageID = "runtime.log.heimdall_operator"
	MessageInferenceLogEdgeDecayStarted           MessageID = "runtime.log.inference_edge_decay_started"
	MessageInferenceLogHeimdallBatchError         MessageID = "runtime.log.inference_heimdall_batch_error"
	MessageInferenceLogOperator                   MessageID = "runtime.log.inference_operator"
	MessageObservabilityLogTenantLabelsResolved   MessageID = "runtime.log.observability_tenant_labels_resolved"
	MessageObservabilityLogInstanceIDResolved     MessageID = "runtime.log.observability_instance_id_resolved"
	MessageObservabilityLogOTLPPlaintextRejected  MessageID = "runtime.log.observability_otlp_plaintext_rejected"
	MessageObservabilityLogSpanExporterInitFailed MessageID = "runtime.log.observability_span_exporter_init_failed"
	MessageObservabilityLogSamplerModeInvalid     MessageID = "runtime.log.observability_sampler_mode_invalid"
	MessageObservabilityLogParentStrictUnbounded  MessageID = "runtime.log.observability_parent_strict_unbounded"
	MessageObservabilityLogOperator               MessageID = "runtime.log.observability_operator"
	MessageReplicationLogRaftBecameLeader         MessageID = "runtime.log.replication_raft_became_leader"
	MessageReplicationLogOperator                 MessageID = "runtime.log.replication_operator"
)

// HeimdallLogOperator preserves dynamically formatted Heimdall operator prose.
func HeimdallLogOperator(message string) Message {
	return Message{ID: MessageHeimdallLogOperator, Fallback: message, Data: map[string]any{"Message": message}}
}

// HeimdallLogToolCall describes an agent tool invocation.
func HeimdallLogToolCall(requestID, action string, params any) Message {
	return Message{ID: MessageHeimdallLogToolCall, Fallback: fmt.Sprintf("[Heimdall] Tool call: request=%s action=%s params=%v", requestID, action, params), Data: map[string]any{"RequestID": requestID, "Action": action, "Params": params}}
}

// InferenceLogEdgeDecayStarted describes edge-decay worker startup.
func InferenceLogEdgeDecayStarted(decayRate, minConfidence float64, scanInterval, gracePeriod time.Duration) Message {
	return Message{ID: MessageInferenceLogEdgeDecayStarted, Fallback: fmt.Sprintf("[EDGE-DECAY] Started | decay_rate=%.2f min_conf=%.2f scan_interval=%v grace=%v", decayRate, minConfidence, scanInterval, gracePeriod), Data: map[string]any{"DecayRate": fmt.Sprintf("%.2f", decayRate), "MinConfidence": fmt.Sprintf("%.2f", minConfidence), "ScanInterval": scanInterval, "GracePeriod": gracePeriod}}
}

// InferenceLogHeimdallBatchError describes a fail-open quality-control batch error.
func InferenceLogHeimdallBatchError(err error) Message {
	return Message{ID: MessageInferenceLogHeimdallBatchError, Fallback: fmt.Sprintf("[HEIMDALL] ⚠️ Batch error, fail-open: %v", err), Data: map[string]any{"Error": err}}
}

// InferenceLogOperator preserves dynamically formatted inference operator prose.
func InferenceLogOperator(message string) Message {
	return Message{ID: MessageInferenceLogOperator, Fallback: message, Data: map[string]any{"Message": message}}
}

// ObservabilityLogTenantLabelsResolved describes tenant-label policy resolution.
func ObservabilityLogTenantLabelsResolved() Message {
	return Message{ID: MessageObservabilityLogTenantLabelsResolved, Fallback: "resolved tenant labels enabled"}
}

// ObservabilityLogInstanceIDResolved describes service instance identity resolution.
func ObservabilityLogInstanceIDResolved(instanceID, source string) Message {
	return Message{ID: MessageObservabilityLogInstanceIDResolved, Fallback: fmt.Sprintf("observability: service.instance.id=%q (resolved from %s)", instanceID, source), Data: map[string]any{"InstanceID": instanceID, "Source": source}}
}

// ObservabilityLogOTLPPlaintextRejected describes rejection of a plaintext OTLP endpoint.
func ObservabilityLogOTLPPlaintextRejected(endpoint string) Message {
	return Message{ID: MessageObservabilityLogOTLPPlaintextRejected, Fallback: fmt.Sprintf("observability: OTLP endpoint %q is plaintext but NORNICDB_OTLP_INSECURE is not set; installing noop tracer provider (TRC-09)", endpoint), Data: map[string]any{"Endpoint": endpoint}}
}

// ObservabilityLogSpanExporterInitFailed describes a failed span exporter initialization.
func ObservabilityLogSpanExporterInitFailed(err error) Message {
	return Message{ID: MessageObservabilityLogSpanExporterInitFailed, Fallback: fmt.Sprintf("observability: span exporter init failed: %v; installing noop tracer provider — process continues", err), Data: map[string]any{"Error": err}}
}

// ObservabilityLogSamplerModeInvalid describes an invalid trace sampler mode.
func ObservabilityLogSamplerModeInvalid(modeErr error) Message {
	return Message{ID: MessageObservabilityLogSamplerModeInvalid, Fallback: fmt.Sprintf("observability: %v; falling back to default sampler mode (TRC-05)", modeErr), Data: map[string]any{"Error": modeErr}}
}

// ObservabilityLogParentStrictUnbounded describes an unbounded parent_strict sampler.
func ObservabilityLogParentStrictUnbounded() Message {
	return Message{ID: MessageObservabilityLogParentStrictUnbounded, Fallback: "observability: parent_strict sampler honors upstream decisions unconditionally; trace volume is unbounded (TRC-07)"}
}

// ObservabilityLogOperator preserves dynamically formatted observability operator prose.
func ObservabilityLogOperator(message string) Message {
	return Message{ID: MessageObservabilityLogOperator, Fallback: message, Data: map[string]any{"Message": message}}
}

// ReplicationLogRaftBecameLeader describes a Raft leader transition.
func ReplicationLogRaftBecameLeader(nodeID string, term uint64) Message {
	return Message{ID: MessageReplicationLogRaftBecameLeader, Fallback: fmt.Sprintf("[Raft %s] Became leader for term %d", nodeID, term), Data: map[string]any{"NodeID": nodeID, "Term": term}}
}

// ReplicationLogOperator preserves dynamically formatted replication operator prose.
func ReplicationLogOperator(message string) Message {
	return Message{ID: MessageReplicationLogOperator, Fallback: message, Data: map[string]any{"Message": message}}
}

// HeimdallToolCallEvent describes an agent tool invocation.
func HeimdallToolCallEvent(requestID, action string, params any) LogEvent {
	return LogEvent{ID: EventHeimdallToolCall, Message: HeimdallLogToolCall(requestID, action, params), Attrs: []slog.Attr{slog.String("request_id", requestID), slog.String("action", action), slog.Any("params", params)}}
}

// HeimdallOperatorEvent converts legacy Heimdall prose into a structured event.
func HeimdallOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	return LogEvent{ID: EventHeimdallOperator, Message: HeimdallLogOperator(message), Attrs: []slog.Attr{slog.String("component", "heimdall")}}
}

// InferenceEdgeDecayStartedEvent describes edge-decay worker startup.
func InferenceEdgeDecayStartedEvent(decayRate, minConfidence float64, scanInterval, gracePeriod time.Duration) LogEvent {
	return LogEvent{ID: EventInferenceEdgeDecayStarted, Message: InferenceLogEdgeDecayStarted(decayRate, minConfidence, scanInterval, gracePeriod), Attrs: []slog.Attr{slog.Float64("decay_rate", decayRate), slog.Float64("min_confidence", minConfidence), slog.Duration("scan_interval", scanInterval), slog.Duration("grace_period", gracePeriod)}}
}

// InferenceHeimdallBatchErrorEvent describes a fail-open quality-control batch error.
func InferenceHeimdallBatchErrorEvent(err error) LogEvent {
	return LogEvent{ID: EventInferenceHeimdallBatchError, Message: InferenceLogHeimdallBatchError(err), Attrs: []slog.Attr{slog.Any("error", err), slog.Bool("fail_open", true)}}
}

// InferenceOperatorEvent converts legacy inference prose into a structured event.
func InferenceOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	return LogEvent{ID: EventInferenceOperator, Message: InferenceLogOperator(message), Attrs: []slog.Attr{slog.String("component", "inference")}}
}

// ObservabilityTenantLabelsResolvedEvent describes tenant-label policy resolution.
func ObservabilityTenantLabelsResolvedEvent(enabled bool, reason string, serviceHostPresent, tokenFilePresent bool) LogEvent {
	return LogEvent{ID: EventObservabilityTenantLabelsResolved, Message: ObservabilityLogTenantLabelsResolved(), Attrs: []slog.Attr{slog.String("component", "observability"), slog.Bool("enabled", enabled), slog.String("reason", reason), slog.Bool("service_host_present", serviceHostPresent), slog.Bool("token_file_present", tokenFilePresent)}}
}

// ObservabilityInstanceIDResolvedEvent describes service instance identity resolution.
func ObservabilityInstanceIDResolvedEvent(instanceID, source string) LogEvent {
	return LogEvent{ID: EventObservabilityInstanceIDResolved, Message: ObservabilityLogInstanceIDResolved(instanceID, source), Attrs: []slog.Attr{slog.String("component", "observability"), slog.String("service_instance_id", instanceID), slog.String("source", source)}}
}

// ObservabilityOperatorEvent converts legacy observability prose into a structured event.
func ObservabilityOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	return LogEvent{ID: EventObservabilityOperator, Message: ObservabilityLogOperator(message), Attrs: []slog.Attr{slog.String("component", "observability")}}
}

// ReplicationRaftBecameLeaderEvent describes a Raft leader transition.
func ReplicationRaftBecameLeaderEvent(nodeID string, term uint64) LogEvent {
	return LogEvent{ID: EventReplicationRaftBecameLeader, Message: ReplicationLogRaftBecameLeader(nodeID, term), Attrs: []slog.Attr{slog.String("node_id", nodeID), slog.Uint64("term", term)}}
}

// ReplicationOperatorEvent converts legacy replication prose into a structured event.
func ReplicationOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	return LogEvent{ID: EventReplicationOperator, Message: ReplicationLogOperator(message), Attrs: []slog.Attr{slog.String("component", "replication")}}
}
