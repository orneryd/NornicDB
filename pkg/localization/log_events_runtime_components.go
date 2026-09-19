package localization

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	EventHeimdallToolCall                       EventID   = "heimdall.tool.call"
	EventHeimdallOperator                       EventID   = "heimdall.operator"
	EventInferenceEdgeDecayStarted              EventID   = "inference.edge_decay.started"
	EventInferenceHeimdallBatchError            EventID   = "inference.heimdall.batch_error"
	EventInferenceOperator                      EventID   = "inference.operator"
	EventObservabilityTenantLabelsResolved      EventID   = "observability.tenant_labels.resolved"
	EventObservabilityInstanceIDResolved        EventID   = "observability.instance_id.resolved"
	EventObservabilityOperator                  EventID   = "observability.operator"
	EventReplicationRaftBecameLeader            EventID   = "replication.raft.became_leader"
	EventReplicationOperator                    EventID   = "replication.operator"
	MessageHeimdallLogToolCall                  MessageID = "runtime.log.heimdall_tool_call"
	MessageHeimdallLogOperator                  MessageID = "runtime.log.heimdall_operator"
	MessageInferenceLogEdgeDecayStarted         MessageID = "runtime.log.inference_edge_decay_started"
	MessageInferenceLogHeimdallBatchError       MessageID = "runtime.log.inference_heimdall_batch_error"
	MessageInferenceLogOperator                 MessageID = "runtime.log.inference_operator"
	MessageObservabilityLogTenantLabelsResolved MessageID = "runtime.log.observability_tenant_labels_resolved"
	MessageObservabilityLogInstanceIDResolved   MessageID = "runtime.log.observability_instance_id_resolved"
	MessageObservabilityLogOperator             MessageID = "runtime.log.observability_operator"
	MessageReplicationLogRaftBecameLeader       MessageID = "runtime.log.replication_raft_became_leader"
	MessageReplicationLogOperator               MessageID = "runtime.log.replication_operator"
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
	return Message{ID: MessageObservabilityLogInstanceIDResolved, Fallback: fmt.Sprintf("INFO observability: service.instance.id=%q (resolved from %s)", instanceID, source), Data: map[string]any{"InstanceID": instanceID, "Source": source}}
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
	attrs := []slog.Attr{slog.String("component", "heimdall"), slog.String("message_template", format)}
	for index, arg := range args {
		attrs = append(attrs, slog.Any(fmt.Sprintf("arg_%d", index), arg))
	}
	return LogEvent{ID: EventHeimdallOperator, Message: HeimdallLogOperator(message), Attrs: attrs}
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
	attrs := []slog.Attr{slog.String("component", "inference"), slog.String("message_template", format)}
	for index, arg := range args {
		attrs = append(attrs, slog.Any(fmt.Sprintf("arg_%d", index), arg))
	}
	return LogEvent{ID: EventInferenceOperator, Message: InferenceLogOperator(message), Attrs: attrs}
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
	attrs := []slog.Attr{slog.String("component", "observability"), slog.String("message_template", format)}
	for index, arg := range args {
		attrs = append(attrs, slog.Any(fmt.Sprintf("arg_%d", index), arg))
	}
	return LogEvent{ID: EventObservabilityOperator, Message: ObservabilityLogOperator(message), Attrs: attrs}
}

// ReplicationRaftBecameLeaderEvent describes a Raft leader transition.
func ReplicationRaftBecameLeaderEvent(nodeID string, term uint64) LogEvent {
	return LogEvent{ID: EventReplicationRaftBecameLeader, Message: ReplicationLogRaftBecameLeader(nodeID, term), Attrs: []slog.Attr{slog.String("node_id", nodeID), slog.Uint64("term", term)}}
}

// ReplicationOperatorEvent converts legacy replication prose into a structured event.
func ReplicationOperatorEvent(format string, args ...any) LogEvent {
	message := fmt.Sprintf(format, args...)
	attrs := []slog.Attr{slog.String("component", "replication"), slog.String("message_template", format)}
	for index, arg := range args {
		attrs = append(attrs, slog.Any(fmt.Sprintf("arg_%d", index), arg))
	}
	return LogEvent{ID: EventReplicationOperator, Message: ReplicationLogOperator(message), Attrs: attrs}
}
