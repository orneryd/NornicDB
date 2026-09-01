package localization

import "fmt"

const (
	MessageReplicationRPCEncodeFailed                MessageID = "replication.rpc.encode_failed"
	MessageReplicationRPCDecodeFailed                MessageID = "replication.rpc.decode_failed"
	MessageReplicationRPCNotConnected                MessageID = "replication.rpc.not_connected"
	MessageReplicationRPCWriteFailed                 MessageID = "replication.rpc.write_failed"
	MessageReplicationRPCConnectionClosed            MessageID = "replication.rpc.connection_closed"
	MessageReplicationRPCCommandRequired             MessageID = "replication.rpc.command_required"
	MessageReplicationRPCRemoteApplyFailed           MessageID = "replication.rpc.remote_apply_failed"
	MessageReplicationRPCAuthenticationFieldsMissing MessageID = "replication.rpc.authentication_fields_missing"
	MessageReplicationRPCTimestampOutsideSkew        MessageID = "replication.rpc.timestamp_outside_allowed_skew"
	MessageReplicationRPCInvalidSignature            MessageID = "replication.rpc.invalid_signature"
	MessageReplicationRPCReadFailed                  MessageID = "replication.rpc.read_failed"
	MessageReplicationRPCMessageTooLarge             MessageID = "replication.rpc.message_too_large"
)

func replicationRPCOperationSuffix(operation string) string {
	if operation == "" {
		return ""
	}
	return " " + operation
}

// ReplicationRPCEncodeFailed identifies RPC payload encoding failure.
func ReplicationRPCEncodeFailed(operation string, cause error) Message {
	return Message{ID: MessageReplicationRPCEncodeFailed, Fallback: "encode" + replicationRPCOperationSuffix(operation) + ": " + cause.Error(), Data: map[string]any{"Operation": operation, "Cause": cause.Error()}}
}

// ReplicationRPCDecodeFailed identifies RPC payload decoding failure.
func ReplicationRPCDecodeFailed(operation string, cause error) Message {
	return Message{ID: MessageReplicationRPCDecodeFailed, Fallback: "decode" + replicationRPCOperationSuffix(operation) + ": " + cause.Error(), Data: map[string]any{"Operation": operation, "Cause": cause.Error()}}
}

// ReplicationRPCNotConnected identifies an RPC attempted without a peer connection.
func ReplicationRPCNotConnected() Message {
	return Message{ID: MessageReplicationRPCNotConnected, Fallback: "not connected"}
}

// ReplicationRPCWriteFailed identifies a failed RPC wire write.
func ReplicationRPCWriteFailed(cause error) Message {
	return Message{ID: MessageReplicationRPCWriteFailed, Fallback: "write: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// ReplicationRPCConnectionClosed identifies a connection closed while awaiting an RPC response.
func ReplicationRPCConnectionClosed() Message {
	return Message{ID: MessageReplicationRPCConnectionClosed, Fallback: "connection closed"}
}

// ReplicationRPCCommandRequired identifies a missing forwarded command.
func ReplicationRPCCommandRequired() Message {
	return Message{ID: MessageReplicationRPCCommandRequired, Fallback: "nil command"}
}

// ReplicationRPCRemoteApplyFailed carries a remote application diagnostic received over the wire.
func ReplicationRPCRemoteApplyFailed(diagnostic string) Message {
	return Message{ID: MessageReplicationRPCRemoteApplyFailed, Fallback: diagnostic, Data: map[string]any{"Cause": diagnostic}}
}

// ReplicationRPCAuthenticationFieldsMissing identifies an unsigned or incomplete authenticated message.
func ReplicationRPCAuthenticationFieldsMissing() Message {
	return Message{ID: MessageReplicationRPCAuthenticationFieldsMissing, Fallback: "missing authentication fields"}
}

// ReplicationRPCTimestampOutsideAllowedSkew identifies a stale or future authenticated message.
func ReplicationRPCTimestampOutsideAllowedSkew() Message {
	return Message{ID: MessageReplicationRPCTimestampOutsideSkew, Fallback: "timestamp outside allowed skew"}
}

// ReplicationRPCInvalidSignature identifies a failed message signature check.
func ReplicationRPCInvalidSignature() Message {
	return Message{ID: MessageReplicationRPCInvalidSignature, Fallback: "invalid signature"}
}

// ReplicationRPCReadFailed carries a wire-read cause without changing its source text.
func ReplicationRPCReadFailed(cause error) Message {
	return Message{ID: MessageReplicationRPCReadFailed, Fallback: cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// ReplicationRPCMessageTooLarge identifies a wire message exceeding the configured limit.
func ReplicationRPCMessageTooLarge(actual uint32, maximum int) Message {
	return Message{ID: MessageReplicationRPCMessageTooLarge, Fallback: fmt.Sprintf("message too large: %d > %d", actual, maximum), Data: map[string]any{"Actual": actual, "Maximum": maximum}}
}
