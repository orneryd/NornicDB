package localization

const (
	MessageRoleEntitlementsUnavailable   MessageID = "server.role_entitlements_unavailable"
	MessageRoleEntitlementsBody          MessageID = "server.role_entitlements_body_invalid"
	MessageRolesUnavailableDetailed      MessageID = "server.roles_unavailable_detailed"
	MessageRoleAlreadyExists             MessageID = "server.role_already_exists"
	MessageRolesUnavailable              MessageID = "server.roles_unavailable"
	MessageNewRoleAlreadyExists          MessageID = "server.new_role_already_exists"
	MessageRoleInUse                     MessageID = "server.role_in_use"
	MessagePatchOrDeleteRequired         MessageID = "server.method_patch_or_delete_required"
	MessageDatabaseAllowlistUnavailable  MessageID = "server.database_allowlist_unavailable"
	MessageDatabasePrivilegesUnavailable MessageID = "server.database_privileges_unavailable"
	MessageDatabaseNameRequired          MessageID = "server.database_name_required"
	MessageUnknownEndpoint               MessageID = "server.unknown_endpoint"
	MessageUnknownTransactionEndpoint    MessageID = "server.unknown_transaction_endpoint"
	MessageGPUUnavailable                MessageID = "server.gpu_unavailable"
	MessageHeimdallDisabled              MessageID = "server.heimdall_disabled"
	MessageHeimdallInitializing          MessageID = "server.heimdall_initializing"
	MessageGPUAccelerationEnabled        MessageID = "server.gpu_acceleration_enabled"
	MessageGPUAccelerationDisabled       MessageID = "server.gpu_acceleration_disabled"
	MessageEmbeddingRegenerationStarted  MessageID = "server.embedding_regeneration_started"
	MessageEmbeddingWorkerAlreadyRunning MessageID = "server.embedding_worker_already_running"
	MessageEmbeddingWorkerTriggered      MessageID = "server.embedding_worker_triggered"
	MessageRetentionPolicyDeleted        MessageID = "server.retention_policy_deleted"
	MessageRetentionHoldReleased         MessageID = "server.retention_hold_released"
	MessageRetentionSweepTriggered       MessageID = "server.retention_sweep_triggered"
	MessageBackupComplete                MessageID = "server.backup_complete"
)

func RoleEntitlementsUnavailable() Message {
	return Message{ID: MessageRoleEntitlementsUnavailable, Fallback: "Role entitlements are not configured (auth disabled or system DB unavailable)."}
}
func RoleEntitlementsBodyInvalid() Message {
	return Message{ID: MessageRoleEntitlementsBody, Fallback: "body must contain role and entitlements or mappings array"}
}
func RolesUnavailableDetailed() Message {
	return Message{ID: MessageRolesUnavailableDetailed, Fallback: "Roles are not configured (auth disabled or system DB unavailable)."}
}
func RoleAlreadyExists() Message {
	return Message{ID: MessageRoleAlreadyExists, Fallback: "role already exists"}
}
func RolesUnavailable() Message {
	return Message{ID: MessageRolesUnavailable, Fallback: "Roles are not configured."}
}
func NewRoleNameAlreadyExists() Message {
	return Message{ID: MessageNewRoleAlreadyExists, Fallback: "new role name already exists"}
}
func RoleCannotDeleteWhileInUse() Message {
	return Message{ID: MessageRoleInUse, Fallback: "cannot delete role: at least one user has this role"}
}
func PatchOrDeleteRequired() Message {
	return Message{ID: MessagePatchOrDeleteRequired, Fallback: "PATCH or DELETE required"}
}
func DatabaseAllowlistUnavailable() Message {
	return Message{ID: MessageDatabaseAllowlistUnavailable, Fallback: "Database access allowlist is not configured (auth disabled or system DB unavailable)."}
}
func DatabasePrivilegesUnavailable() Message {
	return Message{ID: MessageDatabasePrivilegesUnavailable, Fallback: "Database access privileges are not configured (auth disabled or system DB unavailable)."}
}
func DatabaseNameRequired() Message {
	return Message{ID: MessageDatabaseNameRequired, Fallback: "database name required"}
}
func UnknownEndpoint() Message {
	return Message{ID: MessageUnknownEndpoint, Fallback: "unknown endpoint"}
}
func UnknownTransactionEndpoint() Message {
	return Message{ID: MessageUnknownTransactionEndpoint, Fallback: "unknown transaction endpoint"}
}
func GPUUnavailable(cause error) Message {
	return Message{ID: MessageGPUUnavailable, Fallback: "GPU unavailable: " + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}
func HeimdallDisabled() Message {
	return Message{ID: MessageHeimdallDisabled, Fallback: "Heimdall is disabled by configuration"}
}
func HeimdallInitializing() Message {
	return Message{ID: MessageHeimdallInitializing, Fallback: "Heimdall is initializing, please try again shortly"}
}
func GPUAccelerationEnabled() Message {
	return Message{ID: MessageGPUAccelerationEnabled, Fallback: "GPU acceleration enabled"}
}
func GPUAccelerationDisabled() Message {
	return Message{ID: MessageGPUAccelerationDisabled, Fallback: "GPU acceleration disabled (CPU fallback active)"}
}
func EmbeddingRegenerationStarted() Message {
	return Message{ID: MessageEmbeddingRegenerationStarted, Fallback: "Regeneration started - clearing embeddings and regenerating in background. Check /nornicdb/embed/stats for progress."}
}
func EmbeddingWorkerAlreadyRunning() Message {
	return Message{ID: MessageEmbeddingWorkerAlreadyRunning, Fallback: "Embedding worker already running - will continue processing"}
}
func EmbeddingWorkerTriggered() Message {
	return Message{ID: MessageEmbeddingWorkerTriggered, Fallback: "Embedding worker triggered - processing nodes in background"}
}
func RetentionPolicyDeleted() Message {
	return Message{ID: MessageRetentionPolicyDeleted, Fallback: "deleted"}
}
func RetentionHoldReleased() Message {
	return Message{ID: MessageRetentionHoldReleased, Fallback: "released"}
}
func RetentionSweepTriggered() Message {
	return Message{ID: MessageRetentionSweepTriggered, Fallback: "sweep triggered"}
}
func BackupComplete() Message {
	return Message{ID: MessageBackupComplete, Fallback: "backup complete"}
}
