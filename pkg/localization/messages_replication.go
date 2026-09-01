package localization

import (
	"fmt"
	"time"
)

const (
	MessageReplicationNotLeader                        MessageID = "replication.not_leader"
	MessageReplicationNoLeader                         MessageID = "replication.no_leader"
	MessageReplicationOperationTimedOut                MessageID = "replication.operation_timed_out"
	MessageReplicationClosed                           MessageID = "replication.closed"
	MessageReplicationStandbyMode                      MessageID = "replication.standby_mode"
	MessageReplicationNotReady                         MessageID = "replication.not_ready"
	MessageReplicationConfigHARoleRequired             MessageID = "replication.config.ha_role_required"
	MessageReplicationConfigInvalidHARole              MessageID = "replication.config.invalid_ha_role"
	MessageReplicationConfigHAPeerAddressRequired      MessageID = "replication.config.ha_peer_address_required"
	MessageReplicationConfigInvalidHASyncMode          MessageID = "replication.config.invalid_ha_sync_mode"
	MessageReplicationConfigRaftPeersRequired          MessageID = "replication.config.raft_peers_required"
	MessageReplicationConfigRegionIDRequired           MessageID = "replication.config.region_id_required"
	MessageReplicationConfigInvalidCrossRegionSyncMode MessageID = "replication.config.invalid_cross_region_sync_mode"
	MessageReplicationConfigUnknownMode                MessageID = "replication.config.unknown_mode"
	MessageReplicationConfigNodeIDRequired             MessageID = "replication.config.node_id_required"
	MessageReplicationConfigSecretTooShort             MessageID = "replication.config.secret_too_short"
	MessageReplicationConfigTLSCertRequired            MessageID = "replication.config.tls_cert_required"
	MessageReplicationConfigTLSKeyRequired             MessageID = "replication.config.tls_key_required"
	MessageReplicationConfigTLSCARequired              MessageID = "replication.config.tls_ca_required"
	MessageReplicationConfigInvalidTLSMinVersion       MessageID = "replication.config.invalid_tls_min_version"
	MessageReplicationTransportClosed                  MessageID = "replication.transport.closed"
	MessageReplicationTransportConnectFailed           MessageID = "replication.transport.connect_failed"
	MessageReplicationTransportListenFailed            MessageID = "replication.transport.listen_failed"
	MessageReplicationTransportLoadTLSCertKeyFailed    MessageID = "replication.transport.load_tls_cert_key_failed"
	MessageReplicationTransportReadTLSCAFailed         MessageID = "replication.transport.read_tls_ca_failed"
	MessageReplicationTransportInvalidTLSCA            MessageID = "replication.transport.invalid_tls_ca"
	MessageReplicationTransportUnknownTLSCipherSuite   MessageID = "replication.transport.unknown_tls_cipher_suite"
	MessageReplicationHAInitTransportFailed            MessageID = "replication.ha.init_transport_failed"
	MessageReplicationHAStartPrimaryFailed             MessageID = "replication.ha.start_primary_failed"
	MessageReplicationHAStartStandbyFailed             MessageID = "replication.ha.start_standby_failed"
	MessageReplicationHAFlushWALFailed                 MessageID = "replication.ha.flush_wal_failed"
	MessageReplicationHAAckTimedOut                    MessageID = "replication.ha.ack_timed_out"
	MessageReplicationHAPrimaryCannotReceiveWAL        MessageID = "replication.ha.primary_cannot_receive_wal"
	MessageReplicationRaftVoteRequestRequired          MessageID = "replication.raft.vote_request_required"
	MessageReplicationRaftAppendRequestRequired        MessageID = "replication.raft.append_entries_request_required"
	MessageReplicationRaftInitTransportFailed          MessageID = "replication.raft.init_transport_failed"
	MessageReplicationRaftSendVoteFailed               MessageID = "replication.raft.send_vote_request_failed"
	MessageReplicationRaftSendAppendFailed             MessageID = "replication.raft.send_append_entries_failed"
	MessageReplicationRaftCommitTimedOut               MessageID = "replication.raft.commit_timed_out"
	MessageReplicationRaftTransportNotConfigured       MessageID = "replication.raft.transport_not_configured"
	MessageReplicationRaftConnectPeerFailed            MessageID = "replication.raft.connect_peer_failed"
	MessageReplicationRaftForwardToLeaderFailed        MessageID = "replication.raft.forward_to_leader_failed"
	MessageReplicationRaftApplyQueueFull               MessageID = "replication.raft.apply_queue_full"
	MessageReplicationRaftApplyTimedOut                MessageID = "replication.raft.apply_timed_out"
	MessageReplicationStorageWALDirectoryCreateFailed  MessageID = "replication.storage.wal_directory_create_failed"
	MessageReplicationStorageWALCreateFailed           MessageID = "replication.storage.wal_create_failed"
	MessageReplicationStorageCommandRequired           MessageID = "replication.storage.command_required"
	MessageReplicationStorageWALAppendFailed           MessageID = "replication.storage.wal_append_failed"
	MessageReplicationStorageUnknownCommandType        MessageID = "replication.storage.unknown_command_type"
	MessageReplicationStorageDecodeNodeFailed          MessageID = "replication.storage.decode_node_failed"
	MessageReplicationStorageDecodeEdgeFailed          MessageID = "replication.storage.decode_edge_failed"
	MessageReplicationStorageDecodeDeleteEdgeFailed    MessageID = "replication.storage.decode_delete_edge_failed"
	MessageReplicationStorageDecodeSetPropertyFailed   MessageID = "replication.storage.decode_set_property_failed"
	MessageReplicationStorageDecodeBatchFailed         MessageID = "replication.storage.decode_batch_failed"
	MessageReplicationStorageDecodeDeletePrefixFailed  MessageID = "replication.storage.decode_delete_prefix_failed"
	MessageReplicationStoragePrefixRequired            MessageID = "replication.storage.prefix_required"
	MessageReplicationStorageDecodeBulkCreateNodes     MessageID = "replication.storage.decode_bulk_create_nodes_failed"
	MessageReplicationStorageDecodeBulkCreateEdges     MessageID = "replication.storage.decode_bulk_create_edges_failed"
	MessageReplicationStorageDecodeBulkDeleteNodes     MessageID = "replication.storage.decode_bulk_delete_nodes_failed"
	MessageReplicationStorageDecodeBulkDeleteEdges     MessageID = "replication.storage.decode_bulk_delete_edges_failed"
	MessageReplicationStorageCypherUnavailable         MessageID = "replication.storage.cypher_executor_unavailable"
	MessageReplicationStorageDecodeCypherFailed        MessageID = "replication.storage.decode_cypher_command_failed"
	MessageReplicationStorageCypherQueryEmpty          MessageID = "replication.storage.cypher_query_empty"
	MessageReplicationStorageExecuteCypherFailed       MessageID = "replication.storage.execute_cypher_failed"
	MessageReplicationStorageFlushWALFailed            MessageID = "replication.storage.flush_wal_failed"
	MessageReplicationStorageReadWALEntriesFailed      MessageID = "replication.storage.read_wal_entries_failed"
	MessageReplicationStorageGetAllNodesFailed         MessageID = "replication.storage.get_all_nodes_failed"
	MessageReplicationStorageGetAllEdgesFailed         MessageID = "replication.storage.get_all_edges_failed"
	MessageReplicationStorageEncodeSnapshotFailed      MessageID = "replication.storage.encode_snapshot_failed"
	MessageReplicationStorageReadSnapshotFailed        MessageID = "replication.storage.read_snapshot_failed"
	MessageReplicationStorageDecodeSnapshotFailed      MessageID = "replication.storage.decode_snapshot_failed"
	MessageReplicationStorageRestoreNodeFailed         MessageID = "replication.storage.restore_node_failed"
	MessageReplicationStorageRestoreEdgeFailed         MessageID = "replication.storage.restore_edge_failed"
	MessageReplicationEngineNodeRequired               MessageID = "replication.engine.node_required"
	MessageReplicationEngineEdgeRequired               MessageID = "replication.engine.edge_required"
	MessageReplicationEngineEncodeNodeFailed           MessageID = "replication.engine.encode_node_failed"
	MessageReplicationEngineEncodeEdgeFailed           MessageID = "replication.engine.encode_edge_failed"
	MessageReplicationEngineEncodeDeleteEdgeFailed     MessageID = "replication.engine.encode_delete_edge_failed"
	MessageReplicationEngineEncodeBulkCreateNodes      MessageID = "replication.engine.encode_bulk_create_nodes_failed"
	MessageReplicationEngineEncodeBulkCreateEdges      MessageID = "replication.engine.encode_bulk_create_edges_failed"
	MessageReplicationEngineEncodeBulkDeleteNodes      MessageID = "replication.engine.encode_bulk_delete_nodes_failed"
	MessageReplicationEngineEncodeBulkDeleteEdges      MessageID = "replication.engine.encode_bulk_delete_edges_failed"
	MessageReplicationEngineEncodeDeletePrefixFailed   MessageID = "replication.engine.encode_delete_prefix_failed"
)

func replicationCause(id MessageID, prefix string, cause error) Message {
	return Message{ID: id, Fallback: prefix + cause.Error(), Data: map[string]any{"Cause": cause.Error()}}
}

// ReplicationNotLeader identifies a write rejected by a non-leader node.
func ReplicationNotLeader() Message {
	return Message{ID: MessageReplicationNotLeader, Fallback: "not leader"}
}

// ReplicationNoLeader identifies a cluster without an available leader.
func ReplicationNoLeader() Message {
	return Message{ID: MessageReplicationNoLeader, Fallback: "no leader available"}
}

// ReplicationOperationTimedOut identifies a generic replication timeout.
func ReplicationOperationTimedOut() Message {
	return Message{ID: MessageReplicationOperationTimedOut, Fallback: "operation timed out"}
}

// ReplicationClosed identifies an operation on a closed replicator.
func ReplicationClosed() Message {
	return Message{ID: MessageReplicationClosed, Fallback: "replicator is closed"}
}

// ReplicationStandbyMode identifies a write attempted on a standby node.
func ReplicationStandbyMode() Message {
	return Message{ID: MessageReplicationStandbyMode, Fallback: "node is in standby mode"}
}

// ReplicationNotReady identifies an operation attempted before initialization finishes.
func ReplicationNotReady() Message {
	return Message{ID: MessageReplicationNotReady, Fallback: "replicator not ready"}
}

// ReplicationConfigHARoleRequired identifies a missing HA role setting.
func ReplicationConfigHARoleRequired() Message {
	return Message{ID: MessageReplicationConfigHARoleRequired, Fallback: "ha_standby mode requires NORNICDB_CLUSTER_HA_ROLE (primary or standby)", Data: map[string]any{"Mode": "ha_standby", "EnvVar": "NORNICDB_CLUSTER_HA_ROLE", "PrimaryRole": "primary", "StandbyRole": "standby"}}
}

// ReplicationConfigInvalidHARole identifies an unsupported HA role.
func ReplicationConfigInvalidHARole(role string) Message {
	return Message{ID: MessageReplicationConfigInvalidHARole, Fallback: fmt.Sprintf("invalid HA role: %s (must be 'primary' or 'standby')", role), Data: map[string]any{"Role": role, "PrimaryRole": "primary", "StandbyRole": "standby"}}
}

// ReplicationConfigHAPeerAddressRequired identifies a missing HA peer address setting.
func ReplicationConfigHAPeerAddressRequired() Message {
	return Message{ID: MessageReplicationConfigHAPeerAddressRequired, Fallback: "ha_standby mode requires NORNICDB_CLUSTER_HA_PEER_ADDR", Data: map[string]any{"Mode": "ha_standby", "EnvVar": "NORNICDB_CLUSTER_HA_PEER_ADDR"}}
}

// ReplicationConfigInvalidHASyncMode identifies an unsupported HA synchronization mode.
func ReplicationConfigInvalidHASyncMode(mode string) Message {
	return Message{ID: MessageReplicationConfigInvalidHASyncMode, Fallback: fmt.Sprintf("invalid HA sync mode: %s (must be 'async' or 'quorum')", mode), Data: map[string]any{"SyncMode": mode, "AsyncMode": "async", "QuorumMode": "quorum"}}
}

// ReplicationConfigRaftPeersRequired identifies missing Raft peer/bootstrap settings.
func ReplicationConfigRaftPeersRequired() Message {
	return Message{ID: MessageReplicationConfigRaftPeersRequired, Fallback: "raft mode requires NORNICDB_CLUSTER_RAFT_PEERS or NORNICDB_CLUSTER_RAFT_BOOTSTRAP=true", Data: map[string]any{"Mode": "raft", "PeersEnvVar": "NORNICDB_CLUSTER_RAFT_PEERS", "BootstrapEnvVar": "NORNICDB_CLUSTER_RAFT_BOOTSTRAP"}}
}

// ReplicationConfigRegionIDRequired identifies a missing multi-region identifier.
func ReplicationConfigRegionIDRequired() Message {
	return Message{ID: MessageReplicationConfigRegionIDRequired, Fallback: "multi_region mode requires NORNICDB_CLUSTER_REGION_ID", Data: map[string]any{"Mode": "multi_region", "EnvVar": "NORNICDB_CLUSTER_REGION_ID"}}
}

// ReplicationConfigInvalidCrossRegionSyncMode identifies an unsupported cross-region mode.
func ReplicationConfigInvalidCrossRegionSyncMode(mode string) Message {
	return Message{ID: MessageReplicationConfigInvalidCrossRegionSyncMode, Fallback: fmt.Sprintf("invalid cross-region sync mode: %s (must be 'async' or 'quorum')", mode), Data: map[string]any{"SyncMode": mode, "AsyncMode": "async", "QuorumMode": "quorum"}}
}

// ReplicationConfigUnknownMode identifies an unsupported replication mode.
func ReplicationConfigUnknownMode(mode any) Message {
	return Message{ID: MessageReplicationConfigUnknownMode, Fallback: fmt.Sprintf("unknown replication mode: %s", mode), Data: map[string]any{"Mode": mode}}
}

// ReplicationConfigNodeIDRequired identifies a missing node identifier.
func ReplicationConfigNodeIDRequired() Message {
	return Message{ID: MessageReplicationConfigNodeIDRequired, Fallback: "NORNICDB_CLUSTER_NODE_ID is required", Data: map[string]any{"EnvVar": "NORNICDB_CLUSTER_NODE_ID"}}
}

// ReplicationConfigSecretTooShort identifies a replication secret below the minimum length.
func ReplicationConfigSecretTooShort(minLength int) Message {
	return Message{ID: MessageReplicationConfigSecretTooShort, Fallback: fmt.Sprintf("replication secret must be at least %d characters", minLength), Data: map[string]any{"MinLength": minLength}}
}

// ReplicationConfigTLSCertRequired identifies a missing TLS certificate setting.
func ReplicationConfigTLSCertRequired() Message {
	return Message{ID: MessageReplicationConfigTLSCertRequired, Fallback: "TLS enabled but NORNICDB_CLUSTER_TLS_CERT_FILE not set", Data: map[string]any{"EnvVar": "NORNICDB_CLUSTER_TLS_CERT_FILE"}}
}

// ReplicationConfigTLSKeyRequired identifies a missing TLS key setting.
func ReplicationConfigTLSKeyRequired() Message {
	return Message{ID: MessageReplicationConfigTLSKeyRequired, Fallback: "TLS enabled but NORNICDB_CLUSTER_TLS_KEY_FILE not set", Data: map[string]any{"EnvVar": "NORNICDB_CLUSTER_TLS_KEY_FILE"}}
}

// ReplicationConfigTLSCARequired identifies a missing client-verification CA setting.
func ReplicationConfigTLSCARequired() Message {
	return Message{ID: MessageReplicationConfigTLSCARequired, Fallback: "TLS client verification enabled but NORNICDB_CLUSTER_TLS_CA_FILE not set", Data: map[string]any{"EnvVar": "NORNICDB_CLUSTER_TLS_CA_FILE"}}
}

// ReplicationConfigInvalidTLSMinVersion identifies an unsupported TLS minimum version.
func ReplicationConfigInvalidTLSMinVersion(version string) Message {
	return Message{ID: MessageReplicationConfigInvalidTLSMinVersion, Fallback: fmt.Sprintf("invalid TLS min version: %s (must be '1.2' or '1.3')", version), Data: map[string]any{"Version": version, "TLS12": "1.2", "TLS13": "1.3"}}
}

// ReplicationTransportClosed identifies an operation on a closed transport.
func ReplicationTransportClosed() Message {
	return Message{ID: MessageReplicationTransportClosed, Fallback: "transport closed"}
}

// ReplicationTransportConnectFailed identifies a failed peer connection.
func ReplicationTransportConnectFailed(address string, cause error) Message {
	return Message{ID: MessageReplicationTransportConnectFailed, Fallback: "connect to " + address + ": " + cause.Error(), Data: map[string]any{"Address": address, "Cause": cause.Error()}}
}

// ReplicationTransportListenFailed identifies a failed cluster listener initialization.
func ReplicationTransportListenFailed(address string, cause error) Message {
	return Message{ID: MessageReplicationTransportListenFailed, Fallback: "listen on " + address + ": " + cause.Error(), Data: map[string]any{"Address": address, "Cause": cause.Error()}}
}

// ReplicationTransportLoadTLSCertKeyFailed identifies a TLS identity load failure.
func ReplicationTransportLoadTLSCertKeyFailed(cause error) Message {
	return replicationCause(MessageReplicationTransportLoadTLSCertKeyFailed, "load TLS cert/key: ", cause)
}

// ReplicationTransportReadTLSCAFailed identifies a TLS CA file read failure.
func ReplicationTransportReadTLSCAFailed(cause error) Message {
	return replicationCause(MessageReplicationTransportReadTLSCAFailed, "read TLS CA file: ", cause)
}

// ReplicationTransportInvalidTLSCA identifies a CA file without parseable certificates.
func ReplicationTransportInvalidTLSCA() Message {
	return Message{ID: MessageReplicationTransportInvalidTLSCA, Fallback: "invalid TLS CA file"}
}

// ReplicationTransportUnknownTLSCipherSuite identifies an unsupported cipher suite name.
func ReplicationTransportUnknownTLSCipherSuite(cipherSuite string) Message {
	return Message{ID: MessageReplicationTransportUnknownTLSCipherSuite, Fallback: "unknown TLS cipher suite: " + cipherSuite, Data: map[string]any{"CipherSuite": cipherSuite}}
}

// ReplicationHAInitTransportFailed identifies HA transport initialization failure.
func ReplicationHAInitTransportFailed(cause error) Message {
	return replicationCause(MessageReplicationHAInitTransportFailed, "init transport: ", cause)
}

// ReplicationHAStartPrimaryFailed identifies HA primary startup failure.
func ReplicationHAStartPrimaryFailed(cause error) Message {
	return replicationCause(MessageReplicationHAStartPrimaryFailed, "start primary: ", cause)
}

// ReplicationHAStartStandbyFailed identifies HA standby startup failure.
func ReplicationHAStartStandbyFailed(cause error) Message {
	return replicationCause(MessageReplicationHAStartStandbyFailed, "start standby: ", cause)
}

// ReplicationHAFlushWALFailed identifies a failed WAL flush during promotion.
func ReplicationHAFlushWALFailed(cause error) Message {
	return replicationCause(MessageReplicationHAFlushWALFailed, "flush WAL: ", cause)
}

// ReplicationHAAckTimedOut identifies a standby acknowledgement timeout.
func ReplicationHAAckTimedOut(mode string, target uint64, cause error) Message {
	return Message{ID: MessageReplicationHAAckTimedOut, Fallback: fmt.Sprintf("replication ack timeout (mode=%s, target=%d): %s", mode, target, cause.Error()), Data: map[string]any{"SyncMode": mode, "Target": target, "Cause": cause.Error()}}
}

// ReplicationHAPrimaryCannotReceiveWAL identifies a WAL batch sent to a primary.
func ReplicationHAPrimaryCannotReceiveWAL() Message {
	return Message{ID: MessageReplicationHAPrimaryCannotReceiveWAL, Fallback: "primary cannot receive WAL"}
}

// ReplicationRaftVoteRequestRequired identifies a nil vote request.
func ReplicationRaftVoteRequestRequired() Message {
	return Message{ID: MessageReplicationRaftVoteRequestRequired, Fallback: "nil vote request"}
}

// ReplicationRaftAppendRequestRequired identifies a nil append-entries request.
func ReplicationRaftAppendRequestRequired() Message {
	return Message{ID: MessageReplicationRaftAppendRequestRequired, Fallback: "nil append entries request"}
}

// ReplicationRaftInitTransportFailed identifies Raft transport initialization failure.
func ReplicationRaftInitTransportFailed(cause error) Message {
	return replicationCause(MessageReplicationRaftInitTransportFailed, "init transport: ", cause)
}

// ReplicationRaftSendVoteFailed identifies a failed vote RPC.
func ReplicationRaftSendVoteFailed(cause error) Message {
	return replicationCause(MessageReplicationRaftSendVoteFailed, "send vote request: ", cause)
}

// ReplicationRaftSendAppendFailed identifies a failed append-entries RPC.
func ReplicationRaftSendAppendFailed(cause error) Message {
	return replicationCause(MessageReplicationRaftSendAppendFailed, "send append entries: ", cause)
}

// ReplicationRaftCommitTimedOut identifies a log entry commit timeout.
func ReplicationRaftCommitTimedOut(timeout time.Duration) Message {
	return Message{ID: MessageReplicationRaftCommitTimedOut, Fallback: fmt.Sprintf("commit timeout after %v", timeout), Data: map[string]any{"Timeout": timeout}}
}

// ReplicationRaftTransportNotConfigured identifies missing Raft transport configuration.
func ReplicationRaftTransportNotConfigured() Message {
	return Message{ID: MessageReplicationRaftTransportNotConfigured, Fallback: "no transport configured"}
}

// ReplicationRaftConnectPeerFailed identifies a failed peer connection.
func ReplicationRaftConnectPeerFailed(address string, cause error) Message {
	return Message{ID: MessageReplicationRaftConnectPeerFailed, Fallback: "connect to " + address + ": " + cause.Error(), Data: map[string]any{"Address": address, "Cause": cause.Error()}}
}

// ReplicationRaftForwardToLeaderFailed identifies a failed forwarded write.
func ReplicationRaftForwardToLeaderFailed(cause error) Message {
	return replicationCause(MessageReplicationRaftForwardToLeaderFailed, "forward to leader: ", cause)
}

// ReplicationRaftApplyQueueFull identifies an apply queue admission timeout.
func ReplicationRaftApplyQueueFull() Message {
	return Message{ID: MessageReplicationRaftApplyQueueFull, Fallback: "apply queue full"}
}

// ReplicationRaftApplyTimedOut identifies an apply completion timeout.
func ReplicationRaftApplyTimedOut() Message {
	return Message{ID: MessageReplicationRaftApplyTimedOut, Fallback: "apply timeout"}
}

// ReplicationStorageWALDirectoryCreateFailed identifies WAL directory creation failure.
func ReplicationStorageWALDirectoryCreateFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageWALDirectoryCreateFailed, "failed to create WAL directory: ", cause)
}

// ReplicationStorageWALCreateFailed identifies WAL creation failure.
func ReplicationStorageWALCreateFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageWALCreateFailed, "failed to create WAL: ", cause)
}

// ReplicationStorageCommandRequired identifies a nil replication command.
func ReplicationStorageCommandRequired() Message {
	return Message{ID: MessageReplicationStorageCommandRequired, Fallback: "nil command"}
}

// ReplicationStorageWALAppendFailed identifies WAL append failure.
func ReplicationStorageWALAppendFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageWALAppendFailed, "failed to append to WAL: ", cause)
}

// ReplicationStorageUnknownCommandType identifies an unsupported command type.
func ReplicationStorageUnknownCommandType(commandType uint8) Message {
	return Message{ID: MessageReplicationStorageUnknownCommandType, Fallback: fmt.Sprintf("unknown command type: %d", commandType), Data: map[string]any{"CommandType": commandType}}
}

// ReplicationStorageDecodeNodeFailed identifies node payload decoding failure.
func ReplicationStorageDecodeNodeFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeNodeFailed, "decode node: ", cause)
}

// ReplicationStorageDecodeEdgeFailed identifies edge payload decoding failure.
func ReplicationStorageDecodeEdgeFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeEdgeFailed, "decode edge: ", cause)
}

// ReplicationStorageDecodeDeleteEdgeFailed identifies delete-edge payload decoding failure.
func ReplicationStorageDecodeDeleteEdgeFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeDeleteEdgeFailed, "decode delete edge request: ", cause)
}

// ReplicationStorageDecodeSetPropertyFailed identifies set-property payload decoding failure.
func ReplicationStorageDecodeSetPropertyFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeSetPropertyFailed, "decode set property request: ", cause)
}

// ReplicationStorageDecodeBatchFailed identifies batch payload decoding failure.
func ReplicationStorageDecodeBatchFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeBatchFailed, "decode batch: ", cause)
}

// ReplicationStorageDecodeDeletePrefixFailed identifies prefix-delete payload decoding failure.
func ReplicationStorageDecodeDeletePrefixFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeDeletePrefixFailed, "decode delete by prefix request: ", cause)
}

// ReplicationStoragePrefixRequired identifies a missing deletion prefix.
func ReplicationStoragePrefixRequired() Message {
	return Message{ID: MessageReplicationStoragePrefixRequired, Fallback: "prefix is required"}
}

// ReplicationStorageDecodeBulkCreateNodesFailed identifies bulk node-create payload decoding failure.
func ReplicationStorageDecodeBulkCreateNodesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeBulkCreateNodes, "decode bulk create nodes: ", cause)
}

// ReplicationStorageDecodeBulkCreateEdgesFailed identifies bulk edge-create payload decoding failure.
func ReplicationStorageDecodeBulkCreateEdgesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeBulkCreateEdges, "decode bulk create edges: ", cause)
}

// ReplicationStorageDecodeBulkDeleteNodesFailed identifies bulk node-delete payload decoding failure.
func ReplicationStorageDecodeBulkDeleteNodesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeBulkDeleteNodes, "decode bulk delete nodes: ", cause)
}

// ReplicationStorageDecodeBulkDeleteEdgesFailed identifies bulk edge-delete payload decoding failure.
func ReplicationStorageDecodeBulkDeleteEdgesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeBulkDeleteEdges, "decode bulk delete edges: ", cause)
}

// ReplicationStorageCypherUnavailable identifies missing Cypher execution support.
func ReplicationStorageCypherUnavailable() Message {
	return Message{ID: MessageReplicationStorageCypherUnavailable, Fallback: "cypher executor not available - cannot execute Cypher command"}
}

// ReplicationStorageDecodeCypherFailed identifies Cypher command decoding failure.
func ReplicationStorageDecodeCypherFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeCypherFailed, "unmarshal cypher command: ", cause)
}

// ReplicationStorageCypherQueryEmpty identifies an empty replicated Cypher query.
func ReplicationStorageCypherQueryEmpty() Message {
	return Message{ID: MessageReplicationStorageCypherQueryEmpty, Fallback: "cypher query is empty"}
}

// ReplicationStorageExecuteCypherFailed identifies replicated Cypher execution failure.
func ReplicationStorageExecuteCypherFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageExecuteCypherFailed, "execute cypher query: ", cause)
}

// ReplicationStorageFlushWALFailed identifies storage adapter WAL flush failure.
func ReplicationStorageFlushWALFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageFlushWALFailed, "flush wal: ", cause)
}

// ReplicationStorageReadWALEntriesFailed identifies persistent WAL read failure.
func ReplicationStorageReadWALEntriesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageReadWALEntriesFailed, "failed to read WAL entries: ", cause)
}

// ReplicationStorageGetAllNodesFailed identifies snapshot node enumeration failure.
func ReplicationStorageGetAllNodesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageGetAllNodesFailed, "get all nodes: ", cause)
}

// ReplicationStorageGetAllEdgesFailed identifies snapshot edge enumeration failure.
func ReplicationStorageGetAllEdgesFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageGetAllEdgesFailed, "get all edges: ", cause)
}

// ReplicationStorageEncodeSnapshotFailed identifies snapshot encoding failure.
func ReplicationStorageEncodeSnapshotFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageEncodeSnapshotFailed, "encode snapshot: ", cause)
}

// ReplicationStorageReadSnapshotFailed identifies snapshot read failure.
func ReplicationStorageReadSnapshotFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageReadSnapshotFailed, "read snapshot: ", cause)
}

// ReplicationStorageDecodeSnapshotFailed identifies snapshot decoding failure.
func ReplicationStorageDecodeSnapshotFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageDecodeSnapshotFailed, "decode snapshot: ", cause)
}

// ReplicationStorageRestoreNodeFailed identifies snapshot node restoration failure.
func ReplicationStorageRestoreNodeFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageRestoreNodeFailed, "restore node: ", cause)
}

// ReplicationStorageRestoreEdgeFailed identifies snapshot edge restoration failure.
func ReplicationStorageRestoreEdgeFailed(cause error) Message {
	return replicationCause(MessageReplicationStorageRestoreEdgeFailed, "restore edge: ", cause)
}

// ReplicationEngineNodeRequired identifies a nil node write.
func ReplicationEngineNodeRequired() Message {
	return Message{ID: MessageReplicationEngineNodeRequired, Fallback: "nil node"}
}

// ReplicationEngineEdgeRequired identifies a nil edge write.
func ReplicationEngineEdgeRequired() Message {
	return Message{ID: MessageReplicationEngineEdgeRequired, Fallback: "nil edge"}
}

// ReplicationEngineEncodeNodeFailed identifies node encoding failure.
func ReplicationEngineEncodeNodeFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeNodeFailed, "encode node: ", cause)
}

// ReplicationEngineEncodeEdgeFailed identifies edge encoding failure.
func ReplicationEngineEncodeEdgeFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeEdgeFailed, "encode edge: ", cause)
}

// ReplicationEngineEncodeDeleteEdgeFailed identifies delete-edge request encoding failure.
func ReplicationEngineEncodeDeleteEdgeFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeDeleteEdgeFailed, "encode delete edge request: ", cause)
}

// ReplicationEngineEncodeBulkCreateNodesFailed identifies bulk node-create encoding failure.
func ReplicationEngineEncodeBulkCreateNodesFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeBulkCreateNodes, "encode bulk create nodes: ", cause)
}

// ReplicationEngineEncodeBulkCreateEdgesFailed identifies bulk edge-create encoding failure.
func ReplicationEngineEncodeBulkCreateEdgesFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeBulkCreateEdges, "encode bulk create edges: ", cause)
}

// ReplicationEngineEncodeBulkDeleteNodesFailed identifies bulk node-delete encoding failure.
func ReplicationEngineEncodeBulkDeleteNodesFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeBulkDeleteNodes, "encode bulk delete nodes: ", cause)
}

// ReplicationEngineEncodeBulkDeleteEdgesFailed identifies bulk edge-delete encoding failure.
func ReplicationEngineEncodeBulkDeleteEdgesFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeBulkDeleteEdges, "encode bulk delete edges: ", cause)
}

// ReplicationEngineEncodeDeletePrefixFailed identifies prefix-delete encoding failure.
func ReplicationEngineEncodeDeletePrefixFailed(cause error) Message {
	return replicationCause(MessageReplicationEngineEncodeDeletePrefixFailed, "encode delete by prefix: ", cause)
}
