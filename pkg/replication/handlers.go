package replication

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// RegisterClusterHandlers wires a Replicator's handler methods into a ClusterTransport.
// This is intentionally transport-specific because other Transport implementations may
// not support message-type dispatch.
func RegisterClusterHandlers(t *ClusterTransport, r Replicator) {
	if t == nil || r == nil {
		return
	}

	type walBatchHandler interface {
		HandleWALBatch(entries []*WALEntry) (*WALBatchResponse, error)
	}
	if h, ok := r.(walBatchHandler); ok {
		t.RegisterHandler(ClusterMsgWALBatch, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = nodeID
			var entries []*WALEntry
			if err := decodeGob(msg.Payload, &entries); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("wal batch", err), err)
			}
			resp, err := h.HandleWALBatch(entries)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("wal batch resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgWALBatchResponse, Payload: payload}, nil
		})
	}

	type heartbeatHandler interface {
		HandleHeartbeat(req *HeartbeatRequest) (*HeartbeatResponse, error)
	}
	if h, ok := r.(heartbeatHandler); ok {
		t.RegisterHandler(ClusterMsgHeartbeat, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = nodeID
			var req HeartbeatRequest
			if err := decodeGob(msg.Payload, &req); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("heartbeat", err), err)
			}
			resp, err := h.HandleHeartbeat(&req)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("heartbeat resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: payload}, nil
		})
	}

	type fenceHandler interface {
		HandleFence(req *FenceRequest) (*FenceResponse, error)
	}
	if h, ok := r.(fenceHandler); ok {
		t.RegisterHandler(ClusterMsgFence, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = nodeID
			var req FenceRequest
			if err := decodeGob(msg.Payload, &req); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("fence", err), err)
			}
			resp, err := h.HandleFence(&req)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("fence resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgFenceResponse, Payload: payload}, nil
		})
	}

	type promoteHandler interface {
		HandlePromote(req *PromoteRequest) (*PromoteResponse, error)
	}
	if h, ok := r.(promoteHandler); ok {
		t.RegisterHandler(ClusterMsgPromote, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = nodeID
			var req PromoteRequest
			if err := decodeGob(msg.Payload, &req); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("promote", err), err)
			}
			resp, err := h.HandlePromote(&req)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("promote resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgPromoteResponse, Payload: payload}, nil
		})
	}

	type raftVoteHandler interface {
		HandleRaftVote(req *RaftVoteRequest) (*RaftVoteResponse, error)
	}
	if h, ok := r.(raftVoteHandler); ok {
		t.RegisterHandler(ClusterMsgVoteRequest, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = ctx
			_ = nodeID
			var req RaftVoteRequest
			if err := decodeGob(msg.Payload, &req); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("raft vote", err), err)
			}
			resp, err := h.HandleRaftVote(&req)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("raft vote resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgVoteResponse, Payload: payload}, nil
		})
	}

	type raftAppendHandler interface {
		HandleRaftAppendEntries(req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error)
	}
	if h, ok := r.(raftAppendHandler); ok {
		t.RegisterHandler(ClusterMsgAppendEntries, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = ctx
			_ = nodeID
			var req RaftAppendEntriesRequest
			if err := decodeGob(msg.Payload, &req); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("raft append", err), err)
			}
			resp, err := h.HandleRaftAppendEntries(&req)
			if err != nil {
				return nil, err
			}
			payload, err := encodeGob(resp)
			if err != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("raft append resp", err), err)
			}
			return &ClusterMessage{Type: ClusterMsgAppendEntriesResponse, Payload: payload}, nil
		})
	}

	// Write forwarding: leader applies commands forwarded from followers
	type forwardApplyHandler interface {
		HandleForwardApply(cmd *Command, timeout time.Duration) error
	}
	if h, ok := r.(forwardApplyHandler); ok {
		t.RegisterHandler(ClusterMsgForwardApply, func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			_ = nodeID
			var cmd Command
			if err := decodeGob(msg.Payload, &cmd); err != nil {
				return nil, localizedError(localization.ReplicationRPCDecodeFailed("forward apply", err), err)
			}
			timeout := 30 * time.Second
			if deadline, ok := ctx.Deadline(); ok {
				if d := time.Until(deadline); d > 0 && d < timeout {
					timeout = d
				}
			}
			err := h.HandleForwardApply(&cmd, timeout)
			respPayload := forwardApplyResponse{}
			if err != nil {
				respPayload.Err = err.Error()
			}
			payload, encErr := encodeGob(respPayload)
			if encErr != nil {
				return nil, localizedError(localization.ReplicationRPCEncodeFailed("forward apply resp", encErr), encErr)
			}
			return &ClusterMessage{Type: ClusterMsgForwardApplyResponse, Payload: payload}, nil
		})
	}
}
