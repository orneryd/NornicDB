package replication

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplicationCodec_RollingUpgrade verifies that the codec_version field
// round-trips correctly through JSON, and that pre-version peers (which omit
// the field) produce frames that decode cleanly with CodecVersion == 0.
func TestReplicationCodec_RollingUpgrade(t *testing.T) {
	t.Run("pre-version follower accepts versioned frame", func(t *testing.T) {
		req := AppendEntriesRequest{
			Term:         5,
			LeaderID:     "leader-1",
			LeaderAddr:   "10.0.0.1:7474",
			PrevLogIndex: 10,
			PrevLogTerm:  4,
			LeaderCommit: 9,
			CodecVersion: CurrentCodecVersion,
		}
		data, err := json.Marshal(req)
		require.NoError(t, err)

		// Simulate a pre-version follower that doesn't know about codec_version.
		// It just ignores unknown JSON fields.
		type PreVersionRequest struct {
			Term         uint64          `json:"term"`
			LeaderID     string          `json:"leader_id"`
			LeaderAddr   string          `json:"leader_addr"`
			PrevLogIndex uint64          `json:"prev_log_index"`
			PrevLogTerm  uint64          `json:"prev_log_term"`
			Entries      []*RaftLogEntry `json:"entries"`
			LeaderCommit uint64          `json:"leader_commit"`
		}
		var old PreVersionRequest
		err = json.Unmarshal(data, &old)
		require.NoError(t, err, "pre-version peer must accept versioned frame without error")
		assert.Equal(t, uint64(5), old.Term)
		assert.Equal(t, "leader-1", old.LeaderID)
	})

	t.Run("versioned follower accepts pre-version frame", func(t *testing.T) {
		// Simulate a pre-version leader that sends without codec_version.
		preVersionJSON := `{
			"term": 3,
			"leader_id": "old-leader",
			"leader_addr": "10.0.0.2:7474",
			"prev_log_index": 5,
			"prev_log_term": 2,
			"entries": null,
			"leader_commit": 4
		}`
		var req AppendEntriesRequest
		err := json.Unmarshal([]byte(preVersionJSON), &req)
		require.NoError(t, err, "versioned peer must accept pre-version frame without error")
		assert.Equal(t, uint64(3), req.Term)
		assert.Equal(t, "old-leader", req.LeaderID)
		assert.Equal(t, uint32(0), req.CodecVersion, "absent codec_version must decode as 0")
	})

	t.Run("response echoes codec version", func(t *testing.T) {
		resp := AppendEntriesResponse{
			Term:         5,
			Success:      true,
			MatchIndex:   10,
			ResponderID:  "follower-1",
			CodecVersion: CurrentCodecVersion,
		}
		data, err := json.Marshal(resp)
		require.NoError(t, err)

		var decoded AppendEntriesResponse
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, CurrentCodecVersion, decoded.CodecVersion)
	})

	t.Run("pre-version response has zero codec version", func(t *testing.T) {
		preVersionResp := `{"term":3,"success":true,"match_index":7,"responder_id":"old-node"}`
		var resp AppendEntriesResponse
		err := json.Unmarshal([]byte(preVersionResp), &resp)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resp.CodecVersion,
			"pre-version response must decode codec_version as 0")
	})

	t.Run("leader sends version 0 to unknown peer", func(t *testing.T) {
		r := &RaftReplicator{
			config: &Config{NodeID: "leader"},
		}
		// Peer never responded — should get version 0.
		v := r.peerCodecVersion("unknown-peer")
		assert.Equal(t, uint32(0), v,
			"leader must send codec_version=0 to peers that haven't responded yet")
	})

	t.Run("leader sends CurrentCodecVersion to versioned peer", func(t *testing.T) {
		r := &RaftReplicator{
			config:            &Config{NodeID: "leader"},
			peerCodecVersions: map[string]uint32{"peer-1": CurrentCodecVersion},
		}
		v := r.peerCodecVersion("peer-1")
		assert.Equal(t, CurrentCodecVersion, v,
			"leader must send CurrentCodecVersion to peers that have reported it")
	})
}
