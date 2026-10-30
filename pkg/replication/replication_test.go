package replication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockStorage implements the Storage interface for testing.
type MockStorage struct {
	mu           sync.RWMutex
	commands     []*Command
	walPosition  uint64
	walEntries   []*WALEntry
	applyErr     error
	applyCalled  int
	snapshotData []byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		walEntries: make([]*WALEntry, 0),
	}
}

func (m *MockStorage) ApplyCommand(cmd *Command) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applyErr != nil {
		return m.applyErr
	}

	m.commands = append(m.commands, cmd)
	m.applyCalled++
	m.walPosition++

	// Add to WAL entries
	m.walEntries = append(m.walEntries, &WALEntry{
		Position:  m.walPosition,
		Timestamp: time.Now().UnixNano(),
		Command:   cmd,
	})

	return nil
}

func (m *MockStorage) GetWALPosition() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.walPosition, nil
}

func (m *MockStorage) GetWALEntries(fromPosition uint64, maxEntries int) ([]*WALEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*WALEntry, 0)
	for _, entry := range m.walEntries {
		if entry.Position > fromPosition {
			result = append(result, entry)
			if len(result) >= maxEntries {
				break
			}
		}
	}
	return result, nil
}

func (m *MockStorage) WriteSnapshot(w SnapshotWriter) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, err := w.Write(m.snapshotData)
	return err
}

func (m *MockStorage) RestoreSnapshot(r SnapshotReader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotData = make([]byte, 1024)
	_, err := r.Read(m.snapshotData)
	return err
}

func (m *MockStorage) GetApplyCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.applyCalled
}

func (m *MockStorage) SetApplyError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyErr = err
}

// MockTransport implements Transport for testing.
type MockTransport struct {
	mu          sync.RWMutex
	connections map[string]*MockPeerConn
	handler     ConnectionHandler
	listenAddr  string
	closed      bool
	stopCh      chan struct{}
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		connections: make(map[string]*MockPeerConn),
		stopCh:      make(chan struct{}),
	}
}

func (t *MockTransport) Connect(ctx context.Context, addr string) (PeerConnection, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn := &MockPeerConn{
		addr:      addr,
		connected: true,
	}
	t.connections[addr] = conn
	return conn, nil
}

func (t *MockTransport) Listen(ctx context.Context, addr string, handler ConnectionHandler) error {
	t.mu.Lock()
	t.listenAddr = addr
	t.handler = handler
	stopCh := t.stopCh
	t.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopCh:
		return nil
	}
}

func (t *MockTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.stopCh)
	}
	return nil
}

func (t *MockTransport) SimulateIncomingConnection() *MockPeerConn {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn := &MockPeerConn{
		addr:      "incoming",
		connected: true,
	}

	if t.handler != nil {
		go t.handler(conn)
	}

	return conn
}

// MockPeerConn implements PeerConnection for testing.
type MockPeerConn struct {
	mu              sync.RWMutex
	addr            string
	connected       bool
	walBatchCalls   int
	heartbeatCalls  int
	fenceCalls      int
	promoteCalls    int
	walBatchErr     error
	heartbeatErr    error
	fenceErr        error
	promoteErr      error
	lastWALBatch    []*WALEntry
	lastHeartbeat   *HeartbeatRequest
	simulatedLag    int64
	simulatedWALPos uint64

	// Network latency simulation for cross-region testing
	simulatedLatency time.Duration

	// Raft state for this simulated peer node
	raftTerm      uint64
	raftVotedFor  string
	raftLog       []*RaftLogEntry
	raftCommitIdx uint64
	raftLeaderID  string
}

func (c *MockPeerConn) SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error) {
	c.mu.Lock()
	latency := c.simulatedLatency
	c.walBatchCalls++
	c.lastWALBatch = entries
	err := c.walBatchErr
	c.mu.Unlock()

	// Simulate network latency (cross-region can be 2000ms+)
	if latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(latency):
		}
	}

	if err != nil {
		return nil, err
	}

	var lastPos uint64
	if len(entries) > 0 {
		lastPos = entries[len(entries)-1].Position
	}

	return &WALBatchResponse{
		AckedPosition:    lastPos,
		ReceivedPosition: lastPos,
	}, nil
}

func (c *MockPeerConn) SendHeartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	c.mu.Lock()
	latency := c.simulatedLatency
	c.heartbeatCalls++
	c.lastHeartbeat = req
	err := c.heartbeatErr
	walPos := c.simulatedWALPos
	lag := c.simulatedLag
	c.mu.Unlock()

	// Simulate network latency (cross-region can be 2000ms+)
	if latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(latency):
		}
	}

	if err != nil {
		return nil, err
	}

	return &HeartbeatResponse{
		NodeID:      "peer",
		Role:        "standby",
		WALPosition: walPos,
		Lag:         lag,
	}, nil
}

func (c *MockPeerConn) SendFence(ctx context.Context, req *FenceRequest) (*FenceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fenceCalls++

	if c.fenceErr != nil {
		return nil, c.fenceErr
	}

	return &FenceResponse{Fenced: true}, nil
}

func (c *MockPeerConn) SendPromote(ctx context.Context, req *PromoteRequest) (*PromoteResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.promoteCalls++

	if c.promoteErr != nil {
		return nil, c.promoteErr
	}

	return &PromoteResponse{Ready: true}, nil
}

func (c *MockPeerConn) SendRaftVote(ctx context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error) {
	c.mu.Lock()
	latency := c.simulatedLatency
	c.mu.Unlock()

	// Simulate network latency
	if latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(latency):
		}
	}

	return &RaftVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
		VoterID:     c.addr,
	}, nil
}

func (c *MockPeerConn) SendRaftAppendEntries(ctx context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	c.mu.Lock()
	latency := c.simulatedLatency
	c.mu.Unlock()

	// Simulate network latency
	if latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(latency):
		}
	}

	matchIndex := req.PrevLogIndex
	if len(req.Entries) > 0 {
		matchIndex = req.Entries[len(req.Entries)-1].Index
	}

	return &RaftAppendEntriesResponse{
		Term:        req.Term,
		Success:     true,
		MatchIndex:  matchIndex,
		ResponderID: c.addr,
	}, nil
}

func (c *MockPeerConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
	return nil
}

func (c *MockPeerConn) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *MockPeerConn) GetWALBatchCalls() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.walBatchCalls
}

func (c *MockPeerConn) GetHeartbeatCalls() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.heartbeatCalls
}

// =============================================================================
// Configuration Tests
// =============================================================================

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.Equal(t, ModeStandalone, config.Mode)
	assert.Equal(t, "0.0.0.0:7000", config.BindAddr)
	assert.NotEmpty(t, config.HAStandby.HeartbeatInterval)
	assert.NotEmpty(t, config.HAStandby.FailoverTimeout)
	assert.True(t, config.HAStandby.AutoFailover)
}

func TestLoadFromEnv_Standalone(t *testing.T) {
	// Clear any existing env vars
	t.Setenv("NORNICDB_CLUSTER_MODE", "standalone")
	t.Setenv("NORNICDB_CLUSTER_NODE_ID", "test-node")

	config := LoadFromEnv()

	assert.Equal(t, ModeStandalone, config.Mode)
	assert.Equal(t, "test-node", config.NodeID)
}

func TestLoadFromEnv_HAStandby(t *testing.T) {
	t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
	t.Setenv("NORNICDB_CLUSTER_NODE_ID", "primary-1")
	t.Setenv("NORNICDB_CLUSTER_HA_ROLE", "primary")
	t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "standby-1:7000")
	t.Setenv("NORNICDB_CLUSTER_HA_AUTO_FAILOVER", "true")
	t.Setenv("NORNICDB_CLUSTER_HA_HEARTBEAT_MS", "500")
	t.Setenv("NORNICDB_CLUSTER_HA_FAILOVER_TIMEOUT", "15s")

	config := LoadFromEnv()

	assert.Equal(t, ModeHAStandby, config.Mode)
	assert.Equal(t, "primary-1", config.NodeID)
	assert.Equal(t, "primary", config.HAStandby.Role)
	assert.Equal(t, "standby-1:7000", config.HAStandby.PeerAddr)
	assert.True(t, config.HAStandby.AutoFailover)
	assert.Equal(t, 500*time.Millisecond, config.HAStandby.HeartbeatInterval)
	assert.Equal(t, 15*time.Second, config.HAStandby.FailoverTimeout)
}

func TestLoadFromEnv_Raft(t *testing.T) {
	t.Setenv("NORNICDB_CLUSTER_MODE", "raft")
	t.Setenv("NORNICDB_CLUSTER_NODE_ID", "node-1")
	t.Setenv("NORNICDB_CLUSTER_RAFT_BOOTSTRAP", "true")
	t.Setenv("NORNICDB_CLUSTER_RAFT_PEERS", "node-2:host2:7000,node-3:host3:7000")
	t.Setenv("NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT", "2s")

	config := LoadFromEnv()

	assert.Equal(t, ModeRaft, config.Mode)
	assert.Equal(t, "node-1", config.NodeID)
	assert.True(t, config.Raft.Bootstrap)
	assert.Len(t, config.Raft.Peers, 2)
	assert.Equal(t, "node-2", config.Raft.Peers[0].ID)
	assert.Equal(t, "host2:7000", config.Raft.Peers[0].Addr)
	assert.Equal(t, 2*time.Second, config.Raft.ElectionTimeout)
}

func TestConfig_Validate_Standalone(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeStandalone

	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfig_Validate_HAStandby_MissingRole(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.HAStandby.PeerAddr = "peer:7000"

	err := config.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "HA_ROLE")
}

func TestConfig_Validate_HAStandby_MissingPeer(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.HAStandby.Role = "primary"

	err := config.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PEER_ADDR")
}

func TestConfig_Validate_HAStandby_Valid(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "test-node"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"

	err := config.Validate()
	assert.NoError(t, err)
}

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []PeerConfig
	}{
		{
			name:     "empty",
			input:    "",
			expected: nil,
		},
		{
			name:  "single with id",
			input: "node-2:host2:7000",
			expected: []PeerConfig{
				{ID: "node-2", Addr: "host2:7000"},
			},
		},
		{
			name:  "single without id",
			input: "host2:7000",
			expected: []PeerConfig{
				{ID: "peer-1", Addr: "host2:7000"},
			},
		},
		{
			name:  "multiple",
			input: "node-2:host2:7000,node-3:host3:7000",
			expected: []PeerConfig{
				{ID: "node-2", Addr: "host2:7000"},
				{ID: "node-3", Addr: "host3:7000"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePeers(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Standalone Replicator Tests
// =============================================================================

func TestStandaloneReplicator_Start(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	ctx := context.Background()
	err := replicator.Start(ctx)
	require.NoError(t, err)

	assert.True(t, replicator.IsLeader())
	assert.Equal(t, ModeStandalone, replicator.Mode())

	health := replicator.Health()
	assert.True(t, health.Healthy)
	assert.Equal(t, "ready", health.State)
}

func TestStandaloneReplicator_Apply(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	ctx := context.Background()
	err := replicator.Start(ctx)
	require.NoError(t, err)

	cmd := &Command{
		Type:      CmdCreateNode,
		Data:      []byte("test data"),
		Timestamp: time.Now(),
	}

	err = replicator.Apply(cmd, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 1, storage.GetApplyCount())
}

func TestStandaloneReplicator_ApplyBeforeStart(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err := replicator.Apply(cmd, time.Second)
	assert.ErrorIs(t, err, ErrNotReady)
}

func TestStandaloneReplicator_ApplyAfterShutdown(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))
	require.NoError(t, replicator.Shutdown())

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err := replicator.Apply(cmd, time.Second)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestStandaloneReplicator_ApplyBatch(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))

	cmds := []*Command{
		{Type: CmdCreateNode, Data: []byte("1")},
		{Type: CmdCreateNode, Data: []byte("2")},
		{Type: CmdCreateNode, Data: []byte("3")},
	}

	err := replicator.ApplyBatch(cmds, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 3, storage.GetApplyCount())
}

func TestStandaloneReplicator_WaitForLeader(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()

	replicator := NewStandaloneReplicator(config, storage)

	ctx := context.Background()
	err := replicator.WaitForLeader(ctx)
	assert.NoError(t, err) // Should return immediately for standalone
}

// =============================================================================
// HA Standby Replicator Tests
// =============================================================================

func TestHAStandbyReplicator_Primary_Start(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "primary-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	err = replicator.Start(ctx)
	require.NoError(t, err)

	assert.True(t, replicator.IsLeader())
	assert.Equal(t, ModeHAStandby, replicator.Mode())
	assert.Equal(t, "primary-1", replicator.NodeID())

	health := replicator.Health()
	assert.Equal(t, "primary", health.Role)
	assert.True(t, health.IsLeader)

	// Cleanup
	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_Standby_Start(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "standby-1"
	config.HAStandby.Role = "standby"
	config.HAStandby.PeerAddr = "primary-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	err = replicator.Start(ctx)
	require.NoError(t, err)

	assert.False(t, replicator.IsLeader())
	assert.Equal(t, "primary-1:7000", replicator.LeaderAddr())

	health := replicator.Health()
	assert.Equal(t, "standby", health.Role)
	assert.False(t, health.IsLeader)

	// Cancel context and close transport before shutdown to allow goroutines to exit
	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_Primary_Apply(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "primary-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err = replicator.Apply(cmd, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 1, storage.GetApplyCount())

	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_Primary_Apply_SyncAck(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "primary-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby-1:7000"
	config.HAStandby.SyncMode = SyncQuorum
	config.HAStandby.WALBatchTimeout = 20 * time.Millisecond
	config.HAStandby.HeartbeatInterval = 10 * time.Millisecond

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)
	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { _ = transport.Close() })
	t.Cleanup(func() { _ = replicator.Shutdown() })

	require.NoError(t, replicator.Start(ctx))

	// Wait for primary to establish standby connection.
	require.Eventually(t, func() bool {
		replicator.mu.RLock()
		defer replicator.mu.RUnlock()
		return replicator.standbyConn != nil && replicator.standbyConn.IsConnected()
	}, 500*time.Millisecond, 10*time.Millisecond)

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test-sync"),
	}

	start := time.Now()
	err = replicator.Apply(cmd, 500*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, 1, storage.GetApplyCount())
	assert.Less(t, time.Since(start), 500*time.Millisecond)

	// The mock standby should have received the WAL batch.
	replicator.mu.RLock()
	mockConn, _ := transport.connections[config.HAStandby.PeerAddr]
	replicator.mu.RUnlock()
	require.NotNil(t, mockConn)
	assert.GreaterOrEqual(t, mockConn.walBatchCalls, 1)
}

func TestHAStandbyReplicator_Standby_Apply_Rejected(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "standby-1"
	config.HAStandby.Role = "standby"
	config.HAStandby.PeerAddr = "primary-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err = replicator.Apply(cmd, time.Second)
	assert.ErrorIs(t, err, ErrStandbyMode)
	assert.Equal(t, 0, storage.GetApplyCount())

	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_HandleWALBatch(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "standby-1"
	config.HAStandby.Role = "standby"
	config.HAStandby.PeerAddr = "primary-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	entries := []*WALEntry{
		{Position: 1, Command: &Command{Type: CmdCreateNode, Data: []byte("1")}},
		{Position: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("2")}},
	}

	resp, err := replicator.HandleWALBatch(entries)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), resp.AckedPosition)
	assert.Equal(t, 2, storage.GetApplyCount())

	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_HandleHeartbeat(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "standby-1"
	config.HAStandby.Role = "standby"
	config.HAStandby.PeerAddr = "primary-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	req := &HeartbeatRequest{
		NodeID:      "primary-1",
		Role:        "primary",
		WALPosition: 100,
		Timestamp:   time.Now().UnixNano(),
	}

	resp, err := replicator.HandleHeartbeat(req)
	require.NoError(t, err)

	assert.Equal(t, "standby-1", resp.NodeID)
	assert.Equal(t, "standby", resp.Role)

	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_HandleFence(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "primary-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	assert.True(t, replicator.IsLeader())

	req := &FenceRequest{
		Reason:    "standby_promotion",
		RequestID: "test-123",
	}

	resp, err := replicator.HandleFence(req)
	require.NoError(t, err)

	assert.True(t, resp.Fenced)
	assert.False(t, replicator.IsLeader())

	cancel()
	transport.Close()
	replicator.Shutdown()
}

func TestHAStandbyReplicator_Promote(t *testing.T) {
	storage := NewMockStorage()
	transport := NewMockTransport()

	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "standby-1"
	config.HAStandby.Role = "standby"
	config.HAStandby.PeerAddr = "primary-1:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err)

	replicator.SetTransport(transport)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, replicator.Start(ctx))

	assert.False(t, replicator.IsLeader())

	err = replicator.Promote(ctx)
	require.NoError(t, err)

	assert.True(t, replicator.IsLeader())

	health := replicator.Health()
	assert.Equal(t, "primary", health.Role)

	cancel()
	transport.Close()
	replicator.Shutdown()
}

// =============================================================================
// Raft Replicator Tests
// =============================================================================

func TestRaftReplicator_Start(t *testing.T) {
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = true

	replicator, err := NewRaftReplicator(config, storage)
	require.NoError(t, err)

	ctx := context.Background()
	err = replicator.Start(ctx)
	require.NoError(t, err)
	defer replicator.Shutdown()

	// In stub mode, should be leader immediately
	assert.True(t, replicator.IsLeader())
	assert.Equal(t, ModeRaft, replicator.Mode())

	health := replicator.Health()
	assert.Equal(t, "leader", health.Role)
}

func TestRaftReplicator_Apply(t *testing.T) {
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = true

	replicator, err := NewRaftReplicator(config, storage)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))
	defer replicator.Shutdown()

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err = replicator.Apply(cmd, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 1, storage.GetApplyCount())
}

func TestRaftReplicator_ShutdownClosesTransport(t *testing.T) {
	// This test verifies that Shutdown properly closes the transport,
	// which unblocks the listenForPeers goroutine and prevents deadlocks.
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = true
	config.AdvertiseAddr = "localhost:17689" // Need advertise addr for listener to start

	replicator, err := NewRaftReplicator(config, storage)
	require.NoError(t, err)

	// Set up a mock transport that tracks if Close was called
	transport := NewMockTransport()
	replicator.SetTransport(transport)

	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))

	// Give listener time to start
	time.Sleep(10 * time.Millisecond)

	// Verify the transport is listening
	transport.mu.RLock()
	listenAddr := transport.listenAddr
	transport.mu.RUnlock()
	assert.NotEmpty(t, listenAddr, "Transport should be listening")

	// Shutdown should complete without deadlock (timeout would fail this test)
	shutdownDone := make(chan struct{})
	go func() {
		replicator.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Success - shutdown completed
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown deadlocked - transport was not closed properly")
	}

	// Verify transport was closed
	transport.mu.RLock()
	closed := transport.closed
	transport.mu.RUnlock()
	assert.True(t, closed, "Transport should be closed during shutdown")
}

func TestRaftReplicator_ShutdownWithActiveListener(t *testing.T) {
	// This test ensures shutdown works even when the listener is actively
	// blocked waiting for connections.
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = true
	config.AdvertiseAddr = "localhost:17688"

	replicator, err := NewRaftReplicator(config, storage)
	require.NoError(t, err)

	transport := NewMockTransport()
	replicator.SetTransport(transport)

	// Use a context that won't be cancelled (simulating real-world usage)
	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))

	// Give the listener time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Shutdown must complete within reasonable time
	start := time.Now()
	err = replicator.Shutdown()
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, elapsed, 2*time.Second, "Shutdown took too long, possible deadlock")
}

// =============================================================================
// Multi-Region Replicator Tests
// =============================================================================

func TestMultiRegionReplicator_Start(t *testing.T) {
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "us-east-1"
	config.MultiRegion.RegionID = "us-east"
	config.MultiRegion.LocalCluster = config.Raft
	config.MultiRegion.LocalCluster.Bootstrap = true

	replicator, err := NewMultiRegionReplicator(config, storage)
	require.NoError(t, err)

	ctx := context.Background()
	err = replicator.Start(ctx)
	require.NoError(t, err)
	defer replicator.Shutdown()

	assert.Equal(t, ModeMultiRegion, replicator.Mode())
	assert.Equal(t, "us-east", replicator.RegionID())

	health := replicator.Health()
	assert.Equal(t, "us-east", health.Region)
}

func TestMultiRegionReplicator_Apply(t *testing.T) {
	storage := NewMockStorage()

	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "us-east-1"
	config.MultiRegion.RegionID = "us-east"
	config.MultiRegion.LocalCluster = config.Raft
	config.MultiRegion.LocalCluster.Bootstrap = true

	replicator, err := NewMultiRegionReplicator(config, storage)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, replicator.Start(ctx))
	defer replicator.Shutdown()

	cmd := &Command{
		Type: CmdCreateNode,
		Data: []byte("test"),
	}

	err = replicator.Apply(cmd, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 1, storage.GetApplyCount())
}

// =============================================================================
// NewReplicator Factory Tests
// =============================================================================

func TestNewReplicator_Standalone(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeStandalone

	replicator, err := NewReplicator(config, storage)
	require.NoError(t, err)

	assert.IsType(t, &StandaloneReplicator{}, replicator)
}

func TestNewReplicator_HAStandby(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "test"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "peer:7000"

	replicator, err := NewReplicator(config, storage)
	require.NoError(t, err)

	assert.IsType(t, &HAStandbyReplicator{}, replicator)
}

func TestNewReplicator_Raft(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "test"
	config.Raft.Bootstrap = true

	replicator, err := NewReplicator(config, storage)
	require.NoError(t, err)

	assert.IsType(t, &RaftReplicator{}, replicator)
}

func TestNewReplicator_MultiRegion(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "test"
	config.MultiRegion.RegionID = "us-east"
	config.MultiRegion.LocalCluster.Bootstrap = true

	replicator, err := NewReplicator(config, storage)
	require.NoError(t, err)

	assert.IsType(t, &MultiRegionReplicator{}, replicator)
}

func TestNewReplicator_InvalidMode(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ReplicationMode("invalid")

	_, err := NewReplicator(config, storage)
	assert.Error(t, err)
}

// =============================================================================
// WAL Streamer Tests
// =============================================================================

func TestWALStreamer_GetPendingEntries(t *testing.T) {
	storage := NewMockStorage()

	// Add some WAL entries
	for i := 0; i < 5; i++ {
		storage.ApplyCommand(&Command{Type: CmdCreateNode, Data: []byte{byte(i)}})
	}

	streamer := NewWALStreamer(storage, 100)

	entries, err := streamer.GetPendingEntries(10)
	require.NoError(t, err)

	assert.Len(t, entries, 5)
}

func TestWALStreamer_AcknowledgePosition(t *testing.T) {
	storage := NewMockStorage()

	// Add some WAL entries
	for i := 0; i < 5; i++ {
		storage.ApplyCommand(&Command{Type: CmdCreateNode, Data: []byte{byte(i)}})
	}

	streamer := NewWALStreamer(storage, 100)

	// Get all entries
	entries, err := streamer.GetPendingEntries(10)
	require.NoError(t, err)
	assert.Len(t, entries, 5)

	// Acknowledge up to position 3
	streamer.AcknowledgePosition(3)

	// Should only get entries after position 3
	entries, err = streamer.GetPendingEntries(10)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

// =============================================================================
// WAL Applier Tests
// =============================================================================

func TestWALApplier_ApplyBatch(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)

	entries := []*WALEntry{
		{Position: 1, Command: &Command{Type: CmdCreateNode, Data: []byte("1")}},
		{Position: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("2")}},
		{Position: 3, Command: &Command{Type: CmdCreateNode, Data: []byte("3")}},
	}

	lastApplied, err := applier.ApplyBatch(entries)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), lastApplied)
	assert.Equal(t, 3, storage.GetApplyCount())
}

func TestWALApplier_ApplyBatch_SkipAlreadyApplied(t *testing.T) {
	storage := NewMockStorage()
	applier := NewWALApplier(storage)

	// First batch
	entries1 := []*WALEntry{
		{Position: 1, Command: &Command{Type: CmdCreateNode, Data: []byte("1")}},
		{Position: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("2")}},
	}

	_, err := applier.ApplyBatch(entries1)
	require.NoError(t, err)
	assert.Equal(t, 2, storage.GetApplyCount())

	// Second batch with overlap
	entries2 := []*WALEntry{
		{Position: 2, Command: &Command{Type: CmdCreateNode, Data: []byte("2")}}, // Already applied
		{Position: 3, Command: &Command{Type: CmdCreateNode, Data: []byte("3")}},
	}

	_, err = applier.ApplyBatch(entries2)
	require.NoError(t, err)
	assert.Equal(t, 3, storage.GetApplyCount()) // Only one new command applied
}

// =============================================================================
// Integration/E2E Tests
// =============================================================================

func TestE2E_HAStandby_Failover(t *testing.T) {
	// Create primary
	primaryStorage := NewMockStorage()
	primaryTransport := NewMockTransport()

	primaryConfig := DefaultConfig()
	primaryConfig.Mode = ModeHAStandby
	primaryConfig.NodeID = "primary-1"
	primaryConfig.HAStandby.Role = "primary"
	primaryConfig.HAStandby.PeerAddr = "standby-1:7000"
	primaryConfig.HAStandby.FailoverTimeout = 100 * time.Millisecond
	primaryConfig.HAStandby.HeartbeatInterval = 50 * time.Millisecond

	primary, err := NewHAStandbyReplicator(primaryConfig, primaryStorage)
	require.NoError(t, err)
	primary.SetTransport(primaryTransport)

	// Create standby
	standbyStorage := NewMockStorage()
	standbyTransport := NewMockTransport()

	standbyConfig := DefaultConfig()
	standbyConfig.Mode = ModeHAStandby
	standbyConfig.NodeID = "standby-1"
	standbyConfig.HAStandby.Role = "standby"
	standbyConfig.HAStandby.PeerAddr = "primary-1:7000"
	standbyConfig.HAStandby.FailoverTimeout = 100 * time.Millisecond
	standbyConfig.HAStandby.HeartbeatInterval = 50 * time.Millisecond
	standbyConfig.HAStandby.AutoFailover = true

	standby, err := NewHAStandbyReplicator(standbyConfig, standbyStorage)
	require.NoError(t, err)
	standby.SetTransport(standbyTransport)

	// Start both
	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, primary.Start(ctx))
	require.NoError(t, standby.Start(ctx))

	// Verify initial state
	assert.True(t, primary.IsLeader())
	assert.False(t, standby.IsLeader())

	// Write some data on primary
	cmd := &Command{Type: CmdCreateNode, Data: []byte("test")}
	require.NoError(t, primary.Apply(cmd, time.Second))

	// Simulate primary going down by fencing it
	_, err = primary.HandleFence(&FenceRequest{Reason: "test"})
	require.NoError(t, err)
	assert.False(t, primary.IsLeader())

	// Manually promote standby (in real scenario, auto-failover would do this)
	require.NoError(t, standby.Promote(ctx))
	assert.True(t, standby.IsLeader())

	// Verify standby can now accept writes
	cmd2 := &Command{Type: CmdCreateNode, Data: []byte("after failover")}
	err = standby.Apply(cmd2, time.Second)
	assert.NoError(t, err)

	// Cleanup
	cancel()
	primaryTransport.Close()
	standbyTransport.Close()
	primary.Shutdown()
	standby.Shutdown()
}

func TestE2E_StandaloneToHA_Migration(t *testing.T) {
	storage := NewMockStorage()

	// Start as standalone
	standaloneConfig := DefaultConfig()
	standaloneConfig.Mode = ModeStandalone
	standaloneConfig.NodeID = "node-1"

	standalone := NewStandaloneReplicator(standaloneConfig, storage)

	ctx := context.Background()
	require.NoError(t, standalone.Start(ctx))

	// Write some data
	for i := 0; i < 5; i++ {
		cmd := &Command{Type: CmdCreateNode, Data: []byte{byte(i)}}
		require.NoError(t, standalone.Apply(cmd, time.Second))
	}
	require.NoError(t, standalone.Shutdown())

	assert.Equal(t, 5, storage.GetApplyCount())

	// Now start as HA primary (same storage)
	haConfig := DefaultConfig()
	haConfig.Mode = ModeHAStandby
	haConfig.NodeID = "node-1"
	haConfig.HAStandby.Role = "primary"
	haConfig.HAStandby.PeerAddr = "standby:7000"

	transport := NewMockTransport()
	ha, err := NewHAStandbyReplicator(haConfig, storage)
	require.NoError(t, err)
	ha.SetTransport(transport)

	haCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ha.Start(haCtx))

	// Should still be able to write
	cmd := &Command{Type: CmdCreateNode, Data: []byte("after migration")}
	require.NoError(t, ha.Apply(cmd, time.Second))

	assert.Equal(t, 6, storage.GetApplyCount())

	// Cleanup
	cancel()
	transport.Close()
	ha.Shutdown()
}

// =============================================================================
// Bad Configuration Tests - Fail to Start
// =============================================================================

func TestConfig_Validate_Raft_MissingPeersNonBootstrap(t *testing.T) {
	// Raft node that is NOT bootstrap must have peers
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = false
	config.Raft.Peers = nil

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NORNICDB_CLUSTER_RAFT_PEERS")
	assert.Contains(t, err.Error(), "NORNICDB_CLUSTER_RAFT_BOOTSTRAP")
}

func TestConfig_Validate_Raft_BootstrapWithoutPeersAllowed(t *testing.T) {
	// Bootstrap node CAN start without peers (others join later)
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = true
	config.Raft.Peers = nil

	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfig_Validate_Raft_WithPeersAllowed(t *testing.T) {
	// Non-bootstrap with peers is valid
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-2"
	config.Raft.Bootstrap = false
	config.Raft.Peers = []PeerConfig{{ID: "node-1", Addr: "leader:7000"}}

	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfig_Validate_MultiRegion_MissingRegionID(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "node-1"
	config.MultiRegion.RegionID = ""

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NORNICDB_CLUSTER_REGION_ID")
}

func TestConfig_Validate_MultiRegion_Valid(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "node-1"
	config.MultiRegion.RegionID = "us-east"
	config.MultiRegion.LocalCluster.Bootstrap = true

	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfig_Validate_TLS_EnabledMissingCert(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "node-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"
	config.TLS.Enabled = true
	config.TLS.CertFile = ""
	config.TLS.KeyFile = "/path/to/key"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS_CERT_FILE")
}

func TestConfig_Validate_TLS_EnabledMissingKey(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "node-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"
	config.TLS.Enabled = true
	config.TLS.CertFile = "/path/to/cert"
	config.TLS.KeyFile = ""

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS_KEY_FILE")
}

func TestConfig_Validate_TLS_VerifyClientMissingCA(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "node-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"
	config.TLS.Enabled = true
	config.TLS.CertFile = "/path/to/cert"
	config.TLS.KeyFile = "/path/to/key"
	config.TLS.VerifyClient = true
	config.TLS.CAFile = ""

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS_CA_FILE")
}

func TestConfig_Validate_TLS_InvalidMinVersion(t *testing.T) {
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "node-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"
	config.TLS.Enabled = true
	config.TLS.CertFile = "/path/to/cert"
	config.TLS.KeyFile = "/path/to/key"
	config.TLS.MinVersion = "1.0" // Invalid - must be 1.2 or 1.3

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS min version")
}

// =============================================================================
// Replicator Creation Failure Tests
// =============================================================================

func TestNewHAStandbyReplicator_FailsWithBadConfig(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "" // Missing required field
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "standby:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.Error(t, err)
	assert.Nil(t, replicator)
	assert.Contains(t, err.Error(), "NODE_ID")
}

func TestNewRaftReplicator_FailsWithBadConfig(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = false
	config.Raft.Peers = nil // Missing required peers for non-bootstrap

	replicator, err := NewRaftReplicator(config, storage)
	require.Error(t, err)
	assert.Nil(t, replicator)
	assert.Contains(t, err.Error(), "NORNICDB_CLUSTER_RAFT_PEERS")
}

func TestNewMultiRegionReplicator_FailsWithBadConfig(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeMultiRegion
	config.NodeID = "node-1"
	config.MultiRegion.RegionID = "" // Missing required field

	replicator, err := NewMultiRegionReplicator(config, storage)
	require.Error(t, err)
	assert.Nil(t, replicator)
	assert.Contains(t, err.Error(), "REGION_ID")
}

func TestNewReplicator_FailsWithInvalidHARole(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "node-1"
	config.HAStandby.Role = "master" // Invalid - must be "primary" or "standby"
	config.HAStandby.PeerAddr = "standby:7000"

	replicator, err := NewReplicator(config, storage)
	require.Error(t, err)
	assert.Nil(t, replicator)
	assert.Contains(t, err.Error(), "invalid HA role")
}

// =============================================================================
// Startup Failure Tests - Connection Issues
// =============================================================================

func TestHAStandby_StartFailsWhenPeerUnreachable(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeHAStandby
	config.NodeID = "primary-1"
	config.HAStandby.Role = "primary"
	config.HAStandby.PeerAddr = "unreachable-host:7000"

	replicator, err := NewHAStandbyReplicator(config, storage)
	require.NoError(t, err) // Creation succeeds

	// Use a transport that fails to connect
	failingTransport := &FailingTransport{connectError: fmt.Errorf("connection refused: unreachable-host:7000")}
	replicator.SetTransport(failingTransport)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start succeeds but connection fails (HA is resilient to peer unavailability)
	// The primary will keep trying to connect in the background
	err = replicator.Start(ctx)
	require.NoError(t, err) // Start succeeds even when peer is initially unreachable

	// Health should show the node is running but peer status reflects unavailability
	health := replicator.Health()
	assert.True(t, health.IsLeader) // Primary is leader (can accept writes)
	assert.Equal(t, "primary", health.Role)
	// Note: The primary stays healthy even when standby is unavailable
	// but replication will be pending

	replicator.Shutdown()
}

func TestRaft_StartFailsWhenCannotFormQuorum(t *testing.T) {
	storage := NewMockStorage()
	config := DefaultConfig()
	config.Mode = ModeRaft
	config.NodeID = "node-1"
	config.Raft.Bootstrap = false
	config.Raft.Peers = []PeerConfig{
		{ID: "leader", Addr: "unreachable-leader:7000"},
	}
	config.Raft.ElectionTimeout = 500 * time.Millisecond // Fast timeout for test

	replicator, err := NewRaftReplicator(config, storage)
	require.NoError(t, err)

	// Use a transport that fails to connect
	failingTransport := &FailingTransport{connectError: fmt.Errorf("connection refused")}
	replicator.SetTransport(failingTransport)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the replicator - it should start but not become leader
	err = replicator.Start(ctx)
	require.NoError(t, err) // Start succeeds but node is in follower state

	// WaitForLeader should timeout when no quorum
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	err = replicator.WaitForLeader(waitCtx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Health shows not a leader (either follower or candidate trying to become leader)
	health := replicator.Health()
	assert.False(t, health.IsLeader)
	// Role will be "candidate" because node is trying to become leader but can't get votes
	assert.True(t, health.Role == "follower" || health.Role == "candidate")

	replicator.Shutdown()
}

// FailingTransport is a mock transport that always fails to connect
type FailingTransport struct {
	connectError error
}

func (t *FailingTransport) Connect(ctx context.Context, addr string) (PeerConnection, error) {
	return nil, t.connectError
}

func (t *FailingTransport) Listen(ctx context.Context, addr string, handler ConnectionHandler) error {
	return nil // Don't actually listen
}

func (t *FailingTransport) Close() error {
	return nil
}

func (t *FailingTransport) RegisterHandler(msgType ClusterMessageType, handler func(msg *ClusterMessage) *ClusterMessage) {
	// No-op
}

// =============================================================================
// Configuration Edge Cases
// =============================================================================

func TestConfig_Validate_AllValidHARoles(t *testing.T) {
	validRoles := []string{"primary", "standby"}

	for _, role := range validRoles {
		t.Run(role, func(t *testing.T) {
			config := DefaultConfig()
			config.Mode = ModeHAStandby
			config.NodeID = "test-node"
			config.HAStandby.Role = role
			config.HAStandby.PeerAddr = "peer:7000"

			err := config.Validate()
			assert.NoError(t, err)
		})
	}
}

func TestConfig_Validate_InvalidHARoles(t *testing.T) {
	invalidRoles := []string{"master", "slave", "leader", "follower", "main", "replica", ""}

	for _, role := range invalidRoles {
		t.Run("role_"+role, func(t *testing.T) {
			config := DefaultConfig()
			config.Mode = ModeHAStandby
			config.NodeID = "test-node"
			config.HAStandby.Role = role
			config.HAStandby.PeerAddr = "peer:7000"

			err := config.Validate()
			require.Error(t, err)
		})
	}
}

func TestConfig_Validate_AllModes(t *testing.T) {
	tests := []struct {
		name        string
		mode        ReplicationMode
		setup       func(*Config)
		expectError bool
	}{
		{
			name: "standalone_valid",
			mode: ModeStandalone,
			setup: func(c *Config) {
				c.NodeID = "test"
			},
			expectError: false,
		},
		{
			name: "ha_standby_valid_primary",
			mode: ModeHAStandby,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.HAStandby.Role = "primary"
				c.HAStandby.PeerAddr = "standby:7000"
			},
			expectError: false,
		},
		{
			name: "ha_standby_valid_standby",
			mode: ModeHAStandby,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.HAStandby.Role = "standby"
				c.HAStandby.PeerAddr = "primary:7000"
			},
			expectError: false,
		},
		{
			name: "ha_standby_invalid_no_role",
			mode: ModeHAStandby,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.HAStandby.PeerAddr = "primary:7000"
			},
			expectError: true,
		},
		{
			name: "ha_standby_invalid_no_peer",
			mode: ModeHAStandby,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.HAStandby.Role = "primary"
			},
			expectError: true,
		},
		{
			name: "raft_valid_bootstrap",
			mode: ModeRaft,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.Raft.Bootstrap = true
			},
			expectError: false,
		},
		{
			name: "raft_valid_with_peers",
			mode: ModeRaft,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.Raft.Bootstrap = false
				c.Raft.Peers = []PeerConfig{{ID: "leader", Addr: "leader:7000"}}
			},
			expectError: false,
		},
		{
			name: "raft_invalid_no_bootstrap_no_peers",
			mode: ModeRaft,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.Raft.Bootstrap = false
				c.Raft.Peers = nil
			},
			expectError: true,
		},
		{
			name: "multi_region_valid",
			mode: ModeMultiRegion,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.MultiRegion.RegionID = "us-east"
				c.MultiRegion.LocalCluster.Bootstrap = true
			},
			expectError: false,
		},
		{
			name: "multi_region_invalid_no_region",
			mode: ModeMultiRegion,
			setup: func(c *Config) {
				c.NodeID = "test"
				c.MultiRegion.LocalCluster.Bootstrap = true
			},
			expectError: true,
		},
		{
			name: "invalid_mode",
			mode: ReplicationMode("bogus"),
			setup: func(c *Config) {
				c.NodeID = "test"
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Mode = tt.mode
			tt.setup(config)

			err := config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
