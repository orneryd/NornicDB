// Package replication provides cluster replication for NornicDB.
//
// Transport Architecture:
//
// NornicDB cluster communication uses a hybrid approach:
//
// 1. **Client Bolt Protocol (default port 7687)** - Used for:
//
//   - Neo4j driver compatibility for client queries
//
//   - Writes can be sent to any node: if a write hits a follower, the cluster
//     transport automatically forwards it to the leader (ForwardApply) and returns
//     the leader's response, so clients do not need to route writes to the leader.
//
// 2. **Cluster Protocol (default port 7000)** - Used for:
//   - Raft consensus (RequestVote, AppendEntries)
//   - Write forwarding (ForwardApply from follower to leader)
//   - WAL streaming for HA standby
//   - Heartbeats and health checks
//   - Cluster coordination
//
// This separation allows:
//   - Client-facing Bolt remains pure Neo4j compatible
//   - Cluster protocol is optimized for low-latency consensus
//   - Existing Bolt infrastructure reused where appropriate
//
// Example Configuration:
//
//	NORNICDB_CLUSTER_MODE=raft
//	NORNICDB_CLUSTER_BIND_ADDR=0.0.0.0:7000
//	NORNICDB_BOLT_PORT=7687
package replication

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// ClusterMessageType identifies cluster protocol messages.
type ClusterMessageType uint8

const (
	// Raft consensus messages
	ClusterMsgVoteRequest ClusterMessageType = iota + 1
	ClusterMsgVoteResponse
	ClusterMsgAppendEntries
	ClusterMsgAppendEntriesResponse

	// HA standby messages
	ClusterMsgWALBatch
	ClusterMsgWALBatchResponse
	ClusterMsgHeartbeat
	ClusterMsgHeartbeatResponse
	ClusterMsgFence
	ClusterMsgFenceResponse
	ClusterMsgPromote
	ClusterMsgPromoteResponse

	// Cluster management
	ClusterMsgJoin
	ClusterMsgJoinResponse
	ClusterMsgLeave
	ClusterMsgLeaveResponse
	ClusterMsgStatus
	ClusterMsgStatusResponse

	// Write forwarding: follower sends write to leader for application
	ClusterMsgForwardApply
	ClusterMsgForwardApplyResponse
)

// forwardApplyResponse is the payload of ClusterMsgForwardApplyResponse.
// Err is empty on success; otherwise the leader's error message.
type forwardApplyResponse struct {
	Err string
}

// ClusterMessage is the on-wire format for cluster communication.
type ClusterMessage struct {
	Type      ClusterMessageType
	NodeID    string
	Timestamp int64
	Signature string
	Payload   []byte
}

// ClusterTransport handles cluster-to-cluster communication.
//
// For client-facing queries, use the standard Bolt server (pkg/bolt).
// ClusterTransport is specifically for:
//   - Raft consensus protocol
//   - WAL streaming for HA
//   - Cluster coordination
type ClusterTransport struct {
	mu           sync.RWMutex
	nodeID       string
	bindAddr     string
	listener     net.Listener
	connections  map[string]*ClusterConnection
	closed       atomic.Bool
	closeCh      chan struct{}
	wg           sync.WaitGroup
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxMsgSize   int
	tlsServer    *tls.Config
	tlsClient    *tls.Config
	authSecret   []byte
	authMaxSkew  time.Duration
	logger       *slog.Logger
	localizer    *localization.Manager

	// Message handlers
	handlers map[ClusterMessageType]MessageHandler
}

// MessageHandler processes incoming cluster messages.
type MessageHandler func(ctx context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error)

// ClusterTransportConfig configures the cluster transport.
type ClusterTransportConfig struct {
	NodeID       string
	BindAddr     string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	MaxMsgSize   int
	TLSServer    *tls.Config
	TLSClient    *tls.Config
	AuthSecret   []byte
	AuthMaxSkew  time.Duration
	Logger       *slog.Logger
	Localizer    *localization.Manager
}

// DefaultClusterTransportConfig returns production defaults.
func DefaultClusterTransportConfig() *ClusterTransportConfig {
	return &ClusterTransportConfig{
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxMsgSize:   64 * 1024 * 1024, // 64MB max
		AuthMaxSkew:  30 * time.Second,
	}
}

// NewClusterTransport creates a cluster transport.
func NewClusterTransport(config *ClusterTransportConfig) *ClusterTransport {
	defaults := DefaultClusterTransportConfig()
	if config == nil {
		config = defaults
	} else {
		// Fill unset values with sensible defaults to avoid accidental "0 means no timeouts"
		// behavior, which can lead to hangs/timeouts that are hard to debug.
		if config.DialTimeout == 0 {
			config.DialTimeout = defaults.DialTimeout
		}
		if config.ReadTimeout == 0 {
			config.ReadTimeout = defaults.ReadTimeout
		}
		if config.WriteTimeout == 0 {
			config.WriteTimeout = defaults.WriteTimeout
		}
		if config.MaxMsgSize == 0 {
			config.MaxMsgSize = defaults.MaxMsgSize
		}
	}
	return &ClusterTransport{
		nodeID:       config.NodeID,
		bindAddr:     config.BindAddr,
		connections:  make(map[string]*ClusterConnection),
		closeCh:      make(chan struct{}),
		dialTimeout:  config.DialTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
		maxMsgSize:   config.MaxMsgSize,
		tlsServer:    config.TLSServer,
		tlsClient:    config.TLSClient,
		authSecret:   config.AuthSecret,
		authMaxSkew:  config.AuthMaxSkew,
		logger:       config.Logger,
		localizer:    config.Localizer,
		handlers:     make(map[ClusterMessageType]MessageHandler),
	}
}

func (t *ClusterTransport) logPrintf(ctx context.Context, level slog.Level, format string, args ...any) {
	logReplicationEvent(ctx, t.logger, t.localizer, level, localization.ReplicationOperatorEvent(format, args...))
}

// RegisterHandler registers a handler for a message type.
func (t *ClusterTransport) RegisterHandler(msgType ClusterMessageType, handler MessageHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[msgType] = handler
}

// Connect establishes a connection to a peer node.
func (t *ClusterTransport) Connect(ctx context.Context, addr string) (PeerConnection, error) {
	if t.closed.Load() {
		return nil, localizedError(localization.ReplicationTransportClosed(), nil)
	}

	// Check for existing connection
	t.mu.RLock()
	if conn, ok := t.connections[addr]; ok && conn.IsConnected() {
		t.mu.RUnlock()
		return conn, nil
	}
	t.mu.RUnlock()

	// Dial with timeout (also set Dialer.Timeout so the net stack doesn't hang
	// if a caller passes a context without a deadline).
	dialTimeout := t.dialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	var d net.Dialer
	d.Timeout = dialTimeout
	var netConn net.Conn
	var err error
	if t.tlsClient != nil {
		netConn, err = tls.DialWithDialer(&d, "tcp", addr, t.tlsClient)
	} else {
		netConn, err = d.DialContext(dialCtx, "tcp", addr)
	}
	if err != nil {
		return nil, localizedError(localization.ReplicationTransportConnectFailed(addr, err), err)
	}

	conn := t.createConnection(addr, netConn)
	conn.wg.Add(1)
	go conn.readLoop()

	// Store connection
	t.mu.Lock()
	t.connections[addr] = conn
	t.mu.Unlock()

	t.logPrintf(ctx, slog.LevelInfo, "[Cluster] Connected to peer %s", addr)
	return conn, nil
}

func (t *ClusterTransport) createConnection(addr string, netConn net.Conn) *ClusterConnection {
	conn := &ClusterConnection{
		transport:    t,
		addr:         addr,
		conn:         netConn,
		reader:       bufio.NewReader(netConn),
		writer:       bufio.NewWriter(netConn),
		readTimeout:  t.readTimeout,
		writeTimeout: t.writeTimeout,
		maxMsgSize:   t.maxMsgSize,
		closeCh:      make(chan struct{}),
		pendingRPCs:  make(map[uint64]chan *ClusterMessage),
		authSecret:   t.authSecret,
		authMaxSkew:  t.authMaxSkew,
	}
	// Mark connected immediately so callers can send without racing readLoop startup.
	conn.connected.Store(true)
	return conn
}

// Listen starts accepting cluster connections.
func (t *ClusterTransport) Listen(ctx context.Context, addr string, handler ConnectionHandler) error {
	if t.closed.Load() {
		return localizedError(localization.ReplicationTransportClosed(), nil)
	}

	var listener net.Listener
	var err error
	if t.tlsServer != nil {
		listener, err = tls.Listen("tcp", addr, t.tlsServer)
	} else {
		listener, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return localizedError(localization.ReplicationTransportListenFailed(addr, err), err)
	}

	t.mu.Lock()
	t.listener = listener
	// If an ephemeral port was requested (or the caller passes an empty addr in tests),
	// record the actual bound address for later dials/diagnostics.
	t.bindAddr = listener.Addr().String()
	t.mu.Unlock()

	t.logPrintf(ctx, slog.LevelInfo, "[Cluster] Listening on %s", listener.Addr().String())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.closeCh:
			return nil
		default:
		}

		// Set accept deadline for graceful shutdown
		if tcpListener, ok := listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(time.Second))
		}

		netConn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if t.closed.Load() {
				return nil
			}
			t.logPrintf(ctx, slog.LevelError, "[Cluster] Accept error: %v", err)
			continue
		}

		t.wg.Add(1)
		go t.handleIncoming(ctx, netConn, handler)
	}
}

func (t *ClusterTransport) handleIncoming(ctx context.Context, netConn net.Conn, onConnect ConnectionHandler) {
	defer t.wg.Done()

	remoteAddr := netConn.RemoteAddr().String()
	t.logPrintf(ctx, slog.LevelInfo, "[Cluster] Accepted connection from %s", remoteAddr)

	// Treat inbound connections the same as outbound ones so they can both:
	// 1) serve incoming requests via registered handlers
	// 2) be used to send requests back to the peer (e.g., fencing during failover)
	conn := t.createConnection(remoteAddr, netConn)

	t.mu.Lock()
	t.connections[remoteAddr] = conn
	t.mu.Unlock()

	if onConnect != nil {
		onConnect(conn)
	}

	conn.wg.Add(1)
	go conn.readLoopWithContext(ctx)
	conn.wg.Wait()
}

// Close shuts down the transport.
func (t *ClusterTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}

	close(t.closeCh)

	t.mu.Lock()
	if t.listener != nil {
		t.listener.Close()
	}
	for _, conn := range t.connections {
		conn.Close()
	}
	t.mu.Unlock()

	t.wg.Wait()
	t.logPrintf(context.Background(), slog.LevelInfo, "[Cluster] Transport closed")
	return nil
}

// ClusterConnection implements PeerConnection for cluster communication.
type ClusterConnection struct {
	transport    *ClusterTransport
	addr         string
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	mu           sync.Mutex
	connected    atomic.Bool
	closeCh      chan struct{}
	wg           sync.WaitGroup
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxMsgSize   int

	// RPC tracking
	rpcMu       sync.Mutex
	nextRPCID   uint64
	pendingRPCs map[uint64]chan *ClusterMessage
	authSecret  []byte
	authMaxSkew time.Duration
}

func (c *ClusterConnection) sendRPC(ctx context.Context, msg *ClusterMessage) (*ClusterMessage, error) {
	if !c.connected.Load() {
		return nil, localizedError(localization.ReplicationRPCNotConnected(), nil)
	}
	if msg.NodeID == "" && c.transport != nil && c.transport.nodeID != "" {
		msg.NodeID = c.transport.nodeID
	}
	c.signMessage(msg)

	// Create response channel
	c.rpcMu.Lock()
	rpcID := c.nextRPCID
	c.nextRPCID++
	respCh := make(chan *ClusterMessage, 1)
	c.pendingRPCs[rpcID] = respCh
	c.rpcMu.Unlock()

	defer func() {
		c.rpcMu.Lock()
		delete(c.pendingRPCs, rpcID)
		c.rpcMu.Unlock()
	}()

	// Send request
	c.mu.Lock()
	c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	err := writeClusterMessage(c.writer, msg)
	if err == nil {
		err = c.writer.Flush()
	}
	c.mu.Unlock()

	if err != nil {
		return nil, localizedError(localization.ReplicationRPCWriteFailed(err), err)
	}

	// Wait for response
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, localizedError(localization.ReplicationRPCConnectionClosed(), nil)
	case resp := <-respCh:
		return resp, nil
	}
}

func (c *ClusterConnection) readLoop() {
	c.readLoopWithContext(context.Background())
}

func (c *ClusterConnection) readLoopWithContext(ctx context.Context) {
	defer c.wg.Done()
	c.connected.Store(true)
	defer func() {
		c.connected.Store(false)
		close(c.closeCh)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.transport.closeCh:
			return
		default:
		}

		readTimeout := c.readTimeout
		if readTimeout <= 0 {
			readTimeout = 30 * time.Second
		}
		c.conn.SetReadDeadline(time.Now().Add(readTimeout))
		msg, err := readClusterMessage(c.reader, c.maxMsgSize)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				var netErr net.Error
				if !errors.As(err, &netErr) || !netErr.Timeout() {
					c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Read error: %v", err)
				}
			}
			return
		}

		if err := c.verifyMessage(msg); err != nil {
			c.transport.logPrintf(ctx, slog.LevelWarn, "[Cluster] Auth failed from %s: %v", c.addr, err)
			return
		}

		// Dispatch to pending RPC (single outstanding RPC per connection is expected today).
		c.rpcMu.Lock()
		var (
			deliverCh chan *ClusterMessage
			deliverID uint64
		)
		for id, ch := range c.pendingRPCs {
			deliverCh = ch
			deliverID = id
			break
		}
		if deliverCh != nil {
			select {
			case deliverCh <- msg:
			default:
			}
			delete(c.pendingRPCs, deliverID)
			c.rpcMu.Unlock()
			continue
		}
		c.rpcMu.Unlock()

		// No pending RPC: treat as inbound request.
		if c.transport == nil {
			continue
		}

		c.transport.mu.RLock()
		handler, ok := c.transport.handlers[msg.Type]
		c.transport.mu.RUnlock()
		if !ok {
			c.transport.logPrintf(ctx, slog.LevelWarn, "[Cluster] No handler for message type %d", msg.Type)
			continue
		}

		resp, err := handler(ctx, msg.NodeID, msg)
		if err != nil {
			c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Handler error: %v", err)
			continue
		}
		if resp == nil {
			continue
		}

		c.signMessage(resp)
		c.mu.Lock()
		writeTimeout := c.writeTimeout
		if writeTimeout <= 0 {
			writeTimeout = 10 * time.Second
		}
		c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		werr := writeClusterMessage(c.writer, resp)
		if werr == nil {
			werr = c.writer.Flush()
		}
		c.mu.Unlock()

		if werr != nil {
			c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Write error to %s: %v", c.addr, werr)
			return
		}
	}
}

// SendWALBatch sends WAL entries to the peer.
func (c *ClusterConnection) SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error) {
	payload, err := encodeGob(entries)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgWALBatch,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result WALBatchResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendHeartbeat sends a heartbeat to the peer.
func (c *ClusterConnection) SendHeartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	payload, err := encodeGob(req)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgHeartbeat,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result HeartbeatResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendFence sends a fence request to the peer.
func (c *ClusterConnection) SendFence(ctx context.Context, req *FenceRequest) (*FenceResponse, error) {
	payload, err := encodeGob(req)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgFence,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result FenceResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendPromote sends a promote request to the peer.
func (c *ClusterConnection) SendPromote(ctx context.Context, req *PromoteRequest) (*PromoteResponse, error) {
	payload, err := encodeGob(req)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgPromote,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result PromoteResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendRaftVote sends a Raft vote request to the peer.
func (c *ClusterConnection) SendRaftVote(ctx context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error) {
	payload, err := encodeGob(req)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgVoteRequest,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result RaftVoteResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendRaftAppendEntries sends Raft append entries to the peer.
func (c *ClusterConnection) SendRaftAppendEntries(ctx context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	payload, err := encodeGob(req)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed("", err), err)
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgAppendEntries,
		Payload: payload,
	}

	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return nil, err
	}

	var result RaftAppendEntriesResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed("", err), err)
	}
	return &result, nil
}

// SendForwardApply sends a write command to the leader for application.
// Used by followers to forward writes to the leader automatically.
func (c *ClusterConnection) SendForwardApply(ctx context.Context, cmd *Command, timeout time.Duration) error {
	if cmd == nil {
		return localizedError(localization.ReplicationRPCCommandRequired(), nil)
	}
	payload, err := encodeGob(cmd)
	if err != nil {
		return localizedError(localization.ReplicationRPCEncodeFailed("command", err), err)
	}
	msg := &ClusterMessage{
		Type:    ClusterMsgForwardApply,
		Payload: payload,
	}
	resp, err := c.sendRPC(ctx, msg)
	if err != nil {
		return err
	}
	var result forwardApplyResponse
	if err := decodeGob(resp.Payload, &result); err != nil {
		return localizedError(localization.ReplicationRPCDecodeFailed("forward apply response", err), err)
	}
	if result.Err != "" {
		cause := errors.New(result.Err)
		return localizedError(localization.ReplicationRPCRemoteApplyFailed(result.Err), cause)
	}
	return nil
}

// Close closes the connection.
func (c *ClusterConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}
	c.wg.Wait()
	return nil
}

// IsConnected returns true if the connection is active.
func (c *ClusterConnection) IsConnected() bool {
	return c.connected.Load()
}

func (c *ClusterConnection) signMessage(msg *ClusterMessage) {
	if len(c.authSecret) == 0 {
		return
	}
	if msg.NodeID == "" && c.transport != nil && c.transport.nodeID != "" {
		msg.NodeID = c.transport.nodeID
	}
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixNano()
	}
	msg.Signature = computeMessageSignature(c.authSecret, msg)
}

func (c *ClusterConnection) verifyMessage(msg *ClusterMessage) error {
	if len(c.authSecret) == 0 {
		return nil
	}
	if msg.Signature == "" || msg.Timestamp == 0 || msg.NodeID == "" {
		return localizedError(localization.ReplicationRPCAuthenticationFieldsMissing(), nil)
	}
	if c.authMaxSkew > 0 {
		now := time.Now()
		ts := time.Unix(0, msg.Timestamp)
		if now.Sub(ts) > c.authMaxSkew || ts.Sub(now) > c.authMaxSkew {
			return localizedError(localization.ReplicationRPCTimestampOutsideAllowedSkew(), nil)
		}
	}
	expected := computeMessageSignature(c.authSecret, msg)
	if !hmac.Equal([]byte(expected), []byte(msg.Signature)) {
		return localizedError(localization.ReplicationRPCInvalidSignature(), nil)
	}
	return nil
}

// Wire protocol helpers

func writeClusterMessage(w *bufio.Writer, msg *ClusterMessage) error {
	data, err := encodeGob(msg)
	if err != nil {
		return err
	}

	// Length prefix (4 bytes, big endian)
	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

func readClusterMessage(r *bufio.Reader, maxSize int) (*ClusterMessage, error) {
	// Read length prefix
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, localizedError(localization.ReplicationRPCReadFailed(err), err)
	}

	if int(length) > maxSize {
		return nil, localizedError(localization.ReplicationRPCMessageTooLarge(length, maxSize), nil)
	}

	// Read data
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, localizedError(localization.ReplicationRPCReadFailed(err), err)
	}

	var msg ClusterMessage
	if err := decodeGob(data, &msg); err != nil {
		return nil, localizedError(localization.ReplicationRPCReadFailed(err), err)
	}

	return &msg, nil
}

func computeMessageSignature(secret []byte, msg *ClusterMessage) string {
	mac := hmac.New(sha256.New, secret)

	_, _ = mac.Write([]byte{byte(msg.Type)})
	writeStringWithLength(mac, msg.NodeID)
	writeInt64(mac, msg.Timestamp)
	writeBytesWithLength(mac, msg.Payload)

	return hex.EncodeToString(mac.Sum(nil))
}

func writeInt64(w io.Writer, v int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	_, _ = w.Write(buf[:])
}

func writeStringWithLength(w io.Writer, s string) {
	writeBytesWithLength(w, []byte(s))
}

func writeBytesWithLength(w io.Writer, b []byte) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(b)))
	_, _ = w.Write(buf[:])
	if len(b) > 0 {
		_, _ = w.Write(b)
	}
}

// Verify interface compliance
var _ Transport = (*ClusterTransport)(nil)
var _ PeerConnection = (*ClusterConnection)(nil)
