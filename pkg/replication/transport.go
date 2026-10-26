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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	Payload   []byte

	// Nonce and SignatureV2 are required on every signed message.
	Nonce       []byte
	SignatureV2 string
}

// ClusterTransport handles cluster-to-cluster communication.
//
// For client-facing queries, use the standard Bolt server (pkg/bolt).
// ClusterTransport is specifically for:
//   - Raft consensus protocol
//   - WAL streaming for HA
//   - Cluster coordination
type ClusterTransport struct {
	clusterLifecycle[transportResources]
	nodeID       string
	bindAddr     string
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxMsgSize   int
	tlsServer    *tls.Config
	tlsClient    *tls.Config
	authSecret   []byte
	authMaxSkew  time.Duration

	// replay records message nonces for every connection of this transport.
	replay *replayCache

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
		clusterLifecycle: clusterLifecycle[transportResources]{
			closeCh: make(chan struct{}),
			resources: transportResources{
				connections: make(map[string]*ClusterConnection),
				logger:      config.Logger,
				localizer:   config.Localizer,
			},
		},
		nodeID:       config.NodeID,
		bindAddr:     config.BindAddr,
		dialTimeout:  config.DialTimeout,
		readTimeout:  config.ReadTimeout,
		writeTimeout: config.WriteTimeout,
		maxMsgSize:   config.MaxMsgSize,
		tlsServer:    config.TLSServer,
		tlsClient:    config.TLSClient,
		authSecret:   config.AuthSecret,
		authMaxSkew:  config.AuthMaxSkew,
		replay:       newReplayCache(),
		handlers:     make(map[ClusterMessageType]MessageHandler),
	}
}

func (t *ClusterTransport) logPrintf(ctx context.Context, level slog.Level, format string, args ...any) {
	logReplicationEvent(ctx, t.resources.logger, t.resources.localizer, level, localization.ReplicationOperatorEvent(format, args...))
}

// RegisterHandler registers a handler for a message type.
func (t *ClusterTransport) RegisterHandler(msgType ClusterMessageType, handler MessageHandler) {
	t.resources.mu.Lock()
	defer t.resources.mu.Unlock()
	t.handlers[msgType] = handler
}

// Connect establishes a connection to a peer node.
func (t *ClusterTransport) Connect(ctx context.Context, addr string) (PeerConnection, error) {
	if t.resources.closed.Load() {
		return nil, localizedError(localization.ReplicationTransportClosed(), nil)
	}

	// Check for existing connection under the same shutdown admission cut.
	t.resources.mu.RLock()
	if t.resources.closed.Load() {
		t.resources.mu.RUnlock()
		return nil, localizedError(localization.ReplicationTransportClosed(), nil)
	}
	if conn, ok := t.resources.connections[addr]; ok && conn.IsConnected() {
		t.resources.mu.RUnlock()
		return conn, nil
	}
	t.resources.mu.RUnlock()

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
	t.resources.mu.Lock()
	if t.resources.closed.Load() {
		t.resources.mu.Unlock()
		_ = netConn.Close()
		return nil, localizedError(localization.ReplicationTransportClosed(), nil)
	}
	// A concurrent dial may already have installed the reusable peer.
	if existing := t.resources.connections[addr]; existing != nil && existing.IsConnected() {
		t.resources.mu.Unlock()
		_ = netConn.Close()
		return existing, nil
	}
	conn.wg.Add(1)
	t.resources.connections[addr] = conn
	t.resources.mu.Unlock()
	go conn.readLoop()

	t.logPrintf(ctx, slog.LevelInfo, "[Cluster] Connected to peer %s", addr)
	return conn, nil
}

func (t *ClusterTransport) createConnection(addr string, netConn net.Conn) *ClusterConnection {
	conn := &ClusterConnection{
		clusterLifecycle: clusterLifecycle[connectionResources]{
			closeCh:   make(chan struct{}),
			resources: connectionResources{conn: netConn},
		},
		transport:    t,
		addr:         addr,
		reader:       bufio.NewReader(netConn),
		writer:       bufio.NewWriter(netConn),
		readTimeout:  t.readTimeout,
		writeTimeout: t.writeTimeout,
		maxMsgSize:   t.maxMsgSize,
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
	if t.resources.closed.Load() {
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

	t.resources.mu.Lock()
	if t.resources.closed.Load() {
		t.resources.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	t.resources.listener = listener
	// If an ephemeral port was requested (or the caller passes an empty addr in tests),
	// record the actual bound address for later dials/diagnostics.
	t.bindAddr = listener.Addr().String()
	t.resources.mu.Unlock()

	t.logPrintf(ctx, slog.LevelInfo, "[Cluster] Listening on %s", listener.Addr().String())

	// Cancelling ctx closes the listener, so a blocked Accept returns. Only
	// a TCP listener gets the accept deadline below; a TLS listener's
	// Accept would otherwise wait until the transport closed, and an HA
	// standby, which cancels ctx and waits for this loop before closing
	// the transport, could never shut down.
	stopClose := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopClose()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Close closes the listener, so a blocked Accept returns and the
		// loop exits here.
		if t.resources.closed.Load() {
			return nil
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
			if !t.resources.closed.Load() {
				t.logPrintf(ctx, slog.LevelError, "[Cluster] Accept error: %v", err)
			}
			continue
		}

		t.resources.mu.Lock()
		if t.resources.closed.Load() {
			t.resources.mu.Unlock()
			_ = netConn.Close()
			return nil
		}
		t.wg.Add(1)
		t.resources.mu.Unlock()
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

	t.resources.mu.Lock()
	if t.resources.closed.Load() {
		t.resources.mu.Unlock()
		_ = netConn.Close()
		return
	}
	conn.wg.Add(1)
	t.resources.connections[remoteAddr] = conn
	t.resources.mu.Unlock()

	// The peer is announced only once a message from it has been read and
	// verified (TLS handshake done, signature checked), so a bare connect
	// can't change peer state such as an HA standby's primary connection.
	if onConnect != nil {
		conn.onAuthenticated = func() { onConnect(conn) }
	}

	go conn.readLoopWithContext(ctx)
	conn.wg.Wait()
}

// ClusterConnection implements PeerConnection for cluster communication.
type ClusterConnection struct {
	clusterLifecycle[connectionResources]
	transport    *ClusterTransport
	addr         string
	reader       *bufio.Reader
	writer       *bufio.Writer
	connected    atomic.Bool
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxMsgSize   int

	// RPC tracking
	rpcMu       sync.Mutex
	nextRPCID   uint64
	pendingRPCs map[uint64]chan *ClusterMessage
	authSecret  []byte
	authMaxSkew time.Duration

	// onAuthenticated runs once, after the first verified message on an
	// inbound connection. Only the read loop uses it.
	onAuthenticated func()

	// replay is used only by a connection without a transport.
	replayOnce sync.Once
	replay     *replayCache
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
	c.resources.mu.Lock()
	c.resources.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	err := transferClusterMessage(msg, nil, c.writer, 0)
	if err == nil {
		err = c.writer.Flush()
	}
	c.resources.mu.Unlock()

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
		if c.resources.conn != nil {
			_ = c.resources.conn.Close()
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
		c.resources.conn.SetReadDeadline(time.Now().Add(readTimeout))
		msg := new(ClusterMessage)
		err := transferClusterMessage(msg, c.reader, nil, c.maxMsgSize)
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
		if onAuthenticated := c.onAuthenticated; onAuthenticated != nil {
			c.onAuthenticated = nil
			onAuthenticated()
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

		c.transport.resources.mu.RLock()
		handler, ok := c.transport.handlers[msg.Type]
		c.transport.resources.mu.RUnlock()
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
		c.resources.mu.Lock()
		writeTimeout := c.writeTimeout
		if writeTimeout <= 0 {
			writeTimeout = 10 * time.Second
		}
		c.resources.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		werr := transferClusterMessage(resp, nil, c.writer, 0)
		if werr == nil {
			werr = c.writer.Flush()
		}
		c.resources.mu.Unlock()

		if werr != nil {
			c.transport.logPrintf(ctx, slog.LevelError, "[Cluster] Write error to %s: %v", c.addr, werr)
			return
		}
	}
}

// SendWALBatch sends WAL entries to the peer.
func (c *ClusterConnection) SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error) {
	return exchangeClusterRPC[WALBatchResponse](c, ctx, ClusterMsgWALBatch, entries, "", "")
}

// SendHeartbeat sends a heartbeat to the peer.
func (c *ClusterConnection) SendHeartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	return exchangeClusterRPC[HeartbeatResponse](c, ctx, ClusterMsgHeartbeat, req, "", "")
}

// SendFence sends a fence request to the peer.
func (c *ClusterConnection) SendFence(ctx context.Context, req *FenceRequest) (*FenceResponse, error) {
	return exchangeClusterRPC[FenceResponse](c, ctx, ClusterMsgFence, req, "", "")
}

// SendPromote sends a promote request to the peer.
func (c *ClusterConnection) SendPromote(ctx context.Context, req *PromoteRequest) (*PromoteResponse, error) {
	return exchangeClusterRPC[PromoteResponse](c, ctx, ClusterMsgPromote, req, "", "")
}

// SendRaftVote sends a Raft vote request to the peer.
func (c *ClusterConnection) SendRaftVote(ctx context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error) {
	return exchangeClusterRPC[RaftVoteResponse](c, ctx, ClusterMsgVoteRequest, req, "", "")
}

// SendRaftAppendEntries sends Raft append entries to the peer.
func (c *ClusterConnection) SendRaftAppendEntries(ctx context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	return exchangeClusterRPC[RaftAppendEntriesResponse](c, ctx, ClusterMsgAppendEntries, req, "", "")
}

// SendForwardApply sends a write command to the leader for application.
// Used by followers to forward writes to the leader automatically.
func (c *ClusterConnection) SendForwardApply(ctx context.Context, cmd *Command, timeout time.Duration) error {
	if cmd == nil {
		return localizedError(localization.ReplicationRPCCommandRequired(), nil)
	}
	result, err := exchangeClusterRPC[forwardApplyResponse](c, ctx, ClusterMsgForwardApply, cmd, "command", "forward apply response")
	if err != nil {
		return err
	}
	if result.Err != "" {
		cause := errors.New(result.Err)
		return localizedError(localization.ReplicationRPCRemoteApplyFailed(result.Err), cause)
	}
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
	// Every send gets a fresh nonce, a resent message object included.
	msg.Nonce = newMessageNonce()
	msg.SignatureV2 = computeMessageSignatureV2(c.authSecret, msg)
}

// replayState returns the replay cache shared by the transport's
// connections, or this connection's own when it has no transport.
func (c *ClusterConnection) replayState() *replayCache {
	if c.transport != nil && c.transport.replay != nil {
		return c.transport.replay
	}
	c.replayOnce.Do(func() { c.replay = newReplayCache() })
	return c.replay
}

func (c *ClusterConnection) verifyMessage(msg *ClusterMessage) error {
	if len(c.authSecret) == 0 {
		return nil
	}
	if msg.Timestamp == 0 || msg.NodeID == "" {
		return localizedError(localization.ReplicationRPCAuthenticationFieldsMissing(), nil)
	}
	if len(msg.Nonce) != messageNonceSize {
		return localizedError(localization.ReplicationRPCReplayProtectionRequired(), nil)
	}
	if msg.SignatureV2 == "" {
		return localizedError(localization.ReplicationRPCAuthenticationFieldsMissing(), nil)
	}
	// The skew window also bounds the replay cache, so it always applies.
	skew := c.authMaxSkew
	if skew <= 0 {
		skew = defaultAuthMaxSkew
	}
	now := time.Now()
	ts := time.Unix(0, msg.Timestamp)
	if now.Sub(ts) > skew || ts.Sub(now) > skew {
		return localizedError(localization.ReplicationRPCTimestampOutsideAllowedSkew(), nil)
	}

	expected := computeMessageSignatureV2(c.authSecret, msg)
	if !hmac.Equal([]byte(expected), []byte(msg.SignatureV2)) {
		return localizedError(localization.ReplicationRPCInvalidSignature(), nil)
	}
	return c.replayState().admit(msg.NodeID, msg.Nonce, ts.Add(skew).UnixNano(), now.UnixNano())
}

// Wire protocol helpers

func transferClusterMessage(message *ClusterMessage, reader io.Reader, writer io.Writer, maxSize int) error {
	var data []byte
	var err error
	if writer != nil {
		data, err = storage.EncodeCommandPayload(message)
		if err != nil {
			return err
		}
	}
	length := uint32(len(data))
	if writer != nil {
		if err = binary.Write(writer, binary.BigEndian, length); err != nil {
			return err
		}
		_, err = writer.Write(data)
		return err
	}
	if err = binary.Read(reader, binary.BigEndian, &length); err != nil {
		return localizedError(localization.ReplicationRPCReadFailed(err), err)
	}
	if int(length) > maxSize {
		return localizedError(localization.ReplicationRPCMessageTooLarge(length, maxSize), nil)
	}
	data = make([]byte, length)
	if _, err = io.ReadFull(reader, data); err != nil {
		return localizedError(localization.ReplicationRPCReadFailed(err), err)
	}
	if err = storage.DecodeCommandPayload(data, message); err != nil {
		return localizedError(localization.ReplicationRPCReadFailed(err), err)
	}
	return nil
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

// exchangeClusterRPC owns payload encoding, the existing RPC transport, and
// response decoding for the public typed RPC entry points.
func exchangeClusterRPC[Response any](c *ClusterConnection, ctx context.Context, kind ClusterMessageType, request any, encodeLabel, decodeLabel string) (*Response, error) {
	payload, err := storage.EncodeCommandPayload(request)
	if err != nil {
		return nil, localizedError(localization.ReplicationRPCEncodeFailed(encodeLabel, err), err)
	}
	response, err := c.sendRPC(ctx, &ClusterMessage{Type: kind, Payload: payload})
	if err != nil {
		return nil, err
	}
	var result Response
	if err = storage.DecodeCommandPayload(response.Payload, &result); err != nil {
		return nil, localizedError(localization.ReplicationRPCDecodeFailed(decodeLabel, err), err)
	}
	return &result, nil
}

type transportResources struct {
	mu          sync.RWMutex
	closed      atomic.Bool
	listener    net.Listener
	connections map[string]*ClusterConnection
	logger      *slog.Logger
	localizer   *localization.Manager
}

type connectionResources struct {
	mu   sync.Mutex
	conn net.Conn
}

// clusterLifecycle owns the admission/write mutex, worker join and shutdown
// signal for either cluster resource. Its concrete payload contains only that
// role's resources; neither owner keeps a back-pointer or shutdown callback.
type clusterLifecycle[Resources transportResources | connectionResources] struct {
	wg        sync.WaitGroup
	closeCh   chan struct{}
	resources Resources
}

// Close retires admission under the resource's mutex, closes dependent sockets
// and joins workers outside that mutex. Transport repeats return immediately;
// connection repeats still close their socket and join the existing read loop.
func (owner *clusterLifecycle[Resources]) Close() error {
	var mu sync.Locker
	var transport *transportResources
	var connection *connectionResources
	switch payload := any(&owner.resources).(type) {
	case *transportResources:
		transport, mu = payload, &payload.mu
	case *connectionResources:
		connection, mu = payload, &payload.mu
	}
	mu.Lock()
	var resources []io.Closer
	if transport != nil {
		if transport.closed.Swap(true) {
			mu.Unlock()
			return nil
		}
		close(owner.closeCh)
		resources = make([]io.Closer, 0, len(transport.connections)+1)
		if transport.listener != nil {
			resources = append(resources, transport.listener)
		}
		for _, child := range transport.connections {
			resources = append(resources, child)
		}
	} else if connection.conn != nil {
		// Socket close remains excluded against concrete response writers.
		// Worker joins happen only after releasing the same mutex they need.
		_ = connection.conn.Close()
	}
	mu.Unlock()
	for _, resource := range resources {
		_ = resource.Close()
	}
	owner.wg.Wait()
	if transport != nil {
		logReplicationEvent(context.Background(), transport.logger, transport.localizer, slog.LevelInfo, localization.ReplicationOperatorEvent("[Cluster] Transport closed"))
	}
	return nil
}
