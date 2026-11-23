package replication

// N152-64 transport-forwarding regressions.
//
// The approved correction makes a forwarded preaccept inherit the signed
// absolute caller deadline and the connection lifetime, while accepted durable
// work stays owned by the replicator. These tests pin the wire contract
// (inlined Command payload, signature over the whole payload, both
// compatibility directions), the deadline/cancellation behaviour of the
// forwarded handler, and the connection contract that now dispatches a forward
// asynchronously while still routing reverse RPC responses, preserving inbound
// order, suspending the idle read deadline and joining the one enrolled handler
// on Close.
//
// Author: N152-64 unit-test lane, 2026-11-21.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// forwardContextTestTimeout bounds an exchange that is expected to complete.
const forwardContextTestTimeout = 5 * time.Second

// forwardRecorder is the leader-side HandleForwardApply owner these tests
// observe. It reuses the package's existing Replicator test double and replaces
// only the forwarding entry point.
type forwardRecorder struct {
	*coverageReplicator
	prepared      chan context.Context
	gate          chan struct{}
	ignoreContext bool
	once          sync.Once
	calls         atomic.Int32
	returned      atomic.Bool

	mu      sync.Mutex
	cmd     Command
	timeout time.Duration
}

func newForwardRecorder() *forwardRecorder {
	return &forwardRecorder{
		coverageReplicator: &coverageReplicator{},
		prepared:           make(chan context.Context, 4),
	}
}

// holdPreparation parks preparation until the test releases it. With
// ignoreContext the preparation deliberately outlives cancellation, standing in
// for accepted durable work that its owner, not the caller, bounds.
func (r *forwardRecorder) holdPreparation(ignoreContext bool) {
	r.gate = make(chan struct{})
	r.ignoreContext = ignoreContext
}

func (r *forwardRecorder) releasePreparation() {
	if r.gate == nil {
		return
	}
	r.once.Do(func() { close(r.gate) })
}

func (r *forwardRecorder) HandleForwardApply(ctx context.Context, cmd *Command, timeout time.Duration) error {
	r.calls.Add(1)
	r.mu.Lock()
	if cmd != nil {
		r.cmd = *cmd
	}
	r.timeout = timeout
	r.mu.Unlock()
	select {
	case r.prepared <- ctx:
	default:
	}
	if r.gate != nil {
		if r.ignoreContext {
			<-r.gate
		} else {
			select {
			case <-r.gate:
			case <-ctx.Done():
				r.returned.Store(true)
				return ctx.Err()
			}
		}
	}
	r.returned.Store(true)
	return nil
}

func (r *forwardRecorder) preparedCommand() (Command, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cmd, r.timeout
}

// startForwardWireServer is a real cluster transport listener with the
// production forwarding handlers registered for the supplied owner.
func startForwardWireServer(t *testing.T, config *ClusterTransportConfig, owner Replicator, configure ...func(*ClusterTransport)) (*ClusterTransport, string) {
	t.Helper()
	config.BindAddr = "127.0.0.1:0"
	server := NewClusterTransport(config)
	for _, apply := range configure {
		apply(server)
	}
	RegisterClusterHandlers(server, owner)
	listened := make(chan error, 1)
	go func() { listened <- server.Listen(t.Context(), server.bindAddr, nil) }()
	t.Cleanup(func() {
		require.NoError(t, server.Close())
		// The listening owner's context is canceled before cleanup runs, so the
		// accept loop exits either through the admission stop or through that
		// cancellation; both are its documented ends.
		listenErr := awaitReplicationValue(t, listened)
		require.True(t, listenErr == nil || errors.Is(listenErr, context.Canceled), "listen ended with %v", listenErr)
	})
	require.Eventually(t, func() bool {
		server.resources.mu.RLock()
		defer server.resources.mu.RUnlock()
		return server.resources.listener != nil
	}, forwardContextTestTimeout, time.Millisecond, "forward wire server did not start listening")
	server.resources.mu.RLock()
	defer server.resources.mu.RUnlock()
	return server, server.bindAddr
}

// awaitInboundForwardConnection is the peer connection the server accepted: the
// same connection a leader would use to send a reverse RPC to that peer.
func awaitInboundForwardConnection(t *testing.T, transport *ClusterTransport) *ClusterConnection {
	t.Helper()
	var inbound *ClusterConnection
	require.Eventually(t, func() bool {
		transport.resources.mu.RLock()
		defer transport.resources.mu.RUnlock()
		for _, candidate := range transport.resources.connections {
			inbound = candidate
		}
		return inbound != nil
	}, forwardContextTestTimeout, time.Millisecond, "the server did not accept the forwarded connection")
	return inbound
}

// forwardWirePeer is an independent peer speaking the real cluster wire format.
// It signs and frames exactly like a follower but issues no production RPC, so
// a test can pipeline requests, answer reverse requests itself and observe
// exact reply order.
type forwardWirePeer struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	signer *ClusterConnection
}

func dialForwardWirePeer(t *testing.T, address string, secret []byte, nodeID string) *forwardWirePeer {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	require.NoError(t, err)
	signerTransport := NewClusterTransport(&ClusterTransportConfig{NodeID: nodeID, AuthSecret: secret})
	t.Cleanup(func() {
		_ = conn.Close()
		require.NoError(t, signerTransport.Close())
	})
	return &forwardWirePeer{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		signer: signerTransport.createConnection(address, nil),
	}
}

// send signs and writes one complete frame, returning its wire size.
func (p *forwardWirePeer) send(t *testing.T, msg *ClusterMessage) int {
	t.Helper()
	p.signer.signMessage(msg)
	payload, err := storage.EncodeCommandPayload(msg)
	require.NoError(t, err)
	require.NoError(t, p.conn.SetWriteDeadline(time.Now().Add(forwardContextTestTimeout)))
	require.NoError(t, transferClusterMessage(msg, nil, p.writer, 0))
	require.NoError(t, p.writer.Flush())
	return 4 + len(payload)
}

func (p *forwardWirePeer) receive(t *testing.T) *ClusterMessage {
	t.Helper()
	require.NoError(t, p.conn.SetReadDeadline(time.Now().Add(forwardContextTestTimeout)))
	msg := new(ClusterMessage)
	require.NoError(t, transferClusterMessage(msg, p.reader, nil, 64*1024*1024))
	return msg
}

func (p *forwardWirePeer) close() { _ = p.conn.Close() }

// forwardWireRequest builds the forwarded payload a follower writes: a Command
// inlined at the payload's top level plus the optional signed deadline.
func forwardWireRequest(t *testing.T, command Command, deadline time.Time) *ClusterMessage {
	t.Helper()
	request := forwardApplyRequest{Command: command}
	if !deadline.IsZero() {
		request.DeadlineUnixNano = deadline.UnixNano()
	}
	payload, err := storage.EncodeCommandPayload(&request)
	require.NoError(t, err)
	return &ClusterMessage{Type: ClusterMsgForwardApply, Payload: payload}
}

// forwardCommand is one command carrying every durable Command field.
func forwardCommand(id string) Command {
	return Command{
		Type:      CmdCreateNode,
		Data:      []byte(id),
		Timestamp: time.Unix(0, 1_700_000_000_123_456_789).UTC(),
		RequestID: id,
	}
}

// forwardSocketReadObserver counts the bytes the server's read loop has taken
// from the socket, so an ordering test can prove the queued request really
// reached the loop instead of assuming it.
type forwardSocketReadObserver struct {
	total atomic.Int64
}

func (o *forwardSocketReadObserver) listen(network, address string) (net.Listener, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &forwardObservedListener{Listener: listener, observer: o}, nil
}

type forwardObservedListener struct {
	net.Listener
	observer *forwardSocketReadObserver
}

func (l *forwardObservedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &forwardObservedConn{Conn: conn, observer: l.observer}, nil
}

type forwardObservedConn struct {
	net.Conn
	observer *forwardSocketReadObserver
}

func (c *forwardObservedConn) Read(buffer []byte) (int, error) {
	n, err := c.Conn.Read(buffer)
	if n > 0 {
		c.observer.total.Add(int64(n))
	}
	return n, err
}

// TestForwardApplyPayloadInlinesCommandForOldAndNewReaders pins the payload
// contract in both compatibility directions. The forwarding request is a
// top-level Command, never a nested wrapper, so an old reader (bare Command
// decoder) still reads a new payload and a new reader also accepts an old bare
// Command payload without inventing a deadline.
func TestForwardApplyPayloadInlinesCommandForOldAndNewReaders(t *testing.T) {
	command := forwardCommand("n152-64-inlined")
	deadline := command.Timestamp.Add(time.Minute)
	request := forwardApplyRequest{Command: command, DeadlineUnixNano: deadline.UnixNano()}

	payload, err := storage.EncodeCommandPayload(&request)
	require.NoError(t, err)

	var wire map[string]any
	require.NoError(t, storage.DecodeCommandPayload(payload, &wire))
	require.NotContains(t, wire, "Command", "the forwarded payload inlines Command; a nested wrapper is not the wire contract")
	require.Contains(t, wire, "Type")
	require.Contains(t, wire, "Data")
	require.Contains(t, wire, "Timestamp")
	require.Contains(t, wire, "RequestID")
	require.Contains(t, wire, "DeadlineUnixNano")

	// A new payload read by the pre-correction decoder.
	var oldReader Command
	require.NoError(t, storage.DecodeCommandPayload(payload, &oldReader))
	require.Equal(t, command.Type, oldReader.Type)
	require.Equal(t, command.Data, oldReader.Data)
	require.True(t, command.Timestamp.Equal(oldReader.Timestamp))
	require.Equal(t, command.RequestID, oldReader.RequestID)

	// The same bytes read by the current decoder keep the signed deadline too.
	var newReader forwardApplyRequest
	require.NoError(t, storage.DecodeCommandPayload(payload, &newReader))
	require.Equal(t, command.Type, newReader.Type)
	require.Equal(t, command.Data, newReader.Data)
	require.True(t, command.Timestamp.Equal(newReader.Timestamp))
	require.Equal(t, command.RequestID, newReader.RequestID)
	require.Equal(t, deadline.UnixNano(), newReader.DeadlineUnixNano)

	// An old writer sent a bare Command: the new reader must accept it as an
	// undecorated command with no caller deadline.
	legacy, err := storage.EncodeCommandPayload(&command)
	require.NoError(t, err)
	var legacyWire map[string]any
	require.NoError(t, storage.DecodeCommandPayload(legacy, &legacyWire))
	require.NotContains(t, legacyWire, "DeadlineUnixNano")

	var upgraded forwardApplyRequest
	require.NoError(t, storage.DecodeCommandPayload(legacy, &upgraded))
	require.Equal(t, command.Type, upgraded.Type)
	require.Equal(t, command.Data, upgraded.Data)
	require.True(t, command.Timestamp.Equal(upgraded.Timestamp))
	require.Equal(t, command.RequestID, upgraded.RequestID)
	require.Zero(t, upgraded.DeadlineUnixNano, "a legacy payload carries no caller deadline")
}

// TestForwardApplyWireKeepsEveryCommandFieldAndSignedDeadline drives the
// production follower sender into the production leader handler over real TCP:
// every Command field survives the wire and the leader prepares under the
// caller's absolute deadline with the remaining budget, not a fresh local one.
func TestForwardApplyWireKeepsEveryCommandFieldAndSignedDeadline(t *testing.T) {
	recorder := newForwardRecorder()
	_, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-wire-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder)

	client := NewClusterTransport(&ClusterTransportConfig{NodeID: "n152-64-wire-follower", AuthSecret: []byte(replayTestSecret)})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	peer, err := client.Connect(t.Context(), address)
	require.NoError(t, err)
	connection, ok := peer.(*ClusterConnection)
	require.True(t, ok)

	command := forwardCommand("n152-64-wire-command")
	callerDeadline := time.Now().Add(2 * time.Second)
	caller, cancelCaller := context.WithDeadline(t.Context(), callerDeadline)
	defer cancelCaller()
	require.NoError(t, connection.SendForwardApply(caller, &command, forwardContextTestTimeout),
		"the signed response must decode on the follower")

	preparation := awaitReplicationValue(t, recorder.prepared)
	preparedDeadline, bounded := preparation.Deadline()
	require.True(t, bounded, "the forwarded preparation must inherit the caller's deadline")
	require.Equal(t, callerDeadline.UnixNano(), preparedDeadline.UnixNano(),
		"leader preparation must keep the forwarded absolute deadline")

	preparedCommand, timeout := recorder.preparedCommand()
	require.Equal(t, command.Type, preparedCommand.Type)
	require.Equal(t, command.Data, preparedCommand.Data)
	require.True(t, command.Timestamp.Equal(preparedCommand.Timestamp))
	require.Equal(t, command.RequestID, preparedCommand.RequestID)
	require.Greater(t, timeout, time.Duration(0))
	require.InDelta(t, 2.0, timeout.Seconds(), 1.0, "preparation is bounded by the remaining caller budget")
}

// TestForwardApplyDeadlineTamperFailsSignatureWithoutConsumingNonce keeps the
// deadline inside the V2 signature. Rewriting only the inlined deadline while
// keeping the original signature and nonce must fail as a forgery, and the
// rejected attempt must not consume the nonce it borrowed, so the failure is
// never confused with replay protection.
func TestForwardApplyDeadlineTamperFailsSignatureWithoutConsumingNonce(t *testing.T) {
	secret := []byte(replayTestSecret)
	sender := replayTestTransport()
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	receiver := replayTestTransport()
	t.Cleanup(func() { require.NoError(t, receiver.Close()) })

	command := forwardCommand("n152-64-tamper")
	deadline := time.Now().Add(time.Minute)
	request := forwardApplyRequest{Command: command, DeadlineUnixNano: deadline.UnixNano()}
	payload, err := storage.EncodeCommandPayload(&request)
	require.NoError(t, err)

	signed := &ClusterMessage{Type: ClusterMsgForwardApply, Payload: payload}
	sender.createConnection("n152-64-tamper-peer", nil).signMessage(signed)

	rawSignature, err := hex.DecodeString(signed.SignatureV2)
	require.NoError(t, err)
	require.Len(t, rawSignature, sha256.Size, "the signature stays the V2 HMAC")
	require.Equal(t, computeMessageSignatureV2(secret, signed), signed.SignatureV2,
		"the whole signed payload is authenticated")

	// Extend the forwarded budget and keep the original signature and nonce.
	extended := request
	extended.DeadlineUnixNano = deadline.Add(time.Hour).UnixNano()
	forged := replayTestCopy(signed)
	forged.Payload, err = storage.EncodeCommandPayload(&extended)
	require.NoError(t, err)
	require.NotEqual(t, signed.Payload, forged.Payload)
	require.NotEqual(t, computeMessageSignatureV2(secret, forged), signed.SignatureV2,
		"rewriting the inlined deadline changes the authenticated bytes")

	err = receiver.createConnection("n152-64-forged", nil).verifyMessage(forged)
	requireReplicationRPCError(t, err, localization.MessageReplicationRPCInvalidSignature, "invalid signature")

	// The forgery was rejected on its signature, before the nonce was recorded,
	// so the authentic message with that same nonce still verifies once.
	require.NoError(t, receiver.createConnection("n152-64-authentic", nil).verifyMessage(replayTestCopy(signed)),
		"a rejected forgery must not consume the nonce of the authentic message")

	// An authentic but already expired deadline is authenticated: expiry is the
	// forwarded handler's decision, and the next test pins it.
	expired := request
	expired.DeadlineUnixNano = time.Now().Add(-time.Minute).UnixNano()
	expiredPayload, err := storage.EncodeCommandPayload(&expired)
	require.NoError(t, err)
	expiredMessage := &ClusterMessage{Type: ClusterMsgForwardApply, Payload: expiredPayload}
	sender.createConnection("n152-64-expired", nil).signMessage(expiredMessage)
	require.NoError(t, receiver.createConnection("n152-64-expired-check", nil).verifyMessage(expiredMessage))
}

// TestForwardApplyExpiredSignedRequestNeverInvokesPreparation sends an
// authentic request whose signed budget is already spent. The leader must
// report the spent budget without entering preparation, and the same
// connection must still prepare a live request.
func TestForwardApplyExpiredSignedRequestNeverInvokesPreparation(t *testing.T) {
	recorder := newForwardRecorder()
	_, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-expiry-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder)
	peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-expiry-peer")

	command := forwardCommand("n152-64-expired-request")
	peer.send(t, forwardWireRequest(t, command, time.Now().Add(-time.Minute)))
	response := peer.receive(t)
	require.Equal(t, ClusterMsgForwardApplyResponse, response.Type)
	var reported forwardApplyResponse
	require.NoError(t, storage.DecodeCommandPayload(response.Payload, &reported))
	require.Contains(t, reported.Err, context.DeadlineExceeded.Error(),
		"the leader reports the spent forwarded budget")
	require.Zero(t, recorder.calls.Load(), "an expired signed request must never reach preparation")

	// Control: the same peer and signature path prepares a live request and
	// keeps every command field.
	peer.send(t, forwardWireRequest(t, command, time.Now().Add(time.Minute)))
	response = peer.receive(t)
	require.Equal(t, ClusterMsgForwardApplyResponse, response.Type)
	require.NoError(t, storage.DecodeCommandPayload(response.Payload, &reported))
	require.Empty(t, reported.Err)
	require.Equal(t, int32(1), recorder.calls.Load())
	preparedCommand, _ := recorder.preparedCommand()
	require.Equal(t, command.Type, preparedCommand.Type)
	require.Equal(t, command.Data, preparedCommand.Data)
	require.True(t, command.Timestamp.Equal(preparedCommand.Timestamp))
	require.Equal(t, command.RequestID, preparedCommand.RequestID)
}

// TestForwardPreparationStopsOnPeerEndWithoutDeadline covers the lifetime half
// of the contract with no signed deadline at all: preparation must never
// invent a deadline, and the peer ending or the owning listener closing must
// still stop it.
func TestForwardPreparationStopsOnPeerEndWithoutDeadline(t *testing.T) {
	command := forwardCommand("n152-64-lifetime")

	t.Run("peer EOF", func(t *testing.T) {
		recorder := newForwardRecorder()
		recorder.holdPreparation(false)
		t.Cleanup(recorder.releasePreparation)
		transport, address := startForwardWireServer(t, &ClusterTransportConfig{
			NodeID:     "n152-64-eof-leader",
			AuthSecret: []byte(replayTestSecret),
		}, recorder)
		peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-eof-peer")
		inbound := awaitInboundForwardConnection(t, transport)

		peer.send(t, forwardWireRequest(t, command, time.Time{}))
		preparation := awaitReplicationValue(t, recorder.prepared)
		_, bounded := preparation.Deadline()
		require.False(t, bounded, "a request without a signed deadline must not acquire one")

		peer.close()
		requirePreparationStops(t, preparation, context.Canceled)
		require.Eventually(t, recorder.returned.Load, forwardContextTestTimeout, time.Millisecond,
			"the forwarded handler must return after its connection ended")
		require.Eventually(t, func() bool { return !inbound.IsConnected() }, forwardContextTestTimeout, time.Millisecond,
			"the peer's end must retire the accepted connection")
	})

	t.Run("listener owner close", func(t *testing.T) {
		recorder := newForwardRecorder()
		recorder.holdPreparation(false)
		t.Cleanup(recorder.releasePreparation)
		transport, address := startForwardWireServer(t, &ClusterTransportConfig{
			NodeID:     "n152-64-owner-leader",
			AuthSecret: []byte(replayTestSecret),
		}, recorder)
		peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-owner-peer")

		peer.send(t, forwardWireRequest(t, command, time.Time{}))
		preparation := awaitReplicationValue(t, recorder.prepared)
		_, bounded := preparation.Deadline()
		require.False(t, bounded)

		_ = transport.Close()
		requirePreparationStops(t, preparation, context.Canceled)
		require.Eventually(t, recorder.returned.Load, forwardContextTestTimeout, time.Millisecond,
			"the forwarded handler must return after its owner closed")
	})
}

// TestForwardApplyQueuedCallerKeepsOriginalBudget establishes the caller's
// budget before the request waits for the connection, exactly as the corrected
// sender does. A budget spent while queued drops the request instead of
// restarting it, and a request that is admitted still carries the deadline
// measured from the caller's call.
func TestForwardApplyQueuedCallerKeepsOriginalBudget(t *testing.T) {
	recorder := newForwardRecorder()
	_, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-budget-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder)

	client := NewClusterTransport(&ClusterTransportConfig{NodeID: "n152-64-budget-follower", AuthSecret: []byte(replayTestSecret)})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	peer, err := client.Connect(t.Context(), address)
	require.NoError(t, err)
	connection, ok := peer.(*ClusterConnection)
	require.True(t, ok)
	command := forwardCommand("n152-64-budget")

	// One complete exchange owns the connection: hold the admission gate so the
	// next caller has to wait behind it.
	connection.rpcGate <- struct{}{}

	t.Run("expired while queued never reaches preparation", func(t *testing.T) {
		queued, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()
		err := connection.SendForwardApply(queued, &command, 0)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, recorder.calls.Load(), "a queued caller that ran out of budget must not reach the wire")
		require.True(t, connection.IsConnected(), "expiring while queued must not retire the connection")
	})

	t.Run("queued wait consumes the original budget", func(t *testing.T) {
		const hold = 200 * time.Millisecond
		start := time.Now()
		callerDeadline := start.Add(900 * time.Millisecond)
		queued, cancel := context.WithDeadline(t.Context(), callerDeadline)
		defer cancel()
		go func() {
			// The measured hold stands for the connection being busy; the
			// deadline was fixed at the call, not after this wait.
			<-time.After(hold)
			<-connection.rpcGate
		}()
		require.NoError(t, connection.SendForwardApply(queued, &command, 0))
		require.GreaterOrEqual(t, time.Since(start), hold, "the caller really waited for the connection")
		preparation := awaitReplicationValue(t, recorder.prepared)
		preparedDeadline, bounded := preparation.Deadline()
		require.True(t, bounded)
		require.Equal(t, callerDeadline.UnixNano(), preparedDeadline.UnixNano(),
			"the forwarded deadline is the caller's original one, not restarted after the wait")
	})
}

// TestForwardedAcceptedWorkCompletesExactlyOnceAfterCallerCancelAndFollowerClose
// is the accepted half of the correction over real TCP: once a forwarded
// command crossed the accepted boundary, the forwarded
// caller's cancellation and the follower connection's close cannot cancel or
// repeat the leader's durable outcome.
func TestForwardedAcceptedWorkCompletesExactlyOnceAfterCallerCancelAndFollowerClose(t *testing.T) {
	fixture := setupTestAdapter(t)
	gate := newAcceptedApplyGate(fixture.adapter)
	leader := startForwardLeader(t, gate)
	t.Cleanup(gate.releaseApply)
	follower := startForwardFollower(t, leader.addr)

	command := publicationNodeCommand(t, "test:n152-64-accepted-tcp")
	caller, cancelCaller := context.WithCancel(t.Context())
	defer cancelCaller()
	outcome := make(chan error, 1)
	go func() { outcome <- follower.Apply(caller, command, 30*time.Second) }()

	application := awaitReplicationValue(t, gate.entered)
	cancelCaller()
	followerErr := awaitReplicationValue(t, outcome)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, followerErr, &accepted)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	requireAcceptedDiagnosticCause(t, followerErr, context.Canceled)
	require.NoError(t, application.Err(), "accepted durable work is not owned by the forwarded caller")

	followerTransport, ok := follower.transport.(*ClusterTransport)
	require.True(t, ok)
	_ = followerTransport.Close()
	require.NoError(t, application.Err(), "closing the follower transport cannot cancel accepted leader work")

	gate.releaseApply()
	require.Eventually(t, func() bool {
		_, readErr := storage.GetStoredNode(t.Context(), fixture.adapter.engine, "test:n152-64-accepted-tcp")
		return readErr == nil
	}, forwardContextTestTimeout, time.Millisecond, "accepted work must reach its durable outcome")
	require.NoError(t, fixture.adapter.FlushWAL())
	position, err := fixture.adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(1), position, "the accepted forward publishes exactly once")
}

// TestForwardConnectionServesReverseRPCWhilePreparationBlocked keeps the
// reader free during a forwarded preparation: a reverse RPC on the same
// connection must be answered inside that window, and its response must be
// routed to the waiting reverse caller before the forwarded reply is written.
func TestForwardConnectionServesReverseRPCWhilePreparationBlocked(t *testing.T) {
	recorder := newForwardRecorder()
	recorder.holdPreparation(false)
	t.Cleanup(recorder.releasePreparation)
	transport, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-reverse-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder)
	peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-reverse-peer")
	inbound := awaitInboundForwardConnection(t, transport)

	peer.send(t, forwardWireRequest(t, forwardCommand("n152-64-reverse"), time.Now().Add(time.Minute)))
	preparation := awaitReplicationValue(t, recorder.prepared)
	require.NoError(t, preparation.Err())

	type heartbeatResult struct {
		response *HeartbeatResponse
		err      error
	}
	reverse := make(chan heartbeatResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), forwardContextTestTimeout)
		defer cancel()
		response, err := inbound.SendHeartbeat(ctx, &HeartbeatRequest{NodeID: "n152-64-reverse-leader", Role: "leader", WALPosition: 77})
		reverse <- heartbeatResult{response: response, err: err}
	}()

	request := peer.receive(t)
	require.Equal(t, ClusterMsgHeartbeat, request.Type, "the reverse request must reach the peer while preparation is blocked")
	var heartbeat HeartbeatRequest
	require.NoError(t, storage.DecodeCommandPayload(request.Payload, &heartbeat))
	require.Equal(t, uint64(77), heartbeat.WALPosition)
	require.False(t, recorder.returned.Load(), "the forwarded preparation is still running")

	responsePayload, err := storage.EncodeCommandPayload(&HeartbeatResponse{WALPosition: 78})
	require.NoError(t, err)
	peer.send(t, &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: responsePayload})

	reply := awaitReplicationValue(t, reverse)
	require.NoError(t, reply.err)
	require.NotNil(t, reply.response)
	require.Equal(t, uint64(78), reply.response.WALPosition)
	require.False(t, recorder.returned.Load(), "the reverse exchange completed while preparation was blocked")

	recorder.releasePreparation()
	forwarded := peer.receive(t)
	require.Equal(t, ClusterMsgForwardApplyResponse, forwarded.Type, "the forwarded reply follows the reverse exchange")
	var applied forwardApplyResponse
	require.NoError(t, storage.DecodeCommandPayload(forwarded.Payload, &applied))
	require.Empty(t, applied.Err)
}

// TestForwardReplyFlushesBeforeNextInboundRequestIsDispatched pins inbound
// order: a request that is already read while a forward is running waits for
// that forward's reply to be flushed, and the connection then resumes
// immediate dispatch.
func TestForwardReplyFlushesBeforeNextInboundRequestIsDispatched(t *testing.T) {
	recorder := newForwardRecorder()
	recorder.holdPreparation(false)
	t.Cleanup(recorder.releasePreparation)
	observer := &forwardSocketReadObserver{}
	server, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-order-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder, func(transport *ClusterTransport) { transport.listenNative = observer.listen })

	var heartbeats atomic.Int32
	server.RegisterHandler(ClusterMsgHeartbeat, func(_ context.Context, _ string, msg *ClusterMessage) (*ClusterMessage, error) {
		heartbeats.Add(1)
		var request HeartbeatRequest
		if err := storage.DecodeCommandPayload(msg.Payload, &request); err != nil {
			return nil, err
		}
		payload, err := storage.EncodeCommandPayload(&HeartbeatResponse{WALPosition: request.WALPosition})
		if err != nil {
			return nil, err
		}
		return &ClusterMessage{Type: ClusterMsgHeartbeatResponse, Payload: payload}, nil
	})

	peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-order-peer")
	forwardFrame := peer.send(t, forwardWireRequest(t, forwardCommand("n152-64-order"), time.Now().Add(time.Minute)))
	awaitReplicationValue(t, recorder.prepared)

	queuedPayload, err := storage.EncodeCommandPayload(&HeartbeatRequest{NodeID: "n152-64-order-peer", WALPosition: 41})
	require.NoError(t, err)
	queuedFrame := peer.send(t, &ClusterMessage{Type: ClusterMsgHeartbeat, Payload: queuedPayload})

	// Prove the loop actually holds the queued request before asserting order.
	require.Eventually(t, func() bool {
		return observer.total.Load() >= int64(forwardFrame+queuedFrame)
	}, forwardContextTestTimeout, time.Millisecond, "the server never read the queued request")
	require.Never(t, func() bool { return heartbeats.Load() > 0 }, 150*time.Millisecond, 5*time.Millisecond,
		"the next inbound request was handled before the forwarded reply was flushed")

	recorder.releasePreparation()
	first := peer.receive(t)
	second := peer.receive(t)
	require.Equal(t, ClusterMsgForwardApplyResponse, first.Type, "the forwarded reply is written first")
	require.Equal(t, ClusterMsgHeartbeatResponse, second.Type, "the queued request follows it")
	var heartbeatResponse HeartbeatResponse
	require.NoError(t, storage.DecodeCommandPayload(second.Payload, &heartbeatResponse))
	require.Equal(t, uint64(41), heartbeatResponse.WALPosition)
	require.Equal(t, int32(1), heartbeats.Load())

	// The connection is back to immediate dispatch for the next request.
	followUpPayload, err := storage.EncodeCommandPayload(&HeartbeatRequest{NodeID: "n152-64-order-peer", WALPosition: 42})
	require.NoError(t, err)
	peer.send(t, &ClusterMessage{Type: ClusterMsgHeartbeat, Payload: followUpPayload})
	followUp := peer.receive(t)
	require.Equal(t, ClusterMsgHeartbeatResponse, followUp.Type)
	var followUpResponse HeartbeatResponse
	require.NoError(t, storage.DecodeCommandPayload(followUp.Payload, &followUpResponse))
	require.Equal(t, uint64(42), followUpResponse.WALPosition)
	require.Equal(t, int32(2), heartbeats.Load())
}

// TestForwardIdleReadDeadlineSuspendedDuringPreparationThenRestored covers the
// read deadline as an idle bound only: preparation may exceed it, the reply is
// still delivered, and the bound resumes once the connection is idle again.
func TestForwardIdleReadDeadlineSuspendedDuringPreparationThenRestored(t *testing.T) {
	const idle = 250 * time.Millisecond
	recorder := newForwardRecorder()
	recorder.holdPreparation(false)
	t.Cleanup(recorder.releasePreparation)
	transport, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:      "n152-64-idle-leader",
		AuthSecret:  []byte(replayTestSecret),
		ReadTimeout: idle,
	}, recorder)
	peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-idle-peer")
	inbound := awaitInboundForwardConnection(t, transport)

	peer.send(t, forwardWireRequest(t, forwardCommand("n152-64-idle"), time.Time{}))
	preparation := awaitReplicationValue(t, recorder.prepared)
	_, bounded := preparation.Deadline()
	require.False(t, bounded)

	// An active forward owns the connection: three idle bounds must pass
	// without ending preparation or the connection.
	select {
	case <-time.After(3 * idle):
	case <-preparation.Done():
		t.Fatalf("preparation was canceled by the idle read deadline: %v", preparation.Err())
	}
	require.NoError(t, preparation.Err())
	require.True(t, inbound.IsConnected(), "the idle read deadline must be suspended while a forward is active")

	recorder.releasePreparation()
	forwarded := peer.receive(t)
	require.Equal(t, ClusterMsgForwardApplyResponse, forwarded.Type)
	var applied forwardApplyResponse
	require.NoError(t, storage.DecodeCommandPayload(forwarded.Payload, &applied))
	require.Empty(t, applied.Err)

	// Idle again: the restored bound retires the connection.
	require.Eventually(t, func() bool { return !inbound.IsConnected() }, forwardContextTestTimeout, time.Millisecond,
		"the idle read deadline was not restored after the forwarded reply")
	require.NoError(t, peer.conn.SetReadDeadline(time.Now().Add(forwardContextTestTimeout)))
	_, err := peer.reader.ReadByte()
	require.Error(t, err, "the idle connection must actually be closed, not only marked disconnected")
}

// TestForwardConnectionCloseJoinsInFlightHandler proves Close joins the one
// enrolled forward handler instead of returning when its socket closes. The
// handler deliberately outlives cancellation, so only the handler's real
// completion can release Close.
func TestForwardConnectionCloseJoinsInFlightHandler(t *testing.T) {
	recorder := newForwardRecorder()
	recorder.holdPreparation(true)
	transport, address := startForwardWireServer(t, &ClusterTransportConfig{
		NodeID:     "n152-64-join-leader",
		AuthSecret: []byte(replayTestSecret),
	}, recorder)
	// Release the non-cancelable handler before transport cleanup joins it,
	// including when an assertion aborts the test before the explicit release.
	t.Cleanup(recorder.releasePreparation)
	peer := dialForwardWirePeer(t, address, []byte(replayTestSecret), "n152-64-join-peer")
	inbound := awaitInboundForwardConnection(t, transport)

	peer.send(t, forwardWireRequest(t, forwardCommand("n152-64-join"), time.Time{}))
	awaitReplicationValue(t, recorder.prepared)

	closed := make(chan error, 1)
	go func() { closed <- inbound.Close() }()
	require.Never(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, time.Millisecond, "Close returned while the forwarded handler was still running")
	require.False(t, recorder.returned.Load())

	recorder.releasePreparation()
	require.NoError(t, awaitReplicationValue(t, closed))
	require.True(t, recorder.returned.Load(), "Close joins the handler's actual completion")
}
