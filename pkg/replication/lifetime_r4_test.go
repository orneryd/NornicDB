package replication

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r4LifetimePeer struct {
	*MockPeerConn
	failure   error
	probe     func()
	heartbeat func(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	vote      func(context.Context, *RaftVoteRequest) (*RaftVoteResponse, error)
	appendRPC func(context.Context, *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error)
}

func (p *r4LifetimePeer) Close() error {
	if p.probe != nil {
		p.probe()
	}
	return errors.Join(p.failure, p.MockPeerConn.Close())
}
func (p *r4LifetimePeer) SendHeartbeat(ctx context.Context, request *HeartbeatRequest) (*HeartbeatResponse, error) {
	if p.heartbeat != nil {
		return p.heartbeat(ctx, request)
	}
	return p.MockPeerConn.SendHeartbeat(ctx, request)
}
func (p *r4LifetimePeer) SendRaftVote(ctx context.Context, request *RaftVoteRequest) (*RaftVoteResponse, error) {
	if p.vote != nil {
		return p.vote(ctx, request)
	}
	return p.MockPeerConn.SendRaftVote(ctx, request)
}
func (p *r4LifetimePeer) SendRaftAppendEntries(ctx context.Context, request *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
	if p.appendRPC != nil {
		return p.appendRPC(ctx, request)
	}
	return p.MockPeerConn.SendRaftAppendEntries(ctx, request)
}

type r4LifetimeTransport struct {
	*MockTransport
	mu            sync.Mutex
	peers         map[string]*r4LifetimePeer
	peerMade      chan *r4LifetimePeer
	listenEntered chan struct{}
	listenOnce    sync.Once
	inbound       bool
	failure       error
	probe         func()
	setupPeer     func(*r4LifetimePeer)
}

func (tr *r4LifetimeTransport) peer(addr string) *r4LifetimePeer {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if peer := tr.peers[addr]; peer != nil {
		return peer
	}
	peer := &r4LifetimePeer{MockPeerConn: &MockPeerConn{addr: addr, connected: true}, failure: errors.New("r4 peer close " + addr), probe: tr.probe}
	if tr.setupPeer != nil {
		tr.setupPeer(peer)
	}
	tr.peers[addr] = peer
	tr.peerMade <- peer
	return peer
}
func (tr *r4LifetimeTransport) Connect(_ context.Context, stringAddr string) (PeerConnection, error) {
	return tr.peer(stringAddr), nil
}
func (tr *r4LifetimeTransport) Listen(ctx context.Context, addr string, handler ConnectionHandler) error {
	if tr.inbound {
		handler(tr.peer("r4-inbound"))
	}
	tr.listenOnce.Do(func() { close(tr.listenEntered) })
	return tr.MockTransport.Listen(ctx, addr, handler)
}
func (tr *r4LifetimeTransport) Close() error {
	if tr.probe != nil {
		tr.probe()
	}
	return errors.Join(tr.failure, tr.MockTransport.Close())
}

func r4NativeConfig(mode ReplicationMode, role string) *Config {
	cfg := DefaultConfig()
	cfg.Mode = mode
	cfg.NodeID = "r4-native"
	cfg.AdvertiseAddr = "127.0.0.1:17000"
	cfg.Raft.Bootstrap = true
	cfg.HAStandby.Role = role
	cfg.HAStandby.AutoFailover = false
	cfg.HAStandby.PeerAddr = "127.0.0.1:17001"
	cfg.HAStandby.HeartbeatInterval = 5 * time.Millisecond
	cfg.MultiRegion.RegionID = "r4-local"
	cfg.MultiRegion.LocalCluster = cfg.Raft
	return cfg
}
func r4Probe(t *testing.T, call func(), workers *sync.WaitGroup) func() {
	return func() {
		done := make(chan struct{})
		workers.Add(1)
		go func() { defer workers.Done(); call(); close(done) }()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Error("host state read blocked inside physical Close")
		}
	}
}

func TestReplicationR4FailedConstructionAdmission(t *testing.T) {
	for _, spec := range []struct {
		mode ReplicationMode
		role string
	}{{ModeRaft, ""}, {ModeHAStandby, "primary"}, {ModeHAStandby, "standby"}, {ModeMultiRegion, ""}} {
		t.Run(string(spec.mode)+"/"+spec.role, func(t *testing.T) {
			cfg := r4NativeConfig(spec.mode, spec.role)
			cfg.TLS.Enabled = true
			cfg.TLS.CertFile = "r4-missing-certificate.pem"
			cfg.TLS.KeyFile = "r4-missing-key.pem"
			owner, err := NewReplicator(cfg, NewMockStorage())
			require.NoError(t, err)
			require.Error(t, owner.Start(context.Background()))
			defer owner.Shutdown()
			assert.False(t, owner.Health().Healthy, "failed constructor advertised a healthy native lifetime")
			assert.Error(t, owner.Start(context.Background()), "retry bypassed the failed constructor and reported success")
			assert.ErrorIs(t, owner.Apply(&Command{Type: CmdCreateNode, Data: []byte("r4")}, time.Second), ErrNotReady, "failed construction opened command admission")
		})
	}
}

func TestReplicationR4AllNativeRetirementOutcomes(t *testing.T) {
	for _, spec := range []struct {
		mode  ReplicationMode
		role  string
		peers int
	}{{ModeRaft, "", 2}, {ModeHAStandby, "primary", 1}, {ModeHAStandby, "standby", 1}, {ModeMultiRegion, "", 2}} {
		t.Run(string(spec.mode)+"/"+spec.role, func(t *testing.T) {
			cfg := r4NativeConfig(spec.mode, spec.role)
			if spec.mode == ModeRaft {
				cfg.Raft.Peers = []PeerConfig{{ID: "a", Addr: "127.0.0.1:17001"}, {ID: "b", Addr: "127.0.0.1:17002"}}
			}
			if spec.mode == ModeMultiRegion {
				cfg.MultiRegion.RemoteRegions = []RemoteRegionConfig{{RegionID: "a", Addrs: []string{"127.0.0.1:17001"}}, {RegionID: "b", Addrs: []string{"127.0.0.1:17002"}}}
			}
			owner, err := NewReplicator(cfg, NewMockStorage())
			require.NoError(t, err)
			var probes sync.WaitGroup
			tr := &r4LifetimeTransport{MockTransport: NewMockTransport(), peers: map[string]*r4LifetimePeer{}, peerMade: make(chan *r4LifetimePeer, 8), listenEntered: make(chan struct{}), inbound: spec.mode == ModeHAStandby && spec.role == "standby", failure: errors.New("r4 transport close")}
			tr.probe = r4Probe(t, func() { owner.Health() }, &probes)
			owner.(interface{ SetTransport(Transport) }).SetTransport(tr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, owner.Start(ctx))
			var peers []*r4LifetimePeer
			for len(peers) < spec.peers {
				select {
				case peer := <-tr.peerMade:
					peers = append(peers, peer)
				case <-time.After(3 * time.Second):
					t.Fatal("public Start did not construct every configured peer")
				}
			}
			result := owner.Shutdown()
			probes.Wait()
			require.ErrorIs(t, result, tr.failure)
			for _, peer := range peers {
				require.ErrorIs(t, result, peer.failure, "lost a sibling peer close outcome")
			}
			require.Equal(t, "closed", owner.Health().State)
			// A repeat Shutdown joins the same stop and reports its outcome:
			// an early nil would let a second caller dispose shared state
			// while the first stop's cleanup failed.
			repeat := owner.Shutdown()
			require.ErrorIs(t, repeat, tr.failure, "repeat Shutdown must report the stop's outcome")
			for _, peer := range peers {
				require.ErrorIs(t, repeat, peer.failure, "repeat Shutdown lost a sibling peer close outcome")
			}
		})
	}
	t.Run("standalone borrowed storage", func(t *testing.T) {
		owner, err := NewReplicator(r4NativeConfig(ModeStandalone, ""), NewMockStorage())
		require.NoError(t, err)
		require.NoError(t, owner.Start(context.Background()))
		require.NoError(t, owner.Shutdown())
		require.Equal(t, "closed", owner.Health().State)
		require.ErrorIs(t, owner.Start(context.Background()), ErrClosed)
	})
}

type r4StartGate struct {
	needle           string
	entered, release chan struct{}
	once             sync.Once
}

func (h *r4StartGate) Enabled(context.Context, slog.Level) bool { return true }
func (h *r4StartGate) Handle(_ context.Context, record slog.Record) error {
	if strings.Contains(record.Message, h.needle) {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}
func (h *r4StartGate) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *r4StartGate) WithGroup(string) slog.Handler      { return h }
func TestReplicationR4StartCompletionAdmission(t *testing.T) {
	for _, spec := range []struct {
		mode         ReplicationMode
		role, needle string
	}{{ModeRaft, "", "Starting node"}, {ModeHAStandby, "primary", "Starting in role="}, {ModeHAStandby, "standby", "Starting in role="}, {ModeMultiRegion, "", "Started in region"}} {
		t.Run(string(spec.mode)+"/"+spec.role, func(t *testing.T) {
			cfg := r4NativeConfig(spec.mode, spec.role)
			gate := &r4StartGate{needle: spec.needle, entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			unblock := func() { release.Do(func() { close(gate.release) }) }
			t.Cleanup(unblock)
			cfg.Logger = slog.New(gate)
			owner, err := NewReplicator(cfg, NewMockStorage())
			require.NoError(t, err)
			owner.(interface{ SetTransport(Transport) }).SetTransport(NewMockTransport())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan error, 1)
			go func() { started <- owner.Start(ctx) }()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("real Start did not enter its lifecycle logger")
			}
			stopped := make(chan error, 1)
			go func() { stopped <- owner.Shutdown() }()
			select {
			case err := <-stopped:
				t.Errorf("Shutdown completed before admitted Start returned: %v", err)
				unblock()
				<-started
				return
			case <-time.After(25 * time.Millisecond):
			}
			unblock()
			select {
			case err := <-started:
				if err != nil {
					require.ErrorIs(t, err, ErrClosed)
				}
			case <-time.After(time.Second):
				t.Fatal("Start did not complete")
			}
			select {
			case err := <-stopped:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Shutdown did not join Start")
			}
			require.ErrorIs(t, owner.Start(context.Background()), ErrClosed)
		})
	}
}

type r4FaultListener struct {
	net.Listener
	failure error
	probe   func()
	closed  chan struct{}
	once    sync.Once
}

func (l *r4FaultListener) Close() error {
	if l.probe != nil {
		l.probe()
	}
	err := errors.Join(l.failure, l.Listener.Close())
	l.once.Do(func() {
		if l.closed != nil {
			close(l.closed)
		}
	})
	return err
}

type r4FaultConn struct {
	net.Conn
	failure error
	probe   func()
}

func (c *r4FaultConn) Close() error {
	if c.probe != nil {
		c.probe()
	}
	return errors.Join(c.failure, c.Conn.Close())
}

func TestReplicationR4ClusterNativeCloseOutcomes(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "forced listener", true: "listener context completion"}[cancelFirst], func(t *testing.T) {
			owner := NewClusterTransport(nil)
			failure := errors.New("r4 native listener close")
			ready, physicalClosed := make(chan struct{}), make(chan struct{})
			var probes sync.WaitGroup
			probe := r4Probe(t, func() {
				owner.RegisterHandler(ClusterMsgStatus, func(context.Context, string, *ClusterMessage) (*ClusterMessage, error) { return nil, nil })
			}, &probes)
			owner.listenNative = func(network, addr string) (net.Listener, error) {
				listener, err := net.Listen(network, addr)
				if err != nil {
					return nil, err
				}
				close(ready)
				return &r4FaultListener{Listener: listener, failure: failure, probe: probe, closed: physicalClosed}, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listened := make(chan error, 1)
			go func() { listened <- owner.Listen(ctx, "127.0.0.1:0", nil) }()
			<-ready
			if cancelFirst {
				cancel()
				select {
				case <-listened:
				case <-time.After(time.Second):
					t.Fatal("Listen did not finish after cancellation")
				}
				<-physicalClosed
			}
			result := owner.Close()
			if !cancelFirst {
				select {
				case <-listened:
				case <-time.After(time.Second):
					t.Fatal("Close did not join public Listen")
				}
			}
			<-physicalClosed
			probes.Wait()
			require.ErrorIs(t, result, failure)
		})
	}
	t.Run("connection siblings and read loop", func(t *testing.T) {
		owner := NewClusterTransport(nil)
		var probes sync.WaitGroup
		var remote []net.Conn
		t.Cleanup(func() {
			for _, peer := range remote {
				peer.Close()
			}
		})
		var counter atomic.Int32
		failures := []error{errors.New("r4 socket A"), errors.New("r4 socket B")}
		owner.dialNative = func(context.Context, string, string) (net.Conn, error) {
			local, peer := net.Pipe()
			remote = append(remote, peer)
			index := int(counter.Add(1)) - 1
			return &r4FaultConn{Conn: local, failure: failures[index], probe: r4Probe(t, func() { owner.RegisterHandler(ClusterMsgStatus, nil) }, &probes)}, nil
		}
		_, err := owner.Connect(context.Background(), "a")
		require.NoError(t, err)
		_, err = owner.Connect(context.Background(), "b")
		require.NoError(t, err)
		result := owner.Close()
		probes.Wait()
		for _, failure := range failures {
			require.ErrorIs(t, result, failure, "lost a native sibling close outcome")
		}
	})
}

func TestReplicationR4FailoverChildrenActualCompletion(t *testing.T) {
	cfg := r4NativeConfig(ModeMultiRegion, "")
	cfg.MultiRegion.RemoteRegions = []RemoteRegionConfig{{RegionID: "a", Addrs: []string{"127.0.0.1:17001"}}, {RegionID: "b", Addrs: []string{"127.0.0.1:17002"}}}
	owner, err := NewReplicator(cfg, NewMockStorage())
	require.NoError(t, err)
	tr := &r4LifetimeTransport{MockTransport: NewMockTransport(), peers: map[string]*r4LifetimePeer{}, peerMade: make(chan *r4LifetimePeer, 8), listenEntered: make(chan struct{}), failure: errors.New("r4 failover transport close")}
	owner.(interface{ SetTransport(Transport) }).SetTransport(tr)
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	require.NoError(t, owner.Start(rootCtx))
	peers := make([]*r4LifetimePeer, 0, 2)
	for len(peers) < 2 {
		select {
		case peer := <-tr.peerMade:
			peers = append(peers, peer)
		case <-time.After(time.Second):
			t.Fatal("Start did not construct remote members")
		}
	}
	check, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	unblock := func() { finishOnce.Do(func() { close(finish) }) }
	t.Cleanup(unblock)
	checked, completed := make(chan struct{}, 2), make(chan struct{}, 2)
	failures := []error{errors.New("r4 notification A"), errors.New("r4 notification B")}
	type requestKey struct{}
	for index, peer := range peers {
		failure := failures[index]
		peer.heartbeat = func(ctx context.Context, _ *HeartbeatRequest) (*HeartbeatResponse, error) {
			<-check
			if ctx.Err() != nil {
				t.Errorf("accepted notification inherited the ended request: %v", ctx.Err())
			}
			if ctx.Value(requestKey{}) != "r4 request value" {
				t.Error("accepted notification lost request values")
			}
			checked <- struct{}{}
			<-finish
			completed <- struct{}{}
			return nil, failure
		}
	}
	requestCtx, cancelRequest := context.WithCancel(context.WithValue(context.Background(), requestKey{}, "r4 request value"))
	require.NoError(t, owner.(*MultiRegionReplicator).RegionFailover(requestCtx))
	cancelRequest()
	close(check)
	for range peers {
		select {
		case <-checked:
		case <-time.After(time.Second):
			t.Fatal("accepted notification did not enter callback")
		}
	}
	stopped := make(chan error, 1)
	go func() { stopped <- owner.Shutdown() }()
	var result error
	early := false
	select {
	case result = <-stopped:
		early = true
		t.Error("Shutdown completed before accepted notifications returned")
	case <-time.After(25 * time.Millisecond):
	}
	if !early {
		for _, peer := range peers {
			require.True(t, peer.IsConnected(), "peer disposed before its notification completed")
		}
	}
	unblock()
	for range peers {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("notification did not finish")
		}
	}
	if !early {
		select {
		case result = <-stopped:
		case <-time.After(time.Second):
			t.Fatal("Shutdown did not join notifications")
		}
	}
	for _, failure := range failures {
		require.ErrorIs(t, result, failure, "lost an asynchronous notification sibling")
	}
}

func TestReplicationR4RaftRPCActualCompletion(t *testing.T) {
	for _, operation := range []string{"vote", "append"} {
		t.Run(operation, func(t *testing.T) {
			cfg := r4NativeConfig(ModeRaft, "")
			cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
			cfg.Raft.ElectionTimeout = 25 * time.Millisecond
			owner, err := NewReplicator(cfg, NewMockStorage())
			require.NoError(t, err)
			entered, finish := make(chan struct{}), make(chan struct{})
			var enterOnce, finishOnce sync.Once
			unblock := func() { finishOnce.Do(func() { close(finish) }) }
			t.Cleanup(unblock)
			tr := &r4LifetimeTransport{MockTransport: NewMockTransport(), peers: map[string]*r4LifetimePeer{}, peerMade: make(chan *r4LifetimePeer, 8), listenEntered: make(chan struct{})}
			tr.setupPeer = func(peer *r4LifetimePeer) {
				if operation == "vote" {
					peer.vote = func(_ context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error) {
						enterOnce.Do(func() { close(entered) })
						<-finish
						return &RaftVoteResponse{Term: req.Term, VoteGranted: true, VoterID: "peer"}, nil
					}
				}
				if operation == "append" {
					peer.appendRPC = func(_ context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
						enterOnce.Do(func() { close(entered) })
						<-finish
						return &RaftAppendEntriesResponse{Term: req.Term, Success: true, MatchIndex: req.PrevLogIndex}, nil
					}
				}
			}
			owner.(interface{ SetTransport(Transport) }).SetTransport(tr)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, owner.Start(ctx))
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("real Raft worker did not enter the configured RPC")
			}
			stopped := make(chan error, 1)
			go func() { stopped <- owner.Shutdown() }()
			var result error
			early := false
			select {
			case result = <-stopped:
				early = true
				t.Error("Shutdown completed before its admitted RPC returned")
			case <-time.After(40 * time.Millisecond):
			}
			unblock()
			if !early {
				select {
				case result = <-stopped:
				case <-time.After(time.Second):
					t.Fatal("Shutdown did not join the actual RPC")
				}
			}
			peer := <-tr.peerMade
			require.ErrorIs(t, result, peer.failure)
		})
	}
}

type r4ObservedWriter struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *r4ObservedWriter) Write(data []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(data)
}
func TestReplicationR4CloseSignalsBlockedSocketWrite(t *testing.T) {
	cfg := DefaultClusterTransportConfig()
	cfg.WriteTimeout = time.Hour
	owner := NewClusterTransport(cfg)
	local, remote := net.Pipe()
	defer remote.Close()
	writing := make(chan struct{})
	owner.dialNative = func(context.Context, string, string) (net.Conn, error) {
		return &r4ObservedWriter{Conn: local, entered: writing}, nil
	}
	connection, err := owner.Connect(context.Background(), "r4-blocked-write")
	require.NoError(t, err)
	sent := make(chan error, 1)
	go func() {
		_, err := connection.SendHeartbeat(context.Background(), &HeartbeatRequest{NodeID: "r4"})
		sent <- err
	}()
	select {
	case <-writing:
	case <-time.After(time.Second):
		t.Fatal("public RPC did not enter its native Write")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- owner.Close() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(50 * time.Millisecond):
		t.Error("Close waited for the write mutex before signaling the native socket")
		require.NoError(t, remote.Close()) // controlled old-product escape
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("Close did not finish after socket escape")
		}
	}
	select {
	case err := <-sent:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("native Close did not release RPC Write")
	}
}

type r4ReadLogHandler struct {
	needle   string
	fired    atomic.Bool
	callback func()
	observed chan struct{}
}

func (h *r4ReadLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *r4ReadLogHandler) Handle(_ context.Context, record slog.Record) error {
	if strings.Contains(record.Message, h.needle) && h.fired.CompareAndSwap(false, true) {
		h.callback()
		close(h.observed)
	}
	return nil
}
func (h *r4ReadLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *r4ReadLogHandler) WithGroup(string) slog.Handler      { return h }
func TestReplicationR4ForeignObservationReentry(t *testing.T) {
	for _, operation := range []string{"add voter", "remove server", "inbound vote", "apply failure", "vote granted", "vote higher term", "append higher term"} {
		t.Run(operation, func(t *testing.T) {
			needles := map[string]string{"add voter": "Added voter", "remove server": "Removed server", "inbound vote": "Granted vote", "apply failure": "Failed to apply entry", "vote granted": "Received vote from", "vote higher term": "Stepping down: discovered higher term", "append higher term": "Stepping down: discovered higher term"}
			handler := &r4ReadLogHandler{needle: needles[operation], observed: make(chan struct{})}
			cfg := r4NativeConfig(ModeRaft, "")
			cfg.Logger = slog.New(handler)
			outbound := operation == "vote granted" || operation == "vote higher term" || operation == "append higher term"
			if outbound {
				cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
				cfg.Raft.ElectionTimeout = 25 * time.Millisecond
			}
			store := NewMockStorage()
			applyFailure := errors.New("r4 native apply failure")
			if operation == "apply failure" {
				store.applyErr = applyFailure
			}
			instance, err := NewReplicator(cfg, store)
			require.NoError(t, err)
			owner := instance.(*RaftReplicator)
			var probes sync.WaitGroup
			handler.callback = r4Probe(t, func() { owner.Health() }, &probes)
			if outbound {
				tr := &r4LifetimeTransport{MockTransport: NewMockTransport(), peers: map[string]*r4LifetimePeer{}, peerMade: make(chan *r4LifetimePeer, 8), listenEntered: make(chan struct{})}
				tr.setupPeer = func(peer *r4LifetimePeer) {
					peer.failure = nil
					if operation == "vote higher term" {
						peer.vote = func(_ context.Context, req *RaftVoteRequest) (*RaftVoteResponse, error) {
							return &RaftVoteResponse{Term: req.Term + 1, VoterID: "peer"}, nil
						}
					}
					if operation == "append higher term" {
						peer.appendRPC = func(_ context.Context, req *RaftAppendEntriesRequest) (*RaftAppendEntriesResponse, error) {
							return &RaftAppendEntriesResponse{Term: req.Term + 1}, nil
						}
					}
				}
				owner.SetTransport(tr)
			} else {
				owner.SetTransport(NewMockTransport())
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, owner.Start(ctx))
			switch operation {
			case "add voter":
				require.NoError(t, owner.AddVoter("peer", "127.0.0.1:17001"))
			case "remove server":
				require.NoError(t, owner.RemoveServer("absent"))
			case "inbound vote":
				_, err := owner.HandleRaftVote(&RaftVoteRequest{Term: 1000, CandidateID: "peer"})
				require.NoError(t, err)
			case "apply failure":
				require.ErrorIs(t, owner.Apply(&Command{Type: CmdCreateNode, Data: []byte("r4")}, time.Second), applyFailure)
			}
			select {
			case <-handler.observed:
			case <-time.After(3 * time.Second):
				t.Fatal("public operation did not reach the selected observer")
			}
			require.NoError(t, owner.Shutdown())
			probes.Wait()
		})
	}
}
