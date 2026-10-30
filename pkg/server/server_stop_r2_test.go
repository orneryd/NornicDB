package server

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

type stopR2Gate struct {
	entered, release chan struct{}
	enter, exit      sync.Once
}

func newStopR2Gate(t *testing.T) *stopR2Gate {
	g := &stopR2Gate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}
func (g *stopR2Gate) block() { g.enter.Do(func() { close(g.entered) }); <-g.release }
func (g *stopR2Gate) open()  { g.exit.Do(func() { close(g.release) }) }
func stopR2Await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("controlled consumer did not reach/finish its gate")
	}
}
func stopR2Server(t *testing.T) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "slow.log")
	s, _ := setupTestServerWithConfig(t, func(c *Config) {
		c.SlowQueryEnabled = true
		c.Logging.SlowQueryThreshold = 0
		c.Logging.SlowQueryLogFile = path
	})
	require.NotNil(t, s.slowQueryFile)
	_, err := s.executorCache.Get("nornic")
	require.NoError(t, err)
	return s, path
}
func stopR2Live(t *testing.T, s *Server) {
	t.Helper()
	_, err := s.slowQueryFile.Write(nil)
	if err != nil {
		t.Errorf("resource retired while a controlled consumer was still running: file: %v", err)
	}
	s.executorsMu.RLock()
	n := len(s.executors)
	s.executorsMu.RUnlock()
	if n == 0 {
		t.Error("executor cache retired before actual work completion")
	}
}
func stopR2Done(t *testing.T, s *Server) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if waiter, ok := interface{}(s).(interface{ WaitStopped(context.Context) error }); ok {
		err = waiter.WaitStopped(ctx)
	} else {
		// The old head has no actual-completion API; the file check below is still
		// behavioural, and old-head callback tests also record the bounded result.
		require.Eventually(t, func() bool { _, e := s.slowQueryFile.Write(nil); return errors.Is(e, os.ErrClosed) }, 5*time.Second, time.Millisecond)
	}
	if errors.Is(err, os.ErrClosed) {
		t.Error("shutdown attempted to close the real file more than once")
	}
	_, closed := s.slowQueryFile.Write(nil)
	require.ErrorIs(t, closed, os.ErrClosed)
	require.NoError(t, s.Stop(context.Background()))
	require.ErrorIs(t, s.Start(), ErrServerClosed)
	return err
}
func stopR2Bounded(t *testing.T, s *Server) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Stop(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop returned %v while work was gated, want DeadlineExceeded", err)
		}
		return err
	case <-time.After(350 * time.Millisecond):
		t.Error("Stop exceeded its context budget while cleanup/work was gated")
		// Do not deadlock the red run: caller releases all controlled consumers.
		return nil
	}
}
func stopR2HTTP(t *testing.T, s *Server, h http.Handler) (string, <-chan struct{}) {
	t.Helper()
	ts := httptest.NewUnstartedServer(s.wrapWithMiddleware(h))
	s.lifecycleMu.Lock()
	s.httpServer = ts.Config
	s.listener = ts.Listener
	s.lifecycleMu.Unlock()
	ts.Start()
	t.Cleanup(ts.Close)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Get(ts.URL + "/db/nornic/tx/r2")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	return ts.URL, done
}

func TestServerStopR2LeasedSlowLogLifetime(t *testing.T) {
	s, path := stopR2Server(t)
	session, err := s.txSessions.Open(context.Background(), "nornic")
	require.NoError(t, err)
	gate := newStopR2Gate(t)
	_, requestDone := stopR2HTTP(t, s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lease, ok := s.txSessions.AcquireForOwner(session.ID, "")
		if !ok {
			t.Error("lease not acquired")
			return
		}
		defer lease.Release()
		response := TransactionResponse{}
		s.runRequestStatements(r.Context(), "", nil, "nornic", []StatementRequest{{Statement: "RETURN 1 AS value"}},
			func(ctx context.Context, db, q string, p map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
				result, e := lease.ExecuteInSession(ctx, q, p)
				gate.block() // immediately before the common caller's logSlowQuery
				return result, session.Executor, e
			}, func(localization.Message) string { return "test" }, &response)
	}))
	stopR2Await(t, gate.entered)
	stopR2Bounded(t, s)
	stopR2Live(t, s)
	gate.open()
	stopR2Await(t, requestDone)
	_ = stopR2Done(t, s)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(data), "[SLOW QUERY]"), "the accepted request lost its last slow-query record")
}

type stopR2Plugin struct {
	watcher.WatcherPlugin
	stopGate, shutdownGate, workGate *stopR2Gate
	stops, shutdowns                 atomic.Int32
	lateErr                          error
	onStop                           func()
	onShutdown                       func()
}

func (*stopR2Plugin) Name() string { return "stop_r2" }
func (p *stopR2Plugin) Stop() error {
	p.stops.Add(1)
	if p.onStop != nil {
		p.onStop()
	}
	if p.stopGate != nil {
		p.stopGate.block()
	}
	return p.lateErr
}
func (p *stopR2Plugin) Shutdown() error {
	p.shutdowns.Add(1)
	if p.onShutdown != nil {
		p.onShutdown()
	}
	if p.shutdownGate != nil {
		p.shutdownGate.block()
	}
	return nil
}
func (p *stopR2Plugin) PostExecute(*heimdall.PostExecuteContext) {
	if p.workGate != nil {
		p.workGate.block()
	}
}
func (p *stopR2Plugin) OnDatabaseEvent(*heimdall.DatabaseEvent) {
	if p.workGate != nil {
		p.workGate.block()
	}
}
func (p *stopR2Plugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"gate": {Handler: func(heimdall.ActionContext) (*heimdall.ActionResult, error) {
			p.workGate.block()
			return &heimdall.ActionResult{Success: true}, nil
		}},
	}
}
func stopR2Install(t *testing.T, s *Server, p *stopR2Plugin) {
	t.Helper()
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	s.loadHeimdallPlugins(heimdall.SubsystemContext{}, nil)
	require.NoError(t, heimdall.GetSubsystemManager().RegisterPlugin(p, "", true))
}
func TestServerStopR2PluginLifetime(t *testing.T) {
	for _, phase := range []string{"Stop", "Shutdown"} {
		for _, held := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "AfterHTTP", true: "AfterLeaseDeadline"}[held], func(t *testing.T) {
				s, _ := stopR2Server(t)
				gate := newStopR2Gate(t)
				p := &stopR2Plugin{lateErr: errors.New("late plugin outcome")}
				if phase == "Stop" {
					p.stopGate = gate
				} else {
					p.shutdownGate = gate
				}
				stopR2Install(t, s, p)
				var lease *txsession.Lease
				if held {
					session, err := s.txSessions.Open(context.Background(), "nornic")
					require.NoError(t, err)
					var ok bool
					lease, ok = s.txSessions.AcquireForOwner(session.ID, "")
					require.True(t, ok)
					t.Cleanup(lease.Release)
				}
				stopR2Bounded(t, s)
				if held {
					if p.shutdowns.Load() != 0 {
						t.Error("plugin disposal ran before leased rollback completed")
					}
					lease.Release()
				}
				stopR2Await(t, gate.entered)
				stopR2Live(t, s)
				gate.open()
				late := stopR2Done(t, s)
				require.EqualValues(t, 1, p.stops.Load())
				require.EqualValues(t, 1, p.shutdowns.Load())
				require.ErrorContains(t, late, "late plugin outcome", "actual completion must expose late callback errors")
			})
		}
	}
}

func TestServerStopR2SuccessfulComposedLifetime(t *testing.T) {
	s, _ := stopR2Server(t)
	rollback := newStopR2Gate(t)
	rollback.open()
	engine, controller := stopR2RollbackManager(t, s, time.Hour, rollback)
	session, err := s.txSessions.Open(context.Background(), "nornic")
	require.NoError(t, err)
	lease, ok := s.txSessions.AcquireForOwner(session.ID, "")
	require.True(t, ok)
	t.Cleanup(lease.Release)
	_, err = lease.ExecuteInSession(context.Background(), "CREATE (:StopR2Rollback)", nil)
	require.NoError(t, err)
	tail := newStopR2Gate(t)
	releaseLease := newStopR2Gate(t)
	_, requestDone := stopR2HTTP(t, s, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { releaseLease.block(); lease.Release(); tail.block() }))
	stopR2Await(t, releaseLease.entered)
	p := &stopR2Plugin{onShutdown: func() {
		if !controller.completed.Load() {
			t.Error("plugin teardown preceded actual storage rollback completion")
		}
	}}
	stopR2Install(t, s, p)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Stop(ctx) }()
	require.Eventually(t, func() bool { return s.closed.Load() }, time.Second, time.Millisecond)
	releaseLease.open()
	stopR2Await(t, tail.entered)
	require.Eventually(t, controller.completed.Load, time.Second, time.Millisecond, "real storage rollback did not complete after the lease was released")
	result, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic")).Execute(context.Background(), "MATCH (n:StopR2Rollback) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
	stopR2Live(t, s)
	if p.shutdowns.Load() != 0 {
		t.Error("plugin disposed before the HTTP completion tail")
	}
	tail.open()
	stopR2Await(t, requestDone)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("successful composed Stop did not finish")
	}
	require.NoError(t, stopR2Done(t, s))
	require.EqualValues(t, 1, p.stops.Load())
	require.EqualValues(t, 1, p.shutdowns.Load())
}

func TestServerStopR2DetachedHTTPWorkerLifetime(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "15ms")
	s, _ := stopR2Server(t)
	gate := newStopR2Gate(t)
	workerDone := make(chan struct{})
	_, outerDone := stopR2HTTP(t, s, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		defer close(workerDone)
		gate.block()
		s.logSlowQuery("RETURN 1", nil, time.Second, nil)
	}))
	stopR2Await(t, gate.entered)
	stopR2Await(t, outerDone)
	stopR2Bounded(t, s)
	stopR2Live(t, s)
	gate.open()
	stopR2Await(t, workerDone)
	_ = stopR2Done(t, s)
}

type stopR2Health struct {
	health.UnimplementedHealthServer
	gate *stopR2Gate
}

func (h *stopR2Health) Check(context.Context, *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	h.gate.block()
	return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
}
func TestServerStopR2GRPCLifetime(t *testing.T) {
	s, _ := stopR2Server(t)
	gate := newStopR2Gate(t)
	cfg := qdrantgrpc.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	provider, err := qdrantgrpc.NewServerWithDatabaseManager(cfg, s.dbManager, s.db.GetBaseStorageForManager(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, provider.RegisterAdditionalServices(func(g *grpc.Server) { health.RegisterHealthServer(g, &stopR2Health{gate: gate}) }))
	require.NoError(t, provider.Start())
	s.mu.Lock()
	s.qdrantGRPCServer = provider
	s.mu.Unlock()
	conn, err := grpc.NewClient(provider.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	rpcDone := make(chan struct{})
	go func() {
		defer close(rpcDone)
		_, _ = health.NewHealthClient(conn).Check(context.Background(), &health.HealthCheckRequest{})
	}()
	stopR2Await(t, gate.entered)
	stopR2Bounded(t, s)
	stopR2Live(t, s)
	lockDone := make(chan struct{})
	go func() { s.SetHTTPMetrics(nil); close(lockDone) }()
	select {
	case <-lockDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("Server.mu is held while gRPC waits for a live RPC")
	}
	gate.open()
	stopR2Await(t, rpcDone)
	_ = stopR2Done(t, s)
	stopR2Await(t, lockDone)
}

func TestServerStopR2EmbedderLifetime(t *testing.T) {
	s, _ := stopR2Server(t)
	gate := newStopR2Gate(t)
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		gate.block()
		return nil, errors.New("constructor stopped")
	}
	s.startEmbedderInit(&embed.Config{Provider: "openai", Dimensions: 4}, 0, nil, s.db)
	stopR2Await(t, gate.entered)
	stopR2Bounded(t, s)
	stopR2Live(t, s)
	gate.open()
	stopR2Await(t, s.embedderInitDone)
	_ = stopR2Done(t, s)
}

func TestServerStopR2CleanupLocks(t *testing.T) {
	for _, kind := range []string{"lifecycle", "plugin-startup", "executor-cache"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := stopR2Server(t)
			var unlock func()
			switch kind {
			case "lifecycle":
				s.lifecycleMu.Lock()
				unlock = s.lifecycleMu.Unlock
			case "plugin-startup":
				s.heimdallPluginsMu.Lock()
				unlock = s.heimdallPluginsMu.Unlock
			case "executor-cache":
				s.executorsMu.Lock()
				unlock = s.executorsMu.Unlock
			}
			stopR2Bounded(t, s)
			unlock()
			_ = stopR2Done(t, s)
		})
	}
}

// The existing storage lifecycle seam gates real ROLLBACK inside the snapshot
// deregistration, after the manager has removed the session from its map.
type stopR2RollbackController struct {
	storage.MVCCLifecycleController
	gate      *stopR2Gate
	completed atomic.Bool
}

func (*stopR2RollbackController) IsLifecycleEnabled() bool { return true }
func (*stopR2RollbackController) EvaluateSnapshotReader(storage.SnapshotReaderInfo) (bool, bool) {
	return false, false
}
func (c *stopR2RollbackController) AcquireSnapshotReader(storage.SnapshotReaderInfo) (func(), error) {
	return func() { c.gate.block(); c.completed.Store(true) }, nil
}
func (*stopR2RollbackController) StopLifecycle() {}
func stopR2RollbackManager(t *testing.T, s *Server, ttl time.Duration, gate *stopR2Gate) (*storage.BadgerEngine, *stopR2RollbackController) {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	t.Cleanup(gate.open)
	controller := &stopR2RollbackController{gate: gate}
	engine.SetLifecycleController(controller)
	require.NoError(t, s.txSessions.Close())
	s.txSessions = txsession.NewManager(ttl, func(string) (*cypher.StorageExecutor, error) {
		return cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic")), nil
	})
	return engine, controller
}
func TestServerStopR2RollbackWorkerLifetime(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "CloseWorker", true: "AlreadyDetachedExpiry"}[expiry], func(t *testing.T) {
			s, _ := stopR2Server(t)
			gate := newStopR2Gate(t)
			ttl := time.Hour
			if expiry {
				ttl = 20 * time.Millisecond
			}
			_, controller := stopR2RollbackManager(t, s, ttl, gate)
			session, err := s.txSessions.Open(context.Background(), "nornic")
			require.NoError(t, err)
			_, err = s.txSessions.ExecuteInSession(context.Background(), session, "CREATE (:StopR2Worker)", nil)
			require.NoError(t, err)
			if expiry {
				stopR2Await(t, gate.entered)
			}
			stopR2Bounded(t, s)
			stopR2Await(t, gate.entered)
			stopR2Live(t, s)
			gate.open()
			_ = stopR2Done(t, s)
			require.True(t, controller.completed.Load())
		})
	}
}

type stopR2Generator struct {
	heimdall.Generator
	gate *stopR2Gate
}

func (g *stopR2Generator) Generate(context.Context, string, heimdall.GenerateParams) (string, error) {
	g.gate.block()
	return "done", nil
}

func TestServerStopR2ManagedPluginWork(t *testing.T) {
	for _, kind := range []string{"action", "sync-action", "prompt", "post-execute", "database-event"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := stopR2Server(t)
			gate := newStopR2Gate(t)
			p := &stopR2Plugin{workGate: gate}
			stopR2Install(t, s, p)
			switch kind {
			case "action":
				heimdall.NewLiveHeimdallInvoker(heimdall.GetSubsystemManager(), nil, nil, nil, nil).InvokeActionAsync(context.Background(), "heimdall_stop_r2_gate", nil)
			case "sync-action":
				go func() {
					_, _ = heimdall.ExecuteAction("heimdall_stop_r2_gate", heimdall.ActionContext{Context: context.Background()})
				}()
			case "prompt":
				heimdall.NewLiveHeimdallInvoker(heimdall.GetSubsystemManager(), &stopR2Generator{gate: gate}, nil, nil, nil).SendPromptAsync(context.Background(), "test")
			case "post-execute":
				for i := 0; i < 300; i++ {
					heimdall.CallPostExecuteHooks(&heimdall.PostExecuteContext{Context: context.Background(), Action: "gate"})
				}
			case "database-event":
				heimdall.StartEventDispatcher()
				t.Cleanup(heimdall.StopEventDispatcher)
				heimdall.EmitDatabaseEvent(&heimdall.DatabaseEvent{Type: heimdall.EventNodeCreated})
			}
			stopR2Await(t, gate.entered)
			stopR2Bounded(t, s)
			stopR2Live(t, s)
			if p.shutdowns.Load() != 0 {
				t.Error("managed plugin retired before its detached callback finished")
			}
			gate.open()
			_ = stopR2Done(t, s)
			require.EqualValues(t, 1, p.stops.Load())
			require.EqualValues(t, 1, p.shutdowns.Load())
		})
	}
}
