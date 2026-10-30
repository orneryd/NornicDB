package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	cfgpkg "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	watcher "github.com/orneryd/nornicdb/plugins/heimdall"
	"github.com/stretchr/testify/require"
)

type stopR3Plugin struct {
	watcher.WatcherPlugin
	name                 string
	invoker              heimdall.HeimdallInvoker
	signal, held         *stopR2Gate
	actionDone, followed chan struct{}
	followOnce           sync.Once
	stopErr, shutdownErr error
	stops, shutdowns     atomic.Int32
	cancelDriven         bool
	events               atomic.Int32
}

func (p *stopR3Plugin) OnDatabaseEvent(*heimdall.DatabaseEvent) {
	p.events.Add(1)
	if p.followed != nil {
		p.followOnce.Do(func() { close(p.followed) })
	}
}

func TestServerStopR3EventQueueRetirement(t *testing.T) {
	heimdall.StartEventDispatcher()
	t.Cleanup(heimdall.StopEventDispatcher)
	first, _ := stopR2Server(t)
	a := &stopR3Plugin{name: "r3_replacement", followed: make(chan struct{})}
	stopR3Install(t, first, a)
	heimdall.EmitDatabaseEvent(&heimdall.DatabaseEvent{Type: heimdall.EventNodeCreated})
	stopR2Await(t, a.followed)
	require.NoError(t, first.Stop(context.Background()))
	require.NoError(t, first.WaitStopped(context.Background()))
	require.EqualValues(t, 1, a.shutdowns.Load())
	second, _ := stopR2Server(t)
	b := &stopR3Plugin{name: a.name, followed: make(chan struct{})}
	stopR3Install(t, second, b)
	heimdall.EmitDatabaseEvent(&heimdall.DatabaseEvent{Type: heimdall.EventNodeCreated})
	select {
	case <-b.followed:
	case <-time.After(300 * time.Millisecond):
		t.Error("replacement event was delivered to a disposed plugin's retained queue")
	}
	require.EqualValues(t, 1, a.events.Load(), "disposed plugin received another host's event")
	require.NoError(t, second.Stop(context.Background()))
	require.NoError(t, second.WaitStopped(context.Background()))
}

func (p *stopR3Plugin) Name() string { return p.name }
func (p *stopR3Plugin) Initialize(c heimdall.SubsystemContext) error {
	p.invoker = c.Heimdall
	return p.WatcherPlugin.Initialize(c)
}
func (p *stopR3Plugin) Stop() error {
	p.stops.Add(1)
	if p.signal != nil {
		p.signal.open()
		if p.actionDone != nil {
			<-p.actionDone
		}
	}
	if p.held != nil && p.stopErr != nil {
		p.held.block()
	}
	return p.stopErr
}
func (p *stopR3Plugin) Shutdown() error { p.shutdowns.Add(1); return p.shutdownErr }
func (p *stopR3Plugin) Actions() map[string]heimdall.ActionFunc {
	return map[string]heimdall.ActionFunc{
		"hold": {Handler: func(c heimdall.ActionContext) (*heimdall.ActionResult, error) {
			defer close(p.actionDone)
			p.held.enter.Do(func() { close(p.held.entered) })
			if p.cancelDriven {
				select {
				case <-c.Done():
				case <-p.held.release:
				}
			} else {
				<-p.signal.release
			}
			return &heimdall.ActionResult{Success: true}, nil
		}},
		"follow": {Handler: func(heimdall.ActionContext) (*heimdall.ActionResult, error) {
			p.followOnce.Do(func() { close(p.followed) })
			return &heimdall.ActionResult{Success: true}, nil
		}},
	}
}
func (p *stopR3Plugin) nested(c context.Context) {
	p.held.block()
	p.invoker.InvokeActionAsync(c, "heimdall_"+p.name+"_follow", nil)
}
func (p *stopR3Plugin) PostExecute(c *heimdall.PostExecuteContext) {
	p.nested(c.Context)
	c.NotifyInfo("r3", "shared notification")
}
func (p *stopR3Plugin) PrePrompt(c *heimdall.PromptContext) error {
	p.nested(c.Context)
	c.UserMessage = "updated"
	return nil
}
func (p *stopR3Plugin) PreExecute(c *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
	p.nested(c.Context)
	done(heimdall.PreExecuteResult{Continue: true})
}
func (p *stopR3Plugin) Synthesize(c *heimdall.SynthesisContext, done func(string)) {
	p.nested(c.Context)
	done("completed")
}

func stopR3Install(t *testing.T, s *Server, plugins ...*stopR3Plugin) {
	t.Helper()
	heimdall.ResetSubsystemManagerForTests()
	t.Cleanup(heimdall.ResetSubsystemManagerForTests)
	m := heimdall.GetSubsystemManager()
	s.shutdown.init()
	s.loadHeimdallPlugins(heimdall.SubsystemContext{Context: s.shutdown.workContext, Config: heimdall.Config{APIURL: "https://user:password@example.invalid", APIKey: "r3-private-key"}, Heimdall: heimdall.NewLiveHeimdallInvoker(m, nil, nil, nil, nil)}, nil)
	for _, p := range plugins {
		require.NoError(t, m.RegisterPlugin(p, "", true))
	}
}
func stopR3Result(t *testing.T, s *Server) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	return s.WaitStopped(ctx)
}
func stopR3DBClose(t *testing.T, s *Server) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.db.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("DB.Close after lifecycle completion: %v", err)
		}
	case <-time.After(400 * time.Millisecond):
		t.Error("DB.Close waited on a completion cycle")
	}
}
func TestServerStopR3PluginSignal(t *testing.T) {
	for _, cancelDriven := range []bool{false, true} {
		t.Run(fmt.Sprintf("hostCancel=%t", cancelDriven), func(t *testing.T) {
			s, _ := stopR2Server(t)
			held := newStopR2Gate(t)
			signal := newStopR2Gate(t)
			p := &stopR3Plugin{name: "r3_signal", held: held, actionDone: make(chan struct{}), cancelDriven: cancelDriven}
			if !cancelDriven {
				p.signal = signal
			}
			stopR3Install(t, s, p)
			p.invoker.InvokeActionAsync(context.Background(), "heimdall_r3_signal_hold", nil)
			stopR2Await(t, held.entered)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			_ = s.Stop(ctx)
			if err := stopR3Result(t, s); err != nil {
				t.Errorf("shutdown did not signal accepted plugin work before joining it: %v", err)
			}
			stopR3DBClose(t, s)
			// Escape is solely red-run cleanup; success above must come from host/plugin stop.
			signal.open()
			held.open()
			stopR2Await(t, p.actionDone)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel2()
			_ = s.WaitStopped(ctx2)
			require.EqualValues(t, 1, p.stops.Load())
			require.EqualValues(t, 1, p.shutdowns.Load())
		})
	}
}

func TestServerStopR3BackgroundLineage(t *testing.T) {
	for _, fromHTTP := range []bool{false, true} {
		t.Run(fmt.Sprint(fromHTTP), func(t *testing.T) {
			s, _ := stopR2Server(t)
			gate := newStopR2Gate(t)
			canceled := make(chan struct{})
			launch := func(ctx context.Context) {
				require.True(t, s.runBackground(ctx, func(ctx context.Context) {
					gate.enter.Do(func() { close(gate.entered) })
					select {
					case <-ctx.Done():
						close(canceled)
					case <-gate.release:
					}
				}))
			}
			if fromHTTP {
				_, done := stopR2HTTP(t, s, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { launch(r.Context()) }))
				stopR2Await(t, done)
			} else {
				launch(context.Background())
			}
			stopR2Await(t, gate.entered)
			select {
			case <-canceled:
				t.Error("accepted background work inherited the ended HTTP request cancellation")
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			_ = s.Stop(ctx)
			select {
			case <-canceled:
			case <-time.After(400 * time.Millisecond):
				t.Error("host stop did not signal its retained background work")
			}
			err := stopR3Result(t, s)
			if errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("background completion cycle: %v", err)
			}
			gate.open()
		})
	}
}

func TestServerStopR3CleanupOutcomes(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprintf("late=%t", late), func(t *testing.T) {
			s, _ := stopR2Server(t)
			gate := newStopR2Gate(t)
			a := &stopR3Plugin{name: "r3_a", stopErr: errors.New("a-stop r3-private-key"), shutdownErr: errors.New("a-shutdown r3-private-key")}
			b := &stopR3Plugin{name: "r3_b", stopErr: errors.New("b-stop r3-private-key"), shutdownErr: errors.New("b-shutdown r3-private-key")}
			if late {
				a.held = gate
			}
			stopR3Install(t, s, a, b)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			first := s.Stop(ctx)
			if late {
				if !errors.Is(first, context.DeadlineExceeded) {
					t.Errorf("late Stop: %v", first)
				}
				gate.open()
			}
			err := stopR3Result(t, s)
			if err == nil {
				t.Error("cleanup errors disappeared")
				return
			}
			for _, message := range []string{"a-stop", "a-shutdown", "b-stop", "b-shutdown", "r3_a", "r3_b"} {
				if !strings.Contains(err.Error(), message) {
					t.Errorf("missing attributed cleanup outcome %q in %v", message, err)
				}
			}
			require.NotContains(t, err.Error(), "r3-private-key")
			require.NotContains(t, err.Error(), "password")
			require.EqualValues(t, 1, a.stops.Load())
			require.EqualValues(t, 1, a.shutdowns.Load())
			require.EqualValues(t, 1, b.stops.Load())
			require.EqualValues(t, 1, b.shutdowns.Load())
		})
	}
}

func TestServerStopR3HookLineage(t *testing.T) {
	for _, kind := range []string{"post", "pre-prompt", "pre-execute", "synthesis"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := stopR2Server(t)
			gate := newStopR2Gate(t)
			p := &stopR3Plugin{name: "r3_lineage", held: gate, followed: make(chan struct{})}
			stopR3Install(t, s, p)
			finished := make(chan struct{})
			post := &heimdall.PostExecuteContext{Context: context.Background(), Action: "outer"}
			switch kind {
			case "post":
				_, httpDone := stopR2HTTP(t, s, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				stopR2Await(t, httpDone)
				// Use the real HTTP work boundary to issue the queued callback, then end it.
				request := httptest.NewRequest(http.MethodGet, "/r3", nil)
				s.workMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					post.Context = r.Context()
					heimdall.CallPostExecuteHooks(post)
				})).ServeHTTP(httptest.NewRecorder(), request)
				close(finished)
			case "pre-prompt":
				go func() {
					defer close(finished)
					c := &heimdall.PromptContext{Context: context.Background()}
					heimdall.CallPrePromptHooks(c)
					if c.UserMessage != "updated" {
						t.Error("pre-prompt mutable state lost")
					}
				}()
			case "pre-execute":
				go func() {
					defer close(finished)
					heimdall.CallPreExecuteHooks(&heimdall.PreExecuteContext{Context: context.Background()})
				}()
			case "synthesis":
				go func() {
					defer close(finished)
					heimdall.CallSynthesisHooks(&heimdall.SynthesisContext{Context: context.Background()})
				}()
			}
			stopR2Await(t, gate.entered)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			_ = s.Stop(ctx)
			gate.open()
			stopR2Await(t, finished)
			select {
			case <-p.followed:
			case <-time.After(300 * time.Millisecond):
				t.Error("accepted callback's nested managed action was dropped during Stop")
			}
			_ = stopR3Result(t, s)
			if kind == "post" {
				require.Eventually(t, func() bool { return len(post.DrainNotifications()) > 0 }, time.Second, time.Millisecond, "post job lost its original notification owner")
			}
		})
	}
}

type stopR3BeginEngine struct {
	storage.Engine
	beginner interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}
	gate *stopR2Gate
	tx   *storage.BadgerTransaction
}

func (e *stopR3BeginEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	tx, err := e.beginner.BeginTransaction()
	e.tx = tx
	return tx, err
}
func (e *stopR3BeginEngine) GetWAL() *storage.WAL {
	if e.tx != nil {
		e.gate.block()
		_ = e.tx.Rollback()
	}
	return nil
}
func TestServerStopR3LateRollbackOutcomes(t *testing.T) {
	t.Run("losing-begin", func(t *testing.T) {
		s, _ := stopR2Server(t)
		gate := newStopR2Gate(t)
		store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		t.Cleanup(func() { _ = store.Close() })
		t.Cleanup(gate.open)
		engine := &stopR3BeginEngine{Engine: store, beginner: store, gate: gate}
		executor := cypher.NewStorageExecutor(engine)
		opened := make(chan error, 1)
		go func() {
			_, err := s.txSessions.OpenWithExecutorForOwner(context.Background(), "nornic", executor, "")
			opened <- err
		}()
		stopR2Await(t, gate.entered)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		_ = s.Stop(ctx)
		gate.open()
		var openErr error
		select {
		case openErr = <-opened:
		case <-time.After(time.Second):
			t.Fatal("losing BEGIN did not return")
		}
		require.Error(t, openErr)
		require.ErrorContains(t, openErr, "transaction manager is closed", "the fixture must finish BEGIN and lose registration, not fail inside BEGIN")
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		for _, err := range []error{s.txSessions.WaitClosed(ctx2), s.WaitStopped(ctx2)} {
			if !errors.Is(err, storage.ErrTransactionClosed) {
				t.Errorf("losing BEGIN cleanup error absent from final outcome: %v (Open returned %v)", err, openErr)
			}
		}
	})
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote-down/explicit=%t", explicit), func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			gate := newStopR2Gate(t)
			var peer *httptest.Server
			peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodDelete {
					gate.block()
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"errors": []map[string]string{{"code": "Neo.DatabaseError.General.UnknownError", "message": "r3 remote endpoint down"}}})
					return
				}
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tx") {
					w.Header().Set("Location", peer.URL+"/db/nornic/tx/remote")
					w.WriteHeader(http.StatusCreated)
				}
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"commit": peer.URL + "/db/nornic/tx/remote/commit", "results": []interface{}{map[string]interface{}{"columns": []string{"k"}, "data": []interface{}{map[string]interface{}{"row": []interface{}{1}}}}}, "errors": []interface{}{}})
			}))
			t.Cleanup(peer.Close)
			t.Cleanup(gate.open)
			require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "r3_remote", []multidb.ConstituentRef{{Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: peer.URL, AuthMode: "oidc_forwarding"}}))
			require.NoError(t, s.txSessions.Close())
			ttl := 150 * time.Millisecond
			if explicit {
				ttl = time.Hour
			}
			s.txSessions = txsession.NewManager(ttl, s.newExecutorForDatabase)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			status, opened := lifecycleTransactionPost(t, s, token, "/db/r3_remote/tx", "USE r3_remote.remote RETURN 1 AS k")
			require.Equal(t, http.StatusCreated, status)
			require.Empty(t, opened.Errors)
			var rollbackDone chan struct{}
			if explicit {
				rollbackDone = make(chan struct{})
				go func() {
					defer close(rollbackDone)
					_ = makeRequest(t, s, http.MethodDelete, strings.TrimSuffix(opened.Commit, "/commit"), nil, token)
				}()
			}
			stopR2Await(t, gate.entered)
			if explicit {
				gate.open()
				stopR2Await(t, rollbackDone)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			_ = s.Stop(ctx)
			gate.open()
			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel2()
			for _, err := range []error{s.txSessions.WaitClosed(ctx2), s.WaitStopped(ctx2)} {
				if err == nil || !strings.Contains(err.Error(), "r3 remote endpoint down") {
					t.Errorf("remote rollback cleanup error absent (explicit=%t): %v", explicit, err)
				}
			}
		})
	}
}

type stopR3Model struct {
	entered, release, canceled chan struct{}
	once                       sync.Once
	active, closes, unsafe     atomic.Int32
}

func (m *stopR3Model) run(ctx context.Context) {
	m.active.Add(1)
	defer m.active.Add(-1)
	m.once.Do(func() { close(m.entered) })
	select {
	case <-ctx.Done():
		if m.canceled != nil {
			select {
			case <-m.canceled:
			default:
				close(m.canceled)
			}
		}
		<-m.release
	case <-m.release:
	}
}
func (m *stopR3Model) Generate(ctx context.Context, _ string, _ heimdall.GenerateParams) (string, error) {
	m.run(ctx)
	return "model response", nil
}
func (m *stopR3Model) GenerateStream(ctx context.Context, p string, g heimdall.GenerateParams, cb func(string) error) error {
	v, e := m.Generate(ctx, p, g)
	if e != nil {
		return e
	}
	return cb(v)
}
func (*stopR3Model) ModelPath() string { return "r3-local.gguf" }
func (m *stopR3Model) Close() error {
	if m.active.Load() != 0 {
		m.unsafe.Add(1)
	}
	m.closes.Add(1)
	return nil
}
func (m *stopR3Model) Score(ctx context.Context, _, _ string) (float32, error) {
	m.run(ctx)
	return 1, nil
}
func TestServerStopR3ConstructedModels(t *testing.T) {
	for _, kind := range []string{"local-inference", "local-inference-env", "local-reranker"} {
		t.Run(kind, func(t *testing.T) {
			model := &stopR3Model{entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{})}
			var once sync.Once
			allow := func() { once.Do(func() { close(model.release) }) }
			defer allow()
			var s *Server
			if strings.HasPrefix(kind, "local-inference") {
				dir := "models"
				if kind == "local-inference-env" {
					dir = t.TempDir()
					t.Setenv("NORNICDB_MODELS_DIR", dir)
				}
				require.NoError(t, os.MkdirAll(dir, 0700))
				modelFile := filepath.Join(dir, "r3-local.gguf")
				require.NoError(t, os.WriteFile(modelFile, []byte("test backend seam"), 0600))
				defer os.Remove(modelFile)
				previous := heimdall.SetGeneratorLoader(func(string, int, int, int, int) (heimdall.Generator, error) { return model, nil })
				defer heimdall.SetGeneratorLoader(previous)
				s, _ = setupTestServerWithConfig(t, func(c *Config) {
					c.Features = &cfgpkg.FeatureFlagsConfig{HeimdallEnabled: true, HeimdallProvider: "local", HeimdallModel: "r3-local"}
				})
				require.Eventually(t, func() bool { return s.db.GetCypherExecutor().GetInferenceManager() != nil }, 3*time.Second, time.Millisecond)
			} else {
				s, _ = setupTestServer(t)
			}
			t.Cleanup(allow)
			useDone := make(chan struct{})
			if strings.HasPrefix(kind, "local-inference") {
				go func() {
					defer close(useDone)
					_, _ = s.db.GetCypherExecutor().Execute(context.Background(), "CALL db.infer({prompt:'r3'}) YIELD text RETURN text", nil)
				}()
			} else {
				reranker := search.NewLocalReranker(model, nil)
				s.db.SetSearchReranker(reranker)
				go func() {
					defer close(useDone)
					_, _ = reranker.Rerank(context.Background(), "r3", []search.RerankCandidate{{}})
				}()
			}
			stopR2Await(t, model.entered)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, s.Stop(ctx))
			require.NoError(t, s.WaitStopped(ctx))
			require.EqualValues(t, 0, model.closes.Load(), "HTTP stop disposed a model shared with non-HTTP consumers")
			closed := make(chan error, 1)
			go func() { closed <- s.db.Close() }()
			select {
			case <-model.canceled:
			case <-time.After(250 * time.Millisecond):
				t.Error("canonical DB owner did not cancel an active model user")
			}
			require.EqualValues(t, 0, model.closes.Load(), "DB disposed a model before actual provider return")
			allow()
			stopR2Await(t, useDone)
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("DB.Close did not join native provider user")
			}
			require.EqualValues(t, 1, model.closes.Load(), "successful constructed model had no exactly-once disposal owner")
			require.EqualValues(t, 0, model.unsafe.Load())
			require.NoError(t, s.db.Close())
			require.EqualValues(t, 1, model.closes.Load())
		})
	}
}

func TestServerStopR3RepeatedStart(t *testing.T) {
	s, _ := stopR2Server(t)
	s.config.Address = "127.0.0.1"
	s.config.Port = 0
	require.NoError(t, s.Start())
	first := s.httpServer
	listener := s.listener
	err := s.Start()
	if err == nil {
		t.Error("repeat Start replaced a live transport without a disposal owner")
		_ = first.Close()
		_ = listener.Close()
	}
	require.Same(t, first, s.httpServer, "repeat Start must leave the running transport owned")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.Stop(ctx))
	require.NoError(t, s.WaitStopped(ctx))
}

type stopR3LogHandler struct{ fn func(slog.Record) }

func (*stopR3LogHandler) Enabled(context.Context, slog.Level) bool        { return true }
func (h *stopR3LogHandler) Handle(_ context.Context, r slog.Record) error { h.fn(r); return nil }
func (h *stopR3LogHandler) WithAttrs([]slog.Attr) slog.Handler            { return h }
func (h *stopR3LogHandler) WithGroup(string) slog.Handler                 { return h }
func TestServerStopR3FailedStart(t *testing.T) {
	s, authenticator := setupTestServer(t)
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := reserve.Addr().(*net.TCPAddr).Port
	require.NoError(t, reserve.Close())
	s.config.Address = "127.0.0.1"
	s.config.Port = port
	s.config.Features = &cfgpkg.FeatureFlagsConfig{QdrantGRPCEnabled: true, QdrantGRPCListenAddr: "bad grpc address"}
	h := heimdall.NewHandler(&heimdall.Manager{}, heimdall.Config{Enabled: true, BifrostEnabled: true}, nil, nil)
	ready := make(chan struct{})
	var prepared sync.Once
	s.log = slog.New(&stopR3LogHandler{fn: func(r slog.Record) {
		if strings.Contains(strings.ToLower(r.Message), "http/2") || strings.Contains(strings.ToLower(r.Message), "http2") {
			prepared.Do(func() {
				// Hold only gRPC's startup entry. The adapter still runs the real Bifrost
				// SSE handler, auth, request BaseContext and server work boundary.
				s.httpServer.Handler = s.workMiddleware(http.HandlerFunc(s.withAuth(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r) }, auth.PermRead)))
				s.mu.Lock()
				close(ready)
			})
		}
	}})
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach HTTP transport publication")
	}
	var unlock sync.Once
	releaseStart := func() { unlock.Do(s.mu.Unlock) }
	defer releaseStart()
	request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/api/bifrost/events", port), nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+getAuthToken(t, authenticator, "admin"))
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	line := make([]byte, 1)
	_, err = io.ReadFull(resp.Body, line)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- s.Stop(ctx) }()
	releaseStart()
	select {
	case err := <-started:
		require.Error(t, err)
	case <-time.After(400 * time.Millisecond):
		t.Error("Start failure waited under its lifecycle lock on the live SSE stream")
	}
	select {
	case <-stopDone:
	case <-time.After(400 * time.Millisecond):
		t.Error("Stop did not respect its budget during failed Start")
	}
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	final := s.WaitStopped(finalCtx)
	if finalCtx.Err() != nil {
		t.Errorf("startup-error shutdown retained a completion cycle: %v", final)
	}
	finalCancel()
	// Red cleanup disconnects the real SSE only after recording the cycle above.
	_ = resp.Body.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	_ = s.WaitStopped(ctx2)
}
