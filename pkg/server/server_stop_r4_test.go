package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/audit"
	cfgpkg "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localllm"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goplugin "plugin"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type r4ResponseGate struct {
	*httptest.ResponseRecorder
	gate *stopR2Gate
}

func (w *r4ResponseGate) Write(p []byte) (int, error) {
	w.gate.block()
	return w.ResponseRecorder.Write(p)
}

type r4LogGate struct {
	gate   *stopR2Gate
	after  int32
	writes atomic.Int32
}

func (w *r4LogGate) Write(p []byte) (int, error) {
	if w.writes.Add(1) == w.after {
		w.gate.block()
	}
	return len(p), nil
}

func TestServerStopR4OpeningLease(t *testing.T) {
	s, a := setupTestServer(t)
	s.txSessions = txsession.NewManager(120*time.Millisecond, s.newExecutorForDatabase)
	gate := newStopR2Gate(t)
	writer := &r4ResponseGate{httptest.NewRecorder(), gate}
	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx", strings.NewReader(`{"statements":[{"statement":"CREATE (:R4Opening)"}]}`))
	req.Header.Set("Authorization", "Bearer "+getAuthToken(t, a, "admin"))
	done := make(chan struct{})
	go func() { defer close(done); s.buildRouter().ServeHTTP(writer, req) }()
	stopR2Await(t, gate.entered)
	time.Sleep(300 * time.Millisecond)
	// The response body is held. Inspect its already-published Location and the
	// manager through the same owner identity the HTTP transport uses.
	path := writer.Header().Get("Location")
	id := path[strings.LastIndex(path, "/")+1:]
	owner := lifecycleTransactionOwner(t, s, req.Header.Get("Authorization"))
	if _, ok := s.txSessions.GetForOwner(id, owner); !ok {
		t.Error("opening request expired before its response completed")
	}
	gate.open()
	stopR2Await(t, done)
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &opened))
	require.Empty(t, opened.Errors)
	// Idle begins at publication, rather than consuming TTL while writing.
	response := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []any{}}, req.Header.Get("Authorization"))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

func TestServerStopR4OneShotLease(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			s, _ := stopR2Server(t)
			a := s.auth
			s.txSessions = txsession.NewManager(100*time.Millisecond, s.newExecutorForDatabase)
			gate := newStopR2Gate(t)
			logGate := &r4LogGate{gate: gate, after: 1}
			if fallback {
				logGate.after = 2
			}
			s.slowQueryLogger = log.New(logGate, "", 0)
			queries := []string{"CREATE (:R4OneShot)", "RETURN 1"}
			if fallback {
				queries = []string{"CALL { RETURN 1 AS x } IN TRANSACTIONS RETURN x", "CREATE (:R4OneShot)", "RETURN 1"}
			}
			statements := make([]map[string]any, 0, len(queries))
			for _, q := range queries {
				statements = append(statements, map[string]any{"statement": q})
			}
			body, _ := json.Marshal(map[string]any{"statements": statements})
			req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(string(body)))
			req.Header.Set("Authorization", "Bearer "+getAuthToken(t, a, "admin"))
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); s.buildRouter().ServeHTTP(response, req) }()
			stopR2Await(t, gate.entered)
			time.Sleep(280 * time.Millisecond)
			gate.open()
			stopR2Await(t, done)
			var result TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Empty(t, result.Errors, response.Body.String())
			check, err := s.db.GetCypherExecutor().Execute(context.Background(), "MATCH (n:R4OneShot) RETURN count(n)", nil)
			require.NoError(t, err)
			require.EqualValues(t, 1, check.Rows[0][0])
		})
	}
}

type r4AsyncPlugin struct {
	stopR3Plugin
	callbackDone chan struct{}
	lateErr      error
	synthesis    bool
}

func (p *r4AsyncPlugin) PreExecute(c *heimdall.PreExecuteContext, done func(heimdall.PreExecuteResult)) {
	go func() {
		p.held.block()
		p.invoker.InvokeActionAsync(c.Context, "heimdall_"+p.name+"_follow", nil)
		c.NotifyInfo("r4", "late")
		done(heimdall.PreExecuteResult{Continue: true, Error: p.lateErr})
		close(p.callbackDone)
	}()
}
func (p *r4AsyncPlugin) Synthesize(c *heimdall.SynthesisContext, done func(string)) {
	go func() {
		p.held.block()
		p.invoker.InvokeActionAsync(c.Context, "heimdall_"+p.name+"_follow", nil)
		done("late result")
		close(p.callbackDone)
	}()
}
func TestServerStopR4AsyncDone(t *testing.T) {
	for _, synthesis := range []bool{false, true} {
		t.Run(fmt.Sprint(synthesis), func(t *testing.T) {
			s, _ := stopR2Server(t)
			held := newStopR2Gate(t)
			m := heimdall.GetSubsystemManager()
			stopR3Install(t, s)
			m = heimdall.GetSubsystemManager()
			p := &r4AsyncPlugin{stopR3Plugin: stopR3Plugin{name: "r4_async", held: held, followed: make(chan struct{})}, callbackDone: make(chan struct{}), lateErr: errors.New("r4 late pre-execute error"), synthesis: synthesis}
			require.NoError(t, m.RegisterPlugin(p, "", true))
			returned := make(chan struct{})
			go func() {
				defer close(returned)
				if synthesis {
					heimdall.CallSynthesisHooks(&heimdall.SynthesisContext{Context: context.Background()})
				} else {
					heimdall.CallPreExecuteHooks(&heimdall.PreExecuteContext{Context: context.Background()})
				}
			}()
			stopR2Await(t, held.entered)
			select {
			case <-returned:
			case <-time.After(6 * time.Second):
				held.open()
				t.Fatal("response waiter did not keep its five-second timeout")
			}
			stopR2Bounded(t, s)
			if p.shutdowns.Load() != 0 {
				t.Error("async callback disposed at wrapper timeout")
			}
			stopR2Live(t, s)
			held.open()
			stopR2Await(t, p.callbackDone)
			select {
			case <-p.followed:
			case <-time.After(time.Second):
				t.Error("retained done callback lost its nested action lineage")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := s.WaitStopped(ctx)
			if synthesis {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, p.lateErr)
			}
			require.EqualValues(t, 1, p.shutdowns.Load())
		})
	}
}

type r4ErrorListener struct {
	net.Listener
	failure error
	closes  atomic.Int32
}

func (l *r4ErrorListener) Close() error { l.closes.Add(1); return l.failure }
func TestServerStopR4ForcedCloseOutcome(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	s, _ := stopR2Server(t)
	base, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer base.Close()
	closeErr := errors.New("r4 listener close failure")
	listener := &r4ErrorListener{Listener: base, failure: closeErr}
	gate := newStopR2Gate(t)
	server := &http.Server{Handler: s.workMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gate.block() }))}
	s.lifecycleMu.Lock()
	s.httpServer = server
	s.listener = listener
	s.lifecycleMu.Unlock()
	require.True(t, s.runBackground(context.Background(), func(context.Context) {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.shutdown.work.RecordError("", err)
		}
	}))
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, e := http.Get("http://" + base.Addr().String())
		if e == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	stopR2Await(t, gate.entered)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	require.ErrorIs(t, s.Stop(ctx), context.DeadlineExceeded)
	// Close intentionally reports failure without closing the listener. The test
	// releases the real OS listener separately, after the forced owner ran.
	require.Eventually(t, func() bool { return listener.closes.Load() > 0 }, time.Second, time.Millisecond)
	base.Close()
	gate.open()
	stopR2Await(t, requestDone)
	final, cancelFinal := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFinal()
	outcome := s.WaitStopped(final)
	require.ErrorIs(t, outcome, context.DeadlineExceeded)
	require.ErrorIs(t, outcome, closeErr, "forced result was lost before eventual service observation")
}

func TestServerStopR4StartLoggerReentry(t *testing.T) {
	s, _ := stopR2Server(t)
	s.config.Address = "127.0.0.1"
	s.config.Port = 0
	callback := newStopR2Gate(t)
	read := make(chan struct{})
	var once sync.Once
	s.log = slogForR4(func() {
		once.Do(func() {
			go func() { _ = s.Addr(); close(read) }()
			select {
			case <-read:
			case <-callback.release:
			}
		})
	})
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	select {
	case <-read:
	case <-time.After(300 * time.Millisecond):
		t.Error("startup called logger with lifecycle lock held")
	}
	callback.open()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("startup did not complete")
	}
	require.NoError(t, s.Stop(context.Background()))
}

func slogForR4(fn func()) *slog.Logger {
	return slog.New(&stopR3LogHandler{fn: func(slog.Record) { fn() }})
}

func TestServerStopR4ConstructorRetention(t *testing.T) {
	for _, health := range []bool{false, true} {
		t.Run(fmt.Sprint(health), func(t *testing.T) {
			dbCfg := nornicdb.DefaultConfig()
			dbCfg.Memory.DecayEnabled = false
			dbCfg.Memory.AutoLinksEnabled = false
			dbCfg.Database.AsyncWritesEnabled = false
			db, err := nornicdb.Open(t.TempDir(), dbCfg)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			load := newStopR2Gate(t)
			construction := newStopR2Gate(t)
			model := &stopR3Model{entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{})}
			var modelOnce sync.Once
			releaseModel := func() { modelOnce.Do(func() { close(model.release) }) }
			defer releaseModel()
			previous := localllm.SetRerankerLoader(func(localllm.Options) (localllm.RerankerBackend, error) { load.block(); return model, nil })
			defer localllm.SetRerankerLoader(previous)
			cfg := DefaultConfig()
			cfg.EmbeddingEnabled = false
			cfg.MCPEnabled = false
			cfg.SlowQueryEnabled = true
			cfg.ModelsDir = t.TempDir()
			cfg.Features = &cfgpkg.FeatureFlagsConfig{SearchRerankEnabled: true, SearchRerankProvider: "local"}
			cfg.Logger = slog.New(&stopR3LogHandler{fn: func(r slog.Record) {
				if strings.Contains(strings.ToLower(r.Message), "slow query") {
					construction.block()
				}
			}})
			type result struct {
				s   *Server
				err error
			}
			constructed := make(chan result, 1)
			go func() { s, e := New(db, nil, cfg); constructed <- result{s, e} }()
			stopR2Await(t, load.entered)
			stopR2Await(t, construction.entered)
			if health {
				load.open()
				stopR2Await(t, model.entered)
			}
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			select {
			case e := <-closed:
				t.Errorf("DB.Close completed across active constructor/health: %v", e)
				closed = nil
			case <-time.After(150 * time.Millisecond):
			}
			if health {
				require.EqualValues(t, 0, model.closes.Load(), "raw health model disposed while Score was active")
			}
			load.open()
			releaseModel()
			// Construction itself remains a retained user even after its model returns.
			if closed != nil {
				select {
				case e := <-closed:
					t.Errorf("DB.Close omitted New's publication phase: %v", e)
					closed = nil
				case <-time.After(80 * time.Millisecond):
				}
			}
			construction.open()
			select {
			case r := <-constructed:
				if r.s != nil {
					r.s.Stop(context.Background())
				}
				require.ErrorIs(t, r.err, nornicdb.ErrClosed)
			case <-time.After(3 * time.Second):
				t.Fatal("New did not complete common teardown")
			}
			if closed != nil {
				select {
				case e := <-closed:
					require.NoError(t, e)
				case <-time.After(3 * time.Second):
					t.Fatal("DB.Close did not observe actual construction completion")
				}
			}
			require.EqualValues(t, 1, model.closes.Load())
			require.EqualValues(t, 0, model.unsafe.Load())
		})
	}
}

type r4NativeControl interface {
	Entered() <-chan struct{}
	ReadDone() <-chan struct{}
	Release()
}

func TestServerStopR4NativeLoader(t *testing.T) {
	if !pluginTestCGOBuild || runtime.GOOS != "linux" {
		t.Skip("requires Linux CGO plugin loader")
	}
	for _, mode := range []string{"initialize-reentry", "start-reentry", "blocked-initialize", "blocked-start", "failed-initialize"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			source := fmt.Sprintf(`package main
import("context";"fmt";"sync";"github.com/orneryd/nornicdb/pkg/heimdall";"github.com/orneryd/nornicdb/pkg/nornicdb";watcher "github.com/orneryd/nornicdb/plugins/heimdall")
const mode=%q
const identity=%q
type probe struct {watcher.WatcherPlugin; entered,read,release,stop chan struct{}; enterOnce,readOnce,releaseOnce,stopOnce sync.Once}
func (*probe) Name()string{return "r4_native"}
func(p *probe) Entered()<-chan struct{}{return p.entered}
func(p *probe) ReadDone()<-chan struct{}{return p.read}
func(p *probe) Release(){p.releaseOnce.Do(func(){close(p.release)})}
func(p *probe) reader(){done:=make(chan struct{});go func(){nornicdb.ListLoadedPlugins();nornicdb.ListPluginFunctions();nornicdb.PluginsInitialized();close(done)}();select{case <-done:p.readOnce.Do(func(){close(p.read)});case <-p.release:}}
func(p *probe) Initialize(c heimdall.SubsystemContext)error{if mode!="start-reentry"&&mode!="blocked-start"{p.enterOnce.Do(func(){close(p.entered)})};if mode=="initialize-reentry"||mode=="failed-initialize"{p.reader()};if mode=="blocked-initialize"{select{case <-p.stop:case <-p.release:}};if mode=="failed-initialize"{return fmt.Errorf("r4 initialize failure")};return p.WatcherPlugin.Initialize(c)}
func(p *probe) Start()error{if mode=="start-reentry"||mode=="blocked-start"{p.enterOnce.Do(func(){close(p.entered)})};if mode=="start-reentry"{p.reader()};if mode=="blocked-start"{select{case <-p.stop:case <-p.release:}};return nil}
func(p *probe) Stop()error{if mode=="blocked-start"{p.reader()};p.stopOnce.Do(func(){close(p.stop)});return nil}
func(p *probe) Shutdown()error{p.reader();_ = context.Background();return nil}
var Plugin heimdall.HeimdallPlugin=&probe{entered:make(chan struct{}),read:make(chan struct{}),release:make(chan struct{}),stop:make(chan struct{})}
`, mode, filepath.Base(dir))
			sourcePath := filepath.Join(dir, "probe.go")
			so := filepath.Join(dir, "probe.so")
			require.NoError(t, os.WriteFile(sourcePath, []byte(source), 0600))
			args := []string{"build", "-buildmode=plugin", "-tags=noui,nolocalllm", "-o", so}
			if pluginTestRaceBuild {
				args = append(args, "-race")
			}
			args = append(args, sourcePath)
			buildCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(buildCtx, "go", args...)
			cmd.Env = append(os.Environ(), "GOFLAGS=", "GOPROXY=off")
			out, e := cmd.CombinedOutput()
			require.NoError(t, e, "%s", out)
			heimdall.ResetSubsystemManagerForTests()
			t.Cleanup(heimdall.ResetSubsystemManagerForTests)
			s, _ := setupTestServerWithConfig(t, func(c *Config) {
				c.HeimdallPluginsDir = dir
				c.PluginsDir = ""
				c.Features = &cfgpkg.FeatureFlagsConfig{HeimdallEnabled: true, HeimdallProvider: "openai", HeimdallAPIURL: "http://example.invalid", HeimdallAPIKey: "test"}
			})
			loaded, e := goplugin.Open(so)
			require.NoError(t, e)
			symbol, e := loaded.Lookup("Plugin")
			require.NoError(t, e)
			plugin := *(symbol.(*heimdall.HeimdallPlugin))
			control := plugin.(r4NativeControl)
			defer control.Release()
			stopR2Await(t, control.Entered())
			if mode == "initialize-reentry" || mode == "start-reentry" || mode == "failed-initialize" {
				select {
				case <-control.ReadDone():
				case <-time.After(300 * time.Millisecond):
					t.Error("native lifecycle reentry blocked on loader registry mutex")
					control.Release()
				}
			}
			ctx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer stopCancel()
			_ = s.Stop(ctx)
			wait, waitCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer waitCancel()
			outcome := s.WaitStopped(wait)
			if wait.Err() != nil {
				t.Error("constructed native component was joined before its stop signal or loader reader completed")
				control.Release()
				s.WaitStopped(context.Background())
			}
			if mode == "failed-initialize" {
				require.ErrorContains(t, outcome, "r4 initialize failure")
			}
		})
	}
}

type r4Bridge struct {
	heimdall.NoOpBifrost
	message, broadcast       *stopR2Gate
	messageErr, broadcastErr error
}

func (b *r4Bridge) SendMessage(string) error { b.message.block(); return b.messageErr }
func (b *r4Bridge) Broadcast(string) error   { b.broadcast.block(); return b.broadcastErr }
func TestServerStopR4PromptNotificationCompletion(t *testing.T) {
	s, _ := stopR2Server(t)
	stopR3Install(t, s)
	bridge := &r4Bridge{message: newStopR2Gate(t), broadcast: newStopR2Gate(t), messageErr: errors.New("r4 message failed"), broadcastErr: errors.New("r4 broadcast failed")}
	prompt := &heimdall.PromptContext{Context: context.Background()}
	prompt.SetBifrost(bridge)
	prompt.SendMessage("hello")
	prompt.Broadcast("all")
	stopR2Await(t, bridge.message.entered)
	stopR2Await(t, bridge.broadcast.entered)
	stopR2Bounded(t, s)
	stopR2Live(t, s)
	bridge.message.open()
	bridge.broadcast.open()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.WaitStopped(ctx)
	require.ErrorIs(t, err, bridge.messageErr)
	require.ErrorIs(t, err, bridge.broadcastErr)
}

type r4CloseReentryModel struct {
	stopR3Model
	manager               *heimdall.Manager
	entered, read, escape chan struct{}
}

func (m *r4CloseReentryModel) Close() error {
	close(m.entered)
	go func() { m.manager.Stats(); close(m.read) }()
	select {
	case <-m.read:
	case <-m.escape:
	}
	return nil
}
func TestServerStopR4ProviderCloseReentry(t *testing.T) {
	s, _ := stopR2Server(t)
	model := &r4CloseReentryModel{entered: make(chan struct{}), read: make(chan struct{}), escape: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(model.escape) }) }
	defer release()
	heimdall.RegisterHeimdallProvider("r4-close-reentry", func(heimdall.Config) (heimdall.Generator, error) { return model, nil })
	manager, err := heimdall.NewManager(heimdall.Config{Enabled: true, Provider: "r4-close-reentry"})
	require.NoError(t, err)
	model.manager = manager
	require.True(t, s.db.SetInferenceManager(manager))
	require.NoError(t, s.Stop(context.Background()))
	done := make(chan error, 1)
	go func() { done <- s.db.Close() }()
	stopR2Await(t, model.entered)
	select {
	case <-model.read:
	case <-time.After(200 * time.Millisecond):
		t.Error("provider disposal held host manager lock across foreign Close")
	}
	release()
	select {
	case e := <-done:
		require.NoError(t, e)
	case <-time.After(time.Second):
		t.Fatal("DB disposal did not complete")
	}
}

type r4FailedWriter struct{ err error }

func (w r4FailedWriter) Write([]byte) (int, error) { return 0, w.err }

func TestServerStopR4LogOutcomes(t *testing.T) {
	for _, kind := range []string{"slow-query", "audit", "terminal-observer"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := stopR2Server(t)
			failure := errors.New("r4 " + kind + " sink failed")
			if kind == "slow-query" {
				s.slowQueryLogger = log.New(r4FailedWriter{failure}, "", 0)
			} else {
				s.SetAuditLogger(audit.NewLoggerWithWriter(r4FailedWriter{failure}, audit.DefaultConfig()))
			}
			token := "Bearer " + getAuthToken(t, s.auth, "admin")
			switch kind {
			case "slow-query":
				lifecycleTransactionPost(t, s, token, "/db/nornic/tx/commit", "RETURN 1")
			case "audit":
				response := makeRequest(t, s, http.MethodPost, "/nornicdb/embed/trigger", map[string]any{"content": "audit"}, "Bearer "+getAuthToken(t, s.auth, "reader"))
				require.Equal(t, http.StatusForbidden, response.Code)
			case "terminal-observer":
				cypher.ClearUserProcedures()
				t.Cleanup(cypher.ClearUserProcedures)
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "r4.terminal", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
					return nil, storage.ErrMVCCSnapshotHardExpired
				}))
				status, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx")
				require.Equal(t, http.StatusCreated, status)
				require.Empty(t, opened.Errors)
				path := strings.TrimSuffix(opened.Commit, "/commit")
				lifecycleTransactionPost(t, s, token, path, "CALL r4.terminal()")
			}
			require.ErrorIs(t, s.Stop(context.Background()), failure)
			require.ErrorIs(t, s.WaitStopped(context.Background()), failure)
		})
	}
}
func TestServerStopR4RetentionAuditOutcome(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Compliance.RetentionEnabled = true
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()
	serverCfg := DefaultConfig()
	serverCfg.EmbeddingEnabled = false
	s, err := New(db, nil, serverCfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())
	failure := errors.New("r4 retention audit failed")
	s.SetAuditLogger(audit.NewLoggerWithWriter(r4FailedWriter{failure}, audit.DefaultConfig()))
	node, err := db.CreateNode(context.Background(), []string{"Memory"}, map[string]interface{}{"content": "expired"})
	require.NoError(t, err)
	rm := db.GetRetentionManager()
	require.NotNil(t, rm)
	for _, policy := range rm.ListPolicies() {
		rm.DeletePolicy(policy.ID)
	}
	require.NoError(t, rm.AddPolicy(&retention.Policy{ID: "r4", Name: "r4", Category: retention.CategoryUser, Active: true, RetentionPeriod: retention.RetentionPeriod{Duration: time.Second}}))
	result := rm.ProcessRecord(context.Background(), &retention.DataRecord{ID: node.ID, Category: retention.CategoryUser, CreatedAt: time.Now().Add(-time.Hour)})
	require.ErrorContains(t, result, "r4 retention audit failed")
	require.NoError(t, s.Stop(context.Background()))
	require.ErrorIs(t, db.Close(), failure)
}

func TestServerStopR4TransactionCallbackReentry(t *testing.T) {
	s, _ := stopR2Server(t)
	token := "Bearer " + getAuthToken(t, s.auth, "admin")
	_, opened := lifecycleTransactionPost(t, s, token, "/db/nornic/tx")
	require.Empty(t, opened.Errors)
	path := strings.TrimSuffix(opened.Commit, "/commit")
	id := path[strings.LastIndex(path, "/")+1:]
	session, ok := s.txSessions.GetForOwner(id, lifecycleTransactionOwner(t, s, token))
	require.True(t, ok)
	cypher.ClearUserProcedures()
	t.Cleanup(cypher.ClearUserProcedures)
	gate := newStopR2Gate(t)
	read := make(chan struct{})
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "r4.reentry", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		gate.enter.Do(func() { close(gate.entered) })
		go func() { session.ExpiresAt(); close(read) }()
		select {
		case <-read:
		case <-gate.release:
		}
		return &cypher.ExecuteResult{Columns: []string{"ok"}, Rows: [][]interface{}{{true}}}, nil
	}))
	done := make(chan struct{})
	go func() { defer close(done); lifecycleTransactionPost(t, s, token, path, "CALL r4.reentry()") }()
	stopR2Await(t, gate.entered)
	select {
	case <-read:
	case <-time.After(200 * time.Millisecond):
		t.Error("session state mutex held across a public Cypher extension callback")
	}
	gate.open()
	stopR2Await(t, done)
}

type r4FailedReranker struct {
	scoreErr, closeErr error
	closes             atomic.Int32
	scoreEntered       func()
}

func (m *r4FailedReranker) Score(context.Context, string, string) (float32, error) {
	if m.scoreEntered != nil {
		m.scoreEntered()
	}
	return 0, m.scoreErr
}
func (m *r4FailedReranker) Close() error { m.closes.Add(1); return m.closeErr }
func TestServerStopR4ConstructionOutcomes(t *testing.T) {
	for _, kind := range []string{"local-load", "local-health", "heimdall-load", "native-load"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("r4 " + kind + " failure")
			entered := make(chan struct{})
			var once sync.Once
			signal := func() { once.Do(func() { close(entered) }) }
			model := &r4FailedReranker{scoreErr: failure, closeErr: errors.New("r4 rejected model close failure"), scoreEntered: signal}
			previous := localllm.SetRerankerLoader(func(localllm.Options) (localllm.RerankerBackend, error) {
				if kind == "local-load" {
					signal()
				}
				if kind == "local-load" {
					return nil, failure
				}
				return model, nil
			})
			defer localllm.SetRerankerLoader(previous)
			heimdall.RegisterHeimdallProvider("r4-constructor-outcome", func(heimdall.Config) (heimdall.Generator, error) { signal(); return nil, failure })
			s, _ := setupTestServerWithConfig(t, func(c *Config) {
				c.PluginsDir = ""
				c.HeimdallPluginsDir = ""
				c.ModelsDir = t.TempDir()
				c.Features = &cfgpkg.FeatureFlagsConfig{}
				switch kind {
				case "local-load", "local-health":
					c.Features.SearchRerankEnabled = true
					c.Features.SearchRerankProvider = "local"
				case "heimdall-load":
					c.Features.HeimdallEnabled = true
					c.Features.HeimdallProvider = "r4-constructor-outcome"
				case "native-load":
					c.HeimdallPluginsDir = t.TempDir()
					require.NoError(t, os.WriteFile(filepath.Join(c.HeimdallPluginsDir, "broken.so"), []byte("invalid native module"), 0600))
					signal()
				}
			})
			stopR2Await(t, entered)
			err := s.Stop(context.Background())
			if kind == "native-load" {
				require.ErrorContains(t, err, "broken.so")
			} else {
				require.ErrorIs(t, err, failure)
			}
			if kind == "local-health" {
				require.ErrorIs(t, err, model.closeErr)
				require.EqualValues(t, 1, model.closes.Load())
				require.ErrorIs(t, s.db.Close(), model.closeErr)
			}
		})
	}
}
