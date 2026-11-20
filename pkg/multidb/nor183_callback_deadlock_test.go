package multidb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-183 reports DatabaseManager holding its metadata lock across a callback
// into its own public catalog: a remote-engine factory (and, historically, an
// inner engine's Close) calling ListDatabaseNames blocks forever. These tests
// drive only the public DatabaseManager path, and each callback runs in an
// isolated child process: a regression blocks the calling goroutine
// permanently, so an in-process test would hang the package run instead of
// failing. The child watchdog prints a goroutine dump and exits with
// nor183DeadlockExit, which the parent reports as the reproduction.
const (
	nor183ScenarioEnv      = "NOR183_DATABASE_MANAGER_SCENARIO"
	nor183ChildTestPattern = "^TestNOR183DatabaseManagerCallbackChild$"

	nor183ScenarioFactorySuccess    = "remote-factory-success"
	nor183ScenarioFactoryFailure    = "remote-factory-failure"
	nor183ScenarioInnerClose        = "inner-engine-close"
	nor183ScenarioInnerCloseFailure = "inner-engine-close-failure"
	nor183ScenarioRootSuccess       = "composite-root-success"
	nor183ScenarioRootFailure       = "composite-root-failure"

	nor183CompletedMarker = "NOR183_SCENARIO_COMPLETED"
	nor183DeadlockMarker  = "NOR183_SCENARIO_DEADLOCKED"

	nor183DeadlockExit  = 86
	nor183CallBudget    = 10 * time.Second
	nor183ProcessBudget = 90 * time.Second
)

// TestNOR183RemoteEngineFactoryCatalogCallback covers a remote-engine factory
// that reads the manager catalog while the manager creates the constituent
// engine. A factory that succeeds and one that rejects its constituent must
// both return, and cleanup afterwards must complete.
func TestNOR183RemoteEngineFactoryCatalogCallback(t *testing.T) {
	for _, scenario := range []string{nor183ScenarioFactorySuccess, nor183ScenarioFactoryFailure, nor183ScenarioRootSuccess, nor183ScenarioRootFailure} {
		t.Run(scenario, func(t *testing.T) {
			output := nor183RunCallbackScenario(t, scenario)
			require.Contains(t, output, nor183CompletedMarker+" scenario="+scenario,
				"NOR-183 child must reach the scenario completion marker: %s", output)
		})
	}
}

// TestNOR183InnerEngineCloseCatalogCallback covers an inner engine whose Close
// reads the manager catalog and then finishes closing.
func TestNOR183InnerEngineCloseCatalogCallback(t *testing.T) {
	for _, scenario := range []string{nor183ScenarioInnerClose, nor183ScenarioInnerCloseFailure} {
		t.Run(scenario, func(t *testing.T) {
			output := nor183RunCallbackScenario(t, scenario)
			require.Contains(t, output, nor183CompletedMarker+" scenario="+scenario)
		})
	}
}

func nor183RunCallbackScenario(t *testing.T, scenario string) string {
	t.Helper()
	binaryPath, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), nor183ProcessBudget)
	defer cancel()
	child := exec.CommandContext(ctx, binaryPath,
		"-test.run="+nor183ChildTestPattern, "-test.count=1", "-test.timeout=60s")
	child.Env = append(os.Environ(), nor183ScenarioEnv+"="+scenario)
	output, runErr := child.CombinedOutput()
	require.NoError(t, ctx.Err(), "NOR-183 child %q did not exit within %s; output:\n%s",
		scenario, nor183ProcessBudget, output)
	var exit *exec.ExitError
	if errors.As(runErr, &exit) && exit.ExitCode() == nor183DeadlockExit {
		t.Fatalf("NOR-183 reproduced: %q blocked while DatabaseManager held its metadata lock across a callback into ListDatabaseNames\n%s",
			scenario, output)
	}
	require.NoError(t, runErr, "NOR-183 child %q failed: %s", scenario, output)
	require.NotContains(t, string(output), nor183DeadlockMarker,
		"NOR-183 child %q reported a deadlock: %s", scenario, output)
	return string(output)
}

// TestNOR183DatabaseManagerCallbackChild runs exactly one NOR-183 scenario in
// the isolated process started by nor183RunCallbackScenario. It skips during
// ordinary package runs because the parent supplies the scenario.
func TestNOR183DatabaseManagerCallbackChild(t *testing.T) {
	scenario := os.Getenv(nor183ScenarioEnv)
	if scenario == "" {
		t.Skip("NOR-183 child scenario requires " + nor183ScenarioEnv)
	}
	prepared, err := nor183PrepareScenario(t, scenario)
	require.NoError(t, err)

	outcome := make(chan nor183Outcome, 1)
	go func() { outcome <- prepared.trigger() }()

	select {
	case got := <-outcome:
		prepared.verify(t, got)
		fmt.Printf("%s scenario=%s names=%s\n", nor183CompletedMarker, scenario, strings.Join(got.names, ","))
	case <-time.After(nor183CallBudget):
		// The scenario goroutine is blocked inside the manager's own lock, so no
		// progress is possible in this process. Terminate with the dump the parent
		// reports rather than abandoning a goroutine in a half-run test.
		fmt.Printf("%s scenario=%s\n%s", nor183DeadlockMarker, scenario, nor183GoroutineDump())
		os.Exit(nor183DeadlockExit)
	}
}

// nor183Outcome records the scenario's public results. Callback-provided values
// are copied out of the scenario goroutine before it sends.
type nor183Outcome struct {
	names          []string
	callErr        error
	engineReadErr  error
	engineCloseErr error
	closeErr       error
}

type nor183Scenario struct {
	trigger func() nor183Outcome
	verify  func(*testing.T, nor183Outcome)
}

func nor183PrepareScenario(t *testing.T, scenario string) (*nor183Scenario, error) {
	t.Helper()
	switch scenario {
	case "concurrent-open", "close-open-read":
		return nor183PrepareConcurrent(t, scenario)
	case nor183ScenarioFactorySuccess:
		return nor183PrepareRemoteFactory(t, false, false)
	case nor183ScenarioFactoryFailure:
		return nor183PrepareRemoteFactory(t, true, false)
	case nor183ScenarioInnerClose:
		return nor183PrepareInnerClose(t, false)
	case nor183ScenarioInnerCloseFailure:
		return nor183PrepareInnerClose(t, true)
	case nor183ScenarioRootSuccess:
		return nor183PrepareRemoteFactory(t, false, true)
	case nor183ScenarioRootFailure:
		return nor183PrepareRemoteFactory(t, true, true)
	case nor183ScenarioCompositeTwoRemoteSuccess:
		return nor183PrepareCompositeTwoRemote(t)
	}
	return nil, fmt.Errorf("unknown NOR-183 scenario %q", scenario)
}

// nor183PrepareRemoteFactory wires a composite with one remote constituent. The
// factory callback reads the public catalog while the manager resolves the
// constituent engine, which is the reported callback shape.
func nor183PrepareRemoteFactory(t *testing.T, reject, root bool) (*nor183Scenario, error) {
	t.Helper()
	ctx := t.Context()
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	remote := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)

	rejected := errors.New("nor183 remote factory rejected the constituent")
	var manager *DatabaseManager
	var callbackNames []string
	var callbackCalls atomic.Int32
	manager, err := NewDatabaseManager(ctx, inner, &Config{
		RemoteEngineFactory: func(ConstituentRef, string) (storage.Engine, error) {
			callbackCalls.Add(1)
			callbackNames = manager.ListDatabaseNames()
			if reject {
				// A rejected engine never reaches the manager, so this callback
				// keeps ownership of it; close it so the tracked fixture's
				// cleanup audit observes a closed engine.
				_ = remote.Close()
				return nil, rejected
			}
			return remote, nil
		},
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = manager.Close() })
	if err := manager.CreateCompositeDatabase(ctx, "nor183_cmp", []ConstituentRef{{
		Alias:        "r",
		DatabaseName: "nor183_remote",
		Type:         "remote",
		AccessMode:   "read_write",
		URI:          "http://nor183.invalid",
	}}); err != nil {
		return nil, err
	}

	return &nor183Scenario{
		trigger: func() nor183Outcome {
			outcome := nor183Outcome{}
			selection := "nor183_cmp.r"
			if root {
				selection = "nor183_cmp"
			}
			engine, err := manager.GetStorageWithAuth(selection, "")
			outcome.callErr = err
			outcome.names = slices.Clone(callbackNames)
			if err == nil && engine != nil {
				_, outcome.engineReadErr = engine.NodeCount(ctx)
				outcome.engineCloseErr = engine.Close()
			}
			outcome.closeErr = manager.Close()
			return outcome
		},
		verify: func(t *testing.T, got nor183Outcome) {
			require.EqualValues(t, 1, callbackCalls.Load(), "the remote factory must run exactly once")
			require.ElementsMatch(t, []string{"system", "nornic", "nor183_cmp"}, got.names,
				"the factory's catalog read must complete inside the manager call")
			require.NoError(t, got.closeErr, "cleanup after the factory must complete")
			if reject {
				require.Error(t, got.callErr, "a rejected factory must surface its error")
				require.Contains(t, got.callErr.Error(), rejected.Error())
				return
			}
			require.NoError(t, got.callErr)
			require.NoError(t, got.engineReadErr, "the factory engine must be usable")
			require.NoError(t, got.engineCloseErr, "the factory engine must close")
		},
	}, nil
}

// nor183CatalogReadingEngine is an inner engine whose Close reads the manager
// catalog before delegating, the callback shape NOR-183 reports for cleanup.
type nor183CatalogReadingEngine struct {
	storage.Engine
	notify           func()
	callbackCalls    atomic.Int32
	closed           atomic.Bool
	closeOnce        sync.Once
	closeErr         error
	injectedCloseErr error
}

func (e *nor183CatalogReadingEngine) GetInnerEngine() storage.Engine { return e.Engine }

func (e *nor183CatalogReadingEngine) Close() error {
	e.closeOnce.Do(func() {
		e.callbackCalls.Add(1)
		if e.notify != nil {
			e.notify()
		}
		e.closed.Store(true)
		e.closeErr = errors.Join(e.Engine.Close(), e.injectedCloseErr)
	})
	return e.closeErr
}

func nor183PrepareInnerClose(t *testing.T, reject bool) (*nor183Scenario, error) {
	t.Helper()
	inner := fixtureowner.Track(t, &nor183CatalogReadingEngine{Engine: fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)}, storage.ErrStorageClosed)
	var manager *DatabaseManager
	var callbackNames []string
	rejected := errors.New("nor183 inner close failed")
	if reject {
		inner.injectedCloseErr = rejected
	}
	inner.notify = func() { callbackNames = manager.ListDatabaseNames() }
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		if !inner.closed.Load() {
			_ = manager.Close()
		}
	})

	return &nor183Scenario{
		trigger: func() nor183Outcome {
			closeErr := manager.Close()
			return nor183Outcome{names: slices.Clone(callbackNames), closeErr: closeErr}
		},
		verify: func(t *testing.T, got nor183Outcome) {
			if reject {
				require.ErrorIs(t, got.closeErr, rejected)
			} else {
				require.NoError(t, got.closeErr, "DatabaseManager.Close must complete")
			}
			require.True(t, inner.closed.Load(), "the inner engine must close")
			require.EqualValues(t, 1, inner.callbackCalls.Load(), "the inner engine's Close callback must run once")
			require.ElementsMatch(t, []string{"system", "nornic"}, got.names,
				"the close callback's catalog read must complete before the engine closes")
		},
	}, nil
}

// nor183GoroutineDump captures every stack in the deadlocked child so the
// parent's failure names the lock the callback was waiting on.
func nor183GoroutineDump() string {
	buffer := make([]byte, 1<<20)
	return string(buffer[:runtime.Stack(buffer, true)])
}

func TestNOR183ConcurrentLifecycleControls(t *testing.T) {
	for _, scenario := range []string{"concurrent-open", "close-open-read"} {
		t.Run(scenario, func(t *testing.T) {
			output := nor183RunCallbackScenario(t, scenario)
			require.Contains(t, output, nor183CompletedMarker+" scenario="+scenario)
		})
	}
}

// These controls record the current per-request remote factory contract.
// Remote constituent/composite views are deliberately not cached by the manager;
// a single shared remote instance is not assumed across authorization contexts.
func nor183PrepareConcurrent(t *testing.T, scenario string) (*nor183Scenario, error) {
	manager, _ := setupTestManager(t)
	remote := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	err := manager.CreateCompositeDatabase(t.Context(), "nor183_cmp", []ConstituentRef{{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read_write", URI: "http://nor183.invalid"}})
	if err != nil {
		return nil, err
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	manager.remoteEngineFactory = func(_ ConstituentRef, _ string) (storage.Engine, error) {
		if calls.Add(1) == 1 && scenario == "close-open-read" {
			close(entered)
			<-release
		}
		return remote, nil
	}
	return &nor183Scenario{
		trigger: func() nor183Outcome {
			var outcome nor183Outcome
			if scenario == "concurrent-open" {
				const requests = 8
				start := make(chan struct{})
				results := make(chan error, requests)
				for i := 0; i < requests; i++ {
					go func() {
						<-start
						engine, err := manager.GetStorageWithAuth("nor183_cmp.r", "synthetic")
						if err == nil {
							_, err = engine.NodeCount(t.Context())
						}
						results <- err
					}()
				}
				close(start)
				for i := 0; i < requests; i++ {
					outcome.callErr = errors.Join(outcome.callErr, <-results)
				}
				if calls.Load() != requests {
					outcome.callErr = errors.Join(outcome.callErr, fmt.Errorf("factory calls=%d, expected per-request %d", calls.Load(), requests))
				}
				outcome.closeErr = manager.Close()
			} else {
				opened := make(chan error, 1)
				closed := make(chan error, 1)
				read := make(chan []string, 1)
				go func() { _, err := manager.GetStorageWithAuth("nor183_cmp.r", "synthetic"); opened <- err }()
				<-entered
				go func() { closed <- manager.Close() }()
				// Close cancels lifetime before waiting for the metadata lock. This proves
				// overlap without sleep-based scheduling assumptions.
				<-manager.lifetime().Context().Done()
				go func() { read <- manager.ListDatabaseNames() }()
				close(release)
				outcome.callErr = <-opened
				outcome.closeErr = <-closed
				outcome.names = <-read
			}
			return outcome
		},
		verify: func(t *testing.T, got nor183Outcome) {
			require.NoError(t, got.callErr)
			require.NoError(t, got.closeErr)
			if scenario == "close-open-read" {
				require.Contains(t, got.names, "nor183_cmp")
				require.EqualValues(t, 1, calls.Load())
			}
		},
	}, nil
}
