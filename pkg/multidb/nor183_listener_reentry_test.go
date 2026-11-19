package multidb

// NOR-183 (reproduction only): storage listener delivery that reenters
// DatabaseManager.
//
// The historical fail-first file named by the issue,
// `sig/handoff/r73/1636-r4/external-metadata-repro_test.go`, is not present in
// this checkout and cannot be recovered: `git log --all --diff-filter=A --
// '*external-metadata-repro_test.go'`, `git log --all -- 'sig/handoff/*'` and
// `git grep -l external-metadata` over every ref all return nothing. The six
// historical case identities are therefore not recoverable here. This file
// covers the shapes the historical report describes - DROP, reset and metadata
// listener reentry, with write-behind on and off - through the public manager
// methods that exist today:
//
//	drop-purge-reset      DROP purges a namespace; change and reset listeners reenter
//	drop-buffered-events  the events of acknowledged writes are delivered by the DROP
//	drop-metadata         DROP's own catalog (metadata) publication, twice, in order
//	reset-recreate        DROP then CREATE of the same name, ordered after the reset
//	metadata-create       CREATE's catalog publication
//	metadata-update       ALTER metadata publications, in commit order
//
// and one `twelve-operations` scenario that drives every current public manager
// operation that writes the catalog (see
// nor183ListenerTwelveOperationScenario), each with a reentering listener, and
// asserts the ordered catalog image each operation published.
//
// Every scenario runs in an isolated child process. A regression blocks the
// calling goroutine inside the manager's own lock, so an in-process test would
// hang the package run instead of reporting one. The child's watchdog prints
// every stack and exits with nor183ListenerBlockedExit, which the parent
// reports as the reproduction; no goroutine is abandoned in a half-run test.
// TestNOR183ListenerWatchdogReportsBlockedScenario proves that report path, so a
// green scenario cannot come from a watchdog that silently gives up.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nor183ListenerScenarioEnv = "NOR183_LISTENER_SCENARIO"
	nor183ListenerModeEnv     = "NOR183_LISTENER_WRITE_BEHIND"
	nor183ListenerChildRun    = "^TestNOR183ListenerReentryChild$"

	nor183ListenerDropPurgeReset     = "drop-purge-reset"
	nor183ListenerDropBufferedEvents = "drop-buffered-events"
	nor183ListenerDropMetadata       = "drop-metadata"
	nor183ListenerResetRecreate      = "reset-recreate"
	nor183ListenerMetadataCreate     = "metadata-create"
	nor183ListenerMetadataUpdate     = "metadata-update"
	nor183ListenerTwelveOperations   = "twelve-operations"
	nor183ListenerWatchdogProbe      = "watchdog-probe"

	nor183ListenerCompletedMarker = "NOR183_LISTENER_SCENARIO_COMPLETED"
	nor183ListenerBlockedMarker   = "NOR183_LISTENER_SCENARIO_BLOCKED"

	nor183ListenerBlockedExit = 86

	// nor183ListenerCallBudget bounds one scenario. A manager lock held across
	// listener delivery never returns, so the child reports a reproduction
	// instead of waiting for the package timeout.
	nor183ListenerCallBudget = 15 * time.Second
	// nor183ListenerProbeBudget bounds the watchdog probe, which blocks on
	// purpose and only proves the report path.
	nor183ListenerProbeBudget = 3 * time.Second
	// nor183ListenerSettleBudget bounds the wait for asynchronous delivery. It
	// is a wait for storage delivery, never a synchronization sleep.
	nor183ListenerSettleBudget = 5 * time.Second
	// nor183ListenerProcessBudget bounds the child process, including its own
	// start-up, above the child's -test.timeout.
	nor183ListenerProcessBudget = 120 * time.Second
)

// TestNOR183ListenerDropAndResetReentry covers the DROP and reset listener
// shapes in both write-behind modes.
func TestNOR183ListenerDropAndResetReentry(t *testing.T) {
	for _, scenario := range []string{
		nor183ListenerDropPurgeReset,
		nor183ListenerDropBufferedEvents,
		nor183ListenerDropMetadata,
		nor183ListenerResetRecreate,
	} {
		for _, writeBehind := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write-behind=%t", scenario, writeBehind), func(t *testing.T) {
				output := nor183ListenerRunScenario(t, scenario, writeBehind)
				require.Contains(t, output, nor183ListenerCompletedMarker+" scenario="+scenario,
					"NOR-183 listener scenario must reach its completion marker: %s", output)
			})
		}
	}
}

// TestNOR183ListenerMetadataReentry covers database creation and metadata
// alteration whose catalog publication delivers a reentering listener.
func TestNOR183ListenerMetadataReentry(t *testing.T) {
	for _, scenario := range []string{
		nor183ListenerMetadataCreate,
		nor183ListenerMetadataUpdate,
	} {
		for _, writeBehind := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write-behind=%t", scenario, writeBehind), func(t *testing.T) {
				output := nor183ListenerRunScenario(t, scenario, writeBehind)
				require.Contains(t, output, nor183ListenerCompletedMarker+" scenario="+scenario,
					"NOR-183 listener scenario must reach its completion marker: %s", output)
			})
		}
	}
}

// TestNOR183ListenerTwelveManagerOperations drives every current public manager
// operation that writes the catalog, each with a reentering listener, and
// checks the ordered catalog image each operation published.
func TestNOR183ListenerTwelveManagerOperations(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind=%t", writeBehind), func(t *testing.T) {
			output := nor183ListenerRunScenario(t, nor183ListenerTwelveOperations, writeBehind)
			require.Contains(t, output, nor183ListenerCompletedMarker+" scenario="+nor183ListenerTwelveOperations,
				"NOR-183 twelve-operation scenario must reach its completion marker: %s", output)
		})
	}
}

// TestNOR183ListenerWatchdogReportsBlockedScenario proves the child watchdog
// reports a scenario that never returns, with the goroutine that is stuck, so a
// green scenario cannot come from a watchdog that silently gives up.
func TestNOR183ListenerWatchdogReportsBlockedScenario(t *testing.T) {
	output, exitCode := nor183ListenerProbe(t, nor183ListenerWatchdogProbe)
	require.Equal(t, nor183ListenerBlockedExit, exitCode,
		"a blocked scenario must exit with the watchdog's code; output:\n%s", output)
	require.Contains(t, output, nor183ListenerBlockedMarker+" scenario="+nor183ListenerWatchdogProbe,
		"the blocked child must report the scenario it abandoned:\n%s", output)
	require.Contains(t, output, "TestNOR183ListenerReentryChild",
		"the blocked child must dump the goroutine that never returned:\n%s", output)
}

// nor183ListenerRunScenario runs one scenario in a child process and turns a
// blocked child into the reproduction. The child is the test binary itself, so
// it carries the scenario through the real public manager path.
func nor183ListenerRunScenario(t *testing.T, scenario string, writeBehind bool) string {
	t.Helper()
	child := nor183ListenerChildCommand(t, scenario, writeBehind)
	output, runErr := child.CombinedOutput()
	require.NoError(t, child.deadlineErr(), "NOR-183 listener scenario %q (write-behind=%t) did not exit within %s; output:\n%s",
		scenario, writeBehind, nor183ListenerProcessBudget, output)
	var exit *exec.ExitError
	if errors.As(runErr, &exit) && exit.ExitCode() == nor183ListenerBlockedExit {
		t.Fatalf("NOR-183 reproduced: scenario %q (write-behind=%t) blocked while a storage listener reentered DatabaseManager\n%s",
			scenario, writeBehind, output)
	}
	require.NoError(t, runErr, "NOR-183 listener child %q (write-behind=%t) failed:\n%s", scenario, writeBehind, output)
	require.NotContains(t, string(output), nor183ListenerBlockedMarker,
		"NOR-183 listener child %q (write-behind=%t) reported a blocked scenario: %s", scenario, writeBehind, output)
	return string(output)
}

// nor183ListenerProbe runs a scenario expected to block and returns its output
// and exit code without turning either into a failure.
func nor183ListenerProbe(t *testing.T, scenario string) (string, int) {
	t.Helper()
	child := nor183ListenerChildCommand(t, scenario, false)
	output, runErr := child.CombinedOutput()
	require.NoError(t, child.deadlineErr(), "NOR-183 watchdog probe did not exit within %s; output:\n%s",
		nor183ListenerProcessBudget, output)
	var exit *exec.ExitError
	if !errors.As(runErr, &exit) {
		require.NoError(t, runErr, "NOR-183 watchdog probe did not report a blocked child:\n%s", output)
		return string(output), 0
	}
	return string(output), exit.ExitCode()
}

// nor183ListenerChildProcess is a child invocation with the deadline that bounds
// it.
type nor183ListenerChildProcess struct {
	*exec.Cmd
	deadline context.Context
}

func (p nor183ListenerChildProcess) deadlineErr() error { return p.deadline.Err() }

func nor183ListenerChildCommand(t *testing.T, scenario string, writeBehind bool) nor183ListenerChildProcess {
	t.Helper()
	binaryPath, err := os.Executable()
	require.NoError(t, err)
	deadline, cancel := context.WithTimeout(t.Context(), nor183ListenerProcessBudget)
	t.Cleanup(cancel)
	child := exec.CommandContext(deadline, binaryPath,
		"-test.run="+nor183ListenerChildRun, "-test.count=1", "-test.timeout=90s")
	child.Env = append(os.Environ(),
		nor183ListenerScenarioEnv+"="+scenario,
		fmt.Sprintf("%s=%t", nor183ListenerModeEnv, writeBehind))
	return nor183ListenerChildProcess{Cmd: child, deadline: deadline}
}

// TestNOR183ListenerReentryChild runs exactly one scenario in the isolated
// process started by nor183ListenerRunScenario. The environment it needs is
// built on the test goroutine; only the scenario's manager operations run on
// the watched goroutine, so a blocked operation is reported instead of being
// left behind.
func TestNOR183ListenerReentryChild(t *testing.T) {
	scenario := os.Getenv(nor183ListenerScenarioEnv)
	if scenario == "" {
		t.Skip("NOR-183 listener child scenario requires " + nor183ListenerScenarioEnv)
	}
	writeBehind := os.Getenv(nor183ListenerModeEnv) == "true"
	prepared, err := nor183ListenerPrepare(t, scenario, writeBehind)
	require.NoError(t, err)

	outcome := make(chan error, 1)
	go func() { outcome <- prepared.run() }()

	select {
	case runErr := <-outcome:
		require.NoError(t, runErr, "NOR-183 listener scenario %q (write-behind=%t) failed", scenario, writeBehind)
		prepared.verify(t)
		fmt.Printf("%s scenario=%s write-behind=%t events=%d reentrant-reads=%d\n",
			nor183ListenerCompletedMarker, scenario, writeBehind,
			prepared.log.eventCount(), prepared.log.completedReentries())
	case <-time.After(nor183ListenerBudget(scenario)):
		// The scenario goroutine is blocked inside the manager (or the storage
		// delivery it waits for), so no progress is possible in this process.
		fmt.Printf("%s scenario=%s write-behind=%t\n%s",
			nor183ListenerBlockedMarker, scenario, writeBehind, nor183ListenerStacks())
		os.Exit(nor183ListenerBlockedExit)
	}
}

// nor183ListenerBudget is the scenario's call budget. The watchdog probe blocks
// on purpose, so it uses the short one.
func nor183ListenerBudget(scenario string) time.Duration {
	if scenario == nor183ListenerWatchdogProbe {
		return nor183ListenerProbeBudget
	}
	return nor183ListenerCallBudget
}

// nor183ListenerScenario is one prepared reproduction: run performs the
// manager operations on the watched goroutine, verify checks what the
// listeners observed after those operations returned.
type nor183ListenerScenario struct {
	log    *nor183ListenerLog
	run    func() error
	verify func(*testing.T)
}

func nor183ListenerPrepare(t *testing.T, scenario string, writeBehind bool) (*nor183ListenerScenario, error) {
	t.Helper()
	if scenario == nor183ListenerWatchdogProbe {
		return &nor183ListenerScenario{
			log: &nor183ListenerLog{},
			run: func() error {
				<-make(chan struct{})
				return nil
			},
			verify: func(*testing.T) {},
		}, nil
	}
	inner, err := nor183ListenerInnerEngine(t, writeBehind)
	if err != nil {
		return nil, err
	}
	manager, err := NewDatabaseManager(t.Context(), inner, nil)
	if err != nil {
		return nil, fmt.Errorf("new manager: %w", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	log := &nor183ListenerLog{}
	ctx := t.Context()
	var prepared *nor183ListenerScenario
	switch scenario {
	case nor183ListenerDropPurgeReset:
		prepared, err = nor183ListenerDropPurge(t, manager, log, ctx)
	case nor183ListenerDropBufferedEvents:
		prepared, err = nor183ListenerDropBuffered(t, manager, log, ctx)
	case nor183ListenerDropMetadata:
		prepared, err = nor183ListenerDropMetadataScenario(t, manager, inner, log, ctx)
	case nor183ListenerResetRecreate:
		prepared, err = nor183ListenerResetRecreateScenario(t, manager, log, ctx)
	case nor183ListenerMetadataCreate:
		prepared, err = nor183ListenerMetadataCreateScenario(t, manager, inner, log, ctx)
	case nor183ListenerMetadataUpdate:
		prepared, err = nor183ListenerMetadataUpdateScenario(t, manager, inner, log, ctx)
	case nor183ListenerTwelveOperations:
		prepared, err = nor183ListenerTwelveOperationScenario(t, manager, inner, log, ctx)
	default:
		return nil, fmt.Errorf("unknown NOR-183 listener scenario %q", scenario)
	}
	if err != nil {
		return nil, err
	}
	prepared.log = log
	return prepared, nil
}

// nor183ListenerInnerEngine is the store the manager publishes over. Write-behind
// exists only on the native engine, so the buffered mode uses a native engine
// with a one-hour interval that only an explicit flush or a DROP drains.
func nor183ListenerInnerEngine(t *testing.T, writeBehind bool) (storage.Engine, error) {
	t.Helper()
	if !writeBehind {
		return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil
	}
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	if err != nil {
		return nil, fmt.Errorf("write-behind engine: %w", err)
	}
	return base, nil
}

// nor183ListenerDropPurge covers a DROP that purges a namespace. The purge
// reports one namespace reset, after the change events it removed, and never a
// per-entity delete; each listener reenters the manager, including the storage
// size read the historical report names explicitly.
func nor183ListenerDropPurge(t *testing.T, manager *DatabaseManager, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const database = "nor183_drop"
	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
		return nil, fmt.Errorf("create %s: %w", database, err)
	}
	engine, err := manager.GetStorage(database)
	if err != nil {
		return nil, fmt.Errorf("storage for %s: %w", database, err)
	}
	reentry := nor183ListenerReentryTarget{
		ctx: ctx, log: log, manager: manager,
		database: database, sizeDatabase: manager.DefaultDatabaseName(),
	}
	if err := storage.SubscribeEvents(engine, storage.EventHandlers{
		NodeCreated: func(node *storage.Node) {
			log.deliver("created " + string(node.ID))
			reentry.reenter()
		},
		NodeDeleted: func(id storage.NodeID) {
			log.deliver("deleted " + string(id))
			reentry.reenter()
		},
		NamespaceReset: func(prefix string) {
			log.deliver("reset " + prefix)
			reentry.reenter()
		},
	}); err != nil {
		return nil, fmt.Errorf("subscribe %s events: %w", database, err)
	}
	return &nor183ListenerScenario{
		run: func() error {
			for _, id := range []storage.NodeID{"a", "b", "c"} {
				if _, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Listener"}}); err != nil {
					return fmt.Errorf("create node %s: %w", id, err)
				}
			}
			if err := manager.DropDatabase(ctx, database); err != nil {
				return fmt.Errorf("drop %s: %w", database, err)
			}
			return nil
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, 4, 4)
			require.Equal(t, []string{"created a", "created b", "created c", "reset "}, events,
				"the DROP must report the reset after the events it removed, and no per-entity delete")
			log.requireNoOpenCalls(t)
			require.False(t, manager.Exists(database), "the dropped database must leave the catalog")
			require.NotContains(t, manager.ListDatabaseNames(), database)
		},
	}, nil
}

// nor183ListenerDropBuffered covers events of acknowledged writes that the DROP
// itself delivers: with write-behind the acknowledged creates are drained by
// the purge, and their listeners reenter the manager from inside that DROP.
func nor183ListenerDropBuffered(t *testing.T, manager *DatabaseManager, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const database = "nor183_buffered"
	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
		return nil, fmt.Errorf("create %s: %w", database, err)
	}
	engine, err := manager.GetStorage(database)
	if err != nil {
		return nil, fmt.Errorf("storage for %s: %w", database, err)
	}
	reentry := nor183ListenerReentryTarget{ctx: ctx, log: log, manager: manager, database: database}
	if err := storage.SubscribeEvents(engine, storage.EventHandlers{
		NodeCreated: func(node *storage.Node) {
			log.deliver("created " + string(node.ID))
			reentry.reenter()
		},
	}); err != nil {
		return nil, fmt.Errorf("subscribe %s events: %w", database, err)
	}
	return &nor183ListenerScenario{
		run: func() error {
			for _, id := range []storage.NodeID{"a", "b"} {
				if _, err := engine.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"Listener"}}); err != nil {
					return fmt.Errorf("create node %s: %w", id, err)
				}
			}
			if err := manager.DropDatabase(ctx, database); err != nil {
				return fmt.Errorf("drop %s: %w", database, err)
			}
			return nil
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, 2, 2)
			require.Equal(t, []string{"created a", "created b"}, events,
				"acknowledged change events must be delivered, in commit order, rather than suppressed by the DROP")
			log.requireNoOpenCalls(t)
			require.False(t, manager.Exists(database))
		},
	}, nil
}

// nor183ListenerDropMetadataScenario covers DROP's own catalog publications: the
// unavailable-name intent (offline, fence held) and the removal, delivered in
// that order, each with a listener that reenters the manager.
func nor183ListenerDropMetadataScenario(t *testing.T, manager *DatabaseManager, inner storage.Engine, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const database = "nor183_metadata"
	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
		return nil, fmt.Errorf("create %s: %w", database, err)
	}
	if err := nor183ListenerWatchCatalog(inner, log, nor183ListenerReentryTarget{
		ctx: ctx, log: log, manager: manager, database: database,
	}); err != nil {
		return nil, err
	}
	return &nor183ListenerScenario{
		run: func() error {
			if err := manager.DropDatabase(ctx, database); err != nil {
				return fmt.Errorf("drop %s: %w", database, err)
			}
			return nil
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, 2, 2)
			require.Len(t, events, 2, "one DROP publishes its intent and its removal")
			require.Contains(t, events[0], database+":standard:offline",
				"the intent publication must be the offline image of the dropping database")
			require.Contains(t, events[0], "pending="+database,
				"the intent publication must carry the held drop fence")
			require.Equal(t, nor183ListenerPublicImage(manager), events[1],
				"the last publication must be the catalog the drop left behind")
			log.requireNoOpenCalls(t)
			require.False(t, manager.Exists(database))
		},
	}, nil
}

// nor183ListenerResetRecreateScenario covers a DROP followed by a same-name
// CREATE: the reset is delivered before the recreated database's own events,
// and the recreated namespace serves a new generation.
func nor183ListenerResetRecreateScenario(t *testing.T, manager *DatabaseManager, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const database = "nor183_reset"
	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
		return nil, fmt.Errorf("create %s: %w", database, err)
	}
	engine, err := manager.GetStorage(database)
	if err != nil {
		return nil, fmt.Errorf("storage for %s: %w", database, err)
	}
	reentry := nor183ListenerReentryTarget{ctx: ctx, log: log, manager: manager, database: database}
	if err := storage.SubscribeEvents(engine, storage.EventHandlers{
		NodeCreated: func(node *storage.Node) {
			log.deliver("created " + string(node.ID))
			reentry.reenter()
		},
		NamespaceReset: func(prefix string) {
			log.deliver("reset " + prefix)
			reentry.reenter()
		},
	}); err != nil {
		return nil, fmt.Errorf("subscribe %s events: %w", database, err)
	}
	var fresh storage.Engine
	return &nor183ListenerScenario{
		run: func() error {
			if _, err := engine.CreateNode(ctx, &storage.Node{ID: "old", Labels: []string{"Listener"}}); err != nil {
				return fmt.Errorf("create old node: %w", err)
			}
			if err := manager.DropDatabase(ctx, database); err != nil {
				return fmt.Errorf("drop %s: %w", database, err)
			}
			if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
				return fmt.Errorf("recreate %s: %w", database, err)
			}
			recreated, err := manager.GetStorage(database)
			if err != nil {
				return fmt.Errorf("storage for recreated %s: %w", database, err)
			}
			fresh = recreated
			if _, err := recreated.CreateNode(ctx, &storage.Node{ID: "fresh", Labels: []string{"Listener"}}); err != nil {
				return fmt.Errorf("create fresh node: %w", err)
			}
			return nil
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, 3, 3)
			require.Equal(t, []string{"created old", "reset ", "created fresh"}, events,
				"the reset must precede the recreated generation's events")
			log.requireNoOpenCalls(t)
			require.True(t, manager.Exists(database), "the recreated database must be in the catalog")
			require.NotNil(t, fresh)
			count, err := fresh.NodeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, count, "the recreated namespace must hold only the new generation")
		},
	}, nil
}

// nor183ListenerMetadataCreateScenario covers CREATE's catalog publication.
func nor183ListenerMetadataCreateScenario(t *testing.T, manager *DatabaseManager, inner storage.Engine, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const database = "nor183_meta_create"
	if err := nor183ListenerWatchCatalog(inner, log, nor183ListenerReentryTarget{
		ctx: ctx, log: log, manager: manager, database: database,
	}); err != nil {
		return nil, err
	}
	return &nor183ListenerScenario{
		run: func() error {
			if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
				return fmt.Errorf("create %s: %w", database, err)
			}
			return nil
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, 1, 1)
			require.Equal(t, []string{nor183ListenerPublicImage(manager)}, events,
				"CREATE must publish exactly the catalog it left behind")
			log.requireNoOpenCalls(t)
			require.True(t, manager.Exists(database))
		},
	}, nil
}

// nor183ListenerMetadataUpdateScenario covers ALTER metadata publications: each
// operation publishes one catalog image, delivered in commit order.
func nor183ListenerMetadataUpdateScenario(t *testing.T, manager *DatabaseManager, inner storage.Engine, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const (
		database = "nor183_meta_update"
		alias    = "nor183_meta_alias"
	)
	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: database}); err != nil {
		return nil, fmt.Errorf("create %s: %w", database, err)
	}
	if err := nor183ListenerWatchCatalog(inner, log, nor183ListenerReentryTarget{
		ctx: ctx, log: log, manager: manager, database: database,
	}); err != nil {
		return nil, err
	}
	expected := make([]string, 0, 5)
	record := func(name string, apply func() error) error {
		if err := apply(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		expected = append(expected, nor183ListenerPublicImage(manager))
		return nil
	}
	return &nor183ListenerScenario{
		run: func() error {
			if err := record("SetDatabaseStatus", func() error {
				return manager.SetDatabaseStatus(ctx, database, "offline")
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseLimits", func() error {
				return manager.SetDatabaseLimits(ctx, database, &Limits{Storage: StorageLimits{MaxNodes: 4, MaxBytes: 1 << 20}})
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseOption", func() error {
				return manager.SetDatabaseOption(ctx, database, OptionTxLogEnrichment, "DIFF")
			}); err != nil {
				return err
			}
			if err := record("CreateAlias", func() error {
				return manager.CreateAlias(ctx, alias, database)
			}); err != nil {
				return err
			}
			return record("DropAlias", func() error {
				return manager.DropAlias(ctx, alias)
			})
		},
		verify: func(t *testing.T) {
			events := log.requireSettled(t, len(expected), len(expected))
			require.Equal(t, expected, events,
				"each metadata change must be delivered once, in commit order, with the image it published")
			log.requireNoOpenCalls(t)
		},
	}, nil
}

// nor183ListenerOperationExpectation is the catalog image one operation
// published: one event for an ALTER, two for a DROP, none for a recovery entry
// that finds no held fence.
type nor183ListenerOperationExpectation struct {
	name     string
	events   int
	database string
	image    string
}

// nor183ListenerTwelveOperationScenario drives every current public manager
// operation that writes the catalog, in one catalog generation, with a
// reentering listener:
//
//	CreateDatabase, SetDatabaseStatus, SetDatabaseLimits, SetDatabaseOption,
//	CreateAlias, DropAlias, CreateCompositeDatabase, AddConstituent,
//	RemoveConstituent, DropCompositeDatabase, DropDatabase, FinishPendingDrop
//
// FinishPendingDrop is exercised here in its no-fence form. A held fence is
// reachable through public calls: a DropDatabase whose completion Sync fails
// returns the accepted outcome of an applied removal and retains the
// unavailable-name fence, so the recovery call can find one. That
// failed-drop-then-FinishPendingDrop shape, with a reentering listener, is
// covered by nor183_pending_listener_test.go.
func nor183ListenerTwelveOperationScenario(t *testing.T, manager *DatabaseManager, inner storage.Engine, log *nor183ListenerLog, ctx context.Context) (*nor183ListenerScenario, error) {
	t.Helper()
	const (
		target    = "nor183_op_target"
		dropped   = "nor183_op_drop"
		alias     = "nor183_op_alias"
		composite = "nor183_op_cmp"
	)
	if err := nor183ListenerWatchCatalog(inner, log, nor183ListenerReentryTarget{
		ctx: ctx, log: log, manager: manager, database: target,
	}); err != nil {
		return nil, err
	}
	expectations := make([]nor183ListenerOperationExpectation, 0, 14)
	record := func(name string, apply func() error) error {
		if err := apply(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		expectations = append(expectations, nor183ListenerOperationExpectation{
			name: name, events: 1, image: nor183ListenerPublicImage(manager),
		})
		return nil
	}
	return &nor183ListenerScenario{
		run: func() error {
			if err := record("CreateDatabase", func() error {
				return manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: target})
			}); err != nil {
				return err
			}
			if err := record("CreateDatabase(drop target)", func() error {
				return manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: dropped})
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseStatus(offline)", func() error {
				return manager.SetDatabaseStatus(ctx, target, "offline")
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseStatus(online)", func() error {
				return manager.SetDatabaseStatus(ctx, target, "online")
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseLimits", func() error {
				return manager.SetDatabaseLimits(ctx, target, &Limits{Storage: StorageLimits{MaxNodes: 8, MaxEdges: 16}})
			}); err != nil {
				return err
			}
			if err := record("SetDatabaseOption", func() error {
				return manager.SetDatabaseOption(ctx, target, OptionTxLogEnrichment, "FULL")
			}); err != nil {
				return err
			}
			if err := record("CreateAlias", func() error {
				return manager.CreateAlias(ctx, alias, target)
			}); err != nil {
				return err
			}
			if err := record("CreateCompositeDatabase", func() error {
				return manager.CreateCompositeDatabase(ctx, composite, []ConstituentRef{
					{Alias: "first", DatabaseName: target, Type: "local", AccessMode: "read_write"},
				})
			}); err != nil {
				return err
			}
			if err := record("AddConstituent", func() error {
				return manager.AddConstituent(ctx, composite, ConstituentRef{
					Alias: "second", DatabaseName: target, Type: "local", AccessMode: "read_write",
				})
			}); err != nil {
				return err
			}
			if err := record("RemoveConstituent", func() error {
				return manager.RemoveConstituent(ctx, composite, "second")
			}); err != nil {
				return err
			}
			if err := record("DropCompositeDatabase", func() error {
				return manager.DropCompositeDatabase(ctx, composite)
			}); err != nil {
				return err
			}
			if err := record("DropAlias", func() error {
				return manager.DropAlias(ctx, alias)
			}); err != nil {
				return err
			}
			if err := manager.DropDatabase(ctx, dropped); err != nil {
				return fmt.Errorf("DropDatabase: %w", err)
			}
			expectations = append(expectations, nor183ListenerOperationExpectation{
				name: "DropDatabase", events: 2, database: dropped, image: nor183ListenerPublicImage(manager),
			})
			if finishErr := manager.FinishPendingDrop(ctx, dropped); !errors.Is(finishErr, ErrDatabaseNotFound) {
				return fmt.Errorf("FinishPendingDrop without a held fence: got %v, want %v", finishErr, ErrDatabaseNotFound)
			}
			expectations = append(expectations, nor183ListenerOperationExpectation{name: "FinishPendingDrop"})
			return nil
		},
		verify: func(t *testing.T) {
			total := 0
			for _, expectation := range expectations {
				total += expectation.events
			}
			events := log.requireSettled(t, total, len(expectations)-1)
			require.Len(t, events, total,
				"every catalog-writing operation must deliver its publication and nothing else")
			index := 0
			for _, expectation := range expectations {
				switch expectation.events {
				case 0:
					continue
				case 1:
					require.Equal(t, expectation.image, events[index],
						"%s must deliver the catalog image it published", expectation.name)
					index++
				case 2:
					require.Contains(t, events[index], expectation.database+":standard:offline",
						"%s must deliver its unavailable-name intent first", expectation.name)
					require.Contains(t, events[index], "pending="+expectation.database,
						"%s must deliver the held fence with its intent", expectation.name)
					require.Equal(t, expectation.image, events[index+1],
						"%s must deliver the catalog it left behind", expectation.name)
					index += 2
				}
			}
			require.Equal(t, total, index)
			log.requireNoOpenCalls(t)
			require.True(t, manager.Exists(target))
			require.False(t, manager.Exists(dropped))
			require.False(t, manager.Exists(composite))
		},
	}, nil
}

// nor183ListenerWatchCatalog subscribes to the manager's catalog (metadata node)
// publications and reenters the manager from every one of them.
func nor183ListenerWatchCatalog(inner storage.Engine, log *nor183ListenerLog, target nor183ListenerReentryTarget) error {
	deliver := func(node *storage.Node) {
		if node == nil || !strings.HasSuffix(string(node.ID), metadataNodeID) {
			return
		}
		image, err := nor183ListenerNotificationImage(node)
		if err != nil {
			log.deliver("catalog-decode-error " + err.Error())
			return
		}
		log.deliver(image)
		target.reenter()
	}
	if err := storage.SubscribeEvents(inner, storage.EventHandlers{
		NodeCreated: deliver,
		NodeUpdated: deliver,
	}); err != nil {
		return fmt.Errorf("subscribe catalog events: %w", err)
	}
	return nil
}

// nor183ListenerReentryTarget performs the listener's reentrant read of the
// DatabaseManager registry. Every call takes the manager's metadata lock -
// GetStorage takes it exclusively - so a manager operation that delivers this
// listener while holding that lock blocks here.
type nor183ListenerReentryTarget struct {
	ctx          context.Context
	log          *nor183ListenerLog
	manager      *DatabaseManager
	database     string
	sizeDatabase string
}

func (r nor183ListenerReentryTarget) reenter() {
	log := r.log
	log.record("-> reentry")
	defer log.record("<- reentry")
	log.call("ListDatabaseNames", func() error { _ = r.manager.ListDatabaseNames(); return nil })
	log.call("ListDatabases", func() error { _ = r.manager.ListDatabases(); return nil })
	log.call("GetDatabase", func() error { _, err := r.manager.GetDatabase(r.database); return err })
	log.call("DatabaseOptions", func() error { _, err := r.manager.DatabaseOptions(r.database); return err })
	log.call("GetDatabaseLimits", func() error { _, err := r.manager.GetDatabaseLimits(r.database); return err })
	log.call("ListAliases", func() error { _ = r.manager.ListAliases(""); return nil })
	log.call("Exists", func() error { _ = r.manager.Exists(r.database); return nil })
	log.call("ResolveDatabase", func() error { _, err := r.manager.ResolveDatabase(r.database); return err })
	log.call("ServerIdentity", func() error { _, _ = r.manager.ServerIdentity(); return nil })
	log.call("DatabaseIdentity", func() error { _, _ = r.manager.DatabaseIdentity(r.database); return nil })
	if r.sizeDatabase != "" {
		log.call("GetStorage("+r.sizeDatabase+")", func() error {
			_, err := r.manager.GetStorage(r.sizeDatabase)
			return err
		})
		log.call("GetStorageSize("+r.sizeDatabase+")", func() error {
			_, _, _ = r.manager.GetStorageSize(r.ctx, r.sizeDatabase)
			return nil
		})
	}
}

// nor183ListenerLog records what listeners observed, in delivery order, and the
// reentrant manager calls they made. Listener goroutines and the scenario
// goroutine share it, so every access is locked.
type nor183ListenerLog struct {
	mu     sync.Mutex
	events []string
	calls  []string
}

func (l *nor183ListenerLog) deliver(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

// call runs one reentrant manager read and records that it entered and left it,
// so a blocked call is named by the last unmatched entry in the child's dump.
func (l *nor183ListenerLog) call(name string, run func() error) {
	l.record("-> " + name)
	left := "<- " + name
	if err := run(); err != nil {
		left += " err=" + err.Error()
	}
	l.record(left)
}

func (l *nor183ListenerLog) record(entry string) {
	l.mu.Lock()
	l.calls = append(l.calls, entry)
	l.mu.Unlock()
}

func (l *nor183ListenerLog) snapshot() (events, calls []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...), append([]string(nil), l.calls...)
}

func (l *nor183ListenerLog) eventCount() int {
	events, _ := l.snapshot()
	return len(events)
}

// completedReentries counts the reentrant reads that returned; a call that is
// still waiting on a manager lock never records its return.
func (l *nor183ListenerLog) completedReentries() int {
	_, calls := l.snapshot()
	completed := 0
	for _, entry := range calls {
		if entry == "<- reentry" {
			completed++
		}
	}
	return completed
}

// unmatched reports reentrant calls that entered the manager and did not
// return, which is what a listener blocked on a manager lock looks like once the
// outer operation has returned.
func (l *nor183ListenerLog) unmatched() []string {
	_, calls := l.snapshot()
	counts := make(map[string]int)
	for _, entry := range calls {
		switch {
		case strings.HasPrefix(entry, "-> "):
			counts[strings.TrimPrefix(entry, "-> ")]++
		case strings.HasPrefix(entry, "<- "):
			name := strings.SplitN(strings.TrimPrefix(entry, "<- "), " ", 2)[0]
			counts[name]--
		}
	}
	var open []string
	for name, outstanding := range counts {
		if outstanding > 0 {
			open = append(open, fmt.Sprintf("%s x%d", name, outstanding))
		}
	}
	sort.Strings(open)
	return open
}

// requireSettled waits, bounded, for the listeners to deliver wantEvents events
// and complete wantReentries reentrant reads, then returns the delivered events.
func (l *nor183ListenerLog) requireSettled(t *testing.T, wantEvents, wantReentries int) []string {
	t.Helper()
	deadline := time.Now().Add(nor183ListenerSettleBudget)
	var events []string
	for {
		events, _ = l.snapshot()
		if len(events) >= wantEvents && l.completedReentries() >= wantReentries {
			return events
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("listener delivery reached %d events and %d completed reentrant reads; want %d and %d\n%s",
		len(events), l.completedReentries(), wantEvents, wantReentries, nor183ListenerStacks())
	return nil
}

func (l *nor183ListenerLog) requireNoOpenCalls(t *testing.T) {
	t.Helper()
	require.Empty(t, l.unmatched(), "every listener reentry must return from the manager")
}

// nor183ListenerNotificationImage renders the catalog image a metadata
// notification carries. A held DROP fence is rendered with it, so the ordering
// assertions can name the intent publication.
func nor183ListenerNotificationImage(node *storage.Node) (string, error) {
	encoded, ok := node.Properties["data"].(string)
	if !ok {
		return "", fmt.Errorf("catalog notification %s carries no encoded catalog", node.ID)
	}
	var stored map[string]*DatabaseInfo
	if err := jsontext.Unmarshal([]byte(encoded), &stored); err != nil {
		return "", fmt.Errorf("catalog notification %s: %w", node.ID, err)
	}
	pending := map[string]bool{}
	if encodedPending, ok := node.Properties["pending_drops"].(string); ok && encodedPending != "" {
		if err := jsontext.Unmarshal([]byte(encodedPending), &pending); err != nil {
			return "", fmt.Errorf("catalog notification %s pending drops: %w", node.ID, err)
		}
	}
	databases := make([]*DatabaseInfo, 0, len(stored))
	for _, info := range stored {
		databases = append(databases, info)
	}
	image := nor183ListenerRenderCatalog(databases, pending)
	if fenced := nor183ListenerRenderFences(pending); fenced != "" {
		image += " pending=" + fenced
	}
	return image, nil
}

// nor183ListenerRenderFences renders the unavailable-name fences a persisted
// image holds, in name order.
func nor183ListenerRenderFences(pending map[string]bool) string {
	names := make([]string, 0, len(pending))
	for name, held := range pending {
		if held {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, "|")
}

// nor183ListenerPublicImage renders the same image through the public catalog
// readers, whose projection already reports a database a DROP holds as offline.
func nor183ListenerPublicImage(manager *DatabaseManager) string {
	return nor183ListenerRenderCatalog(manager.ListDatabases(), nil)
}

// nor183ListenerRenderCatalog renders the fields the ordering assertions cover.
// A database a DROP holds is offline, whether the image carries the fence
// separately (the persisted catalog) or folded into the status (the public
// projection).
func nor183ListenerRenderCatalog(databases []*DatabaseInfo, pending map[string]bool) string {
	sorted := append([]*DatabaseInfo(nil), databases...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	parts := make([]string, 0, len(sorted))
	for _, info := range sorted {
		if info == nil {
			continue
		}
		status := info.Status
		if pending[info.Name] {
			status = "offline"
		}
		aliases := append([]string(nil), info.Aliases...)
		sort.Strings(aliases)
		constituents := make([]string, 0, len(info.Constituents))
		for _, ref := range info.Constituents {
			constituents = append(constituents, ref.Alias+"="+ref.DatabaseName+"/"+ref.Type+"/"+ref.AccessMode)
		}
		sort.Strings(constituents)
		parts = append(parts, fmt.Sprintf("%s:%s:%s:aliases=%s:limits=%s:options=%s:constituents=%s",
			info.Name, info.Type, status,
			strings.Join(aliases, "|"),
			nor183ListenerRenderLimits(info.Limits),
			nor183ListenerRenderOptions(info.Options),
			strings.Join(constituents, "|")))
	}
	return strings.Join(parts, ",")
}

func nor183ListenerRenderLimits(limits *Limits) string {
	if limits == nil {
		return "none"
	}
	return fmt.Sprintf("nodes=%d,edges=%d,bytes=%d,writes=%d",
		limits.Storage.MaxNodes, limits.Storage.MaxEdges, limits.Storage.MaxBytes,
		limits.Rate.MaxWritesPerSecond)
}

func nor183ListenerRenderOptions(options map[string]string) string {
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+options[key])
	}
	return strings.Join(parts, "|")
}

// nor183ListenerStacks captures every stack in the blocked child so the
// parent's failure names the lock and the reentrant call it waited on.
func nor183ListenerStacks() string {
	buffer := make([]byte, 1<<20)
	return string(buffer[:runtime.Stack(buffer, true)])
}
