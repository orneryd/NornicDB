package heimdall

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// hostAdmissionContextKey marks the registration call context the plugin's
// initialization is actually admitted under.
type hostAdmissionContextKey struct{}

// The retained metadata is copied by value into the published plugin context,
// so every slot uses a distinct non-zero-size stand-in: two empty structs can
// share one address, which would not prove the value crossed the projection.
type projectionDatabase struct {
	*mockDBRouter
	slot int
}

type projectionMetrics struct {
	*mockMetricsReader
	slot int
}

type projectionLogger struct {
	SubsystemLogger
	slot int
}

type projectionBifrost struct {
	*NoOpBifrost
	slot int
}

type projectionInvoker struct {
	*NoOpHeimdallInvoker
	slot int
}

// hostProjectionPlugin records the context its Initialize receives. The
// blocking instance holds the accepted initialization open so the test can
// observe the context it was handed while the work is still admitted.
type hostProjectionPlugin struct {
	*coverageMinimalPlugin
	entered  chan struct{}
	resume   chan struct{}
	mu       sync.Mutex
	observed SubsystemContext
}

func newHostProjectionPlugin(name string) *hostProjectionPlugin {
	return &hostProjectionPlugin{coverageMinimalPlugin: newCoverageMinimalPlugin(name)}
}

func newBlockingHostProjectionPlugin(name string) *hostProjectionPlugin {
	return &hostProjectionPlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin(name),
		entered:               make(chan struct{}),
		resume:                make(chan struct{}),
	}
}

func (p *hostProjectionPlugin) Initialize(ctx SubsystemContext) error {
	p.mu.Lock()
	p.observed = ctx
	p.mu.Unlock()
	if p.entered != nil {
		close(p.entered)
	}
	if p.resume != nil {
		<-p.resume
	}
	return nil
}

func (p *hostProjectionPlugin) publishedContext() SubsystemContext {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observed
}

// newHostedProjectionManager returns a manager whose host lifetime belongs to
// the test, and joins the manager into that lifetime at cleanup.
func newHostedProjectionManager(t *testing.T) (*SubsystemManager, *lifecycle.WorkLifetime) {
	t.Helper()
	owner := lifecycle.NewWorkLifetime(context.Background())
	manager := newMatrixSubsystemManager()
	t.Cleanup(func() {
		manager.CloseAdmission()
		require.NoError(t, owner.Shutdown(context.Background(), manager.ShutdownAll))
	})
	return manager, owner
}

func waitForInitialize(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the registration never reached Initialize")
	}
}

func waitForRegistration(t *testing.T, finished <-chan struct{}, result <-chan error) error {
	t.Helper()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted registration did not finish")
	}
	return <-result
}

// TestSubsystemManagerInitializePublishesAdmittedWorkContext proves all seven
// metadata fields cross the projection, that Initialize receives the work
// context its own registration was admitted under, and that host stop still
// cancels exactly that context.
func TestSubsystemManagerInitializePublishesAdmittedWorkContext(t *testing.T) {
	manager, owner := newHostedProjectionManager(t)

	endpoint := "https://synthetic-projection-user:synthetic-projection-pass@example.invalid/v1"
	apiKey := "synthetic-projection-key"
	fragments := observability.ClientCredentials(endpoint, apiKey)
	require.NotEmpty(t, fragments)

	// The supported host path: the configured client reaches only the manager's
	// private diagnostic context, and the published host context is keyless.
	manager.SetHostContext(SubsystemHost{
		Config:   Config{APIURL: endpoint, APIKey: apiKey},
		Lifetime: owner,
	})
	publishedHost := redactedHostPluginContext(endpoint, apiKey)
	publishedHost.Lifetime = owner
	publishedHost.Database = &projectionDatabase{mockDBRouter: &mockDBRouter{}, slot: 1}
	publishedHost.Metrics = &projectionMetrics{mockMetricsReader: &mockMetricsReader{}, slot: 2}
	publishedHost.Logger = &projectionLogger{SubsystemLogger: NewDefaultLogger("host-projection"), slot: 3}
	publishedHost.Bifrost = &projectionBifrost{NoOpBifrost: &NoOpBifrost{}, slot: 4}
	publishedHost.Heimdall = &projectionInvoker{NoOpHeimdallInvoker: &NoOpHeimdallInvoker{}, slot: 5}
	manager.SetContext(publishedHost)

	manager.mu.RLock()
	retained := append([]string(nil), manager.diagnosticCredentials...)
	manager.mu.RUnlock()
	for _, fragment := range fragments {
		require.Contains(t, retained, fragment,
			"the host's private diagnostic credential was dropped by the projection")
	}

	plugin := newBlockingHostProjectionPlugin("projection-admitted-work")
	var released sync.Once
	release := func() { released.Do(func() { close(plugin.resume) }) }
	callCtx := context.WithValue(context.Background(), hostAdmissionContextKey{}, "registration-call")
	registered := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		registered <- manager.RegisterStartedPlugin(callCtx, plugin, "", false)
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("the accepted initialization did not finish")
		}
	})
	waitForInitialize(t, plugin.entered)
	published := plugin.publishedContext()

	// All seven metadata fields cross the projection unchanged.
	require.True(t, published.Config == publishedHost.Config,
		"the published configuration was not the host's: %+v", published.Config)
	require.True(t, published.Lifetime == owner,
		"the published work owner was not the host's")
	require.True(t, published.Database == publishedHost.Database,
		"the published database router was not the host's")
	require.True(t, published.Metrics == publishedHost.Metrics,
		"the published metrics reader was not the host's")
	require.True(t, published.Logger == publishedHost.Logger,
		"the published logger was not the host's")
	require.True(t, published.Bifrost == publishedHost.Bifrost,
		"the published bifrost bridge was not the host's")
	require.True(t, published.Heimdall == publishedHost.Heimdall,
		"the published invoker was not the host's")
	// The published configuration stays sanitized while the private diagnostics
	// keep the configured client.
	require.Empty(t, published.Config.APIKey)
	require.False(t, managerOutputContainsCredential(published.Config.APIURL, fragments),
		"the plugin's initialization context exposed a configured credential")

	// The initialization context is the work context admitted for this
	// registration, not the registration call context itself.
	require.NotNil(t, published.Context)
	require.True(t, published.Context != callCtx,
		"Initialize received the registration call context it was admitted for")
	require.Equal(t, "registration-call", published.Context.Value(hostAdmissionContextKey{}),
		"the initialization context did not carry the admitted registration call context")
	require.NoError(t, published.Context.Err())

	// Host stop still cancels the accepted initialization, for exactly the
	// context the plugin was handed.
	owner.Cancel()
	select {
	case <-published.Context.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("host stop did not cancel the context Initialize received")
	}
	require.ErrorIs(t, published.Context.Err(), context.Canceled)

	release()
	select {
	case err := <-registered:
		require.NoError(t, err, "the accepted registration reported a failure")
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted registration did not finish")
	}
}

// TestSubsystemManagerReconfigurationPublishesCurrentMetadata proves that
// reconfiguring the host publishes the current metadata to later
// registrations, without retaining or publishing either setter context and
// without disturbing an accepted initialization already in flight.
func TestSubsystemManagerReconfigurationPublishesCurrentMetadata(t *testing.T) {
	manager, owner := newHostedProjectionManager(t)

	firstConfig := Config{Model: "first-model"}
	manager.SetContext(SubsystemHost{
		Lifetime: owner,
		Config:   firstConfig,
		Bifrost:  &projectionBifrost{NoOpBifrost: &NoOpBifrost{}, slot: 1},
	})
	first := newBlockingHostProjectionPlugin("projection-before-reconfiguration")
	firstRegistered := make(chan error, 1)
	firstFinished := make(chan struct{})
	go func() {
		defer close(firstFinished)
		firstRegistered <- manager.RegisterStartedPlugin(context.Background(), first, "", false)
	}()

	secondConfig := Config{Model: "second-model"}
	second := newBlockingHostProjectionPlugin("projection-in-flight")
	secondRegistered := make(chan error, 1)
	secondFinished := make(chan struct{})
	var released sync.Once
	release := func() {
		released.Do(func() {
			close(first.resume)
			close(second.resume)
		})
	}
	t.Cleanup(func() {
		release()
		for _, finished := range []chan struct{}{firstFinished, secondFinished} {
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Error("the accepted registration did not finish")
			}
		}
	})
	waitForInitialize(t, first.entered)
	go func() {
		defer close(secondFinished)
		secondRegistered <- manager.RegisterStartedPlugin(context.Background(), second, "", false)
	}()
	waitForInitialize(t, second.entered)

	before := first.publishedContext()
	require.True(t, before.Config == firstConfig, "the first configuration was not published")
	require.True(t, before.Lifetime == owner, "the first work owner was not the host's")
	require.NotNil(t, before.Context)
	require.NoError(t, before.Context.Err())

	manager.SetContext(SubsystemHost{
		Lifetime: owner,
		Config:   secondConfig,
		Bifrost:  &projectionBifrost{NoOpBifrost: &NoOpBifrost{}, slot: 2},
	})
	third := newHostProjectionPlugin("projection-after-reconfiguration")
	require.NoError(t, manager.RegisterStartedPlugin(context.Background(), third, "", false))
	after := third.publishedContext()
	require.True(t, after.Config == secondConfig,
		"the reconfigured host metadata was not published: %+v", after.Config)
	require.True(t, after.Lifetime == owner,
		"reconfiguration replaced the host work owner")
	require.True(t, after.Bifrost != before.Bifrost,
		"reconfiguration did not publish the current bridge")
	// This registration already completed, so its admitted work context is
	// released; only the still-accepted initializations stay live.
	require.NotNil(t, after.Context)
	require.ErrorIs(t, after.Context.Err(), context.Canceled,
		"completed initialization retained a live work context")

	// The reconfiguration left the accepted initialization in flight under its
	// own admitted context; only its own completion releases that work.
	require.True(t, second.publishedContext().Config == firstConfig,
		"the in-flight initialization observed a reconfiguration it was not admitted under")
	require.NoError(t, first.publishedContext().Context.Err())

	release()
	require.NoError(t, waitForRegistration(t, firstFinished, firstRegistered))
	require.NoError(t, waitForRegistration(t, secondFinished, secondRegistered))
}
