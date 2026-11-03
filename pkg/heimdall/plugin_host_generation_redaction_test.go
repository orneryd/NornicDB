package heimdall

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// joinTestCleanup joins work a failed test could have left running, so an
// early failure never strands accepted work in the package run. It waits on the
// work's own done signal, never on a result channel an assertion consumed, and
// reports work it cannot join instead of hanging the package.
func joinTestCleanup(t *testing.T, done <-chan struct{}, work string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Errorf("cleanup could not join %s", work)
	}
}

// acceptedInitializePlugin is accepted by the first host generation and keeps
// its initialization running until the test releases it, so the generation
// change joins an accepted initialization whose cleanup reports later.
type acceptedInitializePlugin struct {
	*coverageMinimalPlugin
	entered     chan struct{}
	resume      chan struct{}
	failure     error
	stopErr     error
	shutdownErr error
	mu          sync.Mutex
	observed    SubsystemContext
}

func (p *acceptedInitializePlugin) Initialize(ctx SubsystemContext) error {
	p.mu.Lock()
	p.observed = ctx
	p.mu.Unlock()
	close(p.entered)
	<-p.resume
	return p.failure
}

func (p *acceptedInitializePlugin) Stop() error     { return p.stopErr }
func (p *acceptedInitializePlugin) Shutdown() error { return p.shutdownErr }

func (p *acceptedInitializePlugin) context() SubsystemContext {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observed
}

// publishedContextPlugin records the context the supported registration path
// publishes to a plugin of the current host generation.
type publishedContextPlugin struct {
	*coverageMinimalPlugin
	mu       sync.Mutex
	observed SubsystemContext
}

func (p *publishedContextPlugin) Initialize(ctx SubsystemContext) error {
	p.mu.Lock()
	p.observed = ctx
	p.mu.Unlock()
	return nil
}

func (p *publishedContextPlugin) context() SubsystemContext {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observed
}

// A second host generation starts only after the first generation's accepted
// initialization is joined. Its cleanup outcome still contains the first
// generation's configured client, so the manager must keep the prior
// diagnostic credentials private until that outcome is reported, while every
// plugin context stays free of credentials.
func TestSubsystemHostGenerationRetainsDiagnosticCredentials(t *testing.T) {
	firstEndpoint := "https://synthetic-first-host-user:synthetic-first-host-pass@example.invalid/v1"
	firstKey := "synthetic-first-host-key"
	secondEndpoint := "https://synthetic-second-host-user:synthetic-second-host-pass@example.invalid/v1"
	secondKey := "synthetic-second-host-key"
	firstFragments := observability.ClientCredentials(firstEndpoint, firstKey)
	secondFragments := observability.ClientCredentials(secondEndpoint, secondKey)
	require.NotEmpty(t, firstFragments)
	require.NotEmpty(t, secondFragments)

	initializeFailure := errors.New("synthetic accepted initialization failure")
	stopFailure := errors.New("synthetic accepted cleanup stop failure")
	shutdownFailure := errors.New("synthetic accepted cleanup shutdown failure")

	manager := newMatrixSubsystemManager()
	manager.SetHostContext(SubsystemHost{
		Config:  Config{APIURL: firstEndpoint, APIKey: firstKey},
		Bifrost: &NoOpBifrost{},
	})
	manager.SetContext(redactedHostPluginContext(firstEndpoint, firstKey))

	accepted := &acceptedInitializePlugin{
		coverageMinimalPlugin: newCoverageMinimalPlugin("accepted-first-generation"),
		entered:               make(chan struct{}),
		resume:                make(chan struct{}),
		failure:               fmt.Errorf("accepted initialize for %s key %s: %w", firstEndpoint, firstKey, initializeFailure),
		stopErr:               fmt.Errorf("accepted stop for %s key %s: %w", firstEndpoint, firstKey, stopFailure),
		shutdownErr:           fmt.Errorf("accepted shutdown for %s key %s: %w", firstEndpoint, firstKey, shutdownFailure),
	}
	// Every exit releases the held initialization and joins both goroutines that
	// carry accepted work. The cleanups are registered before their goroutines
	// are launched, release through one idempotent gate, and each waits on its
	// goroutine's own done signal: the registration's result channel is consumed
	// by the assertions below, so cleanup must never read it again.
	var released sync.Once
	releaseInitialize := func() { released.Do(func() { close(accepted.resume) }) }
	registeredDone := make(chan struct{})
	t.Cleanup(func() {
		releaseInitialize()
		joinTestCleanup(t, registeredDone, "the accepted initialization")
	})
	registered := make(chan error, 1)
	go func() {
		defer close(registeredDone)
		registered <- manager.RegisterStartedPlugin(t.Context(), accepted, "", false)
	}()
	select {
	case <-accepted.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted initialization did not start")
	}

	// The published context of the first generation never carried the client.
	firstPublished := accepted.context()
	require.False(t, managerOutputContainsCredential(firstPublished.Config.APIURL, firstFragments),
		"the first generation's plugin context exposed a configured credential")
	require.Empty(t, firstPublished.Config.APIKey)

	manager.mu.RLock()
	firstRegistrations := manager.registrations
	manager.mu.RUnlock()
	require.NotNil(t, firstRegistrations)

	transitioned := make(chan struct{})
	transitionDone := make(chan struct{})
	// The transition can only finish after the initialization it joins is
	// released, so its cleanup releases the same idempotent gate first.
	t.Cleanup(func() {
		releaseInitialize()
		joinTestCleanup(t, transitionDone, "the host generation transition")
	})
	go func() {
		defer close(transitionDone)
		manager.SetHostContext(SubsystemHost{
			Config:  Config{APIURL: secondEndpoint, APIKey: secondKey},
			Bifrost: &NoOpBifrost{},
		})
		close(transitioned)
	}()
	// The generation change joins the accepted initialization before it installs
	// the new generation; that join is the window this test reports through.
	deadline := time.Now().Add(5 * time.Second)
	for !firstRegistrations.Closing() {
		if time.Now().After(deadline) {
			t.Fatal("the second host generation did not join the accepted initialization")
		}
		runtime.Gosched()
	}
	select {
	case <-transitioned:
		t.Fatal("the host generation changed while an accepted initialization was still running")
	default:
	}
	releaseInitialize()

	var registrationErr error
	select {
	case registrationErr = <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("the accepted registration did not report its cleanup outcome")
	}
	select {
	case <-transitioned:
	case <-time.After(5 * time.Second):
		t.Fatal("the second host generation did not finish after the join")
	}

	// The outcome accepted under the first generation is reported redacted with
	// that generation's retained credentials, not the new generation's.
	require.Error(t, registrationErr, "the joined registration reported no failure")
	require.True(t, errors.Is(registrationErr, initializeFailure), "lost the initialization cause: %v", registrationErr)
	require.True(t, errors.Is(registrationErr, stopFailure), "lost the stop outcome: %v", registrationErr)
	require.True(t, errors.Is(registrationErr, shutdownFailure), "lost the shutdown outcome: %v", registrationErr)
	require.False(t, managerOutputContainsCredential(registrationErr.Error(), firstFragments),
		"the joined cleanup outcome exposed the first generation's credentials: %q", registrationErr.Error())
	require.Contains(t, registrationErr.Error(), observability.RedactedPlaceholder())

	// The host's retained outcome is redacted too.
	hostOutcome := manager.lifetime().Err()
	require.Error(t, hostOutcome, "the host retained no outcome for the failed accepted initialization")
	require.False(t, managerOutputContainsCredential(hostOutcome.Error(), firstFragments),
		"the host retained outcome exposed the first generation's credentials: %q", hostOutcome.Error())

	// Retention is real: the first generation's credentials are still the
	// manager's private diagnostic context after the generation change.
	manager.mu.RLock()
	retained := append([]string(nil), manager.diagnosticCredentials...)
	manager.mu.RUnlock()
	for _, fragment := range firstFragments {
		kept := false
		for _, value := range retained {
			if value == fragment {
				kept = true
				break
			}
		}
		require.True(t, kept, "the first generation's diagnostic credential was dropped before its cleanup reported")
	}

	// The second generation publishes its own keyless context: neither the
	// retained first generation's credentials nor its own reach the plugin.
	manager.SetContext(redactedHostPluginContext(secondEndpoint, secondKey))
	observer := &publishedContextPlugin{coverageMinimalPlugin: newCoverageMinimalPlugin("published-second-generation")}
	require.NoError(t, manager.RegisterStartedPlugin(t.Context(), observer, "", false))
	secondPublished := observer.context()
	require.False(t, managerOutputContainsCredential(secondPublished.Config.APIURL, firstFragments),
		"the second generation's plugin context exposed the retained first generation's credentials")
	require.False(t, managerOutputContainsCredential(secondPublished.Config.APIURL, secondFragments),
		"the second generation's plugin context exposed its own configured credentials")
	require.Empty(t, secondPublished.Config.APIKey)
	require.Equal(t, redactedHostPluginContext(secondEndpoint, secondKey).Config.APIURL, secondPublished.Config.APIURL)

	// Host disposal reports the retained outcome and still publishes no
	// credential of either generation.
	disposed := manager.ShutdownAll()
	require.Error(t, disposed, "the host reported no retained outcome at disposal")
	require.False(t, managerOutputContainsCredential(disposed.Error(), firstFragments),
		"the host disposal exposed the first generation's credentials: %q", disposed.Error())
	require.False(t, managerOutputContainsCredential(disposed.Error(), secondFragments),
		"the host disposal exposed the second generation's credentials: %q", disposed.Error())
}
