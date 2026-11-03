package heimdall

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// redactedHostPluginContext mirrors the supported publication in
// pkg/server/heimdall_plugins.go: the host keeps the configured client in its
// own private diagnostic context, and plugins receive host metadata holding the
// redacted copy with no API key.
func redactedHostPluginContext(apiURL, apiKey string) SubsystemHost {
	credentials := observability.ClientCredentials(apiURL, apiKey)
	return SubsystemHost{
		Config: Config{
			APIURL: observability.RedactText(apiURL, credentials...),
			APIKey: "",
		},
		Bifrost: &NoOpBifrost{},
	}
}

// unpublishedCleanupCredentialPlugin fails its own initialization or start and
// then reports credential-bearing stop/shutdown failures from the unpublished
// cleanup that registration performs for the instance it constructed. The
// counters report how often that cleanup reached each resource it owns.
type unpublishedCleanupCredentialPlugin struct {
	*coverageMinimalPlugin
	initializeErr error
	startErr      error
	stopErr       error
	shutdownErr   error
	stops         atomic.Int32
	shutdowns     atomic.Int32
}

func (p *unpublishedCleanupCredentialPlugin) Initialize(SubsystemContext) error {
	return p.initializeErr
}
func (p *unpublishedCleanupCredentialPlugin) Start() error { return p.startErr }
func (p *unpublishedCleanupCredentialPlugin) Stop() error {
	p.stops.Add(1)
	return p.stopErr
}
func (p *unpublishedCleanupCredentialPlugin) Shutdown() error {
	p.shutdowns.Add(1)
	return p.shutdownErr
}

// A plugin whose Initialize or Start fails never publishes, so the caller only
// sees the registration error: the plugin's own failure plus the stop/join/
// dispose outcomes of the unpublished instance. All of them carry the configured
// client credentials here, and the supported registration path must publish
// none of them while keeping every cause classifiable.
func TestSubsystemRegistrationRedactsUnpublishedCleanupErrors(t *testing.T) {
	endpoint := "https://synthetic-unpublished-user:synthetic-unpublished-pass@example.invalid/v1"
	apiKey := "synthetic-unpublished-key"
	fragments := observability.ClientCredentials(endpoint, apiKey)
	require.NotEmpty(t, fragments)

	initializeFailure := errors.New("synthetic initialize failure")
	startFailure := errors.New("synthetic start failure")
	stopFailure := errors.New("synthetic stop failure")
	shutdownFailure := errors.New("synthetic shutdown failure")

	for _, tc := range []struct {
		name    string
		start   bool
		failure error
	}{
		{name: "initialize", start: false, failure: initializeFailure},
		{name: "start", start: true, failure: startFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newMatrixSubsystemManager()
			// This test owns the manager: every exit completes it. A failed
			// registration retains its redacted outcome in the manager, so its
			// disposal is expected to report that outcome rather than being
			// dropped by a later assertion failure.
			var registrationErr error
			t.Cleanup(func() {
				disposed := manager.ShutdownAll()
				if registrationErr == nil {
					require.NoError(t, disposed, "an unused test-owned manager reported a disposal failure")
					return
				}
				require.Error(t, disposed, "the test-owned manager dropped the failed registration outcome")
				require.False(t, managerOutputContainsCredential(disposed.Error(), fragments),
					"the test-owned manager's disposal exposed a configured credential: %q", disposed.Error())
				require.Contains(t, disposed.Error(), observability.RedactedPlaceholder())
			})
			// The supported host path: the configured client reaches only the
			// manager's private diagnostic context, the published context is
			// already redacted and keyless.
			manager.SetHostContext(SubsystemHost{
				Config:  Config{APIURL: endpoint, APIKey: apiKey},
				Bifrost: &NoOpBifrost{},
			})
			manager.SetContext(redactedHostPluginContext(endpoint, apiKey))
			// The cleanup of the unpublished instance redacts with the host
			// configuration it was registered under, which holds no credential:
			// only the manager's retained diagnostic credentials can remove the
			// values below from the reported error.
			manager.mu.RLock()
			hostConfig := manager.host.Config
			manager.mu.RUnlock()
			require.Empty(t, hostConfig.APIKey)
			require.False(t, managerOutputContainsCredential(hostConfig.APIURL, fragments),
				"the published plugin context still carried the configured client")

			name := "unpublished-cleanup-" + tc.name
			stopErr := fmt.Errorf("synthetic stop for %s key %s: %w", endpoint, apiKey, stopFailure)
			shutdownErr := fmt.Errorf("synthetic shutdown for %s key %s: %w", endpoint, apiKey, shutdownFailure)
			primaryErr := fmt.Errorf("synthetic %s for %s key %s: %w", tc.name, endpoint, apiKey, tc.failure)
			plugin := &unpublishedCleanupCredentialPlugin{
				coverageMinimalPlugin: newCoverageMinimalPlugin(name),
				stopErr:               stopErr,
				shutdownErr:           shutdownErr,
			}
			if tc.start {
				plugin.startErr = primaryErr
			} else {
				plugin.initializeErr = primaryErr
			}
			// The synthetic failures themselves carry the configured client.
			for _, raw := range []error{primaryErr, stopErr, shutdownErr} {
				require.True(t, managerOutputContainsCredential(raw.Error(), fragments),
					"synthetic failure did not carry the configured credentials: %q", raw.Error())
			}

			registrationErr = manager.RegisterStartedPlugin(t.Context(), plugin, "", false)
			require.Error(t, registrationErr, "the unpublished registration reported no failure")
			// Redaction must not drop causes: the primary failure and both
			// cleanup outcomes stay classifiable through the published error.
			require.True(t, errors.Is(registrationErr, tc.failure), "lost the primary failure classification: %v", registrationErr)
			require.True(t, errors.Is(registrationErr, stopFailure), "lost the stop outcome classification: %v", registrationErr)
			require.True(t, errors.Is(registrationErr, shutdownFailure), "lost the shutdown outcome classification: %v", registrationErr)
			require.Contains(t, registrationErr.Error(), "failed to stop "+name)
			require.Contains(t, registrationErr.Error(), "failed to shutdown "+name)
			require.False(t, managerOutputContainsCredential(registrationErr.Error(), fragments),
				"registration error exposed a configured credential: %q", registrationErr.Error())
			require.Contains(t, registrationErr.Error(), observability.RedactedPlaceholder())
			// The failed registration's cleanup completed every resource of the
			// unpublished instance exactly once.
			require.EqualValues(t, 1, plugin.stops.Load(),
				"the failed registration did not stop its unpublished instance exactly once")
			require.EqualValues(t, 1, plugin.shutdowns.Load(),
				"the failed registration did not dispose its unpublished instance exactly once")
			// The failed initialization is unpublished: no registration remains.
			_, published := manager.GetPlugin(name)
			require.False(t, published, "a failed unpublished registration was published")
			manager.mu.RLock()
			registered, running := len(manager.plugins), len(manager.actions)
			manager.mu.RUnlock()
			require.Zero(t, registered)
			require.Zero(t, running)
		})
	}
}
