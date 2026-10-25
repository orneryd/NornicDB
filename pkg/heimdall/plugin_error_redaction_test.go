package heimdall

import (
	"errors"
	"strings"
	"testing"
)

type credentialErrorPlugin struct {
	*MockHeimdallPlugin
	endpoint string
	apiKey   string
}

func (p *credentialErrorPlugin) Stop() error {
	return errors.New("plugin stop failed: " + p.endpoint + " key " + p.apiKey)
}

func (p *credentialErrorPlugin) Shutdown() error {
	return errors.New("plugin shutdown failed: " + p.endpoint + " key " + p.apiKey)
}

func TestSubsystemLifecycleErrorsRedactConfiguredCredentials(t *testing.T) {
	endpoint := "https://synthetic-user:synthetic-pass@example.invalid/v1"
	apiKey := "synthetic-api-key"
	manager := &SubsystemManager{plugins: make(map[string]*LoadedHeimdallPlugin), actions: make(map[string]ActionFunc)}
	manager.SetContext(SubsystemContext{Config: Config{APIURL: endpoint, APIKey: apiKey}, Bifrost: &NoOpBifrost{}})
	plugin := &credentialErrorPlugin{MockHeimdallPlugin: NewMockPlugin("credential-redaction"), endpoint: endpoint, apiKey: apiKey}
	if err := manager.RegisterPlugin(plugin, "synthetic-plugin", true); err != nil {
		t.Fatal("failed to register the test plugin")
	}

	for _, err := range []error{manager.StopAll(), manager.ShutdownAll()} {
		if err == nil {
			t.Fatal("expected the test plugin to return a lifecycle error")
		}
		for _, value := range []string{"synthetic-user", "synthetic-pass", apiKey} {
			if strings.Contains(err.Error(), value) {
				t.Fatal("plugin lifecycle error exposed a credential")
			}
		}
	}
}
