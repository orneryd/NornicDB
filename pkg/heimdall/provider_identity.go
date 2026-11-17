package heimdall

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// Protocol selection and display identity are resolved together. The optional
// display override never changes the protocol or authentication policy. Absent
// overrides retain each diagnostic/model path's historical fallback.
type providerIdentity struct {
	input    providerIdentityInput
	protocol string
	name     string
	override bool
	source   string
}

// Factories may copy and adapt Config before delegating to another provider.
// Reuse an identity only while all inputs to its resolution are unchanged.
type providerIdentityInput struct {
	Provider, ProviderName, APIURL, APIKey string
	defaultProvider                        string
}

func (c Config) resolveProviderIdentity(defaultProvider string) Config {
	input := providerIdentityInput{Provider: c.Provider, ProviderName: c.ProviderName, APIURL: c.APIURL, APIKey: c.APIKey}
	if strings.TrimSpace(c.Provider) == "" {
		input.defaultProvider = defaultProvider
	}
	if c.identity != nil && c.identity.input == input {
		return c
	}
	protocol := strings.ToLower(strings.TrimSpace(c.Provider))
	if protocol == "" {
		protocol = defaultProvider
	}
	name := strings.TrimSpace(c.ProviderName)
	identity := &providerIdentity{input: input, protocol: protocol, name: protocol, source: "NORNICDB_HEIMDALL_PROVIDER"}
	if name != "" {
		identity.name, identity.override, identity.source = name, true, "NORNICDB_HEIMDALL_PROVIDER_NAME"
	}
	identity.name = observability.RedactText(identity.name, observability.ClientCredentials(c.APIURL, c.APIKey)...)
	c.identity = identity
	return c
}

func (i *providerIdentity) label(legacy string) string {
	if i.override {
		return i.name
	}
	return legacy
}

// ProviderName exposes the construction-owned display identity to host logging.
func (m *Manager) ProviderName() string { return m.config.identity.name }

// ProviderNameSource identifies the setting that supplied the displayed name.
func (m *Manager) ProviderNameSource() string { return m.config.identity.source }
