package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var securityBoolKeys = []string{
	"NORNICDB_CLUSTER_TLS_ENABLED",
	"NORNICDB_CLUSTER_TLS_VERIFY_CLIENT",
	"NORNICDB_CLUSTER_TLS_INSECURE_SKIP_VERIFY",
}

func securityBoolTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
	t.Setenv("NORNICDB_CLUSTER_NODE_ID", "standby-1")
	t.Setenv("NORNICDB_CLUSTER_HA_ROLE", "standby")
	t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "primary-1:7000")
	// Valid TLS material paths, so only the boolean decides the outcome.
	t.Setenv("NORNICDB_CLUSTER_TLS_CERT_FILE", "/tls/cert.pem")
	t.Setenv("NORNICDB_CLUSTER_TLS_KEY_FILE", "/tls/key.pem")
	t.Setenv("NORNICDB_CLUSTER_TLS_CA_FILE", "/tls/ca.pem")
}

func securityBoolValue(c *Config, key string) bool {
	switch key {
	case "NORNICDB_CLUSTER_TLS_ENABLED":
		return c.TLS.Enabled
	case "NORNICDB_CLUSTER_TLS_VERIFY_CLIENT":
		return c.TLS.VerifyClient
	default:
		return c.TLS.InsecureSkipVerify
	}
}

// Each security boolean accepts the usual true and false words.
func TestLoadFromEnv_SecurityBooleansParse(t *testing.T) {
	cases := map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, " On ": true,
		"false": false, "False": false, "0": false, "no": false, "OFF": false,
	}
	for _, key := range securityBoolKeys {
		for value, want := range cases {
			t.Run(key+"="+value, func(t *testing.T) {
				securityBoolTestEnv(t)
				t.Setenv(key, value)
				cfg := LoadFromEnv()
				require.NoError(t, cfg.Validate())
				require.Equal(t, want, securityBoolValue(cfg, key))
			})
		}
	}
}

// A security boolean that doesn't parse makes Validate fail, naming the
// setting, instead of reading as false.
func TestLoadFromEnv_MalformedSecurityBooleanFailsValidation(t *testing.T) {
	for _, key := range securityBoolKeys {
		for _, value := range []string{"tru", "enabled", "2", "yes please"} {
			t.Run(key+"="+value, func(t *testing.T) {
				securityBoolTestEnv(t)
				t.Setenv("NORNICDB_CLUSTER_TLS_ENABLED", "true")
				t.Setenv(key, value)
				err := LoadFromEnv().Validate()
				require.Error(t, err)
				require.Contains(t, err.Error(), key)
			})
		}
	}
}
