package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSecurityConfiguration(t *testing.T) {
	require.ErrorContains(t, ValidateSecurityConfiguration(nil), "config is required")

	secure := func() *Config {
		config := LoadDefaults()
		config.Server.Environment = "production"
		config.Auth.Enabled = true
		config.Auth.InitialPassword = "operator-supplied-secret"
		config.Server.EnableCORS = true
		config.Server.CORSOrigins = []string{"https://console.example.com"}
		config.Server.HTTPAddress = "127.0.0.1"
		config.Server.BoltAddress = "127.0.0.1"
		config.Features.QdrantGRPCEnabled = false
		return config
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{name: "secure", mutate: func(*Config) {}},
		{name: "explicit no auth remains allowed", mutate: func(c *Config) { c.Auth.Enabled = false }},
		{name: "no auth ignores empty password", mutate: func(c *Config) { c.Auth.Enabled = false; c.Auth.InitialPassword = "" }},
		{name: "no auth ignores default password", mutate: func(c *Config) { c.Auth.Enabled = false; c.Auth.InitialPassword = "password" }},
		{name: "default password is allowed", mutate: func(c *Config) { c.Auth.InitialPassword = "password" }},
		{name: "empty password", mutate: func(c *Config) { c.Auth.InitialPassword = "" }, match: "initial password"},
		{name: "loopback wildcard cors", mutate: func(c *Config) { c.Server.CORSOrigins = []string{"*"} }},
		{name: "public wildcard cors", mutate: func(c *Config) { c.Server.CORSOrigins = []string{"*"}; c.Server.HTTPAddress = "0.0.0.0" }, match: "wildcard CORS"},
		{name: "public http", mutate: func(c *Config) { c.Server.HTTPAddress = "0.0.0.0" }, match: "plaintext HTTP"},
		{name: "public http behind trusted proxy", mutate: func(c *Config) {
			c.Server.HTTPAddress = "0.0.0.0"
			c.Server.HTTPTrustedProxies = []string{"10.0.0.0/8"}
		}},
		{name: "invalid trusted proxy", mutate: func(c *Config) { c.Server.HTTPTrustedProxies = []string{"not-a-cidr"} }, match: "trusted proxy"},
		{name: "native https", mutate: func(c *Config) {
			c.Server.HTTPAddress = "0.0.0.0"
			c.Server.HTTPSEnabled = true
			c.Server.HTTPTLSCert = "/tls/server.crt"
			c.Server.HTTPTLSKey = "/tls/server.key"
		}},
		{name: "https missing certificate", mutate: func(c *Config) {
			c.Server.HTTPSEnabled = true
			c.Server.HTTPTLSKey = "/tls/server.key"
		}, match: "HTTPS certificate and key"},
		{name: "public bolt without required tls", mutate: func(c *Config) { c.Server.BoltAddress = "192.0.2.1" }, match: "must require TLS"},
		{name: "public bolt requires configured tls", mutate: func(c *Config) {
			c.Server.BoltAddress = "192.0.2.1"
			c.Server.BoltTLSRequire = true
		}, match: "Bolt TLS certificate and key"},
		{name: "public bolt with native tls", mutate: func(c *Config) {
			c.Server.BoltAddress = "192.0.2.1"
			c.Server.BoltTLSEnabled = true
			c.Server.BoltTLSRequire = true
			c.Server.BoltTLSCert = "/tls/bolt.crt"
			c.Server.BoltTLSKey = "/tls/bolt.key"
		}},
		{name: "public grpc without tls", mutate: func(c *Config) { c.Features.QdrantGRPCEnabled = true; c.Features.QdrantGRPCListenAddr = ":6334" }, match: "must use TLS"},
		{name: "grpc tls missing certificate", mutate: func(c *Config) {
			c.Features.QdrantGRPCEnabled = true
			c.Features.QdrantGRPCListenAddr = "127.0.0.1:6334"
			c.Features.QdrantGRPCTLSEnabled = true
			c.Features.QdrantGRPCTLSKey = "/tls/grpc.key"
		}, match: "gRPC TLS certificate and key"},
		{name: "public grpc with native tls", mutate: func(c *Config) {
			c.Features.QdrantGRPCEnabled = true
			c.Features.QdrantGRPCListenAddr = ":6334"
			c.Features.QdrantGRPCTLSEnabled = true
			c.Features.QdrantGRPCTLSCert = "/tls/grpc.crt"
			c.Features.QdrantGRPCTLSKey = "/tls/grpc.key"
		}},
		{name: "grpc mtls missing client ca", mutate: func(c *Config) {
			c.Features.QdrantGRPCEnabled = true
			c.Features.QdrantGRPCListenAddr = ":6334"
			c.Features.QdrantGRPCTLSEnabled = true
			c.Features.QdrantGRPCTLSCert = "/tls/grpc.crt"
			c.Features.QdrantGRPCTLSKey = "/tls/grpc.key"
			c.Features.QdrantGRPCTLSClientAuthMode = "require_verify"
		}, match: "gRPC TLS client CA"},
		{name: "grpc invalid client auth mode", mutate: func(c *Config) {
			c.Features.QdrantGRPCEnabled = true
			c.Features.QdrantGRPCListenAddr = ":6334"
			c.Features.QdrantGRPCTLSEnabled = true
			c.Features.QdrantGRPCTLSCert = "/tls/grpc.crt"
			c.Features.QdrantGRPCTLSKey = "/tls/grpc.key"
			c.Features.QdrantGRPCTLSClientAuthMode = "invalid"
		}, match: "gRPC TLS client auth mode"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := secure()
			test.mutate(config)
			err := ValidateSecurityConfiguration(config)
			if test.match == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.match)
			}
		})
	}
}

func TestValidateSecurityConfigurationAllowsDocumentedDefaults(t *testing.T) {
	config := LoadDefaults()
	config.Server.HTTPAddress = "127.0.0.1"
	config.Server.BoltAddress = "127.0.0.1"

	require.NoError(t, ValidateSecurityConfiguration(config))
}

func TestValidateSecurityConfigurationAppliesToEveryEnvironment(t *testing.T) {
	for _, environment := range []string{"", "development", "test"} {
		t.Run(environment, func(t *testing.T) {
			config := LoadDefaults()
			config.Server.Environment = environment
			config.Auth.Enabled = true
			config.Auth.InitialPassword = "operator-supplied-secret"
			config.Server.EnableCORS = true
			config.Server.CORSOrigins = []string{"*"}
			config.Server.HTTPAddress = "0.0.0.0"

			require.ErrorContains(t, ValidateSecurityConfiguration(config), "wildcard CORS")
		})
	}
}

func TestValidateSecurityConfigurationAllowsExplicitNoAuthContainerStartup(t *testing.T) {
	for _, environment := range []string{"", "development", "production"} {
		t.Run(environment, func(t *testing.T) {
			config := LoadDefaults()
			config.Server.Environment = environment
			config.Auth.Enabled = false
			config.Server.EnableCORS = true
			config.Server.CORSOrigins = []string{"*"}
			config.Server.HTTPAddress = "0.0.0.0"
			config.Server.BoltAddress = "0.0.0.0"
			config.Server.BoltTLSRequire = false

			require.NoError(t, ValidateSecurityConfiguration(config))
		})
	}
}

func TestValidateSecurityConfigurationRejectsIncompleteHTTPSWithoutAuth(t *testing.T) {
	config := LoadDefaults()
	config.Auth.Enabled = false
	config.Server.HTTPSEnabled = true
	config.Server.HTTPTLSCert = "/tls/server.crt"

	require.ErrorContains(t, ValidateSecurityConfiguration(config), "HTTPS certificate and key")
}
