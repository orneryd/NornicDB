package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateProductionSecurity(t *testing.T) {
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
		{name: "no auth", mutate: func(c *Config) { c.Auth.Enabled = false }, match: "authentication"},
		{name: "default password", mutate: func(c *Config) { c.Auth.InitialPassword = "password" }, match: "initial password"},
		{name: "wildcard cors", mutate: func(c *Config) { c.Server.CORSOrigins = []string{"*"} }, match: "wildcard CORS"},
		{name: "public http", mutate: func(c *Config) { c.Server.HTTPAddress = "0.0.0.0" }, match: "plaintext HTTP"},
		{name: "public bolt without required tls", mutate: func(c *Config) { c.Server.BoltAddress = "192.0.2.1" }, match: "must require TLS"},
		{name: "public grpc", mutate: func(c *Config) { c.Features.QdrantGRPCEnabled = true; c.Features.QdrantGRPCListenAddr = ":6334" }, match: "plaintext gRPC"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := secure()
			test.mutate(config)
			err := ValidateProductionSecurity(config)
			if test.match == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.match)
			}
		})
	}
}

func TestValidateProductionSecurityPreservesDevelopmentDefaults(t *testing.T) {
	require.NoError(t, ValidateProductionSecurity(LoadDefaults()))
}
