package main

import (
	"testing"

	appconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestApplyHTTPTransportConfig(t *testing.T) {
	cfg := appconfig.LoadDefaults()
	cfg.Server.HTTPPort = 7474
	cfg.Server.HTTPSEnabled = true
	cfg.Server.HTTPSPort = 8443
	cfg.Server.HTTPTLSCert = "/tls/server.crt"
	cfg.Server.HTTPTLSKey = "/tls/server.key"
	cfg.Server.HTTPTrustedProxies = []string{"10.0.0.0/8"}

	serverConfig := server.DefaultConfig()
	applyHTTPTransportConfig(serverConfig, cfg, 7474, false)

	require.Equal(t, 8443, serverConfig.Port)
	require.Equal(t, "/tls/server.crt", serverConfig.TLSCertFile)
	require.Equal(t, "/tls/server.key", serverConfig.TLSKeyFile)
	require.Equal(t, []string{"10.0.0.0/8"}, serverConfig.TrustedProxies)
}

func TestApplyHTTPTransportConfigHonorsExplicitPort(t *testing.T) {
	cfg := appconfig.LoadDefaults()
	cfg.Server.HTTPSEnabled = true
	cfg.Server.HTTPSPort = 8443

	serverConfig := server.DefaultConfig()
	applyHTTPTransportConfig(serverConfig, cfg, 9443, true)

	require.Equal(t, 9443, serverConfig.Port)
}
