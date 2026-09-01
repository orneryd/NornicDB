package replication

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// NewDefaultTransportFromConfig creates a ClusterTransport using replication config.
func NewDefaultTransportFromConfig(cfg *Config) (Transport, error) {
	if cfg == nil {
		return NewClusterTransport(nil), nil
	}

	transportCfg := DefaultClusterTransportConfig()
	transportCfg.NodeID = cfg.NodeID
	transportCfg.BindAddr = cfg.BindAddr

	if cfg.ReplicationSecret != "" {
		transportCfg.AuthSecret = []byte(cfg.ReplicationSecret)
	}

	if cfg.TLS.Enabled {
		serverTLS, clientTLS, err := buildTLSConfigs(cfg.TLS)
		if err != nil {
			return nil, err
		}
		transportCfg.TLSServer = serverTLS
		transportCfg.TLSClient = clientTLS
	}

	if transportCfg.AuthMaxSkew == 0 {
		transportCfg.AuthMaxSkew = 30 * time.Second
	}

	return NewClusterTransport(transportCfg), nil
}

func buildTLSConfigs(cfg TLSConfig) (*tls.Config, *tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, localizedError(localization.ReplicationTransportLoadTLSCertKeyFailed(err), err)
	}

	rootCAs := x509.NewCertPool()
	if cfg.CAFile != "" {
		caBytes, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, nil, localizedError(localization.ReplicationTransportReadTLSCAFailed(err), err)
		}
		if !rootCAs.AppendCertsFromPEM(caBytes) {
			return nil, nil, localizedError(localization.ReplicationTransportInvalidTLSCA(), nil)
		}
	}

	minVersion, err := parseTLSVersion(cfg.MinVersion)
	if err != nil {
		return nil, nil, err
	}

	cipherSuites, err := parseCipherSuites(cfg.CipherSuites)
	if err != nil {
		return nil, nil, err
	}

	serverTLS := &tls.Config{
		MinVersion:   minVersion,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    rootCAs,
	}
	if len(cipherSuites) > 0 {
		serverTLS.CipherSuites = cipherSuites
	}
	if cfg.VerifyClient {
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		serverTLS.ClientAuth = tls.NoClientCert
	}

	clientTLS := &tls.Config{
		MinVersion:         minVersion,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		RootCAs:            rootCAs,
		ServerName:         cfg.ServerName,
		Certificates:       []tls.Certificate{cert},
	}
	if len(cipherSuites) > 0 {
		clientTLS.CipherSuites = cipherSuites
	}

	return serverTLS, clientTLS, nil
}

func parseTLSVersion(version string) (uint16, error) {
	switch version {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, localizedError(localization.ReplicationConfigInvalidTLSMinVersion(version), nil)
	}
}

func parseCipherSuites(values []string) ([]uint16, error) {
	if len(values) == 0 {
		return nil, nil
	}

	cipherMap := map[string]uint16{
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		"TLS_AES_128_GCM_SHA256":                  tls.TLS_AES_128_GCM_SHA256,
		"TLS_AES_256_GCM_SHA384":                  tls.TLS_AES_256_GCM_SHA384,
		"TLS_CHACHA20_POLY1305_SHA256":            tls.TLS_CHACHA20_POLY1305_SHA256,
	}

	out := make([]uint16, 0, len(values))
	for _, v := range values {
		if id, ok := cipherMap[v]; ok {
			out = append(out, id)
			continue
		}
		return nil, localizedError(localization.ReplicationTransportUnknownTLSCipherSuite(v), nil)
	}
	return out, nil
}
