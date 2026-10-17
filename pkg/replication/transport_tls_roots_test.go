package replication

import (
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Without a CA file, outbound connections verify peers against the system
// roots: RootCAs stays nil, not an empty pool that trusts nothing.
func TestBuildTLSConfigs_NoCAFileUsesSystemRoots(t *testing.T) {
	certFile, keyFile, _ := writeSelfSignedTLSMaterial(t, t.TempDir())
	serverTLS, clientTLS, err := buildTLSConfigs(TLSConfig{CertFile: certFile, KeyFile: keyFile, VerifyClient: false})
	require.NoError(t, err)
	require.Nil(t, clientTLS.RootCAs, "no CA file must mean the system roots")
	require.Nil(t, serverTLS.ClientCAs)
}

// A configured CA file is the only trust root, outbound and for client
// certificates.
func TestBuildTLSConfigs_CAFileIsTheOnlyTrustRoot(t *testing.T) {
	certFile, keyFile, caFile := writeSelfSignedTLSMaterial(t, t.TempDir())
	serverTLS, clientTLS, err := buildTLSConfigs(TLSConfig{CertFile: certFile, KeyFile: keyFile, CAFile: caFile, VerifyClient: true})
	require.NoError(t, err)

	caPEM, err := os.ReadFile(caFile)
	require.NoError(t, err)
	want := x509.NewCertPool()
	require.True(t, want.AppendCertsFromPEM(caPEM))
	require.NotNil(t, clientTLS.RootCAs)
	require.True(t, want.Equal(clientTLS.RootCAs))
	require.True(t, want.Equal(serverTLS.ClientCAs))
}

// Client verification has no system-root fallback, and a CA file that
// can't be loaded fails closed.
func TestBuildTLSConfigs_CAFailuresFailClosed(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, _ := writeSelfSignedTLSMaterial(t, dir)
	notPEM := filepath.Join(dir, "not-a-ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))

	for name, cfg := range map[string]TLSConfig{
		"client verification without a CA file": {CertFile: certFile, KeyFile: keyFile, VerifyClient: true},
		"unreadable CA file":                    {CertFile: certFile, KeyFile: keyFile, CAFile: filepath.Join(dir, "missing.pem")},
		"CA file that is not PEM":               {CertFile: certFile, KeyFile: keyFile, CAFile: notPEM},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := buildTLSConfigs(cfg)
			require.Error(t, err)
		})
	}
}
