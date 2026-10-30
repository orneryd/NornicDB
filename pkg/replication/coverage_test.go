package replication

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigHelpers_IsStandaloneAndString(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Mode:     ModeStandalone,
		NodeID:   "node-a",
		BindAddr: "127.0.0.1:7000",
	}

	assert.True(t, cfg.IsStandalone())
	assert.Contains(t, cfg.String(), "Mode: standalone")
	assert.Contains(t, cfg.String(), "NodeID: node-a")
	assert.Contains(t, cfg.String(), "Bind: 127.0.0.1:7000")

	cfg.Mode = ModeRaft
	assert.False(t, cfg.IsStandalone())
}

func TestParseRemoteRegions(t *testing.T) {
	t.Parallel()

	regions := parseRemoteRegions("us-east:host1:7000, eu-west:host2:7000, us-east:host3:7000, invalid")
	require.Len(t, regions, 2)

	assert.Equal(t, "us-east", regions[0].RegionID)
	assert.Equal(t, []string{"host1:7000", "host3:7000"}, regions[0].Addrs)
	assert.Equal(t, 1, regions[0].Priority)

	assert.Equal(t, "eu-west", regions[1].RegionID)
	assert.Equal(t, []string{"host2:7000"}, regions[1].Addrs)
	assert.Equal(t, 2, regions[1].Priority)

	assert.Nil(t, parseRemoteRegions(""))
}

func TestParseCSV(t *testing.T) {
	t.Parallel()

	assert.Nil(t, parseCSV(""))
	assert.Equal(t, []string{"a", "b", "c"}, parseCSV(" a, b ,, c "))
}

func TestParseTLSVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    uint16
		wantErr string
	}{
		{name: "default", input: "", want: tls.VersionTLS12},
		{name: "tls12", input: "1.2", want: tls.VersionTLS12},
		{name: "tls13", input: "1.3", want: tls.VersionTLS13},
		{name: "invalid", input: "1.1", wantErr: "invalid TLS min version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTLSVersion(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCipherSuites(t *testing.T) {
	t.Parallel()

	suites, err := parseCipherSuites(nil)
	require.NoError(t, err)
	assert.Nil(t, suites)

	suites, err = parseCipherSuites([]string{"TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256"})
	require.NoError(t, err)
	require.Len(t, suites, 2)
	assert.Equal(t, tls.TLS_AES_128_GCM_SHA256, suites[0])
	assert.Equal(t, tls.TLS_CHACHA20_POLY1305_SHA256, suites[1])

	_, err = parseCipherSuites([]string{"TLS_UNKNOWN_CIPHER"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown TLS cipher suite")
}

func TestBuildTLSConfigs_SuccessAndErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certFile, keyFile, caFile := writeSelfSignedTLSMaterial(t, dir)

	cfg := TLSConfig{
		CertFile:           certFile,
		KeyFile:            keyFile,
		CAFile:             caFile,
		MinVersion:         "1.3",
		CipherSuites:       []string{"TLS_AES_128_GCM_SHA256"},
		VerifyClient:       true,
		InsecureSkipVerify: true,
		ServerName:         "localhost",
	}

	serverTLS, clientTLS, err := buildTLSConfigs(cfg)
	require.NoError(t, err)
	require.NotNil(t, serverTLS)
	require.NotNil(t, clientTLS)
	assert.Equal(t, uint16(tls.VersionTLS13), serverTLS.MinVersion)
	assert.Equal(t, tls.RequireAndVerifyClientCert, serverTLS.ClientAuth)
	require.Len(t, serverTLS.Certificates, 1)
	require.Len(t, serverTLS.CipherSuites, 1)
	assert.Equal(t, tls.TLS_AES_128_GCM_SHA256, serverTLS.CipherSuites[0])

	assert.Equal(t, uint16(tls.VersionTLS13), clientTLS.MinVersion)
	assert.True(t, clientTLS.InsecureSkipVerify)
	assert.Equal(t, "localhost", clientTLS.ServerName)
	require.Len(t, clientTLS.CipherSuites, 1)
	assert.Equal(t, tls.TLS_AES_128_GCM_SHA256, clientTLS.CipherSuites[0])

	_, _, err = buildTLSConfigs(TLSConfig{CertFile: "missing.pem", KeyFile: "missing.key"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load TLS cert/key")

	badCA := filepath.Join(dir, "bad-ca.pem")
	require.NoError(t, os.WriteFile(badCA, []byte("not a cert"), 0o600))

	_, _, err = buildTLSConfigs(TLSConfig{CertFile: certFile, KeyFile: keyFile, CAFile: badCA})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid TLS CA file")
}

func TestClusterConnectionSignAndVerify(t *testing.T) {
	t.Parallel()

	conn := &ClusterConnection{
		transport:   &ClusterTransport{nodeID: "node-signer"},
		authSecret:  []byte("top-secret"),
		authMaxSkew: time.Minute,
	}

	msg := &ClusterMessage{
		Type:    ClusterMsgHeartbeat,
		Payload: []byte("payload"),
	}

	conn.signMessage(msg)
	require.NotEmpty(t, msg.NodeID)
	require.NotZero(t, msg.Timestamp)
	require.NotEmpty(t, msg.SignatureV2)
	require.NoError(t, conn.verifyMessage(msg))

	tampered := *msg
	tampered.Payload = []byte("tampered")
	err := conn.verifyMessage(&tampered)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid signature")

	missing := &ClusterMessage{Type: ClusterMsgHeartbeat}
	err = conn.verifyMessage(missing)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing authentication fields")

	old := &ClusterMessage{
		Type:      ClusterMsgHeartbeat,
		NodeID:    "node-signer",
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		Payload:   []byte("payload"),
	}
	conn.signMessage(old)
	err = conn.verifyMessage(old)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timestamp outside allowed skew")
}

func TestComputeMessageSignatureAndWireHelpers(t *testing.T) {
	t.Parallel()

	secret := []byte("test-secret")
	msg := &ClusterMessage{
		Type:      ClusterMsgForwardApply,
		NodeID:    "node-1",
		Timestamp: 12345,
		Payload:   []byte("abc"),
	}

	sig1 := computeMessageSignatureV2(secret, msg)
	sig2 := computeMessageSignatureV2(secret, msg)
	assert.Equal(t, sig1, sig2)

	msg.Payload = []byte("abcd")
	sig3 := computeMessageSignatureV2(secret, msg)
	assert.NotEqual(t, sig1, sig3)

	var buf bytes.Buffer
	writeInt64(&buf, 0x0102030405060708)
	writeStringWithLength(&buf, "go")
	writeBytesWithLength(&buf, []byte{0xAA, 0xBB})

	got := buf.Bytes()
	require.GreaterOrEqual(t, len(got), 20)
	assert.Equal(t, uint64(0x0102030405060708), binary.BigEndian.Uint64(got[0:8]))
	assert.Equal(t, uint32(2), binary.BigEndian.Uint32(got[8:12]))
	assert.Equal(t, "go", string(got[12:14]))
	assert.Equal(t, uint32(2), binary.BigEndian.Uint32(got[14:18]))
	assert.Equal(t, []byte{0xAA, 0xBB}, got[18:20])

	buf.Reset()
	writeBytesWithLength(&buf, nil)
	encodedNil := buf.Bytes()
	require.Len(t, encodedNil, 4)
	assert.Equal(t, uint32(0), binary.BigEndian.Uint32(encodedNil))
}

func writeSelfSignedTLSMaterial(t *testing.T, dir string) (certFile, keyFile, caFile string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	certFile = filepath.Join(dir, "tls-cert.pem")
	keyFile = filepath.Join(dir, "tls-key.pem")
	caFile = filepath.Join(dir, "tls-ca.pem")

	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	require.NoError(t, os.WriteFile(caFile, certPEM, 0o600))

	for _, p := range []string{certFile, keyFile, caFile} {
		content, readErr := os.ReadFile(p)
		require.NoError(t, readErr)
		require.NotEmpty(t, strings.TrimSpace(string(content)))
	}

	return certFile, keyFile, caFile
}
