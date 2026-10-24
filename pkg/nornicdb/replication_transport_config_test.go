package nornicdb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const replTestSecret = "replication-secret-of-forty-characters!!"

// replTestTLSMaterial writes a self-signed certificate that is also its own
// CA, valid for 127.0.0.1, and returns the cert, key and CA paths.
func replTestTLSMaterial(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "replication-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	caFile = filepath.Join(dir, "ca.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	require.NoError(t, os.WriteFile(caFile, certPEM, 0o600))
	return certFile, keyFile, caFile
}

func replTestFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// replTestSetEnv configures an HA standby the way an operator does, through
// the NORNICDB_CLUSTER_* variables (the CLI flags feed the same ones).
func replTestSetEnv(t *testing.T, addr string, extra map[string]string) {
	t.Helper()
	env := map[string]string{
		"NORNICDB_CLUSTER_MODE":             "ha_standby",
		"NORNICDB_CLUSTER_NODE_ID":          "standby-1",
		"NORNICDB_CLUSTER_HA_ROLE":          "standby",
		"NORNICDB_CLUSTER_HA_PEER_ADDR":     "127.0.0.1:1",
		"NORNICDB_CLUSTER_HA_AUTO_FAILOVER": "false",
		"NORNICDB_CLUSTER_BIND_ADDR":        addr,
		"NORNICDB_CLUSTER_ADVERTISE_ADDR":   addr,
		"NORNICDB_CLUSTER_DATA_DIR":         t.TempDir(),
	}
	for k, v := range extra {
		env[k] = v
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
}

// replTestEnable runs the server's replication setup and shuts it down at the
// end of the test.
func replTestEnable(t *testing.T) (*DB, error) {
	t.Helper()
	db := &DB{config: DefaultConfig()}
	db.config.Database.DataDir = t.TempDir()
	_, err := db.maybeEnableReplication(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
	if err == nil {
		t.Cleanup(func() {
			_ = db.replicator.Shutdown()
			_ = db.replicationTrans.Close()
			_ = db.replicationAdapter.Close()
		})
	}
	return db, err
}

func replTestWaitListening(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "nothing listening on %s", addr)
}

// replTestHeartbeat plays a peer that reaches the replication port with the
// given secret (nil: none) and TLS client config (nil: plaintext), and
// reports whether its heartbeat was answered.
func replTestHeartbeat(t *testing.T, addr string, secret []byte, tlsClient *tls.Config) error {
	t.Helper()
	cfg := replication.DefaultClusterTransportConfig()
	cfg.NodeID = "peer-1"
	cfg.AuthSecret = secret
	cfg.TLSClient = tlsClient
	tr := replication.NewClusterTransport(cfg)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tr.Connect(ctx, addr)
	if err != nil {
		return err
	}
	_, err = conn.SendHeartbeat(ctx, &replication.HeartbeatRequest{
		NodeID: "peer-1", Role: "primary", Timestamp: time.Now().UnixNano(),
	})
	return err
}

func replTestClientTLS(t *testing.T, caFile string, withCert bool, certFile, keyFile string) *tls.Config {
	t.Helper()
	caPEM, err := os.ReadFile(caFile)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	if withCert {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		require.NoError(t, err)
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// The server's replication port applies the configured secret and TLS: it
// speaks TLS, and answers only messages signed with the secret.
func TestMaybeEnableReplication_AppliesSecretAndTLS(t *testing.T) {
	certFile, keyFile, caFile := replTestTLSMaterial(t)
	addr := replTestFreeAddr(t)
	replTestSetEnv(t, addr, map[string]string{
		"NORNICDB_CLUSTER_REPLICATION_SECRET": replTestSecret,
		"NORNICDB_CLUSTER_TLS_ENABLED":        "true",
		"NORNICDB_CLUSTER_TLS_CERT_FILE":      certFile,
		"NORNICDB_CLUSTER_TLS_KEY_FILE":       keyFile,
		"NORNICDB_CLUSTER_TLS_VERIFY_CLIENT":  "false",
	})
	_, err := replTestEnable(t)
	require.NoError(t, err)
	replTestWaitListening(t, addr)

	clientTLS := replTestClientTLS(t, caFile, false, "", "")
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", addr, clientTLS)
	require.NoError(t, err, "TLS is configured, so the replication port must speak TLS")
	_ = conn.Close()

	require.Error(t, replTestHeartbeat(t, addr, nil, nil), "plaintext unsigned heartbeat must be refused")
	require.Error(t, replTestHeartbeat(t, addr, nil, clientTLS), "unsigned heartbeat over TLS must be refused")
	require.Error(t, replTestHeartbeat(t, addr, []byte(strings.Repeat("w", 40)), clientTLS), "heartbeat signed with another secret must be refused")
	require.NoError(t, replTestHeartbeat(t, addr, []byte(replTestSecret), clientTLS), "heartbeat signed with the secret must be answered")
}

// With client verification on, a peer without a certificate from the
// configured CA is refused even when it has the secret.
func TestMaybeEnableReplication_EnforcesClientCertificate(t *testing.T) {
	certFile, keyFile, caFile := replTestTLSMaterial(t)
	addr := replTestFreeAddr(t)
	replTestSetEnv(t, addr, map[string]string{
		"NORNICDB_CLUSTER_REPLICATION_SECRET": replTestSecret,
		"NORNICDB_CLUSTER_TLS_ENABLED":        "true",
		"NORNICDB_CLUSTER_TLS_CERT_FILE":      certFile,
		"NORNICDB_CLUSTER_TLS_KEY_FILE":       keyFile,
		"NORNICDB_CLUSTER_TLS_CA_FILE":        caFile,
		"NORNICDB_CLUSTER_TLS_VERIFY_CLIENT":  "true",
	})
	_, err := replTestEnable(t)
	require.NoError(t, err)
	replTestWaitListening(t, addr)

	require.Error(t, replTestHeartbeat(t, addr, []byte(replTestSecret), replTestClientTLS(t, caFile, false, "", "")),
		"a peer without a client certificate must be refused")
	require.NoError(t, replTestHeartbeat(t, addr, []byte(replTestSecret), replTestClientTLS(t, caFile, true, certFile, keyFile)),
		"a peer with a client certificate from the CA and the secret must be answered")
}

// Security settings that are set but can't be applied stop replication from
// starting; it never falls back to plaintext or unsigned messages.
func TestMaybeEnableReplication_RefusesSecurityConfigItCannotApply(t *testing.T) {
	certFile, keyFile, caFile := replTestTLSMaterial(t)
	notPEM := filepath.Join(t.TempDir(), "not-a-ca.pem")
	require.NoError(t, os.WriteFile(notPEM, []byte("not a certificate"), 0o600))
	missing := filepath.Join(t.TempDir(), "missing.pem")

	tlsOn := func(cert, key, ca, verify string) map[string]string {
		return map[string]string{
			"NORNICDB_CLUSTER_REPLICATION_SECRET": replTestSecret,
			"NORNICDB_CLUSTER_TLS_ENABLED":        "true",
			"NORNICDB_CLUSTER_TLS_CERT_FILE":      cert,
			"NORNICDB_CLUSTER_TLS_KEY_FILE":       key,
			"NORNICDB_CLUSTER_TLS_CA_FILE":        ca,
			"NORNICDB_CLUSTER_TLS_VERIFY_CLIENT":  verify,
		}
	}
	cases := []struct {
		name string
		env  map[string]string
	}{
		{"unreadable certificate", tlsOn(missing, keyFile, "", "false")},
		{"unreadable key", tlsOn(certFile, missing, "", "false")},
		{"CA file that is not PEM", tlsOn(certFile, keyFile, notPEM, "true")},
		{"unreadable CA file", tlsOn(certFile, keyFile, missing, "true")},
		{"TLS enabled without a certificate", tlsOn("", keyFile, "", "false")},
		{"client verification without a CA", tlsOn(certFile, keyFile, "", "true")},
		{"unknown TLS version", func() map[string]string {
			env := tlsOn(certFile, keyFile, caFile, "true")
			env["NORNICDB_CLUSTER_TLS_MIN_VERSION"] = "1.1"
			return env
		}()},
		{"secret too short", map[string]string{"NORNICDB_CLUSTER_REPLICATION_SECRET": "short"}},
	}
	// A security boolean that doesn't parse is refused, never read as false.
	for _, key := range []string{
		"NORNICDB_CLUSTER_TLS_ENABLED",
		"NORNICDB_CLUSTER_TLS_VERIFY_CLIENT",
		"NORNICDB_CLUSTER_TLS_INSECURE_SKIP_VERIFY",
	} {
		for _, value := range []string{"tru", "enabled", "2"} {
			env := tlsOn(certFile, keyFile, caFile, "true")
			env[key] = value
			cases = append(cases, struct {
				name string
				env  map[string]string
			}{key + "=" + value, env})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := replTestFreeAddr(t)
			replTestSetEnv(t, addr, tc.env)
			db, err := replTestEnable(t)
			require.Error(t, err)
			require.Nil(t, db.replicator)
			require.Nil(t, db.replicationTrans)
			c, dialErr := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if dialErr == nil {
				_ = c.Close()
			}
			require.Error(t, dialErr, "nothing may listen on the replication port")
		})
	}
}
