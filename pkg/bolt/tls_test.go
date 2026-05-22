package bolt

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateSelfSignedPEM returns PEM-encoded cert+key bytes for a
// self-signed certificate whose Subject.CommonName is set to cn. It is
// used to make cert A vs cert B trivially distinguishable in rotation
// tests.
func generateSelfSignedPEM(t *testing.T, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		DNSNames:              []string{cn},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// writeCertKey writes cert+key PEM bytes to certPath/keyPath inside dir
// and returns the absolute paths.
func writeCertKey(t *testing.T, dir string, certPEM, keyPEM []byte) (certPath, keyPath string) {
	t.Helper()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// commonName returns the Subject.CommonName of the leaf certificate
// referenced by the supplied tls.Certificate. It is used to assert which
// underlying cert (A or B) the rotator currently serves.
func commonName(t *testing.T, cert *tls.Certificate) string {
	t.Helper()
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatalf("nil or empty cert")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf.Subject.CommonName
}

func TestParseClientAuthMode(t *testing.T) {
	cases := []struct {
		in      string
		want    ClientAuthMode
		wantErr bool
	}{
		{"", ClientAuthNone, false},
		{"none", ClientAuthNone, false},
		{"request", ClientAuthRequest, false},
		{"request_verify", ClientAuthRequestVerify, false},
		{"require_verify", ClientAuthRequireVerify, false},
		{"garbage", ClientAuthNone, true},
	}
	for _, c := range cases {
		got, err := ParseClientAuthMode(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseClientAuthMode(%q) expected error, got nil", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseClientAuthMode(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseClientAuthMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestLoadTLSConfig_Success(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedPEM(t, "cert-A")
	certPath, keyPath := writeCertKey(t, dir, certPEM, keyPEM)

	cfg, err := LoadTLSConfig(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want %x", cfg.MinVersion, tls.VersionTLS12)
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("Certificates expected empty, got %d", len(cfg.Certificates))
	}
	if cfg.GetCertificate == nil {
		t.Fatalf("GetCertificate is nil")
	}
	got, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if cn := commonName(t, got); cn != "cert-A" {
		t.Errorf("common name = %q, want cert-A", cn)
	}
}

func TestLoadTLSConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadTLSConfig(filepath.Join(dir, "nope.pem"), filepath.Join(dir, "nope.key"))
	if err == nil {
		t.Fatalf("expected error for missing files")
	}
}

func TestLoadTLSConfig_MalformedPEM(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, []byte("not a pem"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte("not a pem either"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTLSConfig(certPath, keyPath); err == nil {
		t.Fatalf("expected error for malformed PEM")
	}
}

func TestLoadTLSConfigWithClientCA_RequireVerify(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedPEM(t, "server-cn")
	certPath, keyPath := writeCertKey(t, dir, certPEM, keyPEM)
	caPEM, _ := generateSelfSignedPEM(t, "client-ca")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadTLSConfigWithClientCA(certPath, keyPath, caPath, ClientAuthRequireVerify)
	if err != nil {
		t.Fatalf("LoadTLSConfigWithClientCA: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("ClientAuth = %v, want RequireAndVerifyClientCert", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Errorf("ClientCAs is nil")
	}
}

func TestLoadTLSConfigWithClientCA_AllModes(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM := generateSelfSignedPEM(t, "server-cn")
	certPath, keyPath := writeCertKey(t, dir, certPEM, keyPEM)
	caPEM, _ := generateSelfSignedPEM(t, "client-ca")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		mode ClientAuthMode
		want tls.ClientAuthType
	}{
		{ClientAuthNone, tls.NoClientCert},
		{ClientAuthRequest, tls.RequestClientCert},
		{ClientAuthRequestVerify, tls.VerifyClientCertIfGiven},
		{ClientAuthRequireVerify, tls.RequireAndVerifyClientCert},
	}
	for _, c := range cases {
		cfg, err := LoadTLSConfigWithClientCA(certPath, keyPath, caPath, c.mode)
		if err != nil {
			t.Fatalf("mode %v: %v", c.mode, err)
		}
		if cfg.ClientAuth != c.want {
			t.Errorf("mode %v: ClientAuth = %v, want %v", c.mode, cfg.ClientAuth, c.want)
		}
	}
}

func TestLoadTLSConfig_CertRotation(t *testing.T) {
	dir := t.TempDir()
	certA, keyA := generateSelfSignedPEM(t, "cert-A")
	certPath, keyPath := writeCertKey(t, dir, certA, keyA)

	cfg, err := loadTLSConfigWithRotation(certPath, keyPath, "", ClientAuthNone, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("loadTLSConfigWithRotation: %v", err)
	}
	first, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("initial GetCertificate: %v", err)
	}
	if cn := commonName(t, first); cn != "cert-A" {
		t.Fatalf("initial common name = %q, want cert-A", cn)
	}

	// Generate cert B and atomically rename over the existing files.
	certB, keyB := generateSelfSignedPEM(t, "cert-B")
	certBPath := filepath.Join(dir, "cert.pem.new")
	keyBPath := filepath.Join(dir, "key.pem.new")
	if err := os.WriteFile(certBPath, certB, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyBPath, keyB, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(certBPath, certPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(keyBPath, keyPath); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
		if err == nil && commonName(t, got) == "cert-B" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cert did not rotate to cert-B within 7s")
}

func TestLoadTLSConfig_MidWriteSurvives(t *testing.T) {
	dir := t.TempDir()
	certA, keyA := generateSelfSignedPEM(t, "cert-A")
	certPath, keyPath := writeCertKey(t, dir, certA, keyA)

	cfg, err := loadTLSConfigWithRotation(certPath, keyPath, "", ClientAuthNone, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("loadTLSConfigWithRotation: %v", err)
	}
	if got, err := cfg.GetCertificate(&tls.ClientHelloInfo{}); err != nil {
		t.Fatalf("initial GetCertificate: %v", err)
	} else if cn := commonName(t, got); cn != "cert-A" {
		t.Fatalf("initial common name = %q, want cert-A", cn)
	}

	// Truncate the cert file to half its bytes, simulating a non-atomic
	// mid-write. The rotator must survive several reload attempts.
	half := len(certA) / 2
	if err := os.WriteFile(certPath, certA[:half], 0600); err != nil {
		t.Fatal(err)
	}

	// Wait for several ticker cycles. The rotator must continue to
	// serve cert-A and not crash.
	time.Sleep(600 * time.Millisecond)

	got, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate after truncation: %v", err)
	}
	if cn := commonName(t, got); cn != "cert-A" {
		t.Fatalf("after truncation common name = %q, want cert-A", cn)
	}

	// Atomically restore: write to a sibling and rename over.
	certB, keyB := generateSelfSignedPEM(t, "cert-B")
	certBPath := filepath.Join(dir, "cert.pem.new")
	keyBPath := filepath.Join(dir, "key.pem.new")
	if err := os.WriteFile(certBPath, certB, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyBPath, keyB, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(certBPath, certPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(keyBPath, keyPath); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
		if err == nil && commonName(t, got) == "cert-B" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cert did not rotate to cert-B after restore")
}
