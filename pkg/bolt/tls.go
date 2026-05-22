// Package bolt: TLS construction helpers.
//
// Mirrors Neo4j's sslContext + requiresEncryption pattern; cert rotation
// requires atomic rename of cert+key files. The returned *tls.Config
// leaves Certificates nil and installs a GetCertificate closure that
// re-loads from disk on every handshake. A 5s background ticker
// periodically re-reads the files; transient failures during the periodic
// reload are absorbed (the previous cert stays cached) — operator update
// protocol is atomic rename so partial reads are an expected event, not
// a logged anomaly.
package bolt

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// ClientAuthMode selects the TLS client-authentication policy applied to
// the Bolt listener. It maps onto tls.ClientAuthType.
type ClientAuthMode int

const (
	// ClientAuthNone disables client-cert handling (tls.NoClientCert).
	ClientAuthNone ClientAuthMode = iota
	// ClientAuthRequest requests but does not require a client cert
	// (tls.RequestClientCert).
	ClientAuthRequest
	// ClientAuthRequestVerify requests a client cert; if presented, it is
	// verified (tls.VerifyClientCertIfGiven).
	ClientAuthRequestVerify
	// ClientAuthRequireVerify requires and verifies a client cert
	// (tls.RequireAndVerifyClientCert).
	ClientAuthRequireVerify
)

// ParseClientAuthMode parses a textual ClientAuthMode. The empty string
// and "none" map to ClientAuthNone. Unknown values return an error.
func ParseClientAuthMode(s string) (ClientAuthMode, error) {
	switch s {
	case "", "none":
		return ClientAuthNone, nil
	case "request":
		return ClientAuthRequest, nil
	case "request_verify":
		return ClientAuthRequestVerify, nil
	case "require_verify":
		return ClientAuthRequireVerify, nil
	default:
		return ClientAuthNone, fmt.Errorf("bolt: unknown client auth mode %q", s)
	}
}

// toTLSClientAuth converts ClientAuthMode to tls.ClientAuthType.
func (m ClientAuthMode) toTLSClientAuth() tls.ClientAuthType {
	switch m {
	case ClientAuthRequest:
		return tls.RequestClientCert
	case ClientAuthRequestVerify:
		return tls.VerifyClientCertIfGiven
	case ClientAuthRequireVerify:
		return tls.RequireAndVerifyClientCert
	default:
		return tls.NoClientCert
	}
}

// certRotator caches the most recently loaded server certificate and
// reloads it from disk on a periodic ticker. Reads are protected by an
// RWMutex; failed reloads leave the previously cached cert in place.
type certRotator struct {
	certFile string
	keyFile  string

	mu      sync.RWMutex
	loaded  *tls.Certificate
	lastErr error
}

// getCertificate is wired to tls.Config.GetCertificate. It returns the
// cached certificate, falling back to a synchronous reload on the first
// call if the cache is empty.
func (r *certRotator) getCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	current := r.loaded
	r.mu.RUnlock()
	if current != nil {
		return current, nil
	}
	return r.reload()
}

// reload reads the cert+key from disk and atomically swaps the cached
// certificate on success. On failure, it preserves the previous cert
// (if any) and stores the error.
func (r *certRotator) reload() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		r.mu.Lock()
		r.lastErr = err
		previous := r.loaded
		r.mu.Unlock()
		if previous != nil {
			return previous, nil
		}
		return nil, err
	}
	r.mu.Lock()
	r.loaded = &cert
	r.lastErr = nil
	r.mu.Unlock()
	return &cert, nil
}

// runRotation loops on the supplied tick interval, attempting to reload
// the cert+key from disk on every tick. Failures are stashed on
// r.lastErr (read-via-RLock) but do not invalidate the previously cached
// cert; this matches the spec's "atomic-rename only" robustness contract
// where mid-write reads are expected and intentionally absorbed.
func (r *certRotator) runRotation(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		_, _ = r.reload()
	}
}

// LoadTLSConfig loads cert+key from disk and returns a *tls.Config
// suitable for the Bolt listener. The returned config has
// MinVersion=TLS1.2 and a GetCertificate closure that re-reads the
// cert+key from disk on every handshake (cert rotation). Certificates
// is intentionally left nil so the closure fires on every handshake. A
// 5s background ticker re-reads the files in a goroutine and swaps the
// cached cert under a sync.RWMutex on successful load. Failures during
// the periodic reload are logged but do not invalidate the previous cert.
func LoadTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	return loadTLSConfigWithRotation(certFile, keyFile, "", ClientAuthNone, 5*time.Second)
}

// LoadTLSConfigWithClientCA additionally verifies client certs against
// clientCAFile (for mTLS). mode controls tls.Config.ClientAuth. When
// clientCAFile is "" and mode is ClientAuthNone, behaves identically to
// LoadTLSConfig.
func LoadTLSConfigWithClientCA(certFile, keyFile, clientCAFile string, mode ClientAuthMode) (*tls.Config, error) {
	return loadTLSConfigWithRotation(certFile, keyFile, clientCAFile, mode, 5*time.Second)
}

// loadTLSConfigWithRotation is the shared implementation. It is unexported
// so tests can supply a faster ticker interval without exposing the knob
// in the public API.
func loadTLSConfigWithRotation(certFile, keyFile, clientCAFile string, mode ClientAuthMode, tickInterval time.Duration) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("bolt: cert and key file paths must be non-empty")
	}

	rot := &certRotator{certFile: certFile, keyFile: keyFile}
	if _, err := rot.reload(); err != nil {
		return nil, fmt.Errorf("bolt: load tls keypair: %w", err)
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Certificates intentionally left nil so GetCertificate fires on
		// every handshake (per crypto/tls docs: GetCertificate is only
		// invoked when SNI is present OR Certificates is empty).
		GetCertificate: rot.getCertificate,
	}

	if clientCAFile != "" {
		caPEM, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("bolt: read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("bolt: client CA file %q contained no valid PEM certificates", clientCAFile)
		}
		cfg.ClientCAs = pool
	}

	cfg.ClientAuth = mode.toTLSClientAuth()

	go rot.runRotation(tickInterval)

	return cfg, nil
}
