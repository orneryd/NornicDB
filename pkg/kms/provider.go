package kms

import (
	"context"
	"time"
)

// KeyProvider abstracts KMS/HSM implementations for envelope workflows.
type KeyProvider interface {
	GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error)
	DecryptDataKey(ctx context.Context, encryptedKey []byte, opts DecryptOpts) ([]byte, error)
	RotateDataKey(ctx context.Context, encryptedKey []byte, opts RotateOpts) (*DataKey, error)
	GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error)
	SignAuditEvent(ctx context.Context, event AuditEvent) error
	Close(ctx context.Context) error
}

type KeyGenOpts struct {
	Algorithm string
	TTL       time.Duration
	Label     string
}

type DecryptOpts struct {
	KeyURI string
}

type RotateOpts struct {
	KeyURI string
	Label  string
	TTL    time.Duration
}

// DataKey is an encrypted DEK plus runtime metadata.
type DataKey struct {
	KeyURI     string
	Ciphertext []byte
	Plaintext  []byte
	Version    uint32
	CreatedAt  time.Time
	ExpiresAt  time.Time
	Algorithm  string
}

type KeyMetadata struct {
	KeyURI     string
	Version    uint32
	Algorithm  string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	Provider   string
	FIPSLevel  string
	Properties map[string]interface{}
}
