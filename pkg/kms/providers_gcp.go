package kms

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"

	kmsapi "cloud.google.com/go/kms/apiv1"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/orneryd/nornicdb/pkg/observability"
	"google.golang.org/api/option"
)

type GCPProviderConfig struct {
	Project         string
	Location        string
	KeyRing         string
	KeyName         string
	CredentialsFile string
}

type GCPProvider struct {
	client      gcpKMSClient
	cryptoKeyID string
}

type gcpKMSClient interface {
	Encrypt(context.Context, *kmspb.EncryptRequest, ...gax.CallOption) (*kmspb.EncryptResponse, error)
	Decrypt(context.Context, *kmspb.DecryptRequest, ...gax.CallOption) (*kmspb.DecryptResponse, error)
	GetCryptoKey(context.Context, *kmspb.GetCryptoKeyRequest, ...gax.CallOption) (*kmspb.CryptoKey, error)
	Close() error
}

var newGCPKMSClient = func(ctx context.Context, opts ...option.ClientOption) (gcpKMSClient, error) {
	return kmsapi.NewKeyManagementClient(ctx, opts...)
}

func NewGCPProvider(ctx context.Context, cfg GCPProviderConfig) (KeyProvider, error) {
	if cfg.Project == "" || cfg.Location == "" || cfg.KeyRing == "" || cfg.KeyName == "" {
		return nil, fmt.Errorf("%w: gcp provider requires project/location/key_ring/key_name", ErrInvalidConfig)
	}
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}
	client, err := newGCPKMSClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	cryptoKeyID := fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		cfg.Project, cfg.Location, cfg.KeyRing, cfg.KeyName)
	return &GCPProvider{
		client:      client,
		cryptoKeyID: cryptoKeyID,
	}, nil
}

func (p *GCPProvider) GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error) {
	plain := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, plain); err != nil {
		return nil, err
	}
	resp, err := p.client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:      p.cryptoKeyID,
		Plaintext: plain,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptFailed, observability.RedactError(err, string(plain)))
	}
	now := time.Now().UTC()
	exp := time.Time{}
	if opts.TTL > 0 {
		exp = now.Add(opts.TTL)
	}
	alg := opts.Algorithm
	if alg == "" {
		alg = "AES-256-GCM"
	}
	return &DataKey{
		KeyURI:     p.cryptoKeyID,
		Ciphertext: resp.Ciphertext,
		Plaintext:  plain,
		Version:    1,
		CreatedAt:  now,
		ExpiresAt:  exp,
		Algorithm:  alg,
	}, nil
}

func (p *GCPProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, _ DecryptOpts) ([]byte, error) {
	resp, err := p.client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:       p.cryptoKeyID,
		Ciphertext: encryptedKey,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptFailed, err)
	}
	return resp.Plaintext, nil
}

func (p *GCPProvider) RotateDataKey(ctx context.Context, encryptedKey []byte, opts RotateOpts) (*DataKey, error) {
	plain, err := p.DecryptDataKey(ctx, encryptedKey, DecryptOpts{KeyURI: opts.KeyURI})
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:      p.cryptoKeyID,
		Plaintext: plain,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptFailed, observability.RedactError(err, string(plain)))
	}
	now := time.Now().UTC()
	exp := time.Time{}
	if opts.TTL > 0 {
		exp = now.Add(opts.TTL)
	}
	return &DataKey{
		KeyURI:     p.cryptoKeyID,
		Ciphertext: resp.Ciphertext,
		Plaintext:  plain,
		Version:    1,
		CreatedAt:  now,
		ExpiresAt:  exp,
		Algorithm:  "AES-256-GCM",
	}, nil
}

func (p *GCPProvider) GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error) {
	name := p.cryptoKeyID
	if keyURI != "" {
		name = keyURI
	}
	meta, err := p.client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return &KeyMetadata{
		KeyURI:    name,
		Algorithm: meta.Purpose.String(),
		Provider:  "gcp-cloudkms",
		FIPSLevel: "provider-managed",
	}, nil
}

func (p *GCPProvider) SignAuditEvent(context.Context, AuditEvent) error { return nil }

func (p *GCPProvider) Close(context.Context) error {
	return p.client.Close()
}
