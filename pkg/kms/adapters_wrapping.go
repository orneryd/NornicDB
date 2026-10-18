package kms

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	"github.com/orneryd/nornicdb/pkg/observability"
)

type wrappingAdapter struct {
	w           wrapperCloser
	credentials []string
}

type wrapperCloser interface {
	wrapping.Wrapper
}

type wrappedBlob struct {
	Blob *wrapping.BlobInfo `json:"blob"`
}

func newWrappingAdapter(w wrapperCloser, credentials ...string) *wrappingAdapter {
	return &wrappingAdapter{w: w, credentials: credentials}
}

func (a *wrappingAdapter) GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error) {
	plain := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, plain); err != nil {
		return nil, err
	}
	blob, err := a.w.Encrypt(ctx, plain)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptFailed, observability.RedactError(observability.RedactError(err, string(plain)), a.credentials...))
	}
	ct, err := encodeBlob(blob)
	if err != nil {
		return nil, err
	}
	keyID, _ := a.w.KeyId(ctx)
	alg := opts.Algorithm
	if alg == "" {
		alg = "AES-256-GCM"
	}
	now := time.Now().UTC()
	exp := time.Time{}
	if opts.TTL > 0 {
		exp = now.Add(opts.TTL)
	}
	return &DataKey{
		KeyURI:     keyID,
		Ciphertext: ct,
		Plaintext:  plain,
		Version:    1,
		CreatedAt:  now,
		ExpiresAt:  exp,
		Algorithm:  alg,
	}, nil
}

func (a *wrappingAdapter) DecryptDataKey(ctx context.Context, encryptedKey []byte, _ DecryptOpts) ([]byte, error) {
	blob, err := decodeBlob(encryptedKey)
	if err != nil {
		return nil, err
	}
	plain, err := a.w.Decrypt(ctx, blob)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptFailed, observability.RedactError(err, a.credentials...))
	}
	return plain, nil
}

func (a *wrappingAdapter) RotateDataKey(ctx context.Context, encryptedKey []byte, opts RotateOpts) (*DataKey, error) {
	plain, err := a.DecryptDataKey(ctx, encryptedKey, DecryptOpts{KeyURI: opts.KeyURI})
	if err != nil {
		return nil, err
	}
	blob, err := a.w.Encrypt(ctx, plain)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryptFailed, observability.RedactError(observability.RedactError(err, string(plain)), a.credentials...))
	}
	ct, err := encodeBlob(blob)
	if err != nil {
		return nil, err
	}
	keyID, _ := a.w.KeyId(ctx)
	now := time.Now().UTC()
	exp := time.Time{}
	if opts.TTL > 0 {
		exp = now.Add(opts.TTL)
	}
	return &DataKey{
		KeyURI:     keyID,
		Ciphertext: ct,
		Plaintext:  plain,
		Version:    1,
		CreatedAt:  now,
		ExpiresAt:  exp,
		Algorithm:  "AES-256-GCM",
	}, nil
}

func (a *wrappingAdapter) GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error) {
	t, _ := a.w.Type(ctx)
	kid, _ := a.w.KeyId(ctx)
	if keyURI != "" && kid != "" && keyURI != kid {
		return nil, ErrKeyNotFound
	}
	return &KeyMetadata{
		KeyURI:    kid,
		Algorithm: "AES-256-GCM",
		Provider:  string(t),
	}, nil
}

func (a *wrappingAdapter) SignAuditEvent(context.Context, AuditEvent) error {
	return nil
}

func (a *wrappingAdapter) Close(context.Context) error {
	return nil
}

func encodeBlob(blob *wrapping.BlobInfo) ([]byte, error) {
	return json.Marshal(wrappedBlob{Blob: blob})
}

func decodeBlob(b []byte) (*wrapping.BlobInfo, error) {
	var wb wrappedBlob
	if err := json.Unmarshal(b, &wb); err != nil {
		return nil, err
	}
	if wb.Blob == nil {
		return nil, fmt.Errorf("%w: empty wrapped blob", ErrDecryptFailed)
	}
	return wb.Blob, nil
}
