package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
)

type EnvelopeConfig struct {
	DEKCacheTTL time.Duration
	Label       string
}

type EnvelopeEncryptor struct {
	provider kms.KeyProvider
	cache    *dekCache
}

type envelopePayload struct {
	Version      int    `json:"v"`
	KeyURI       string `json:"key_uri"`
	KeyVersion   uint32 `json:"key_version"`
	Algorithm    string `json:"alg"`
	EncryptedDEK []byte `json:"encrypted_dek"`
	Nonce        []byte `json:"nonce"`
	Ciphertext   []byte `json:"ciphertext"`
}

func NewEnvelopeEncryptor(provider kms.KeyProvider, cfg EnvelopeConfig) *EnvelopeEncryptor {
	return &EnvelopeEncryptor{
		provider: provider,
		cache:    newDEKCache(provider, cfg.DEKCacheTTL, cfg.Label),
	}
}

func (e *EnvelopeEncryptor) Encrypt(ctx context.Context, plaintext []byte) ([]byte, error) {
	dek, err := e.cache.GetOrGenerate(ctx)
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(dek.Plaintext)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)

	payload := envelopePayload{
		Version:      1,
		KeyURI:       dek.KeyURI,
		KeyVersion:   dek.Version,
		Algorithm:    dek.Algorithm,
		EncryptedDEK: dek.Ciphertext,
		Nonce:        nonce,
		Ciphertext:   ciphertext,
	}
	return json.Marshal(payload)
}

func (e *EnvelopeEncryptor) Decrypt(ctx context.Context, envelope []byte) ([]byte, error) {
	var payload envelopePayload
	if err := json.Unmarshal(envelope, &payload); err != nil {
		return nil, err
	}
	if payload.Version != 1 {
		return nil, fmt.Errorf("unsupported envelope version %d", payload.Version)
	}
	dek, err := e.provider.DecryptDataKey(ctx, payload.EncryptedDEK, kms.DecryptOpts{
		KeyURI: payload.KeyURI,
	})
	if err != nil {
		_ = e.provider.SignAuditEvent(ctx, kms.AuditEvent{
			EventType: "KEY_DECRYPT_FAILED",
			KeyURI:    payload.KeyURI,
			Status:    "FAILURE",
			Timestamp: time.Now().UTC(),
			ErrorCode: "DEK_DECRYPT_FAILED",
		})
		return nil, err
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(payload.Nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("invalid nonce size")
	}
	return gcm.Open(nil, payload.Nonce, payload.Ciphertext, nil)
}
