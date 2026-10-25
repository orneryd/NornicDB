package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	defaultAlgorithm = "AES-256-GCM"
	headerVersionV1  = byte(1)
)

type LocalConfig struct {
	MasterKey []byte
	KeyURI    string
}

// LocalProvider provides a minimal software-only KEK wrapper.
// It is intended for local/dev and compatibility tests.
type LocalProvider struct {
	mu       sync.RWMutex
	master   []byte
	keyURI   string
	version  uint32
	closed   bool
	fipsMode string
}

func NewLocalProvider(cfg LocalConfig) (*LocalProvider, error) {
	if len(cfg.MasterKey) != 32 {
		return nil, fmt.Errorf("%w: local provider requires 32-byte master key", ErrInvalidConfig)
	}
	keyURI := cfg.KeyURI
	if keyURI == "" {
		keyURI = "kms://local/default"
	}
	m := make([]byte, 32)
	copy(m, cfg.MasterKey)
	return &LocalProvider{
		master:   m,
		keyURI:   keyURI,
		version:  1,
		fipsMode: "software-module",
	}, nil
}

func (p *LocalProvider) GenerateDataKey(_ context.Context, opts KeyGenOpts) (*DataKey, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	return generateDataKey(opts, func(plain []byte) ([]byte, string, uint32, error) {
		ct, err := wrapWithMaster(p.master, p.version, plain)
		return ct, p.keyURI, p.version, err
	})
}

func (p *LocalProvider) DecryptDataKey(_ context.Context, encryptedKey []byte, _ DecryptOpts) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	plain, err := unwrapWithMaster(p.master, encryptedKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptFailed, err)
	}
	return plain, nil
}

func (p *LocalProvider) RotateDataKey(ctx context.Context, encryptedKey []byte, opts RotateOpts) (*DataKey, error) {
	plain, err := p.DecryptDataKey(ctx, encryptedKey, DecryptOpts{KeyURI: opts.KeyURI})
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	p.version++
	ct, err := wrapWithMaster(p.master, p.version, plain)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	exp := time.Time{}
	if opts.TTL > 0 {
		exp = now.Add(opts.TTL)
	}
	return &DataKey{
		KeyURI:     p.keyURI,
		Ciphertext: ct,
		Plaintext:  plain,
		Version:    p.version,
		CreatedAt:  now,
		ExpiresAt:  exp,
		Algorithm:  defaultAlgorithm,
	}, nil
}

func (p *LocalProvider) GetKeyMetadata(_ context.Context, keyURI string) (*KeyMetadata, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if keyURI != "" && keyURI != p.keyURI {
		return nil, ErrKeyNotFound
	}
	return &KeyMetadata{
		KeyURI:    p.keyURI,
		Version:   p.version,
		Algorithm: defaultAlgorithm,
		Provider:  "local",
		FIPSLevel: p.fipsMode,
	}, nil
}

func (p *LocalProvider) SignAuditEvent(context.Context, AuditEvent) error {
	return nil
}

func (p *LocalProvider) Close(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for i := range p.master {
		p.master[i] = 0
	}
	p.closed = true
	return nil
}

func wrapWithMaster(master []byte, version uint32, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(master)
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
	buf := make([]byte, 1+4+len(nonce))
	buf[0] = headerVersionV1
	binary.BigEndian.PutUint32(buf[1:5], version)
	copy(buf[5:], nonce)
	sealed := gcm.Seal(nil, nonce, plaintext, nil)
	return append(buf, sealed...), nil
}

func unwrapWithMaster(master []byte, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 1+4+12 {
		return nil, fmt.Errorf("short ciphertext")
	}
	if ciphertext[0] != headerVersionV1 {
		return nil, fmt.Errorf("unsupported header version")
	}
	block, err := aes.NewCipher(master)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	offset := 5
	if len(ciphertext) < offset+gcm.NonceSize() {
		return nil, fmt.Errorf("invalid nonce size")
	}
	nonce := ciphertext[offset : offset+gcm.NonceSize()]
	body := ciphertext[offset+gcm.NonceSize():]
	return gcm.Open(nil, nonce, body, nil)
}
