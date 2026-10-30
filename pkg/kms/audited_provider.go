package kms

import (
	"context"
	"time"
)

type auditedProvider struct {
	inner      KeyProvider
	archiver   *AuditArchiver
	providerID string
}

func WithAudit(provider KeyProvider, archiver *AuditArchiver, providerID string) KeyProvider {
	if provider == nil || archiver == nil {
		return provider
	}
	return &auditedProvider{
		inner:      provider,
		archiver:   archiver,
		providerID: providerID,
	}
}

func (p *auditedProvider) GenerateDataKey(ctx context.Context, opts KeyGenOpts) (*DataKey, error) {
	key, err := p.inner.GenerateDataKey(ctx, opts)
	if err != nil {
		p.archive(ctx, AuditEvent{
			EventType:  "KEY_GENERATED",
			Status:     "FAILURE",
			ErrorCode:  "GENERATE_DATA_KEY_FAILED",
			Timestamp:  time.Now().UTC(),
			ProviderID: p.providerID,
			Metadata: map[string]interface{}{
				"label":     opts.Label,
				"algorithm": opts.Algorithm,
			},
		})
		return nil, err
	}
	p.archive(ctx, AuditEvent{
		EventType:  "KEY_GENERATED",
		KeyURI:     key.KeyURI,
		Status:     "SUCCESS",
		Timestamp:  time.Now().UTC(),
		ProviderID: p.providerID,
		Metadata: map[string]interface{}{
			"label":       opts.Label,
			"algorithm":   key.Algorithm,
			"key_version": key.Version,
		},
	})
	return key, nil
}

func (p *auditedProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, opts DecryptOpts) ([]byte, error) {
	plain, err := p.inner.DecryptDataKey(ctx, encryptedKey, opts)
	if err != nil {
		p.archive(ctx, AuditEvent{
			EventType:  "KEY_DECRYPTED",
			KeyURI:     opts.KeyURI,
			Status:     "FAILURE",
			ErrorCode:  "DECRYPT_DATA_KEY_FAILED",
			Timestamp:  time.Now().UTC(),
			ProviderID: p.providerID,
		})
		return nil, err
	}
	p.archive(ctx, AuditEvent{
		EventType:  "KEY_DECRYPTED",
		KeyURI:     opts.KeyURI,
		Status:     "SUCCESS",
		Timestamp:  time.Now().UTC(),
		ProviderID: p.providerID,
	})
	return plain, nil
}

func (p *auditedProvider) RotateDataKey(ctx context.Context, encryptedKey []byte, opts RotateOpts) (*DataKey, error) {
	key, err := p.inner.RotateDataKey(ctx, encryptedKey, opts)
	if err != nil {
		p.archive(ctx, AuditEvent{
			EventType:  "KEY_ROTATED",
			KeyURI:     opts.KeyURI,
			Status:     "FAILURE",
			ErrorCode:  "ROTATE_DATA_KEY_FAILED",
			Timestamp:  time.Now().UTC(),
			ProviderID: p.providerID,
			Metadata: map[string]interface{}{
				"label": opts.Label,
			},
		})
		return nil, err
	}
	p.archive(ctx, AuditEvent{
		EventType:  "KEY_ROTATED",
		KeyURI:     key.KeyURI,
		Status:     "SUCCESS",
		Timestamp:  time.Now().UTC(),
		ProviderID: p.providerID,
		Metadata: map[string]interface{}{
			"label":       opts.Label,
			"key_version": key.Version,
		},
	})
	return key, nil
}

func (p *auditedProvider) GetKeyMetadata(ctx context.Context, keyURI string) (*KeyMetadata, error) {
	metadata, err := p.inner.GetKeyMetadata(ctx, keyURI)
	if err != nil {
		p.archive(ctx, AuditEvent{
			EventType:  "KEY_METADATA_READ",
			KeyURI:     keyURI,
			Status:     "FAILURE",
			ErrorCode:  "GET_KEY_METADATA_FAILED",
			Timestamp:  time.Now().UTC(),
			ProviderID: p.providerID,
		})
		return nil, err
	}
	p.archive(ctx, AuditEvent{
		EventType:  "KEY_METADATA_READ",
		KeyURI:     metadata.KeyURI,
		Status:     "SUCCESS",
		Timestamp:  time.Now().UTC(),
		ProviderID: p.providerID,
		Metadata: map[string]interface{}{
			"provider":   metadata.Provider,
			"fips_level": metadata.FIPSLevel,
		},
	})
	return metadata, nil
}

func (p *auditedProvider) SignAuditEvent(ctx context.Context, event AuditEvent) error {
	p.archive(ctx, event)
	return p.inner.SignAuditEvent(ctx, event)
}

func (p *auditedProvider) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

func (p *auditedProvider) archive(ctx context.Context, event AuditEvent) {
	if p.archiver == nil {
		return
	}
	_ = p.archiver.Archive(ctx, event)
}
