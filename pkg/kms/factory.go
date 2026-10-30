package kms

import (
	"context"
	"fmt"
)

type FactoryConfig struct {
	Provider   string
	KeyURI     string
	MasterKey  []byte
	Archiver   *AuditArchiver
	ProviderID string

	AWS   AWSProviderConfig
	Azure AzureProviderConfig
	GCP   GCPProviderConfig
}

func NewProvider(cfg FactoryConfig) (KeyProvider, error) {
	var provider KeyProvider
	var err error
	switch cfg.Provider {
	case "", "local":
		provider, err = NewLocalProvider(LocalConfig{
			MasterKey: cfg.MasterKey,
			KeyURI:    cfg.KeyURI,
		})
		if err != nil {
			return nil, err
		}
	case "aws-kms":
		provider, err = NewAWSProvider(context.Background(), cfg.AWS)
		if err != nil {
			return nil, err
		}
	case "azure-keyvault":
		provider, err = NewAzureProvider(context.Background(), cfg.Azure)
		if err != nil {
			return nil, err
		}
	case "gcp-cloudkms":
		provider, err = NewGCPProvider(context.Background(), cfg.GCP)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedProvider, cfg.Provider)
	}
	provider = WithAudit(provider, cfg.Archiver, providerID(cfg))
	return provider, nil
}

func providerID(cfg FactoryConfig) string {
	if cfg.ProviderID != "" {
		return cfg.ProviderID
	}
	if cfg.Provider == "" {
		return "local"
	}
	return cfg.Provider
}

func CloseProvider(provider KeyProvider) {
	if provider == nil {
		return
	}
	_ = provider.Close(context.Background())
}
