package kms

import (
	"context"

	azurekeyvault "github.com/hashicorp/go-kms-wrapping/wrappers/azurekeyvault/v2"
)

type AzureProviderConfig struct {
	VaultName             string
	KeyName               string
	TenantID              string
	ClientID              string
	ClientSecret          string
	Environment           string
	Resource              string
	DisallowEnvCredential bool
}

type AzureProvider struct {
	*wrappingAdapter
}

func NewAzureProvider(ctx context.Context, cfg AzureProviderConfig) (*AzureProvider, error) {
	w := azurekeyvault.NewWrapper()
	configMap := map[string]string{
		"vault_name":    cfg.VaultName,
		"key_name":      cfg.KeyName,
		"tenant_id":     cfg.TenantID,
		"client_id":     cfg.ClientID,
		"client_secret": cfg.ClientSecret,
		"environment":   cfg.Environment,
		"resource":      cfg.Resource,
	}
	return configureWrappingProvider[AzureProvider](ctx, w, configMap, cfg.DisallowEnvCredential, cfg.ClientSecret)
}
