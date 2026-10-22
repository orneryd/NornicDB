package kms

import (
	"context"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	azurekeyvault "github.com/hashicorp/go-kms-wrapping/wrappers/azurekeyvault/v2"
	"github.com/orneryd/nornicdb/pkg/observability"
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
	configMap := map[string]string{}
	if cfg.VaultName != "" {
		configMap["vault_name"] = cfg.VaultName
	}
	if cfg.KeyName != "" {
		configMap["key_name"] = cfg.KeyName
	}
	if cfg.TenantID != "" {
		configMap["tenant_id"] = cfg.TenantID
	}
	if cfg.ClientID != "" {
		configMap["client_id"] = cfg.ClientID
	}
	if cfg.ClientSecret != "" {
		configMap["client_secret"] = cfg.ClientSecret
	}
	if cfg.Environment != "" {
		configMap["environment"] = cfg.Environment
	}
	if cfg.Resource != "" {
		configMap["resource"] = cfg.Resource
	}
	if cfg.DisallowEnvCredential {
		configMap["disallow_env_vars"] = "true"
	}
	if _, err := w.SetConfig(ctx, wrapping.WithConfigMap(configMap)); err != nil {
		return nil, observability.RedactError(err, cfg.ClientSecret)
	}
	return &AzureProvider{wrappingAdapter: newWrappingAdapter(w, cfg.ClientSecret)}, nil
}
