package kms

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/observability"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	awskms "github.com/hashicorp/go-kms-wrapping/wrappers/awskms/v2"
)

type AWSProviderConfig struct {
	KeyID                 string
	Region                string
	Endpoint              string
	RoleARN               string
	RoleSessionName       string
	AccessKey             string
	SecretKey             string
	SessionToken          string
	SharedCredsFilename   string
	SharedCredsProfile    string
	WebIdentityTokenFile  string
	DisallowEnvCredential bool
}

type AWSProvider struct {
	*wrappingAdapter
}

func NewAWSProvider(ctx context.Context, cfg AWSProviderConfig) (*AWSProvider, error) {
	w := awskms.NewWrapper()
	configMap := map[string]string{}
	if cfg.KeyID != "" {
		configMap["kms_key_id"] = cfg.KeyID
	}
	if cfg.Region != "" {
		configMap["region"] = cfg.Region
	}
	if cfg.Endpoint != "" {
		configMap["endpoint"] = cfg.Endpoint
	}
	if cfg.RoleARN != "" {
		configMap["role_arn"] = cfg.RoleARN
	}
	if cfg.RoleSessionName != "" {
		configMap["role_session_name"] = cfg.RoleSessionName
	}
	if cfg.AccessKey != "" {
		configMap["access_key"] = cfg.AccessKey
	}
	if cfg.SecretKey != "" {
		configMap["secret_key"] = cfg.SecretKey
	}
	if cfg.SessionToken != "" {
		configMap["session_token"] = cfg.SessionToken
	}
	if cfg.SharedCredsFilename != "" {
		configMap["shared_creds_filename"] = cfg.SharedCredsFilename
	}
	if cfg.SharedCredsProfile != "" {
		configMap["shared_creds_profile"] = cfg.SharedCredsProfile
	}
	if cfg.WebIdentityTokenFile != "" {
		configMap["web_identity_token_file"] = cfg.WebIdentityTokenFile
	}
	if cfg.DisallowEnvCredential {
		configMap["disallow_env_vars"] = "true"
	}
	if _, err := w.SetConfig(ctx, wrapping.WithConfigMap(configMap)); err != nil {
		return nil, observability.RedactError(err, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken, cfg.Endpoint)
	}
	return &AWSProvider{wrappingAdapter: newWrappingAdapter(w, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken, cfg.Endpoint)}, nil
}
