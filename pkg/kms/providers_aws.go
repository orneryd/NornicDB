package kms

import (
	"context"

	awskms "github.com/hashicorp/go-kms-wrapping/wrappers/awskms/v2"
	"github.com/orneryd/nornicdb/pkg/observability"
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
	configMap := map[string]string{
		"kms_key_id":              cfg.KeyID,
		"region":                  cfg.Region,
		"endpoint":                cfg.Endpoint,
		"role_arn":                cfg.RoleARN,
		"role_session_name":       cfg.RoleSessionName,
		"access_key":              cfg.AccessKey,
		"secret_key":              cfg.SecretKey,
		"session_token":           cfg.SessionToken,
		"shared_creds_filename":   cfg.SharedCredsFilename,
		"shared_creds_profile":    cfg.SharedCredsProfile,
		"web_identity_token_file": cfg.WebIdentityTokenFile,
	}
	return configureWrappingProvider[AWSProvider](ctx, w, configMap, cfg.DisallowEnvCredential, observability.ClientCredentials(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)...)
}
