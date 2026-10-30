package kms

import (
	"context"
	"errors"
	"testing"
	"time"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type fakeGCPKMSClient struct {
	encryptCiphertext []byte
	decryptPlaintext  []byte
	metadata          *kmspb.CryptoKey
	encryptErr        error
	decryptErr        error
	metadataErr       error
	closeErr          error
	closed            bool
	lastEncryptName   string
	lastDecryptName   string
	lastMetadataName  string
}

func (c *fakeGCPKMSClient) Encrypt(ctx context.Context, req *kmspb.EncryptRequest, opts ...gax.CallOption) (*kmspb.EncryptResponse, error) {
	c.lastEncryptName = req.Name
	if c.encryptErr != nil {
		return nil, c.encryptErr
	}
	return &kmspb.EncryptResponse{Ciphertext: c.encryptCiphertext}, nil
}

func (c *fakeGCPKMSClient) Decrypt(ctx context.Context, req *kmspb.DecryptRequest, opts ...gax.CallOption) (*kmspb.DecryptResponse, error) {
	c.lastDecryptName = req.Name
	if c.decryptErr != nil {
		return nil, c.decryptErr
	}
	return &kmspb.DecryptResponse{Plaintext: c.decryptPlaintext}, nil
}

func (c *fakeGCPKMSClient) GetCryptoKey(ctx context.Context, req *kmspb.GetCryptoKeyRequest, opts ...gax.CallOption) (*kmspb.CryptoKey, error) {
	c.lastMetadataName = req.Name
	if c.metadataErr != nil {
		return nil, c.metadataErr
	}
	return c.metadata, nil
}

func (c *fakeGCPKMSClient) Close() error {
	c.closed = true
	return c.closeErr
}

func TestCloudProviderConstructorsConfigBranches(t *testing.T) {
	ctx := context.Background()

	aws, err := NewAWSProvider(ctx, AWSProviderConfig{
		KeyID:                 "alias/test",
		Region:                "us-east-1",
		Endpoint:              "http://localhost:4566",
		RoleARN:               "arn:aws:iam::123456789012:role/test",
		RoleSessionName:       "session",
		AccessKey:             "access",
		SecretKey:             "secret",
		SessionToken:          "token",
		SharedCredsFilename:   "/tmp/creds",
		SharedCredsProfile:    "profile",
		WebIdentityTokenFile:  "/tmp/token",
		DisallowEnvCredential: true,
	})
	if err == nil {
		require.NotNil(t, aws)
		require.NoError(t, aws.Close(ctx))
	} else {
		require.Error(t, err)
	}

	azure, err := NewAzureProvider(ctx, AzureProviderConfig{
		VaultName:             "vault",
		KeyName:               "key",
		TenantID:              "tenant",
		ClientID:              "client",
		ClientSecret:          "secret",
		Environment:           "AzurePublicCloud",
		Resource:              "https://vault.azure.net",
		DisallowEnvCredential: true,
	})
	if err == nil {
		require.NotNil(t, azure)
		require.NoError(t, azure.Close(ctx))
	} else {
		require.Error(t, err)
	}
}

func TestGCPProviderInvalidConfigAndNoopMethods(t *testing.T) {
	_, err := NewGCPProvider(context.Background(), GCPProviderConfig{})
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorContains(t, err, "project/location/key_ring/key_name")

	provider := &GCPProvider{cryptoKeyID: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}
	require.NoError(t, provider.SignAuditEvent(context.Background(), AuditEvent{}))
}

func TestGCPProviderConstructorSeamBranches(t *testing.T) {
	original := newGCPKMSClient
	t.Cleanup(func() { newGCPKMSClient = original })

	fakeClient := &fakeGCPKMSClient{}
	var optionCount int
	newGCPKMSClient = func(ctx context.Context, opts ...option.ClientOption) (gcpKMSClient, error) {
		optionCount = len(opts)
		return fakeClient, nil
	}

	provider, err := NewGCPProvider(context.Background(), GCPProviderConfig{
		Project:         "p",
		Location:        "l",
		KeyRing:         "r",
		KeyName:         "k",
		CredentialsFile: "/tmp/creds.json",
	})
	require.NoError(t, err)
	require.IsType(t, &GCPProvider{}, provider)
	require.Equal(t, 1, optionCount)
	require.Equal(t, "projects/p/locations/l/keyRings/r/cryptoKeys/k", provider.(*GCPProvider).cryptoKeyID)

	newGCPKMSClient = func(ctx context.Context, opts ...option.ClientOption) (gcpKMSClient, error) {
		return nil, errors.New("client failed")
	}
	_, err = NewGCPProvider(context.Background(), GCPProviderConfig{Project: "p", Location: "l", KeyRing: "r", KeyName: "k"})
	require.ErrorContains(t, err, "client failed")
}

func TestGCPProviderFakeClientBranches(t *testing.T) {
	ctx := context.Background()
	client := &fakeGCPKMSClient{
		encryptCiphertext: []byte("cipher"),
		decryptPlaintext:  []byte("plain"),
		metadata:          &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	}
	provider := &GCPProvider{client: client, cryptoKeyID: "projects/p/locations/l/keyRings/r/cryptoKeys/k"}

	key, err := provider.GenerateDataKey(ctx, KeyGenOpts{TTL: time.Minute, Algorithm: "custom"})
	require.NoError(t, err)
	require.Equal(t, []byte("cipher"), key.Ciphertext)
	require.Len(t, key.Plaintext, 32)
	require.Equal(t, "custom", key.Algorithm)
	require.False(t, key.ExpiresAt.IsZero())
	require.Equal(t, provider.cryptoKeyID, client.lastEncryptName)

	plain, err := provider.DecryptDataKey(ctx, []byte("cipher"), DecryptOpts{})
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), plain)
	require.Equal(t, provider.cryptoKeyID, client.lastDecryptName)

	rotated, err := provider.RotateDataKey(ctx, []byte("old"), RotateOpts{TTL: time.Minute})
	require.NoError(t, err)
	require.Equal(t, []byte("cipher"), rotated.Ciphertext)
	require.Equal(t, "AES-256-GCM", rotated.Algorithm)
	require.False(t, rotated.ExpiresAt.IsZero())

	metadata, err := provider.GetKeyMetadata(ctx, "override-key")
	require.NoError(t, err)
	require.Equal(t, "override-key", metadata.KeyURI)
	require.Equal(t, "gcp-cloudkms", metadata.Provider)
	require.Equal(t, "override-key", client.lastMetadataName)
	require.NoError(t, provider.Close(ctx))
	require.True(t, client.closed)
}

func TestGCPProviderFakeClientErrorBranches(t *testing.T) {
	ctx := context.Background()
	provider := &GCPProvider{client: &fakeGCPKMSClient{encryptErr: errors.New("encrypt failed")}, cryptoKeyID: "key"}
	_, err := provider.GenerateDataKey(ctx, KeyGenOpts{})
	require.ErrorIs(t, err, ErrEncryptFailed)

	provider.client = &fakeGCPKMSClient{decryptErr: errors.New("decrypt failed")}
	_, err = provider.DecryptDataKey(ctx, []byte("cipher"), DecryptOpts{})
	require.ErrorIs(t, err, ErrDecryptFailed)
	_, err = provider.RotateDataKey(ctx, []byte("cipher"), RotateOpts{})
	require.ErrorIs(t, err, ErrDecryptFailed)

	provider.client = &fakeGCPKMSClient{decryptPlaintext: []byte("plain"), encryptErr: errors.New("encrypt failed")}
	_, err = provider.RotateDataKey(ctx, []byte("cipher"), RotateOpts{})
	require.ErrorIs(t, err, ErrEncryptFailed)

	provider.client = &fakeGCPKMSClient{metadataErr: errors.New("metadata failed")}
	_, err = provider.GetKeyMetadata(ctx, "")
	require.ErrorContains(t, err, "metadata failed")

	provider.client = &fakeGCPKMSClient{closeErr: errors.New("close failed")}
	require.ErrorContains(t, provider.Close(ctx), "close failed")
}

func TestFactoryCloudProviderErrorBranches(t *testing.T) {
	_, err := NewProvider(FactoryConfig{Provider: "gcp-cloudkms"})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewProvider(FactoryConfig{Provider: "unsupported-cloud"})
	require.ErrorIs(t, err, ErrUnsupportedProvider)
}
