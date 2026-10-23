package kms

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMasterKey() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

func TestNewProvider_Local(t *testing.T) {
	t.Parallel()
	p, err := NewProvider(FactoryConfig{
		Provider:  "local",
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	defer CloseProvider(p)

	// Should be usable
	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.NoError(t, err)
	require.Len(t, dek.Plaintext, 32)
}

func TestNewProvider_EmptyStringDefaultsToLocal(t *testing.T) {
	t.Parallel()
	p, err := NewProvider(FactoryConfig{
		Provider:  "",
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	defer CloseProvider(p)
}

func TestNewProvider_LocalInvalidKey(t *testing.T) {
	t.Parallel()
	_, err := NewProvider(FactoryConfig{
		Provider:  "local",
		MasterKey: []byte("short"),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidConfig))
}

func TestNewProvider_UnsupportedProvider(t *testing.T) {
	t.Parallel()
	_, err := NewProvider(FactoryConfig{
		Provider: "not-a-real-provider",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedProvider))
	assert.Contains(t, err.Error(), "not-a-real-provider")
}

func TestNewProvider_GCPInvalidConfig(t *testing.T) {
	t.Parallel()
	_, err := NewProvider(FactoryConfig{
		Provider: "gcp-cloudkms",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidConfig))
	assert.Contains(t, err.Error(), "gcp provider requires")
}

func TestNewProvider_WithAuditArchiver(t *testing.T) {
	t.Parallel()
	archiver, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath: filepath.Join(t.TempDir(), "audit.jsonl"),
	})
	require.NoError(t, err)

	p, err := NewProvider(FactoryConfig{
		Provider:   "local",
		MasterKey:  testMasterKey(),
		Archiver:   archiver,
		ProviderID: "test-provider",
	})
	require.NoError(t, err)
	require.NotNil(t, p)

	// The returned provider should be an audited wrapper
	_, isAudited := p.(*auditedProvider)
	assert.True(t, isAudited, "provider should be wrapped with audit")
	CloseProvider(p)
}

func TestProviderID(t *testing.T) {
	t.Parallel()

	// Explicit ProviderID takes precedence
	assert.Equal(t, "my-id", providerID(FactoryConfig{
		Provider:   "local",
		ProviderID: "my-id",
	}))

	// Falls back to Provider name
	assert.Equal(t, "aws-kms", providerID(FactoryConfig{
		Provider: "aws-kms",
	}))

	// Empty Provider defaults to "local"
	assert.Equal(t, "local", providerID(FactoryConfig{
		Provider: "",
	}))
}

func TestCloseProvider_NilSafe(t *testing.T) {
	t.Parallel()
	// Should not panic
	CloseProvider(nil)
}

func TestCloseProvider_ClosesProvider(t *testing.T) {
	t.Parallel()
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: testMasterKey(),
	})
	require.NoError(t, err)

	CloseProvider(p)

	// After close, operations should fail
	_, err = p.GenerateDataKey(context.Background(), KeyGenOpts{})
	assert.Equal(t, ErrClosed, err)
}
