package kms

import (
	"context"
	"errors"
	"testing"
	"time"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeWrapper struct {
	configErr    error
	wrapperType  wrapping.WrapperType
	keyID        string
	encryptBlob  *wrapping.BlobInfo
	encryptErr   error
	decryptPlain []byte
	decryptErr   error
}

func (f *fakeWrapper) Type(context.Context) (wrapping.WrapperType, error) {
	return f.wrapperType, nil
}

func (f *fakeWrapper) KeyId(context.Context) (string, error) {
	return f.keyID, nil
}

func (f *fakeWrapper) SetConfig(context.Context, ...wrapping.Option) (*wrapping.WrapperConfig, error) {
	return &wrapping.WrapperConfig{}, f.configErr
}

func (f *fakeWrapper) Encrypt(context.Context, []byte, ...wrapping.Option) (*wrapping.BlobInfo, error) {
	if f.encryptErr != nil {
		return nil, f.encryptErr
	}
	if f.encryptBlob != nil {
		return f.encryptBlob, nil
	}
	return &wrapping.BlobInfo{Ciphertext: []byte("ciphertext")}, nil
}

func (f *fakeWrapper) Decrypt(context.Context, *wrapping.BlobInfo, ...wrapping.Option) ([]byte, error) {
	if f.decryptErr != nil {
		return nil, f.decryptErr
	}
	if f.decryptPlain != nil {
		return f.decryptPlain, nil
	}
	return []byte("plaintext"), nil
}

func TestWrappingAdapter_RoundTripAndRotate(t *testing.T) {
	t.Parallel()
	tw := wrapping.NewTestEnvelopeWrapper([]byte("0123456789abcdef0123456789abcdef"))
	tw.SetKeyId("kms://test/wrapper")
	a := newWrappingAdapter(tw)

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{
		TTL:       time.Minute,
		Algorithm: "AES-256-GCM",
	})
	require.NoError(t, err)
	require.NotEmpty(t, dek.Ciphertext)
	require.Len(t, dek.Plaintext, 32)

	plain, err := a.DecryptDataKey(context.Background(), dek.Ciphertext, DecryptOpts{})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, plain)

	rot, err := a.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{TTL: time.Minute})
	require.NoError(t, err)
	require.NotEmpty(t, rot.Ciphertext)
	rotPlain, err := a.DecryptDataKey(context.Background(), rot.Ciphertext, DecryptOpts{})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, rotPlain)
}

func TestWrappingAdapter_DefaultAlgorithmAndMetadata(t *testing.T) {
	t.Parallel()
	a := newWrappingAdapter(&fakeWrapper{
		wrapperType: wrapping.WrapperType("fake-provider"),
		keyID:       "kms://fake/key",
		encryptBlob: &wrapping.BlobInfo{Ciphertext: []byte("ciphertext")},
	})

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.NoError(t, err)
	require.Equal(t, "kms://fake/key", dek.KeyURI)
	require.Equal(t, "AES-256-GCM", dek.Algorithm)
	require.True(t, dek.ExpiresAt.IsZero())
	require.NotEmpty(t, dek.Ciphertext)

	meta, err := a.GetKeyMetadata(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, "kms://fake/key", meta.KeyURI)
	require.Equal(t, "fake-provider", meta.Provider)

	_, err = a.GetKeyMetadata(context.Background(), "kms://other")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestWrappingAdapter_ErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("generate_data_key_encrypt_error", func(t *testing.T) {
		a := newWrappingAdapter(&fakeWrapper{encryptErr: errors.New("boom")})
		_, err := a.GenerateDataKey(context.Background(), KeyGenOpts{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEncryptFailed)
	})

	t.Run("decrypt_data_key_decrypt_error", func(t *testing.T) {
		encrypted, err := encodeBlob(&wrapping.BlobInfo{Ciphertext: []byte("ciphertext")})
		require.NoError(t, err)
		a := newWrappingAdapter(&fakeWrapper{decryptErr: errors.New("boom")})
		_, err = a.DecryptDataKey(context.Background(), encrypted, DecryptOpts{})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDecryptFailed)
	})

	t.Run("rotate_data_key_zero_ttl_keeps_expires_zero", func(t *testing.T) {
		encrypted, err := encodeBlob(&wrapping.BlobInfo{Ciphertext: []byte("ciphertext")})
		require.NoError(t, err)
		a := newWrappingAdapter(&fakeWrapper{
			keyID:        "kms://fake/key",
			decryptPlain: []byte("plaintext"),
			encryptBlob:  &wrapping.BlobInfo{Ciphertext: []byte("rotated")},
		})

		rot, err := a.RotateDataKey(context.Background(), encrypted, RotateOpts{})
		require.NoError(t, err)
		require.Equal(t, "kms://fake/key", rot.KeyURI)
		require.Equal(t, "AES-256-GCM", rot.Algorithm)
		require.True(t, rot.ExpiresAt.IsZero())
		require.Equal(t, []byte("plaintext"), rot.Plaintext)
	})
}

func TestConfigureWrappingProviderPolicy(t *testing.T) {
	const credential = "kms-config-test-credential"
	config := map[string]string{"region": "test-region", "endpoint": ""}
	adapter, err := configureWrappingProvider[AWSProvider](context.Background(), &fakeWrapper{encryptErr: errors.New(credential)}, config, true, credential)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"region": "test-region", "disallow_env_vars": "true"}, config)
	_, err = adapter.GenerateDataKey(context.Background(), KeyGenOpts{})
	require.ErrorIs(t, err, ErrEncryptFailed)
	require.NotContains(t, err.Error(), credential)

	config = map[string]string{"region": "test-region"}
	adapter, err = configureWrappingProvider[AWSProvider](context.Background(), &fakeWrapper{configErr: errors.New(credential)}, config, false, credential)
	require.Nil(t, adapter)
	require.Error(t, err)
	require.NotContains(t, err.Error(), credential)
	require.NotContains(t, config, "disallow_env_vars")
}
