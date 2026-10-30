package kms

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalProvider_DataKeyRoundTripAndRotate(t *testing.T) {
	t.Parallel()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i + 1)
	}
	p, err := NewLocalProvider(LocalConfig{
		MasterKey: master,
		KeyURI:    "kms://local/test",
	})
	require.NoError(t, err)
	defer CloseProvider(p)

	dek, err := p.GenerateDataKey(context.Background(), KeyGenOpts{
		Algorithm: "AES-256-GCM",
		TTL:       time.Hour,
		Label:     "unit",
	})
	require.NoError(t, err)
	require.Equal(t, "kms://local/test", dek.KeyURI)
	require.Len(t, dek.Plaintext, 32)
	require.NotEmpty(t, dek.Ciphertext)

	plain, err := p.DecryptDataKey(context.Background(), dek.Ciphertext, DecryptOpts{KeyURI: dek.KeyURI})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, plain)

	rot, err := p.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{
		KeyURI: dek.KeyURI,
		TTL:    time.Hour,
	})
	require.NoError(t, err)
	require.Greater(t, rot.Version, dek.Version)

	rotPlain, err := p.DecryptDataKey(context.Background(), rot.Ciphertext, DecryptOpts{KeyURI: dek.KeyURI})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, rotPlain)
}

func TestLocalProvider_InvalidMasterKey(t *testing.T) {
	t.Parallel()
	_, err := NewLocalProvider(LocalConfig{
		MasterKey: []byte("short"),
	})
	require.Error(t, err)
}
