package encryption

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/require"
)

func TestEnvelopeEncryptor_RoundTrip(t *testing.T) {
	t.Parallel()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(255 - i)
	}
	provider, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: master,
		KeyURI:    "kms://local/envelope",
	})
	require.NoError(t, err)
	defer kms.CloseProvider(provider)

	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{
		DEKCacheTTL: time.Minute,
		Label:       "test",
	})

	plaintext := []byte("hello-cmek-envelope")
	payload, err := enc.Encrypt(context.Background(), plaintext)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	out, err := enc.Decrypt(context.Background(), payload)
	require.NoError(t, err)
	require.Equal(t, plaintext, out)
}

func TestEnvelopeEncryptor_TamperFails(t *testing.T) {
	t.Parallel()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i + 11)
	}
	provider, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: master,
	})
	require.NoError(t, err)
	defer kms.CloseProvider(provider)

	enc := NewEnvelopeEncryptor(provider, EnvelopeConfig{})
	payload, err := enc.Encrypt(context.Background(), []byte("tamper-me"))
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	payload[len(payload)-1] ^= 0xFF
	_, err = enc.Decrypt(context.Background(), payload)
	require.Error(t, err)
}
