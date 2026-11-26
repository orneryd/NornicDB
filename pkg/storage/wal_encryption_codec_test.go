package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALFrameCipherAuthentication(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	cipher, err := newWALFrameCipher(key)
	require.NoError(t, err)
	secret := []byte("private-node-property-marker")
	for _, version := range []uint8{walFormatVersionCurrent, walFormatVersionSpanned} {
		sealed, encryptedVersion, err := cipher.seal(secret, version)
		require.NoError(t, err)
		require.True(t, walFrameEncrypted(encryptedVersion))
		require.NotContains(t, string(sealed), string(secret))
		opened, err := cipher.open(sealed, encryptedVersion)
		require.NoError(t, err)
		require.Equal(t, secret, opened)
		require.Equal(t, version, walPlaintextFrameVersion(encryptedVersion))

		wrongKey, err := newWALFrameCipher(bytes.Repeat([]byte{0x24}, 32))
		require.NoError(t, err)
		_, err = wrongKey.open(sealed, encryptedVersion)
		require.ErrorIs(t, err, ErrWALCorrupted)
		_, err = (*walFrameCipher)(nil).open(sealed, encryptedVersion)
		require.True(t, errors.Is(err, ErrWALKeyRequired))

		for _, position := range []int{0, len(sealed) / 2, len(sealed) - 1} {
			mutated := append([]byte(nil), sealed...)
			mutated[position] ^= 1
			_, err := cipher.open(mutated, encryptedVersion)
			require.ErrorIs(t, err, ErrWALCorrupted)
		}
		_, err = cipher.open(sealed[:len(sealed)-1], encryptedVersion)
		require.ErrorIs(t, err, ErrWALCorrupted)
	}
}
