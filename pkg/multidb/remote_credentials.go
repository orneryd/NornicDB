package multidb

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/url"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
)

const remoteCredentialPrefix = "enc:v1:"

// resolveRemoteCredentials converts stored constituent metadata to the runtime
// identity used by its remote engine. Only this manager boundary decrypts stored
// passwords. URI credentials become reusable Basic credentials so transaction
// URLs returned by the remote cannot drop authentication on later requests.
// The returned copy is never persisted or used by metadata/display callers.
func (m *DatabaseManager) resolveRemoteCredentials(ref ConstituentRef, authToken string) (ConstituentRef, error) {
	endpoint, err := url.Parse(strings.TrimSpace(ref.URI))
	if err != nil {
		return ConstituentRef{}, observability.RedactError(err, observability.ClientCredentials(ref.URI)...)
	}
	ref.AuthMode = strings.ToLower(strings.TrimSpace(ref.AuthMode))
	if ref.AuthMode == "user_password" {
		ref.Password, err = m.decryptStoredRemotePassword(ref.Password)
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
		}
	} else if endpoint.User != nil && strings.TrimSpace(authToken) == "" {
		// Preserve explicit credentials and forwarded-token precedence. URL
		// userinfo supplies the Basic identity when neither is configured.
		ref.AuthMode = "user_password"
		ref.User = endpoint.User.Username()
		ref.Password, _ = endpoint.User.Password()
	}
	endpoint.User = nil
	ref.URI = endpoint.String()
	return ref, nil
}

type remoteCredentialCipher struct {
	aead cipher.AEAD
}

func newRemoteCredentialCipher(secret string) (*remoteCredentialCipher, error) {
	keyMaterial := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(keyMaterial[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &remoteCredentialCipher{aead: aead}, nil
}

func (c *remoteCredentialCipher) encrypt(plaintext string) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := c.aead.Seal(nil, nonce, []byte(plaintext), nil)
	payload := append(nonce, ciphertext...)
	return remoteCredentialPrefix + base64.StdEncoding.EncodeToString(payload), nil
}

func (c *remoteCredentialCipher) decrypt(encoded string) (string, error) {
	if !strings.HasPrefix(encoded, remoteCredentialPrefix) {
		return "", localizedError(localization.MultidbCredentialFormatInvalid(), nil)
	}
	raw := strings.TrimPrefix(encoded, remoteCredentialPrefix)
	payload, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", err
	}
	nonceSize := c.aead.NonceSize()
	if len(payload) < nonceSize {
		return "", localizedError(localization.MultidbCredentialPayloadTruncated(), nil)
	}
	nonce := payload[:nonceSize]
	ciphertext := payload[nonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func isEncryptedRemoteCredential(v string) bool {
	return strings.HasPrefix(strings.TrimSpace(v), remoteCredentialPrefix)
}

func (m *DatabaseManager) encryptRemotePassword(password string) (string, error) {
	if strings.TrimSpace(password) == "" {
		return "", localizedError(localization.MultidbRemotePasswordEmpty(), nil)
	}
	if m.remoteCredentialCipher == nil {
		return "", localizedError(localization.MultidbCredentialKeyConfigurationRequired(), nil)
	}
	return m.remoteCredentialCipher.encrypt(password)
}

func (m *DatabaseManager) decryptStoredRemotePassword(stored string) (string, error) {
	stored = strings.TrimSpace(stored)
	if stored == "" {
		return "", localizedError(localization.MultidbStoredPasswordMissing(), nil)
	}
	if !isEncryptedRemoteCredential(stored) {
		// Backward-compatibility path: existing plaintext metadata.
		return stored, nil
	}
	if m.remoteCredentialCipher == nil {
		return "", localizedError(localization.MultidbCredentialDecryptKeyRequired(), nil)
	}
	return m.remoteCredentialCipher.decrypt(stored)
}
