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
	if ref.AuthMode == "user_password" || (ref.Password != "" && strings.TrimSpace(authToken) == "") {
		ref.Password, err = m.decryptStoredRemotePassword(ref.Password)
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
		}
		ref.AuthMode = "user_password"
	} else if endpoint.User != nil && strings.TrimSpace(authToken) == "" {
		// Legacy records remain usable before a writable, keyed migration.
		ref.AuthMode = "user_password"
		ref.User = endpoint.User.Username()
		ref.Password, _ = endpoint.User.Password()
	} else {
		// URI-origin credentials are encrypted fallback fields in metadata.
		// A forwarded token takes precedence over that fallback.
		ref.User, ref.Password = "", ""
	}
	endpoint.User = nil
	if err := mapRemoteURLCredentials(endpoint, func(value string) (string, error) {
		if !isEncryptedRemoteCredential(value) {
			return value, nil
		}
		return m.decryptStoredRemotePassword(value)
	}); err != nil {
		return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
	}
	ref.URI = endpoint.String()
	return ref, nil
}

// normalizeRemoteCredentials is the persistence representation owner. New
// explicit input is always encrypted, even if its text starts with enc:v1:.
// Existing ciphertext is retained unchanged by metadata rewrites and migration.
// URI-origin fallback credentials keep their forwarding auth mode, so a caller
// token still takes precedence; explicit user_password credentials always win.
func (m *DatabaseManager) normalizeRemoteCredentials(ref ConstituentRef, newInput bool) (ConstituentRef, error) {
	if ref.Type != "remote" {
		return ref, nil
	}
	endpoint, err := url.Parse(strings.TrimSpace(ref.URI))
	if err != nil {
		if !newInput {
			// Do not turn an existing unusable endpoint into an open failure.
			return ref, nil
		}
		return ConstituentRef{}, observability.RedactError(err, observability.ClientCredentials(ref.URI)...)
	}
	explicit := strings.EqualFold(strings.TrimSpace(ref.AuthMode), "user_password")
	uriCredential := endpoint.User != nil && !explicit
	if uriCredential {
		ref.User = endpoint.User.Username()
		ref.Password, _ = endpoint.User.Password()
	}
	endpoint.User = nil
	err = mapRemoteURLCredentials(endpoint, func(value string) (string, error) {
		if !newInput && isEncryptedRemoteCredential(value) {
			return value, nil
		}
		return m.encryptRemoteCredential(value)
	})
	if err != nil {
		return ConstituentRef{}, localizedError(localization.MultidbCompositeSecureRemoteCredentialsFailed(ref.Alias, err), err)
	}
	ref.URI = endpoint.String()
	if uriCredential || (ref.Password != "" && (newInput || !isEncryptedRemoteCredential(ref.Password))) {
		if explicit {
			ref.Password, err = m.encryptRemotePassword(ref.Password)
		} else {
			// URL userinfo permits an empty password; explicit DDL does not.
			ref.Password, err = m.encryptRemoteCredential(ref.Password)
		}
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbCompositeSecureRemoteCredentialsFailed(ref.Alias, err), err)
		}
	}
	return ref, nil
}

// mapRemoteURLCredentials preserves query spelling, ordering and repeated keys.
// Display, persistence and runtime all use observability's canonical classifier.
// Only credential values change; escaped delimiters remain part of their value.
func mapRemoteURLCredentials(endpoint *url.URL, transform func(string) (string, error)) error {
	mapQuery := func(query string) (string, error) {
		if query == "" {
			return query, nil
		}
		parts := strings.Split(query, "&")
		for i, part := range parts {
			key, value, found := strings.Cut(part, "=")
			decodedKey, err := url.QueryUnescape(key)
			if err != nil {
				return "", err
			}
			if !found || !observability.IsSensitiveURLQueryName(decodedKey) {
				continue
			}
			decoded, err := url.QueryUnescape(value)
			if err != nil {
				return "", err
			}
			mapped, err := transform(decoded)
			if err != nil {
				return "", err
			}
			if mapped != decoded {
				parts[i] = key + "=" + url.QueryEscape(mapped)
			}
		}
		return strings.Join(parts, "&"), nil
	}
	query, err := mapQuery(endpoint.RawQuery)
	if err != nil {
		return err
	}
	endpoint.RawQuery = query
	if fragment := endpoint.EscapedFragment(); strings.Contains(fragment, "=") {
		mapped, err := mapQuery(fragment)
		if err != nil {
			return err
		}
		endpoint.Fragment, err = url.PathUnescape(mapped)
		if err != nil {
			return err
		}
		endpoint.RawFragment = mapped
	}
	return nil
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
	return m.encryptRemoteCredential(password)
}

func (m *DatabaseManager) encryptRemoteCredential(password string) (string, error) {
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
