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

// isCredentialFreeSimpleHTTPURL recognizes the common canonical endpoint form
// that cannot contain user information, a credential query, or a fragment.
// Keeping this conservative lets runtime lookup avoid parsing and serializing
// an unchanged URL while all other forms continue through the full mapper.
func isCredentialFreeSimpleHTTPURL(raw string) bool {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return false
	}
	var rest string
	switch {
	case strings.HasPrefix(raw, "http://"):
		rest = raw[len("http://"):]
	case strings.HasPrefix(raw, "https://"):
		rest = raw[len("https://"):]
	default:
		return false
	}

	authority := rest
	path := ""
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		authority, path = rest[:slash], rest[slash:]
	}
	if authority == "" || strings.ContainsAny(authority, "@?#\\%[]") {
		return false
	}
	host := authority
	if colon := strings.LastIndexByte(authority, ':'); colon >= 0 {
		if strings.IndexByte(authority, ':') != colon {
			return false
		}
		host = authority[:colon]
		port := authority[colon+1:]
		if port == "" {
			return false
		}
		for i := 0; i < len(port); i++ {
			if port[i] < '0' || port[i] > '9' {
				return false
			}
		}
	}
	if host == "" {
		return false
	}
	for i := 0; i < len(host); i++ {
		c := host[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '-') {
			return false
		}
	}
	for i := 0; i < len(path); i++ {
		c := path[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~' ||
			c == '!' || c == '$' || c == '&' || c == '\'' || c == '(' || c == ')' ||
			c == '*' || c == '+' || c == ',' || c == ';' || c == '=' || c == ':' || c == '/' {
			continue
		}
		if c != '%' || i+2 >= len(path) || !isHexByte(path[i+1]) || !isHexByte(path[i+2]) {
			return false
		}
		i += 2
	}
	return true
}

func isHexByte(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

type remoteCredentialFastPathKey struct {
	database *DatabaseInfo
	index    int
}

func runtimeCredentialFreeRemoteRef(ref ConstituentRef) bool {
	return ref.Type == "remote" &&
		ref.User == "" && ref.Password == "" && ref.SecretRef == "" &&
		(ref.AuthMode == "" || ref.AuthMode == "oidc_forwarding") &&
		isCredentialFreeSimpleHTTPURL(ref.URI)
}

// primeRuntimeCredentialFastPaths caches only a negative credential finding
// after the URL and stored fields have been checked.
func (m *DatabaseManager) primeRuntimeCredentialFastPaths() {
	fastPaths := make(map[remoteCredentialFastPathKey]struct{})
	for _, database := range m.databases {
		for i := range database.Constituents {
			if runtimeCredentialFreeRemoteRef(database.Constituents[i]) {
				fastPaths[remoteCredentialFastPathKey{database: database, index: i}] = struct{}{}
			}
		}
	}
	m.runtimeCredentialFreeRemoteRefs = fastPaths
}

// resolveRemoteCredentials converts stored constituent metadata to the runtime
// identity used by its remote engine. Only this manager boundary decrypts stored
// passwords. URI credentials become reusable Basic credentials so transaction
// URLs returned by the remote cannot drop authentication on later requests.
// The returned copy is never persisted or used by metadata/display callers.
func (m *DatabaseManager) resolveRemoteCredentials(ref ConstituentRef, authToken string) (ConstituentRef, error) {
	simpleCredentialFreeURL := isCredentialFreeSimpleHTTPURL(ref.URI)
	var endpoint *url.URL
	var err error
	if !simpleCredentialFreeURL {
		endpoint, err = url.Parse(strings.TrimSpace(ref.URI))
		if err != nil {
			return ConstituentRef{}, observability.RedactError(err, observability.ClientCredentials(ref.URI)...)
		}
	}
	ref.AuthMode = strings.ToLower(strings.TrimSpace(ref.AuthMode))
	explicit := ref.AuthMode == "user_password"
	useStored := explicit || ((ref.Password != "" || ref.User != "") && strings.TrimSpace(authToken) == "")
	if useStored && !explicit && isEncryptedRemoteCredential(ref.User) {
		ref.User, err = m.decryptStoredRemotePassword(ref.User)
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
		}
	}
	if useStored {
		if ref.Password != "" || explicit {
			ref.Password, err = m.decryptStoredRemotePassword(ref.Password)
			if err != nil {
				return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
			}
		}
		ref.AuthMode = "user_password"
	} else {
		// Forwarded tokens take precedence over URI-origin fallback fields.
		ref.User, ref.Password = "", ""
	}
	if !simpleCredentialFreeURL {
		err = observability.MapURLCredentials(endpoint, func(user *url.Userinfo) *url.Userinfo {
			if !useStored && strings.TrimSpace(authToken) == "" {
				// Legacy records remain usable before a writable, keyed migration.
				ref.AuthMode = "user_password"
				ref.User = user.Username()
				ref.Password, _ = user.Password()
			}
			return nil
		}, func(value string, decodeErr error) (string, error) {
			if decodeErr != nil {
				return "", decodeErr
			}
			if !isEncryptedRemoteCredential(value) {
				return value, nil
			}
			return m.decryptStoredRemotePassword(value)
		})
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
		}
		ref.URI = endpoint.String()
	}
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
	uriCredential := false
	err = observability.MapURLCredentials(endpoint, func(user *url.Userinfo) *url.Userinfo {
		if !explicit {
			uriCredential = true
			ref.User = user.Username()
			ref.Password, _ = user.Password()
		}
		return nil
	}, func(value string, decodeErr error) (string, error) {
		if decodeErr != nil {
			return "", decodeErr
		}
		if !newInput && isEncryptedRemoteCredential(value) {
			return value, nil
		}
		return m.encryptRemoteCredential(value)
	})
	if err != nil {
		return ConstituentRef{}, localizedError(localization.MultidbCompositeSecureRemoteCredentialsFailed(ref.Alias, err), err)
	}
	ref.URI = endpoint.String()
	// URI userinfo may be a username-only token. Its username is credential
	// material just like its password; a separate explicit USER remains the
	// public identity selected by the explicit user_password protocol.
	if !explicit && ref.User != "" && (newInput || !isEncryptedRemoteCredential(ref.User)) {
		ref.User, err = m.encryptRemoteCredential(ref.User)
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbCompositeSecureRemoteCredentialsFailed(ref.Alias, err), err)
		}
	}
	if uriCredential || (!explicit && ref.User != "" && ref.Password == "") || (ref.Password != "" && (newInput || !isEncryptedRemoteCredential(ref.Password))) {
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

// RemoteUserForDisplay distinguishes an explicit public USER identity from
// credential material extracted from URL userinfo, including legacy records.
func RemoteUserForDisplay(user, authMode string) string {
	if user != "" && (!strings.EqualFold(strings.TrimSpace(authMode), "user_password") || isEncryptedRemoteCredential(user)) {
		return observability.RedactedPlaceholder()
	}
	return user
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
