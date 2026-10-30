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

	"github.com/orneryd/nornicdb/internal/cyphertext"
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

// runtimeConstituent is a private, in-memory resolution of one composite
// constituent. It is rebuilt with metadata writes and never serialized.
type runtimeConstituent struct {
	ref                  ConstituentRef
	resolvedDatabaseName string
	storedRef            ConstituentRef
	forwardedRef         ConstituentRef
	explicitAuth         bool
	err                  error
	forwardedErr         error
	redactedIdentity     string
	redactionValues      []string
}

// rebuildRuntimeConstituents resolves credentials and aliases once when the
// composite catalog is loaded or updated. Exact registered spellings map
// directly to the canonical last-registration-wins entry. Mixed-case aliases
// still use the canonical fallback at lookup to preserve catalog semantics.
func (m *DatabaseManager) rebuildRuntimeConstituents() {
	for _, database := range m.databases {
		m.rebuildRuntimeConstituentsFor(database)
	}
}

func (m *DatabaseManager) rebuildRuntimeConstituentsFor(database *DatabaseInfo) {
	if database == nil || database.Type != "composite" {
		if database != nil {
			database.runtimeConstituentsByAlias = nil
		}
		return
	}

	existing := database.runtimeConstituentsByAlias
	byAlias := make(map[string]*runtimeConstituent, len(database.Constituents))
	canonical := make(map[string]*runtimeConstituent, len(database.Constituents))
	for i := len(database.Constituents) - 1; i >= 0; i-- {
		ref := database.Constituents[i]
		key := cyphertext.NameLower(ref.Alias)
		resolved, exists := canonical[key]
		if !exists {
			if strings.EqualFold(strings.TrimSpace(ref.Type), "remote") {
				if cached := existing[ref.Alias]; cached != nil && cached.ref == ref {
					resolved = cached
				}
			}
			if resolved == nil {
				resolved = m.resolveRuntimeConstituent(ref)
			}
			canonical[key] = resolved
		}
		if _, exists := byAlias[ref.Alias]; !exists {
			byAlias[ref.Alias] = resolved
		}
	}
	database.runtimeConstituentsByAlias = byAlias
}

func (m *DatabaseManager) resolveRuntimeConstituent(ref ConstituentRef) *runtimeConstituent {
	resolved := &runtimeConstituent{ref: ref}
	if ref.Type != "remote" {
		resolved.resolvedDatabaseName, resolved.err = m.resolveDatabaseInternal(ref.DatabaseName)
		return resolved
	}

	authMode := ref.AuthMode
	resolved.explicitAuth = strings.EqualFold(strings.TrimSpace(authMode), "user_password")
	if ref.ConnectionV1 != "" {
		connection, err := m.openRemoteConnection(ref)
		if err == nil {
			resolved.explicitAuth = connection.origin == remoteConnectionExplicit
			authMode = connection.authMode
		}
	}
	resolved.storedRef, resolved.err = m.resolveRemoteCredentials(ref, "")
	resolved.redactionValues = remoteCredentialRedactionValues(ref, resolved.storedRef)
	if resolved.err == nil {
		resolved.redactedIdentity = observability.RedactText(resolved.storedRef.URI, resolved.redactionValues...)
	} else {
		resolved.redactedIdentity = observability.RedactText(ref.URI, resolved.redactionValues...)
	}

	if resolved.explicitAuth {
		resolved.forwardedRef = resolved.storedRef
		resolved.forwardedErr = resolved.err
		return resolved
	}
	if resolved.err == nil {
		resolved.forwardedRef = resolved.storedRef
		resolved.forwardedRef.User = ""
		resolved.forwardedRef.Password = ""
		resolved.forwardedRef.AuthMode = strings.ToLower(strings.TrimSpace(authMode))
		return resolved
	}

	// A forwarded token historically bypasses stored credentials. Preserve that
	// fallback if resolving the stored identity fails, while still keeping it out
	// of the per-request lookup path.
	resolved.forwardedRef, resolved.forwardedErr = m.resolveRemoteCredentialsWithAuth(ref, true)
	if resolved.forwardedErr == nil {
		resolved.redactionValues = remoteCredentialRedactionValues(ref, resolved.forwardedRef)
		resolved.redactedIdentity = observability.RedactText(resolved.forwardedRef.URI, resolved.redactionValues...)
	}
	return resolved
}

func remoteCredentialRedactionValues(ref, runtimeRef ConstituentRef) []string {
	values := observability.ClientCredentials(ref.URI)
	values = append(values, observability.ClientCredentials(runtimeRef.URI)...)
	for _, value := range []string{ref.User, ref.Password, runtimeRef.User, runtimeRef.Password} {
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

// resolveRemoteCredentials converts stored constituent metadata to the runtime
// identity used by its remote engine. Only this manager boundary decrypts stored
// passwords. URI credentials become reusable Basic credentials so transaction
// URLs returned by the remote cannot drop authentication on later requests.
// The returned copy is never persisted or used by metadata/display callers.
func (m *DatabaseManager) resolveRemoteCredentials(ref ConstituentRef, authToken string) (ConstituentRef, error) {
	return m.resolveRemoteCredentialsWithAuth(ref, strings.TrimSpace(authToken) != "")
}

func (m *DatabaseManager) resolveRemoteCredentialsWithAuth(ref ConstituentRef, hasForwardedAuth bool) (ConstituentRef, error) {
	tagged := ref.ConnectionV1 != ""
	if tagged {
		connection, err := m.openRemoteConnection(ref)
		if err != nil {
			return ConstituentRef{}, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(ref.Alias, err), err)
		}
		ref.URI, ref.User, ref.Password, ref.AuthMode = connection.uri, connection.user, connection.password, connection.authMode
		ref.ConnectionV1 = ""
	}

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
	useStored := explicit || ((ref.Password != "" || ref.User != "") && !hasForwardedAuth)

	if useStored {
		if !tagged && (ref.Password != "" || explicit) {
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
		if user := endpoint.User; user != nil {
			if !useStored && !hasForwardedAuth {
				ref.AuthMode = "user_password"
				ref.User = user.Username()
				ref.Password, _ = user.Password()
			}
			endpoint.User = nil
		}
		// URI query and fragment values have always been literal data in the
		// legacy representation. Only the tagged descriptor and old Password
		// field are ciphertext domains; never guess from text in URI or USER.
		ref.URI = endpoint.String()
	}

	return ref, nil
}

// prepareRemoteConnectionInput validates the existing URI admission boundary
// and encrypts newly admitted credential material into the tagged authority.
// Input strings never acquire ciphertext semantics from their contents.
func (m *DatabaseManager) prepareRemoteConnectionInput(ref ConstituentRef) (ConstituentRef, error) {
	if ref.Type != "remote" {
		return ref, nil
	}
	endpoint, err := url.Parse(strings.TrimSpace(ref.URI))
	if err != nil {
		return ConstituentRef{}, observability.RedactError(err, observability.ClientCredentials(ref.URI)...)
	}
	// Retain existing sensitive-query/fragment escape validation. The mapped
	// copy is not persisted: the descriptor keeps the exact admitted input URI.
	err = observability.MapURLCredentials(endpoint, nil, func(value string, decodeErr error) (string, error) { return value, decodeErr })
	if err != nil {
		return ConstituentRef{}, localizedError(localization.MultidbCompositeSecureRemoteCredentialsFailed(ref.Alias, err), err)
	}
	if !remoteConnectionNeedsDescriptor(ref) {
		return ref, nil
	}
	return m.sealRemoteConnection(ref, literalRemoteConnection)
}

// RemoteUserForDisplay distinguishes an explicit public USER identity from
// credential material extracted from URL userinfo, including legacy records.
func RemoteUserForDisplay(user, authMode string) string {
	if user != "" && !strings.EqualFold(strings.TrimSpace(authMode), "user_password") {
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

// remoteCredentialPolicy is the private typed policy for
// encryptRemoteCredential. It states whether a blank value is a caller contract
// violation or a legitimate byte-exact credential. The zero value is strict.
type remoteCredentialPolicy uint8

const (
	// remoteCredentialRequireValue rejects empty and whitespace-only values.
	remoteCredentialRequireValue remoteCredentialPolicy = iota
	// remoteCredentialAllowEmpty encrypts the value unchanged, including a
	// blank one, so URL-origin credentials stay byte-exact.
	remoteCredentialAllowEmpty
)

// encryptRemoteCredential is the single credential encryption owner. Strict
// callers get the localized empty-password rejection before any cipher check;
// allow-empty callers encrypt URL-origin values byte-exactly. Both paths share
// the missing-cipher rejection and the unchanged nonce-bearing output format.
func (m *DatabaseManager) encryptRemoteCredential(policy remoteCredentialPolicy, value string) (string, error) {
	if policy == remoteCredentialRequireValue && strings.TrimSpace(value) == "" {
		return "", localizedError(localization.MultidbRemotePasswordEmpty(), nil)
	}
	if m.remoteCredentialCipher == nil {
		return "", localizedError(localization.MultidbCredentialKeyConfigurationRequired(), nil)
	}
	return m.remoteCredentialCipher.encrypt(value)
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
