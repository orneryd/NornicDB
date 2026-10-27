package config

import (
	"os"
	"strings"
)

// RemoteCredentialEncryptionKey resolves the durable remote-credential key and
// its nonsecret source. Bootstrap must retain this selection for both storage
// upgrade and runtime decryption. The returned material is secret and must not
// be logged. An empty result requires explicit configuration before storing
// credentials; a newly generated JWT default is deliberately ineligible.
//
// For example, set NORNICDB_REMOTE_CREDENTIALS_KEY for a dedicated key, or use
// an explicitly configured database password or JWT signing secret as fallback.
func (c *Config) RemoteCredentialEncryptionKey() (material, source string) {
	if value := strings.TrimSpace(os.Getenv("NORNICDB_REMOTE_CREDENTIALS_KEY")); value != "" {
		return value, "environment"
	}
	if c == nil {
		return "", ""
	}
	if value := strings.TrimSpace(c.Database.EncryptionPassword); value != "" {
		return value, "database_encryption_password"
	}
	if value := strings.TrimSpace(c.Auth.JWTSecret); value != "" && value != strings.TrimSpace(c.generatedJWTSecret) {
		return value, "jwt_signing_secret"
	}
	return "", ""
}
