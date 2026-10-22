package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigDisplaySecretDecode(t *testing.T) {
	const credential = "synthetic-yaml-credential-314159"
	path := filepath.Join(t.TempDir(), "config.yaml")
	// A misplaced credential triggers YAML's scalar-quoting type error. The
	// correctly named API-key field supplies the same owned credential context.
	data := []byte("embedding:\n  api_key: " + credential + "\nserver:\n  bolt_port: " + credential + "\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("expected configuration type error")
	}
	var typed *yaml.TypeError
	if !errors.As(err, &typed) {
		t.Error("YAML error identity lost")
	}
	if strings.Contains(err.Error(), credential[:7]) {
		t.Error("YAML diagnostic disclosed a credential (details withheld)")
	}
}

func TestConfigDisplaySecretSyntaxAndCause(t *testing.T) {
	const credential = "synthetic-yaml-syntax-credential-314159"
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("embedding:\n  api_key: *"+credential+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFromFile(path)
	if err == nil || !strings.Contains(err.Error(), "failed to parse config file") {
		t.Fatal("configuration parser failure was not preserved")
	}
	if strings.Contains(err.Error(), credential) {
		t.Error("parser diagnostic disclosed credential (details withheld)")
	}
	cause := err
	for errors.Unwrap(cause) != nil {
		cause = errors.Unwrap(cause)
	}
	if !strings.Contains(cause.Error(), credential) {
		t.Error("original parser cause was not preserved")
	}
}

func TestConfigDisplayAuditSigningKey(t *testing.T) {
	const credential = "synthetic-audit-yaml-sign-key-42813695"
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("encryption_audit_sign_key: &audit " + credential + "\nserver:\n  bolt_port: *audit\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("audit-key alias in integer field did not fail decoding")
	}
	var typed *yaml.TypeError
	if !errors.As(err, &typed) {
		t.Error("YAML error identity lost")
	}
	if strings.Contains(err.Error(), credential[:7]) {
		t.Error("YAML diagnostic disclosed signing key material; output withheld")
	}
}
