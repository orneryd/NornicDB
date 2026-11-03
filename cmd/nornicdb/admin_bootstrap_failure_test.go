// Package main tests the serve command's admin bootstrap outcome.
// Author: Codex, F3 CLI bootstrap error correction, 2026-11-02.
package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

const (
	// adminBootstrapSetupFailedText is the English catalog text of
	// nornicdbcli.serve.admin_setup_failed.
	adminBootstrapSetupFailedText = "Admin user setup failed"
	// adminBootstrapCreatedText is the English catalog text of
	// nornicdbcli.serve.admin_created.
	adminBootstrapCreatedText = "Admin user created"
	// adminBootstrapPassword is a password that satisfies the default
	// eight-character minimum, so only an existing account can reject it.
	adminBootstrapPassword = "admin-bootstrap-test-password"
)

// setAdminBootstrapGuardEnv pins the inputs the serve bootstrap reads and makes
// any run that continues past the bootstrap terminate deterministically: an
// invalid Bolt TLS client auth mode is rejected before any listener starts, so
// a bootstrap that does not abort ends at that later stage instead of serving.
func setAdminBootstrapGuardEnv(t *testing.T, password string) {
	t.Helper()
	t.Setenv("NORNICDB_LANGUAGE", "en")
	t.Setenv("NORNICDB_AUTH", "admin:"+password)
	t.Setenv("NORNICDB_HTTP_ADDRESS", "127.0.0.1")
	t.Setenv("NORNICDB_BOLT_ADDRESS", "127.0.0.1")
	t.Setenv("NORNICDB_CORS_ORIGINS", "http://localhost")
	t.Setenv("NORNICDB_QDRANT_GRPC_ENABLED", "false")
	t.Setenv("NORNICDB_BOLT_TLS_ENABLED", "true")
	t.Setenv("NORNICDB_BOLT_TLS_CERT", "unused-cert")
	t.Setenv("NORNICDB_BOLT_TLS_KEY", "unused-key")
	t.Setenv("NORNICDB_BOLT_TLS_CLIENT_AUTH_MODE", "admin-bootstrap-test-invalid-mode")
}

// executeServeAdminBootstrap runs the real serve command through the registered
// root with the native database, auth and localizer stack, and returns the command
// outcome together with the captured CLI output.
func executeServeAdminBootstrap(t *testing.T, cancelBeforeExecute bool, dataDir string, extraArgs ...string) (error, string) {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "settings.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("{}\n"), 0600))
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	root := newTestServerRoot(manager)
	var output bytes.Buffer
	root.SetOut(&output)
	root.SetErr(&output)
	if ctx := root.Context(); cancelBeforeExecute && ctx != nil {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		root.SetContext(canceled)
	}
	args := []string{
		"serve",
		"--config=" + configPath,
		"--headless",
		"--data-dir=" + dataDir,
		"--embedding-enabled=false",
		"--search-bm25-enabled=false",
		"--search-vector-enabled=false",
		"--http-port=0",
		"--bolt-port=0",
	}
	root.SetArgs(append(args, extraArgs...))
	return root.Execute(), output.String()
}

// requireCommandErrorID returns the localized command error a CLI run produced and
// asserts which message it rendered.
func requireCommandErrorID(t *testing.T, err error, id localization.MessageID) *commandError {
	t.Helper()
	require.Error(t, err)
	var commandErr *commandError
	require.ErrorAs(t, err, &commandErr)
	require.Equal(t, id, commandErr.message.ID)
	return commandErr
}

// TestServeAdminBootstrapRejectedCredentialAbortsStartup proves that a bootstrap
// failure other than an existing account ends the serve command with the original
// error instead of starting a server without the intended admin.
func TestServeAdminBootstrapRejectedCredentialAbortsStartup(t *testing.T) {
	const rejectedPassword = "short"
	setAdminBootstrapGuardEnv(t, rejectedPassword)
	dataDir := filepath.Join(t.TempDir(), "data")

	result, output := executeServeAdminBootstrap(t, false, dataDir, "--admin-password="+rejectedPassword)

	commandErr := requireCommandErrorID(t, result, localization.MessageNornicDBCLICreateAuthenticatorFailed)
	require.ErrorIs(t, result, auth.ErrPasswordTooShort,
		"the command error must preserve the original CreateUser cause")
	require.Contains(t, commandErr.Error(), "minimum 8 characters required")
	require.NotContains(t, output, adminBootstrapSetupFailedText,
		"a rejected admin credential must not be reported as a possible existing account")
	require.NotContains(t, output, adminBootstrapCreatedText)
}

// TestServeAdminBootstrapCancellationAbortsStartup proves that a canceled command
// stops at the bootstrap instead of starting a server without the intended admin.
func TestServeAdminBootstrapCancellationAbortsStartup(t *testing.T) {
	setAdminBootstrapGuardEnv(t, adminBootstrapPassword)
	dataDir := filepath.Join(t.TempDir(), "data")

	result, output := executeServeAdminBootstrap(t, true, dataDir, "--admin-password="+adminBootstrapPassword)

	requireCommandErrorID(t, result, localization.MessageNornicDBCLICreateAuthenticatorFailed)
	require.ErrorIs(t, result, context.Canceled,
		"the command error must preserve the cancellation cause")
	require.NotContains(t, output, adminBootstrapSetupFailedText,
		"a canceled bootstrap must not be reported as a possible existing account")
	require.NotContains(t, output, adminBootstrapCreatedText)
}

// TestServeAdminBootstrapExistingAccountRemainsNonFatal proves the healthy
// control: a real auth.ErrUserExists from an account created by an earlier start
// stays nonfatal, so startup continues exactly as before.
func TestServeAdminBootstrapExistingAccountRemainsNonFatal(t *testing.T) {
	setAdminBootstrapGuardEnv(t, adminBootstrapPassword)
	dataDir := filepath.Join(t.TempDir(), "data")

	first, firstOutput := executeServeAdminBootstrap(t, false, dataDir, "--admin-password="+adminBootstrapPassword)
	requireCommandErrorID(t, first, localization.MessageNornicDBCLIBoltTLSClientAuthFailed)
	require.Contains(t, firstOutput, adminBootstrapCreatedText,
		"the first start must create the intended admin")

	second, secondOutput := executeServeAdminBootstrap(t, false, dataDir, "--admin-password="+adminBootstrapPassword)
	requireCommandErrorID(t, second, localization.MessageNornicDBCLIBoltTLSClientAuthFailed)
	require.Contains(t, secondOutput, adminBootstrapSetupFailedText,
		"the existing account keeps the established nonfatal wording")
	require.NotContains(t, secondOutput, adminBootstrapCreatedText)
}
