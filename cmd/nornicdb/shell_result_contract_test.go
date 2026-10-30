package main

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestR3ShellResultValues(t *testing.T) {
	const homepage = "https://user:synthetic-result-pass-846291@example.test"
	const document = `{"password":"synthetic-business-value-846291"}`
	const value = "synthetic-password-column-846291"
	cmd := cliSecretProbeCommand(t, t.TempDir(), nil, []string{"shell", "--data-dir=" + t.TempDir()})
	cmd.Stdin = strings.NewReader("RETURN '" + homepage + "' AS homepage;\nRETURN '" + document + "' AS document;\nRETURN '" + value + "' AS password;\nexit\n")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal("shell failed; capture withheld")
	}
	for _, expected := range []string{homepage, document, value} {
		if !bytes.Contains(output, []byte(expected)) {
			t.Error("shell changed a query result cell; capture withheld")
		}
	}
}

func TestR3OAuthStartupRejectsCredentialIssuer(t *testing.T) {
	const credential = "synthetic-startup-issuer-846291"
	fixture := cliSecretProbeCommand(t, t.TempDir(), []string{
		"NORNICDB_AUTH_PROVIDER=oauth",
		"NORNICDB_OAUTH_ISSUER=https://user:" + credential + "@idp.example",
		"NORNICDB_OAUTH_CLIENT_ID=public-client",
		"NORNICDB_OAUTH_CLIENT_SECRET=synthetic-client-value",
		"NORNICDB_OAUTH_CALLBACK_URL=https://app.example/callback",
	}, []string{"serve", "--no-auth", "--headless", "--http-port=0", "--data-dir=" + t.TempDir()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, fixture.Path, fixture.Args[1:]...)
	cmd.Env, cmd.Dir = fixture.Env, fixture.Dir
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err == nil || !bytes.Contains(output, []byte("OAuth issuer")) {
		t.Error("server did not reject its credential-bearing issuer before startup; capture withheld")
	}
	if bytes.Contains(output, []byte(credential)) {
		t.Error("issuer startup error disclosed credential; capture withheld")
	}
}
