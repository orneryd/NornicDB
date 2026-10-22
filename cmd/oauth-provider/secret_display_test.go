package main

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"testing"
)

func TestOAuthSecretProcess(t *testing.T) {
	if os.Getenv("NORNICDB_TEST_OAUTH_HELP") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"oauth-provider"}, os.Args[i+1:]...)
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
			main()
			os.Exit(0)
		}
	}
	os.Exit(2)
}

func TestOAuthSecretHelp(t *testing.T) {
	const credential = "synthetic-oauth-cli-credential-314159"
	for _, args := range [][]string{{"--help"}, {"--client-secret=" + credential, "--help"}} {
		argv := append([]string{"-test.run=^TestOAuthSecretProcess$", "--"}, args...)
		cmd := exec.Command(os.Args[0], argv...)
		cmd.Env = []string{"NORNICDB_TEST_OAUTH_HELP=1", "HOME=" + t.TempDir(), "PATH=/usr/local/go/bin:/usr/bin:/bin"}
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatal("OAuth CLI help failed (output withheld)")
		}
		if bytes.Contains(output, []byte(credential)) || bytes.Contains(output, []byte(defaultClientSecret)) {
			t.Error("OAuth help disclosed a credential (output withheld)")
		}
		if !bytes.Contains(output, []byte("-client-secret")) || !bytes.Contains(output, []byte(defaultClientID)) {
			t.Error("OAuth help or ordinary defaults changed")
		}
	}
}
