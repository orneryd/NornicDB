package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/spf13/cobra"
)

func TestCanonicalRedactKeysCLIExtensions(t *testing.T) {
	previous := observability.DefaultRedactKeys
	t.Cleanup(func() { observability.DefaultRedactKeys = previous })
	observability.DefaultRedactKeys = append(append([]string(nil), previous...), "custom.sealed_value")
	const credential = "synthetic-canonical-cli-719832"
	var runtimeValue string
	var output bytes.Buffer
	cmd := &cobra.Command{Use: "canonical-probe", Run: func(*cobra.Command, []string) {}}
	cmd.SetOut(&output)
	cmd.Flags().StringVar(&runtimeValue, "custom-sealed-value", credential, "Custom credential")
	cmd.Flags().Int("public-limit", 42, "Public limit")
	protectFlagDefaults(cmd)
	if cmd.Flags().Lookup("custom-sealed-value").DefValue != observability.RedactedPlaceholder() {
		t.Error("canonical extension escaped CLI flag metadata; capture withheld")
	}
	if err := cmd.Help(); err != nil {
		t.Fatal("help did not render")
	}
	if strings.Contains(output.String(), credential) || !strings.Contains(output.String(), observability.RedactedPlaceholder()) {
		t.Error("canonical extension escaped CLI help; capture withheld")
	}
	actual, err := cmd.Flags().GetString("custom-sealed-value")
	if err != nil || actual != credential || runtimeValue != credential {
		t.Error("display projection changed runtime credentials")
	}
	if cmd.Flags().Lookup("public-limit").DefValue != "42" {
		t.Error("public flag default changed")
	}
	if shellDisplayColumn("custom.sealed_value") != observability.RedactedPlaceholder() ||
		shellDisplayValue("custom.sealed_value", credential) != observability.RedactedPlaceholder() {
		t.Error("canonical extension escaped CLI config columns/values")
	}
}
