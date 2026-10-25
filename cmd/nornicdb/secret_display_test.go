package main

import (
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/spf13/cobra"
	"testing"
)

func TestSecretFlagDefaultsPreserveRuntime(t *testing.T) {
	for _, value := range []string{"", "synthetic-runtime-credential-817236"} {
		command := &cobra.Command{Use: "probe"}
		command.Flags().String("api-key", value, "probe")
		command.Flags().String("http-port", "7474", "port")
		protectFlagDefaults(command)
		flag := command.Flags().Lookup("api-key")
		got, err := command.Flags().GetString("api-key")
		if err != nil || got != value || flag.Value.String() != value {
			t.Error("display projection changed runtime flag values")
		}
		if flag.DefValue != observability.RedactedPlaceholder() {
			t.Error("secret flag default metadata was not hidden")
		}
		if command.Flags().Lookup("http-port").DefValue != "7474" {
			t.Error("ordinary flag default changed")
		}
	}
}
