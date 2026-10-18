package main

import (
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// DefValue belongs to help/usage; Value belongs to execution. Do not wrap Value:
// pflag's typed getters depend on its real String representation.
func protectFlagDefaults(cmd *cobra.Command) {
	project := func(flag *pflag.Flag) {
		flag.DefValue = observability.DisplayValue(flag.Name, flag.DefValue)
	}
	cmd.LocalNonPersistentFlags().VisitAll(project)
	cmd.PersistentFlags().VisitAll(project)
	for _, child := range cmd.Commands() {
		protectFlagDefaults(child)
	}
}
