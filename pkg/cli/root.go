// Package cli owns common construction of localized Cobra command roots.
package cli

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/spf13/cobra"
)

// Render returns a localized command message, retaining its fallback when
// localization is unavailable. It is safe to use during command construction.
func Render(manager *localization.Manager, message localization.Message) string {
	if manager != nil {
		if rendered, _, err := manager.Render(context.Background(), message); err == nil {
			return rendered
		}
	}
	return message.Fallback
}

// NewRoot constructs the shared localized Cobra root. Each binary owns its
// subcommands and runtime context; only root identity and text live here.
func NewRoot(use string, manager *localization.Manager, short, long localization.Message) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: Render(manager, short),
		Long:  Render(manager, long),
	}
}
