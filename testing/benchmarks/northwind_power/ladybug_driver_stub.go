//go:build !ladybug

package main

import (
	"context"
	"fmt"
)

// runLadybugReport is unavailable without the embedded LadybugDB backend.
// Rebuild with `-tags ladybug,system_ladybug` and a downloaded liblbug to use
// `-driver ladybug`; the benchmark script does this automatically.
func runLadybugReport(_ context.Context, _ string, _ seedConfig, _ string, _, _ int, _ bool, _ string) error {
	return fmt.Errorf("this build does not include the embedded LadybugDB driver; rebuild with -tags ladybug,system_ladybug")
}
