// Author: Codex,NOR-180 public shell outcome contract,29October2026.
// The generated composition population (per-child WAL) by Claude, 30 October 2026.
package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestCLIWALTerminalOutcomeValueViews(t *testing.T) {
	t.Setenv("NORNICDB_EMBEDDING_ENABLED", "false")
	t.Setenv("NORNICDB_TELEMETRY_ENABLED", "false")
	t.Setenv("NORNICDB_WAL_AUTO_COMPACTION_ENABLED", "false")
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%v", profile.Name, parallel), func(t *testing.T) {
				c := walterminal.NewOutcomeContract(t, profile, parallel)
				input, writer, err := os.Pipe()
				require.NoError(t, err)
				t.Cleanup(func() { _ = input.Close() })
				_, err = writer.WriteString(c.Flow.Query + "\nexit\n")
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				original := os.Stdin
				os.Stdin = input
				t.Cleanup(func() { os.Stdin = original })
				var output bytes.Buffer
				cmd := &cobra.Command{}
				cmd.SetContext(context.Background())
				cmd.SetOut(&output)
				cmd.SetErr(&output)
				cmd.Flags().String("data-dir", t.TempDir(), "")
				require.NoError(t, runShell(cmd, nil))
				c.RequireNative(t)
				if !c.Flow.WantError {
					require.NotContains(t, output.String(), "failed to write WAL tx")
					return
				}
				c.RequireView(t, "", output.String())
			})
		}
	}
}
