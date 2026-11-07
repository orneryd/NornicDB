package apocexport_test

// The empty-destination path is the shared writer's no-file guard. These checks
// pin that the public export entry point decides it before any content
// conversion or filesystem work, so a caller that names no file pays nothing for
// the document it does not write.

import (
	"context"
	"os"
	"strings"
	"testing"

	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// TestToFileEmptyDestinationAllocatesNothing proves apocexport.ToFile reaches the
// shared no-file guard without copying the payload. The 1 MiB document is built
// outside the measured closure, so an eager string-to-bytes conversion inside the
// call is the only way the measurement can be nonzero. Both admission sides are
// covered: no capability at all (an empty destination needs none) and an admitted
// caller whose configured export root must stay empty, and the error is inspected
// outside the measured closure.
func TestToFileEmptyDestinationAllocatesNothing(t *testing.T) {
	root := t.TempDir()
	payload := strings.Repeat("x", 1<<20)
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		empty string
	}{
		{
			name: "no capability",
			ctx:  context.Background(),
		},
		{
			name:  "admitted caller",
			ctx:   apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{AllowExportFileAccess: true, FileAccessRoot: root}).Capability()),
			empty: root,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			var writeErr error
			allocations := testing.AllocsPerRun(5, func() {
				writeErr = apocexport.ToFile(ctx, payload, "")
			})
			require.NoError(t, writeErr, "an empty destination reports no error")
			require.Zero(t, allocations, "writing no file must not allocate or convert the 1 MiB payload")
			if tc.empty != "" {
				entries, err := os.ReadDir(tc.empty)
				require.NoError(t, err)
				require.Empty(t, entries, "an empty destination performs no filesystem work")
			}
		})
	}
}
