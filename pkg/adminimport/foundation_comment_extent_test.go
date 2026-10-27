package adminimport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoundationImportCommentExtents(t *testing.T) {
	for _, ending := range []string{"\r", "\n", "\r\n"} {
		for _, prefix := range []string{"// comment", "# import wrapper"} {
			require.Equal(t, []string{"RETURN 1", "RETURN 2"}, splitCypherStatements(prefix+ending+"RETURN 1; RETURN 2"))
		}
		require.Equal(t, []string{"RETURN '// kept' AS text", "RETURN 2"}, splitCypherStatements("RETURN '// kept' AS text;"+ending+"// comment"+ending+"RETURN 2"))
	}
}
