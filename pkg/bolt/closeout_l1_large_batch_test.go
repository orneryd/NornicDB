package bolt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseoutTwentyOneMiBNodesCommit(t *testing.T) {
	lab := journalOpen(t, t.TempDir(), journalConfig())
	const create = "UNWIND range(1, 20) AS i CREATE (:CloseoutWide {i: i, text: $text})"
	_, err := lab.run(create, map[string]any{"text": strings.Repeat("x", 1<<20)}, true)
	require.NoError(t, err)
	lab = lab.reopen(journalConfig())
	require.Equal(t, int64(20), lab.count("MATCH (n:CloseoutWide) WHERE size(n.text) = 1048576 RETURN count(n)", nil))
}
