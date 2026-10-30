package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallTailReturnOptionsAndVersionNonDevCommit(t *testing.T) {
	ret, orderBy, limit, skip := splitCallTailProjectionModifiers("n ORDER BY n.name ASC LIMIT not_int SKIP 3")
	require.Equal(t, "n", ret)
	require.Equal(t, "ORDER BY n.name ASC", orderBy)
	require.Equal(t, "not_int", limit)
	require.Equal(t, "3", skip)

	ret, orderBy, limit, skip = splitCallTailProjectionModifiers("n SKIP 1 LIMIT 2")
	require.Equal(t, "n", ret)
	require.Equal(t, "", orderBy)
	require.Equal(t, "2", limit)
	require.Equal(t, "1", skip)

	originalCommit := buildinfo.Commit
	defer func() { buildinfo.Commit = originalCommit }()
	buildinfo.Commit = "278e403abcd"

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_version_nondev"))
	res, err := exec.callNornicDbVersion()
	require.NoError(t, err)
	require.Equal(t, []string{"version", "build", "edition"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "278e403", res.Rows[0][1])
}
