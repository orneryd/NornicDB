package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeCgroupFS is a root holding the given files (slash paths relative to
// "/", e.g. "proc/self/cgroup").
func fakeCgroupFS(t *testing.T, files map[string]string) cgroupFS {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	return cgroupFS{root: root}
}

func TestDetectCgroupMemoryLimit(t *testing.T) {
	const gib = int64(1) << 30
	type files = map[string]string
	for _, test := range []struct {
		name  string
		files files
		limit int64
		found bool
	}{
		// v2 inside a cgroup namespace: the mount's root is the container's.
		{"v2 with a cgroup namespace", files{"proc/self/cgroup": "0::/\n", "sys/fs/cgroup/memory.max": "6442450944\n", "sys/fs/cgroup/memory.high": "max\n"}, 6 * gib, true},
		{"v2 without a limit", files{"proc/self/cgroup": "0::/\n", "sys/fs/cgroup/memory.max": "max\n"}, 0, false},
		// v2 without a namespace: the path names the container's cgroup, and
		// the tightest of it and its ancestors applies.
		{"v2 parent's limit below the container's", files{"proc/self/cgroup": "0::/system.slice/docker-abc.scope\n", "sys/fs/cgroup/system.slice/docker-abc.scope/memory.max": "4294967296\n", "sys/fs/cgroup/system.slice/memory.max": "2147483648\n"}, 2 * gib, true},
		{"v2 container's limit below its parent's", files{"proc/self/cgroup": "0::/kubepods/pod1/c1\n", "sys/fs/cgroup/kubepods/pod1/c1/memory.max": "1073741824\n", "sys/fs/cgroup/kubepods/pod1/memory.max": "3221225472\n", "sys/fs/cgroup/kubepods/memory.max": "max\n"}, gib, true},
		// v1: the memory controller's hierarchy, "unlimited" a huge value.
		{"v1 with the container's cgroup mounted at the root", files{"proc/self/cgroup": "12:memory:/docker/abc\n11:cpu,cpuacct:/docker/abc\n0::/\n", "sys/fs/cgroup/memory/memory.limit_in_bytes": "1073741824\n"}, gib, true},
		{"v1 memory listed with other controllers", files{"proc/self/cgroup": "4:cpuacct,memory:/app\n", "sys/fs/cgroup/memory/app/memory.limit_in_bytes": "536870912\n", "sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}, gib / 2, true},
		{"v1 unlimited", files{"proc/self/cgroup": "12:memory:/\n", "sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}, 0, false},
		{"no cgroup files", nil, 0, false},
		{"unreadable value", files{"proc/self/cgroup": "0::/\n", "sys/fs/cgroup/memory.max": "lots\n"}, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit, found := fakeCgroupFS(t, test.files).memoryLimit()
			require.Equal(t, test.found, found)
			require.Equal(t, test.limit, limit)
		})
	}
}

func TestResolveRuntimeLimit(t *testing.T) {
	const gib = int64(1) << 30
	cgroup := 6 * gib
	derived := cgroup / 100 * RuntimeLimitCgroupPercent

	t.Run("auto derives the limit from the cgroup", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto, TransactionTotalMax: -1}
		resolution := c.resolveRuntimeLimit(cgroup, 0)
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup, Derived: true}, resolution)
		require.Equal(t, derived, c.RuntimeLimit)
		require.Less(t, c.RuntimeLimit, cgroup)
		// dbms.memory.transaction.total.max does not follow it: unset, the
		// pool is unlimited (#1343).
		require.Zero(t, c.EffectiveTransactionTotalMax())
	})
	t.Run("unset is auto", func(t *testing.T) {
		c := &MemoryConfig{}
		require.True(t, c.resolveRuntimeLimit(cgroup, 0).Derived)
		require.Equal(t, derived, c.RuntimeLimit)
	})
	t.Run("an explicit limit above the cgroup is clamped", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "7168", RuntimeLimit: 7 * gib, TransactionTotalMax: -1}
		resolution := c.resolveRuntimeLimit(cgroup, 0)
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup, Clamped: true, Configured: 7 * gib}, resolution)
		require.Equal(t, derived, c.RuntimeLimit)
		require.Zero(t, c.EffectiveTransactionTotalMax())
	})
	t.Run("an explicit limit within the cgroup stands", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "4096", RuntimeLimit: 4 * gib}
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup}, c.resolveRuntimeLimit(cgroup, 0))
		require.Equal(t, 4*gib, c.RuntimeLimit)
	})
	t.Run("an explicit pool stands", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto, TransactionTotalMax: gib}
		c.resolveRuntimeLimit(cgroup, 0)
		require.Equal(t, gib, c.EffectiveTransactionTotalMax())
	})
	t.Run("0 stays unlimited", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "0"}
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup}, c.resolveRuntimeLimit(cgroup, 0))
		require.Zero(t, c.RuntimeLimit)
	})
	t.Run("no cgroup limit leaves auto unlimited", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto}
		require.Equal(t, RuntimeLimitResolution{}, c.resolveRuntimeLimit(0, 0))
		require.Zero(t, c.RuntimeLimit)
	})
	t.Run("GOMEMLIMIT within the cgroup stands", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto}
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup}, c.resolveRuntimeLimit(cgroup, 3*gib))
		require.Equal(t, 3*gib, c.RuntimeLimit)
	})
	t.Run("GOMEMLIMIT above the cgroup is clamped", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto}
		resolution := c.resolveRuntimeLimit(cgroup, 8*gib)
		require.True(t, resolution.Clamped)
		require.Equal(t, 8*gib, resolution.Configured)
		require.Equal(t, derived, c.RuntimeLimit)
	})
	t.Run("an explicit limit wins over GOMEMLIMIT", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "2048", RuntimeLimit: 2 * gib}
		c.resolveRuntimeLimit(cgroup, 8*gib)
		require.Equal(t, 2*gib, c.RuntimeLimit)
	})
}

func TestParseRuntimeLimit(t *testing.T) {
	for _, raw := range []string{"auto", "AUTO", " Auto "} {
		limit, err := ParseRuntimeLimit(raw)
		require.NoError(t, err, raw)
		require.Zero(t, limit, raw)
	}
	limit, err := ParseRuntimeLimit("500")
	require.NoError(t, err)
	require.Equal(t, int64(500)<<20, limit)
	_, err = ParseRuntimeLimit("lots")
	require.Error(t, err)
}

func TestRuntimeLimitDefaultsToAuto(t *testing.T) {
	require.Equal(t, RuntimeLimitAuto, LoadDefaults().Memory.RuntimeLimitStr)

	t.Setenv("NORNICDB_MEMORY_LIMIT", "auto")
	config := LoadDefaults()
	require.NoError(t, applyEnvVars(config))
	require.Equal(t, RuntimeLimitAuto, config.Memory.RuntimeLimitStr)
	require.Zero(t, config.Memory.RuntimeLimit)
}
