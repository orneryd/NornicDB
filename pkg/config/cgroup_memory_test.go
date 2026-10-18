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
		c := &MemoryConfig{RuntimeLimitStr: RuntimeLimitAuto}
		resolution := c.resolveRuntimeLimit(cgroup, 0)
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup, Derived: true}, resolution)
		require.Equal(t, derived, c.RuntimeLimit)
		require.Less(t, c.RuntimeLimit, cgroup)
	})
	t.Run("unset is auto", func(t *testing.T) {
		c := &MemoryConfig{}
		require.True(t, c.resolveRuntimeLimit(cgroup, 0).Derived)
		require.Equal(t, derived, c.RuntimeLimit)
	})
	t.Run("an explicit limit above the cgroup is clamped", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "7168", RuntimeLimit: 7 * gib}
		resolution := c.resolveRuntimeLimit(cgroup, 0)
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup, Clamped: true, Configured: 7 * gib}, resolution)
		require.Equal(t, derived, c.RuntimeLimit)
	})
	t.Run("an explicit limit within the cgroup stands", func(t *testing.T) {
		c := &MemoryConfig{RuntimeLimitStr: "4096", RuntimeLimit: 4 * gib}
		require.Equal(t, RuntimeLimitResolution{CgroupLimit: cgroup}, c.resolveRuntimeLimit(cgroup, 0))
		require.Equal(t, 4*gib, c.RuntimeLimit)
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
	require.Equal(t, "auto", LoadDefaults().Memory.RuntimeLimitStr)

	t.Setenv("NORNICDB_MEMORY_LIMIT", "auto")
	config := LoadDefaults()
	require.NoError(t, applyEnvVars(config))
	require.Equal(t, "auto", config.Memory.RuntimeLimitStr)
	require.Zero(t, config.Memory.RuntimeLimit)
}

// TestDetectCgroupMemoryLimitFromMountinfo: the hierarchies are found where
// mountinfo says they are mounted, the process's path is read relative to
// each mount's root, and v1's inherited hierarchical limit counts.
func TestDetectCgroupMemoryLimitFromMountinfo(t *testing.T) {
	const gib = int64(1) << 30
	type files = map[string]string
	for _, test := range []struct {
		name  string
		files files
		limit int64
		found bool
	}{
		// The review's layout: hierarchy root /tenant mounted at
		// /sys/fs/cgroup; the process is in /tenant/app, whose limit is
		// sys/fs/cgroup/app/memory.max.
		{"v2 partially mounted hierarchy", files{
			"proc/self/cgroup":             "0::/tenant/app\n",
			"proc/self/mountinfo":          "30 25 0:26 /tenant /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate\n",
			"sys/fs/cgroup/app/memory.max": "6442450944\n",
			"sys/fs/cgroup/memory.max":     "max\n",
		}, 6 * gib, true},
		{"v2 mounted elsewhere, with an escaped space", files{
			"proc/self/cgroup":         "0::/c1\n",
			"proc/self/mountinfo":      "30 25 0:26 / /run/cg\\040two rw,relatime - cgroup2 cgroup2 rw\n",
			"run/cg two/c1/memory.max": "2147483648\n",
		}, 2 * gib, true},
		// v1 memory controller co-mounted with cpu at a combined mount point.
		{"v1 combined controllers mount", files{
			"proc/self/cgroup":    "5:cpu,memory:/docker/abc\n4:pids:/docker/abc\n",
			"proc/self/mountinfo": "35 25 0:31 / /sys/fs/cgroup/cpu,memory rw,nosuid - cgroup cgroup rw,cpu,memory\n36 25 0:32 / /sys/fs/cgroup/pids rw - cgroup cgroup rw,pids\n",
			"sys/fs/cgroup/cpu,memory/docker/abc/memory.limit_in_bytes": "5368709120\n",
		}, 5 * gib, true},
		// v1 with only the container's subtree mounted: its own file says
		// unlimited, its parent's limit shows only in memory.stat.
		{"v1 inherited hierarchical limit", files{
			"proc/self/cgroup":                           "9:memory:/docker/abc\n",
			"proc/self/mountinfo":                        "40 25 0:35 /docker/abc /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
			"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
			"sys/fs/cgroup/memory/memory.stat":           "cache 0\nrss 0\nhierarchical_memory_limit 6442450944\nhierarchical_memsw_limit 9223372036854771712\n",
		}, 6 * gib, true},
		{"v1 hierarchical limit unlimited too", files{
			"proc/self/cgroup":                           "9:memory:/\n",
			"proc/self/mountinfo":                        "40 25 0:35 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
			"sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n",
			"sys/fs/cgroup/memory/memory.stat":           "hierarchical_memory_limit 9223372036854771712\n",
		}, 0, false},
		{"mountinfo without cgroup mounts", files{
			"proc/self/cgroup":    "0::/\n",
			"proc/self/mountinfo": "22 1 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n",
		}, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit, found := fakeCgroupFS(t, test.files).memoryLimit()
			require.Equal(t, test.found, found)
			require.Equal(t, test.limit, limit)
		})
	}
	require.Equal(t, "a b\tc", unescapeMountPath(`a\040b\011c`))
	require.Equal(t, `no\escape`, unescapeMountPath(`no\escape`))
}

// A mount exposing another part of the hierarchy doesn't hold the process's
// limit, wherever mountinfo lists it: detection reads the mounts that
// contain the process's cgroup and takes the tightest limit among them.
func TestDetectCgroupMemoryLimitSkipsMountsOfOtherCgroups(t *testing.T) {
	const gib = int64(1) << 30
	type files = map[string]string
	v2Other := "31 25 0:26 /other /run/other-cg rw,relatime - cgroup2 cgroup2 rw\n"
	v2Root := "30 25 0:26 / /sys/fs/cgroup rw,relatime - cgroup2 cgroup2 rw\n"
	v1Other := "41 25 0:35 /other /run/other-mem rw - cgroup cgroup rw,memory\n"
	v1Root := "40 25 0:35 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n"
	for _, test := range []struct {
		name  string
		files files
		limit int64
		found bool
	}{
		{"v2 unrelated subtree listed first", files{
			"proc/self/cgroup":                    "0::/tenant/app\n",
			"proc/self/mountinfo":                 v2Other + v2Root,
			"run/other-cg/memory.max":             "8589934592\n",
			"sys/fs/cgroup/tenant/app/memory.max": "1073741824\n",
		}, 1 * gib, true},
		{"v2 unrelated subtree listed last", files{
			"proc/self/cgroup":                    "0::/tenant/app\n",
			"proc/self/mountinfo":                 v2Root + v2Other,
			"run/other-cg/memory.max":             "8589934592\n",
			"sys/fs/cgroup/tenant/app/memory.max": "1073741824\n",
		}, 1 * gib, true},
		{"v1 unrelated subtree listed first", files{
			"proc/self/cgroup":                                      "9:memory:/tenant/app\n",
			"proc/self/mountinfo":                                   v1Other + v1Root,
			"run/other-mem/memory.limit_in_bytes":                   "8589934592\n",
			"sys/fs/cgroup/memory/tenant/app/memory.limit_in_bytes": "1073741824\n",
		}, 1 * gib, true},
		{"v1 unrelated subtree listed last", files{
			"proc/self/cgroup":                                      "9:memory:/tenant/app\n",
			"proc/self/mountinfo":                                   v1Root + v1Other,
			"run/other-mem/memory.limit_in_bytes":                   "8589934592\n",
			"sys/fs/cgroup/memory/tenant/app/memory.limit_in_bytes": "1073741824\n",
		}, 1 * gib, true},
		{"only an unrelated subtree mounted", files{
			"proc/self/cgroup":        "0::/tenant/app\n",
			"proc/self/mountinfo":     v2Other,
			"run/other-cg/memory.max": "8589934592\n",
		}, 0, false},
		// Outside the process's cgroup namespace: its path can't be mapped
		// onto a mount, and the namespace root's limit isn't its own.
		{"v2 cgroup outside the namespace", files{
			"proc/self/cgroup":                 "0::/../sibling\n",
			"proc/self/mountinfo":              v2Root,
			"sys/fs/cgroup/memory.max":         "8589934592\n",
			"sys/fs/cgroup/sibling/memory.max": "1073741824\n",
		}, 0, false},
		{"v2 unlimited leaf below a finite parent", files{
			"proc/self/cgroup":                          "0::/kubepods/pod1/c1\n",
			"proc/self/mountinfo":                       v2Root,
			"sys/fs/cgroup/kubepods/pod1/c1/memory.max": "max\n",
			"sys/fs/cgroup/kubepods/pod1/memory.max":    "3221225472\n",
		}, 3 * gib, true},
		{"v1 limit without memory.stat", files{
			"proc/self/cgroup":    "9:memory:/docker/abc\n",
			"proc/self/mountinfo": v1Root,
			"sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes": "2147483648\n",
		}, 2 * gib, true},
		{"v1 limit with a malformed memory.stat", files{
			"proc/self/cgroup":    "9:memory:/docker/abc\n",
			"proc/self/mountinfo": v1Root,
			"sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes": "2147483648\n",
			"sys/fs/cgroup/memory/docker/abc/memory.stat":           "hierarchical_memory_limit lots\n",
		}, 2 * gib, true},
		{"v1 hierarchical limit without a leaf limit file", files{
			"proc/self/cgroup":                            "9:memory:/docker/abc\n",
			"proc/self/mountinfo":                         v1Root,
			"sys/fs/cgroup/memory/docker/abc/memory.stat": "hierarchical_memory_limit 4294967296\n",
		}, 4 * gib, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			limit, found := fakeCgroupFS(t, test.files).memoryLimit()
			require.Equal(t, test.found, found)
			require.Equal(t, test.limit, limit)
		})
	}
}
