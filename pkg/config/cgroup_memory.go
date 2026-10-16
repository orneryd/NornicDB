package config

import (
	"bufio"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

// Runtime memory limit from the container's cgroup.
//
// Go's soft memory limit (GOMEMLIMIT) only helps while it sits below the
// hard limit the kernel enforces: a soft target of 7 GiB in a 6 GiB cgroup
// lets the heap grow until the process is OOM-killed. memory.runtime_limit
// unset or "auto" is therefore derived from the cgroup limit, and an
// explicit limit above it is clamped.

// RuntimeLimitAuto is the memory.runtime_limit value (and default) that
// derives the limit from the container's cgroup memory limit.
const RuntimeLimitAuto = "auto"

// RuntimeLimitCgroupPercent is the share of the cgroup memory limit the
// derived runtime limit takes. The Go limit covers Go-managed memory only
// (heap, goroutine stacks, runtime metadata); the cgroup also charges cgo
// allocations (llama.cpp embedding models, GPU drivers), C thread stacks
// and kernel memory such as socket buffers and page tables, and the soft
// limit itself is overshot briefly while a GC cycle catches up. 15% of
// headroom (0.9 GiB of a 6 GiB container) covers those on a server that
// isn't loading a local model; the Go GC guide suggests 5-10% for the GC
// alone.
const RuntimeLimitCgroupPercent = 85

// cgroupUnlimited is the smallest value treated as "no limit": cgroup v1
// reports an unlimited memory.limit_in_bytes as the page-aligned maximum
// (9223372036854771712), not "max".
const cgroupUnlimited = int64(1) << 62

// cgroupFS locates the files cgroup detection reads; tests point root at a
// directory of fake files.
type cgroupFS struct {
	root string
}

func (fs cgroupFS) path(name string) string {
	return filepath.Join(fs.root, filepath.FromSlash(name))
}

// DetectCgroupMemoryLimit is the memory limit, in bytes, of the cgroup the
// process runs in (v2 memory.max, v1 memory.limit_in_bytes), the tightest
// of its own and its ancestors' limits; false when none is set or the
// files can't be read (not on Linux, no cgroup mounted).
func DetectCgroupMemoryLimit() (int64, bool) {
	return cgroupFS{root: "/"}.memoryLimit()
}

func (fs cgroupFS) memoryLimit() (int64, bool) {
	unified, v1 := fs.selfCgroups()
	// cgroup v2: one unified hierarchy.
	if limit, ok := fs.tightestLimit("sys/fs/cgroup", unified, "memory.max"); ok {
		return limit, true
	}
	// cgroup v1: the memory controller's hierarchy.
	return fs.tightestLimit("sys/fs/cgroup/memory", v1, "memory.limit_in_bytes")
}

// selfCgroups is the process's cgroup paths from /proc/self/cgroup: the v2
// unified one ("0::/path") and the v1 memory controller's
// ("N:memory:/path", possibly with other controllers listed alongside).
// Each is "/" when absent, so the mount's root is still read.
func (fs cgroupFS) selfCgroups() (unified, v1 string) {
	unified, v1 = "/", "/"
	file, err := os.Open(fs.path("proc/self/cgroup"))
	if err != nil {
		return unified, v1
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.SplitN(scanner.Text(), ":", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[0] == "0" && fields[1] == "" {
			unified = fields[2]
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				v1 = fields[2]
			}
		}
	}
	return unified, v1
}

// tightestLimit is the lowest limit in file along cgroup's path and its
// ancestors under mount. Inside a container with its own cgroup namespace
// the path is "/" and the mount's root is the container's cgroup; without
// one, the path names the container's cgroup below the host's root, and
// parts of it that aren't mounted are skipped.
func (fs cgroupFS) tightestLimit(mount, cgroup, file string) (int64, bool) {
	var tightest int64
	found := false
	for dir := path.Clean("/" + cgroup); ; dir = path.Dir(dir) {
		if limit, ok := readCgroupLimit(fs.path(path.Join(mount, dir, file))); ok && (!found || limit < tightest) {
			tightest, found = limit, true
		}
		if dir == "/" {
			break
		}
	}
	return tightest, found
}

// readCgroupLimit reads a memory limit file: a byte count, or "max" (v2)
// or a value of at least cgroupUnlimited (v1) for none.
func readCgroupLimit(name string) (int64, bool) {
	data, err := os.ReadFile(name)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(data))
	if value == "max" {
		return 0, false
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil || limit <= 0 || limit >= cgroupUnlimited {
		return 0, false
	}
	return limit, true
}

// RuntimeLimitResolution is how ResolveRuntimeLimit set the runtime limit.
type RuntimeLimitResolution struct {
	// CgroupLimit is the container's memory limit, 0 when none was found.
	CgroupLimit int64

	// Derived is true when the limit was derived from CgroupLimit
	// (memory.runtime_limit unset or "auto").
	Derived bool

	// Clamped is true when the configured limit, Configured, was above
	// CgroupLimit and was lowered to the derived limit.
	Clamped    bool
	Configured int64
}

// ResolveRuntimeLimit settles RuntimeLimit against the container's cgroup
// memory limit, before ApplyRuntimeMemory applies it:
//   - unset or "auto": RuntimeLimitCgroupPercent of the cgroup limit, or a
//     GOMEMLIMIT from the environment; unlimited without either;
//   - an explicit limit, or GOMEMLIMIT, above the cgroup limit is clamped
//     to the derived limit (the caller warns);
//   - "0" stays unlimited.
//
// dbms.memory.transaction.total.max does not follow it: unset, the pool is
// unlimited (EffectiveTransactionTotalMax).
func (c *MemoryConfig) ResolveRuntimeLimit() RuntimeLimitResolution {
	cgroup, _ := DetectCgroupMemoryLimit()
	return c.resolveRuntimeLimit(cgroup, environmentMemoryLimit())
}

// environmentMemoryLimit is the Go runtime's limit when GOMEMLIMIT set it,
// 0 otherwise.
func environmentMemoryLimit() int64 {
	if os.Getenv("GOMEMLIMIT") == "" {
		return 0
	}
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < cgroupUnlimited {
		return limit
	}
	return 0
}

func (c *MemoryConfig) resolveRuntimeLimit(cgroup, environment int64) RuntimeLimitResolution {
	resolution := RuntimeLimitResolution{CgroupLimit: cgroup}
	auto := c.RuntimeLimit <= 0 && (c.RuntimeLimitStr == "" || strings.EqualFold(strings.TrimSpace(c.RuntimeLimitStr), RuntimeLimitAuto))
	configured := c.RuntimeLimit
	if auto && environment > 0 {
		configured = environment
	}
	if cgroup <= 0 {
		if auto {
			c.RuntimeLimit = configured
		}
		return resolution
	}
	derived := cgroup / 100 * RuntimeLimitCgroupPercent
	switch {
	case configured > cgroup:
		resolution.Clamped = true
		resolution.Configured = configured
		c.RuntimeLimit = derived
	case configured > 0:
		c.RuntimeLimit = configured
	case auto:
		resolution.Derived = true
		c.RuntimeLimit = derived
	}
	return resolution
}
