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
	mounts := fs.cgroupMounts()
	if len(mounts) == 0 {
		// No mountinfo (an older kernel, a test layout): the conventional
		// mount points.
		if limit, ok := fs.tightestLimit("sys/fs/cgroup", unified, "memory.max"); ok {
			return limit, true
		}
		return fs.tightestLimit("/sys/fs/cgroup/memory", path.Clean("/"+v1), "memory.limit_in_bytes")
	}
	// Every mount that contains the process's cgroup, in whatever order
	// mountinfo lists them: the unified hierarchy (v2) and the memory
	// controller's (v1, possibly mounted with other controllers). A mount of
	// another part of the hierarchy holds another cgroup's limit.
	var tightest int64
	found := false
	for _, mount := range mounts {
		cgroup := v1
		if mount.unified {
			cgroup = unified
		}
		relative, contains := mount.relative(cgroup)
		if !contains {
			continue
		}
		var limit int64
		var ok bool
		if mount.unified {
			limit, ok = fs.tightestLimit(mount.point, relative, "memory.max")
		} else {
			limit, ok = fs.tightestLimit(mount.point, relative, "memory.limit_in_bytes")
		}
		if ok && (!found || limit < tightest) {
			tightest, found = limit, true
		}
	}
	return tightest, found
}

// cgroupMount is a cgroup hierarchy mounted in the process's mount
// namespace: root is the hierarchy path mounted, point where it is mounted.
type cgroupMount struct {
	root, point string
	unified     bool // cgroup v2
	memory      bool // cgroup v1 with the memory controller
}

// relative is cgroup's path below the mount's point, and whether the mount
// contains it: the process's cgroup path (from /proc/self/cgroup) with the
// mount's root removed. Both paths are relative to the process's cgroup
// namespace; a path outside it ("/../sibling") names no mounted cgroup,
// and a mount of another part of the hierarchy doesn't contain the
// process's.
func (mount cgroupMount) relative(cgroup string) (string, bool) {
	if outsideCgroupNamespace(cgroup) || outsideCgroupNamespace(mount.root) {
		return "", false
	}
	cgroup, root := path.Clean("/"+cgroup), path.Clean("/"+mount.root)
	switch {
	case root == "/":
		return cgroup, true
	case cgroup == root:
		return "/", true
	case strings.HasPrefix(cgroup, root+"/"):
		return cgroup[len(root):], true
	default:
		return "", false
	}
}

// outsideCgroupNamespace reports whether a cgroup path climbs above the
// namespace's root, as the kernel shows a cgroup outside it.
func outsideCgroupNamespace(cgroup string) bool {
	for _, element := range strings.Split(cgroup, "/") {
		if element == ".." {
			return true
		}
	}
	return false
}

// cgroupMounts reads the process's cgroup mounts from /proc/self/mountinfo
// ("id parent major:minor root mount-point options [optional...] - type
// source super-options"), nil when it can't be read.
func (fs cgroupFS) cgroupMounts() []cgroupMount {
	file, err := os.Open(fs.path("proc/self/mountinfo"))
	if err != nil {
		return nil
	}
	defer file.Close()
	var mounts []cgroupMount
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for index := 6; index < len(fields); index++ {
			if fields[index] == "-" {
				separator = index
				break
			}
		}
		if separator < 0 || separator+1 >= len(fields) || len(fields) < 5 {
			continue
		}
		mount := cgroupMount{root: unescapeMountPath(fields[3]), point: unescapeMountPath(fields[4])}
		switch fields[separator+1] {
		case "cgroup2":
			mount.unified = true
		case "cgroup":
			if separator+3 < len(fields) {
				for _, option := range strings.Split(fields[separator+3], ",") {
					if option == "memory" {
						mount.memory = true
					}
				}
			}
		}
		if mount.unified || mount.memory {
			mounts = append(mounts, mount)
		}
	}
	return mounts
}

// unescapeMountPath undoes mountinfo's octal escapes (\040 for a space,
// \011, \012, \134).
func unescapeMountPath(value string) string {
	if !strings.Contains(value, `\`) {
		return value
	}
	var out strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] == '\\' && index+3 < len(value) {
			if code, err := strconv.ParseUint(value[index+1:index+4], 8, 8); err == nil {
				out.WriteByte(byte(code))
				index += 3
				continue
			}
		}
		out.WriteByte(value[index])
	}
	return out.String()
}

// readHierarchicalLimit reads v1 memory.stat's hierarchical_memory_limit:
// the effective limit of the cgroup and every ancestor.
func readHierarchicalLimit(name string) (int64, bool) {
	file, err := os.Open(name)
	if err != nil {
		return 0, false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && fields[0] == "hierarchical_memory_limit" {
			limit, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || limit <= 0 || limit >= cgroupUnlimited {
				return 0, false
			}
			return limit, true
		}
	}
	return 0, false
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
	walkCgroupAncestorDirs(cgroup, func(dir string) bool {
		if limit, ok := readCgroupLimit(fs.path(path.Join(mount, dir, file))); ok && (!found || limit < tightest) {
			tightest, found = limit, true
		}
		return true
	})
	// A cgroup v1 container may hide ancestors above its mount. The effective
	// hierarchy limit reported in memory.stat covers those hidden ancestors.
	if file == "memory.limit_in_bytes" {
		if hierarchical, ok := readHierarchicalLimit(fs.path(path.Join(mount, cgroup, "memory.stat"))); ok && (!found || hierarchical < tightest) {
			tightest, found = hierarchical, true
		}
	}
	return tightest, found
}

func walkCgroupAncestorDirs(cgroup string, visit func(string) bool) {
	for dir := path.Clean("/" + cgroup); ; dir = path.Dir(dir) {
		if !visit(dir) || dir == "/" {
			return
		}
	}
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
