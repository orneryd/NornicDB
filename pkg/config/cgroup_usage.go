package config

import "path"

// DetectCgroupMemoryUsage reports the current memory charged to this process's
// cgroup. The value includes other processes in a shared cgroup and, on Linux,
// file cache and kernel charges; callers must not add RSS to it. A zero or
// unreadable value is unavailable, so an enabled pressure policy rejects new
// expensive work until a positive sample arrives.
func DetectCgroupMemoryUsage() (int64, bool) {
	return cgroupFS{root: "/"}.memoryUsage()
}

func (fs cgroupFS) memoryUsage() (int64, bool) {
	unified, v1 := fs.selfCgroups()
	mounts := fs.cgroupMounts()
	if len(mounts) == 0 {
		if usage, ok := readCgroupLimit(fs.path(path.Join("sys/fs/cgroup", unified, "memory.current"))); ok {
			return usage, true
		}
		return readCgroupLimit(fs.path(path.Join("sys/fs/cgroup/memory", v1, "memory.usage_in_bytes")))
	}
	for _, mount := range mounts {
		cgroup, file := v1, "memory.usage_in_bytes"
		if mount.unified {
			cgroup, file = unified, "memory.current"
		}
		relative, contains := mount.relative(cgroup)
		if !contains {
			continue
		}
		if usage, ok := readCgroupLimit(fs.path(path.Join(mount.point, relative, file))); ok {
			return usage, true
		}
	}
	return 0, false
}
