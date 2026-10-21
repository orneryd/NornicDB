package config

import "path"

// DetectCgroupMemoryHeadroom returns the least free space across every
// visible cgroup memory ceiling and its own usage counter. A caller must not
// combine an ancestor's limit with the leaf's smaller usage reading.
func DetectCgroupMemoryHeadroom() (int64, bool) {
	return (cgroupFS{root: "/"}).memoryHeadroom()
}

func (fs cgroupFS) memoryHeadroom() (int64, bool) {
	unified, v1 := fs.selfCgroups()
	mounts := fs.cgroupMounts()
	if len(mounts) == 0 {
		mounts = []cgroupMount{{point: "/sys/fs/cgroup", unified: true}, {point: "/sys/fs/cgroup/memory", memory: true}}
	}
	var available int64
	found := false
	for _, mount := range mounts {
		cgroup, limitFile, usageFile := v1, "memory.limit_in_bytes", "memory.usage_in_bytes"
		if mount.unified {
			cgroup, limitFile, usageFile = unified, "memory.max", "memory.current"
		}
		relative, contains := mount.relative(cgroup)
		if !contains {
			continue
		}
		var visibleLimit int64
		readable := true
		walkCgroupAncestorDirs(relative, func(dir string) bool {
			if limit, ok := readCgroupLimit(fs.path(path.Join(mount.point, dir, limitFile))); ok {
				if visibleLimit == 0 || limit < visibleLimit {
					visibleLimit = limit
				}
				usage, measured := readCgroupLimit(fs.path(path.Join(mount.point, dir, usageFile)))
				if !measured {
					readable = false
					return false
				}
				free := limit - usage
				if !found || free < available {
					available = free
					found = true
				}
			}
			return true
		})
		if !readable {
			return 0, false
		}
		if !mount.unified {
			if hierarchical, ok := readHierarchicalLimit(fs.path(path.Join(mount.point, relative, "memory.stat"))); ok &&
				(visibleLimit == 0 || hierarchical < visibleLimit) {
				// A stricter ancestor above the visible mount has no usage
				// counter we can pair with it. Admission must wait for a
				// measurable hierarchy instead of using leaf usage.
				return 0, false
			}
		}
	}
	return available, found
}

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
