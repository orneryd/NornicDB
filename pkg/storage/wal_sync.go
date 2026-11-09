package storage

// SyncDir makes directory metadata durable using the platform policy shared
// with WAL and snapshot persistence. Windows validates the directory without
// attempting the unsupported directory fsync; Unix syncs the directory.
func SyncDir(dir string) error {
	return syncDir(dir)
}
