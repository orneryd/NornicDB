package storage

// DataDirFreeSpace returns free bytes available to the current process for the
// underlying Badger data directory.
func (b *BadgerEngine) DataDirFreeSpace() (int64, error) {
	if !b.work.Enter() {
		return 0, ErrStorageClosed
	}
	defer b.work.Leave()
	return dataDirFreeSpace(b.db.Opts().Dir)
}
