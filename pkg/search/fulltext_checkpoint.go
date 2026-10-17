package search

import (
	"errors"
	"os"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
)

// errFulltextCheckpointRetired reports a parked BM25 checkpoint that could not
// be reloaded. The checkpoint was retired: BM25 is empty and the text source is
// incomplete, so the next population rebuilds it from storage.
var errFulltextCheckpointRetired = errors.New("search: BM25 checkpoint retired")

// fulltextCheckpointRecord is saved next to every BM25 snapshot. It records
// what the snapshot held and the storage it was taken from, so a restart only
// restores a snapshot that still matches storage. Docs is -1 when the save
// could not be confirmed as the current index.
type fulltextCheckpointRecord struct {
	Docs         int   `msgpack:"docs"`
	StorageNodes int64 `msgpack:"storage_nodes"`
}

func fulltextCheckpointRecordPath(path string) string {
	return path + ".checkpoint"
}

// recordFulltextCheckpoint describes the BM25 snapshot at path, just saved or
// confirmed unchanged. Caller holds persistRunMu and BM25 is not parked.
// Storage is counted before the index is read; a node written in between is
// counted but missing from the snapshot, so the restart rebuilds.
func (s *Service) recordFulltextCheckpoint(path string) {
	record := fulltextCheckpointRecord{Docs: -1, StorageNodes: -1}
	s.mu.RLock()
	engine := s.engine
	s.mu.RUnlock()
	if engine != nil {
		if n, err := engine.NodeCount(); err == nil {
			record.StorageNodes = n
		}
	}
	s.indexMu.RLock()
	if !s.fulltext().IsDirty() {
		record.Docs = s.fulltext().Count()
	}
	s.indexMu.RUnlock()
	if err := writeMsgpackSnapshot(fulltextCheckpointRecordPath(path), &record); err != nil {
		s.logPrintf("⚠️ Persist: failed to save the BM25 checkpoint record for %s: %v", path, err)
	}
}

// fulltextCheckpointMatchesStorage reports whether the BM25 snapshot loaded
// from path may be restored: its record must exist, the loaded index must hold
// the recorded documents, and storage must hold the recorded nodes. A snapshot
// without a record (written before records existed) is accepted as before.
func (s *Service) fulltextCheckpointMatchesStorage(path string) bool {
	file, err := security.OpenRootedFile(fulltextCheckpointRecordPath(path), os.O_RDONLY, 0)
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	defer file.Close()
	var record fulltextCheckpointRecord
	if err := util.DecodeMsgpackFile(file.File, &record); err != nil {
		return false
	}
	if record.Docs < 0 || record.Docs != s.fulltext().Count() {
		return false
	}
	s.mu.RLock()
	engine := s.engine
	s.mu.RUnlock()
	if engine == nil || record.StorageNodes < 0 {
		return record.StorageNodes < 0 && engine == nil
	}
	n, err := engine.NodeCount()
	return err == nil && n == record.StorageNodes
}

// removeFulltextCheckpoint deletes a retired BM25 snapshot and its record, so
// no later build or restart restores it.
func removeFulltextCheckpoint(path string) error {
	var errs error
	for _, p := range []string{path, fulltextCheckpointRecordPath(path)} {
		if err := security.RemoveRootedPath(p); err != nil && !os.IsNotExist(err) {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}
