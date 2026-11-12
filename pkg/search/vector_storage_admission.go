package search

import (
	"context"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// VectorStorageStatus is exposed through the existing database search status.
// RAM fallback is an operator-visible mode transition, not a successful disk
// admission. ResidentBytes is charged by the existing index accounting owner;
// actual RAM remains subject to the applied runtime/application memory limits.
type VectorStorageStatus struct {
	Configured    string `json:"configured"`
	Active        string `json:"active"`
	Reason        string `json:"reason,omitempty"`
	ResidentBytes int64  `json:"resident_bytes"`
	LastRebuild   string `json:"last_rebuild_reason,omitempty"`
}

func (s *Service) VectorStorageStatus() VectorStorageStatus {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := s.vectorStorage
	status.Configured = s.vectorStorageMode
	status.ResidentBytes = s.vectorResidentBytes
	if status.Active == "" {
		status.Active = "memory"
		if s.vectorFileStore != nil {
			status.Active = "disk"
		}
	}
	return status
}

func (s *Service) vectorStorageTransition(active, reason string) {
	s.mu.Lock()
	before := s.vectorStorage
	mode := s.vectorStorageMode
	s.vectorStorage = VectorStorageStatus{Configured: mode, Active: active, Reason: reason}
	s.vectorStorage.LastRebuild = before.LastRebuild
	if active == "rebuilding" {
		s.vectorStorage.LastRebuild = reason
	}
	s.mu.Unlock()
	if before.Active == active && before.Reason == reason {
		return
	}
	if reason != "" {
		s.loggerOrDefault().Warn("search vector storage changed", "database", s.cacheNamespace, "index", "vector",
			"configured", mode, "active", active, "reason", reason)
	}
}

func (s *Service) vectorSourcePolicy() string {
	space, configured := s.embeddingSpace.Load().(string)
	// Temporal constraints already decline static source certification below.
	// Without one, mere exposure of that optional reader capability changes no
	// selected row and cannot invalidate a forwarding reader's saved source.
	return fmt.Sprintf("%s;embedding-space-configured=%t;embedding-space=%q",
		s.composeVectorBuildSettings(), configured, space)
}

// vectorSourceStamp asks the original reader for its source metadata. Temporal
// selection can change with time without a commit; such views are rebuilt by
// their ordinary reader rather than assigned a durable static certificate.
func (s *Service) vectorSourceStamp(ctx context.Context, view storage.ReadSnapshotView) (string, error) {
	for _, constraint := range s.schemaConstraints(ctx) {
		if constraint.Type == storage.ConstraintTemporal {
			return "", nil
		}
	}
	source := any(s.engine)
	if view != nil {
		source = view
	}
	stamp, err := storage.ReadIndexSourceStamp(ctx, source)
	if errors.Is(err, storage.ErrReadSnapshotUnsupported) {
		return "", nil
	}
	return stamp, err
}

// openVectorStore is the only Service constructor/Load and policy decision.
// Caller owns indexMu. rejected means the ordinary population owner must
// rebuild before installing a source. It is not permission to use stale rows.
func (s *Service) openVectorStore(ctx context.Context, path string, dimensions int, stamp string) (store *VectorFileStore, rejected bool, err error) {
	store, err = NewVectorFileStore(path, dimensions)
	if err == nil {
		err = store.Load()
	}
	if err == nil && store.checkpointLoaded {
		var admitted bool
		admitted, err = s.vectorStoreSourceAdmitted(store, stamp)
		if err == nil && !admitted {
			err = rejectVectorCheckpoint(errors.New("vector checkpoint has no equal database population/projection certificate"))
		}
	}
	if err == nil {
		s.vectorStorageTransition("disk", "")
		return store, false, nil
	}
	if store != nil {
		err = errors.Join(err, store.Close())
		store = nil
	}
	if isRejectedCheckpoint(err) {
		s.vectorStorageTransition("rebuilding", err.Error())
		return nil, true, nil
	}
	if s.vectorStorageMode == "auto" {
		s.vectorStorageTransition("memory", err.Error())
		return nil, false, nil
	}
	s.vectorStorageTransition("error", err.Error())
	return nil, false, fmt.Errorf("search: explicit vector disk storage at %s: %w", path, err)
}

// ensureVectorFileStore is first-use acquisition by the same owner BuildIndexes
// uses. A rejected saved source is rebuilt by BuildIndexes outside mutation
// locks (prepareVectorMutation); the population itself opens a fresh private
// destination. Caller holds indexMu.
func (s *Service) ensureVectorFileStore() error {
	if s.vectorStorageMode == "memory" {
		return nil
	}
	s.mu.RLock()
	_, path, _ := s.persistencePathsLocked()
	vi, existing := s.vectorIndex, s.vectorFileStore
	s.mu.RUnlock()
	if !s.persistEnabled.Load() || path == "" {
		if s.vectorStorageMode == "disk" {
			return fmt.Errorf("search: vector disk storage requires index persistence and a configured vector index path")
		}
		return nil
	}
	if existing != nil {
		return existing.checkpointHealth()
	}
	if vi == nil || vi.GetDimensions() <= 0 {
		return nil
	}
	stamp, err := s.vectorSourceStamp(s.lifecycleCtx, nil)
	if err != nil {
		return err
	}
	store, rejected, err := s.openVectorStore(s.lifecycleCtx, path, vi.GetDimensions(), stamp)
	if err != nil {
		return err
	}
	if rejected {
		if s.vectorStorageMode == "auto" {
			// The saved checkpoint is not this database's projection. Until the
			// population owner rebuilds it, live writes are indexed in memory
			// and the recorded fallback is operator-visible. The rejected
			// checkpoint stays untouched and is never served.
			return nil
		}
		return errVectorSourceNeedsBuild
	}
	if store == nil {
		return nil // Explicitly reported auto fallback.
	}
	// A genuinely new store may inherit already installed in-memory entries;
	// this is migration of one source, not a database completeness certificate.
	vi.mu.RLock()
	for id, normalized := range vi.vectors {
		value := normalized
		if raw, ok := vi.rawVectors[id]; ok {
			value = raw
		}
		if err := store.Add(id, value); err != nil {
			vi.mu.RUnlock()
			_ = store.Close()
			if s.vectorStorageMode == "auto" {
				s.vectorStorageTransition("memory", err.Error())
				return nil
			}
			return err
		}
	}
	vi.mu.RUnlock()
	s.mu.Lock()
	s.replaceVectorFileStoreLocked(store)
	if labels, types, named, properties, chunks, complete := store.NodeQueryMetadataWithTypes(); complete {
		s.nodeLabels, s.nodeTypes = labels, types
		s.nodeNamedVector, s.nodePropVector, s.nodeChunkVectors = named, properties, chunks
		s.rebuildNodeMembersLocked()
		clear(s.nodeChunkSpans)
	}
	vi.mu.Lock()
	vi.vectors, vi.rawVectors = make(map[string][]float32), make(map[string][]float32)
	vi.mu.Unlock()
	s.mu.Unlock()
	return nil
}

var errVectorSourceNeedsBuild = errors.New("search: vector source requires population")

// prepareVectorMutation runs while buildMu is read-held, before any node entry
// changes. The caller releases that read lock before invoking BuildIndexes on
// errVectorSourceNeedsBuild. Replay calls indexNodeOwned and cannot recurse.
func (s *Service) prepareVectorMutation(write deferredIndexWrite) error {
	if write.node == nil || !s.vectorEnabled.Load() || firstVectorDimensions(write.node) == 0 || s.buildInProgress.Load() {
		return nil
	}
	current, err := s.shouldIndexNode(s.lifecycleCtx, write.node)
	if err != nil || !current {
		return err
	}
	s.maybeAutoSetVectorDimensions(firstVectorDimensions(write.node))
	if err := s.checkEmbeddingDimensions(write.node); err != nil {
		return err
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	return s.ensureVectorFileStore()
}

// checkpointVectorStore captures associations and ordinals under the same
// Service mutation turn. Save/compaction/relocation all publish through the raw
// tuple owner; the Service never encodes or renames a vector metadata file.
func (s *Service) checkpointVectorStore(vfs *VectorFileStore, compact bool) error {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	vfs.mu.RLock()
	path := vfs.vecPath
	vfs.mu.RUnlock()
	// Preserve the existing operator event at the one checkpoint owner.
	s.logPrintf("📇 Persist: syncing %s and saving its metadata...", path)
	s.mu.RLock()
	vfs.SetNodeQueryMetadataWithTypes(s.nodeLabels, s.nodeTypes, s.nodeNamedVector, s.nodePropVector, s.nodeChunkVectors)
	s.mu.RUnlock()
	if compact {
		if _, err := vfs.CompactIfNeeded(); err != nil {
			return err
		}
	}
	return vfs.Save()
}

func (s *Service) vectorStorageError() error {
	s.mu.RLock()
	store := s.vectorFileStore
	s.mu.RUnlock()
	if store != nil {
		return store.checkpointHealth()
	}
	return nil
}

func vectorGenerationMatches(store *VectorFileStore, expected vectorCheckpointGeneration) bool {
	if store == nil || expected == (vectorCheckpointGeneration{}) {
		return false
	}
	_, _, generation := store.stateVersion()
	return expected == generation
}
