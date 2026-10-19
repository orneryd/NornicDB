package search

import (
	"context"
	"os"
	"path/filepath"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
)

type ivfpqMetaSnapshot struct {
	FormatVersion   int          `msgpack:"format_version"`
	Profile         IVFPQProfile `msgpack:"profile"`
	BuiltAtUnixNano int64        `msgpack:"built_at_unix_nano"`
	// MaxResidual bounds PQ scores (IVFPQIndex.scoreBound); a bundle saved
	// before it loads with no bound.
	MaxResidual   float64 `msgpack:"max_residual,omitempty"`
	ResidualKnown bool    `msgpack:"residual_known,omitempty"`
}

type ivfpqCodebooksSnapshot struct {
	Codebooks []ivfpqCodebook `msgpack:"codebooks"`
}

type ivfpqListsSnapshot struct {
	Lists    []ivfpqList           `msgpack:"lists"`
	Overflow []ivfpqOverflowVector `msgpack:"overflow,omitempty"`
}

func ivfpqBundleDir(basePath string) string {
	if basePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(basePath), "ivfpq")
}

// SaveIVFPQBundle persists an IVFPQ index as an atomic multipart bundle.
func SaveIVFPQBundle(basePath string, idx *IVFPQIndex) error {
	return saveIVFPQBundle(basePath, idx, nil, nil)
}

func saveIVFPQBundle(basePath string, idx *IVFPQIndex, overlay *annMutationOverlay, vectorStore *VectorFileStore) error {
	mutations := overlay.snapshot()
	mutations.VectorStoreCount, mutations.VectorStoreSlots = vectorStore.stateVersion()
	return saveIVFPQBundleSnapshot(basePath, idx, mutations)
}

func saveIVFPQBundleSnapshot(basePath string, idx *IVFPQIndex, mutations annMutationOverlaySnapshot) error {
	if basePath == "" || idx == nil {
		return nil
	}
	dir := ivfpqBundleDir(basePath)
	if dir == "" {
		return nil
	}
	return writeMsgpackSnapshotsAtomic(dir, map[string]any{
		"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion, Profile: idx.profile, BuiltAtUnixNano: idx.builtAtUnixNano, MaxResidual: idx.maxResidual, ResidualKnown: idx.residualKnown},
		"centroids": idx.centroids,
		"codebooks": ivfpqCodebooksSnapshot{Codebooks: idx.codebooks},
		"lists":     ivfpqListsSnapshot{Lists: idx.lists, Overflow: idx.overflow},
		"mutations": mutations,
	})
}

// LoadIVFPQBundle loads an IVFPQ multipart snapshot bundle.
func LoadIVFPQBundle(basePath string) (*IVFPQIndex, error) {
	idx, _, err := loadIVFPQBundle(basePath)
	return idx, err
}

func loadIVFPQBundle(basePath string) (*IVFPQIndex, annMutationOverlaySnapshot, error) {
	if basePath == "" {
		return nil, annMutationOverlaySnapshot{}, nil
	}
	dir := ivfpqBundleDir(basePath)
	if dir == "" {
		return nil, annMutationOverlaySnapshot{}, nil
	}
	if _, err := security.RootedStat(dir); err != nil {
		if os.IsNotExist(err) {
			return nil, annMutationOverlaySnapshot{}, nil
		}
		return nil, annMutationOverlaySnapshot{}, err
	}

	meta := ivfpqMetaSnapshot{}
	if err := decodeMsgpackFile(filepath.Join(dir, "meta"), &meta); err != nil {
		return nil, annMutationOverlaySnapshot{}, err
	}
	if meta.FormatVersion != ivfpqBundleFormatVersion {
		return nil, annMutationOverlaySnapshot{}, nil
	}
	centroids := make([][]float32, 0)
	if err := decodeMsgpackFile(filepath.Join(dir, "centroids"), &centroids); err != nil {
		return nil, annMutationOverlaySnapshot{}, err
	}
	codebooks := ivfpqCodebooksSnapshot{}
	if err := decodeMsgpackFile(filepath.Join(dir, "codebooks"), &codebooks); err != nil {
		return nil, annMutationOverlaySnapshot{}, err
	}
	lists := ivfpqListsSnapshot{}
	if err := decodeMsgpackFile(filepath.Join(dir, "lists"), &lists); err != nil {
		return nil, annMutationOverlaySnapshot{}, err
	}
	mutations := annMutationOverlaySnapshot{}
	if err := decodeMsgpackFile(filepath.Join(dir, "mutations"), &mutations); err != nil && !os.IsNotExist(err) {
		return nil, annMutationOverlaySnapshot{}, err
	}
	idx := &IVFPQIndex{
		profile:         meta.Profile,
		centroids:       centroids,
		centroidNorm:    normalizeCentroids(centroids),
		codebooks:       codebooks.Codebooks,
		lists:           lists.Lists,
		overflow:        lists.Overflow,
		formatVersion:   meta.FormatVersion,
		builtAtUnixNano: meta.BuiltAtUnixNano,
		maxResidual:     meta.MaxResidual,
		residualKnown:   meta.ResidualKnown,
	}
	idx.initScratchPool()
	return idx, mutations, nil
}

func decodeMsgpackFile(path string, dst any) error {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := util.DecodeMsgpackFile(file.File, dst); err != nil {
		return err
	}
	return nil
}

func (s *Service) ivfpqPersistenceBasePath(vectorPath, hnswPath string) string {
	if hnswPath != "" {
		return hnswPath
	}
	return vectorPath
}

func (s *Service) persistIVFPQBackground(vectorPath, hnswPath string) {
	basePath := s.ivfpqPersistenceBasePath(vectorPath, hnswPath)
	if basePath == "" {
		return
	}
	s.indexMu.Lock()
	s.ivfpqMu.RLock()
	idx := s.servingIVFPQ()
	if s.ivfpqEpoch != s.vectorSourceEpoch.Load() {
		// Built for a retired source: saving it under the current store's
		// version would let a later load reuse it for different vectors.
		idx = nil
	}
	mutations := s.ivfpqOverlay.snapshot()
	s.ivfpqMu.RUnlock()
	s.mu.RLock()
	vectorStore := s.vectorFileStore
	s.mu.RUnlock()
	mutations.VectorStoreCount, mutations.VectorStoreSlots = vectorStore.stateVersion()
	s.indexMu.Unlock()
	if idx == nil || idx.Count() == 0 {
		return
	}
	if err := saveIVFPQBundleSnapshot(basePath, idx, mutations); err != nil {
		logSearchPrintf("⚠️ Background persist: failed to save IVFPQ bundle (%s): %v", basePath, err)
		return
	}
	logSearchPrintf("📇 Background persist: IVFPQ bundle saved (%s, vectors=%d)", basePath, idx.Count())
}

// getOrBuildIVFPQIndex returns the compressed index for vfs. sourceEpoch is
// the vectorSourceEpoch the caller observed before reading vfs: an index is
// reused only within its own epoch, and one loaded or built for a source that
// has since been retired is returned to this caller but never published.
func (s *Service) getOrBuildIVFPQIndex(ctx context.Context, profile IVFPQProfile, vfs *VectorFileStore, sourceEpoch uint64) (*IVFPQIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.ivfpqMu.RLock()
	if s.servingIVFPQ() != nil && s.ivfpqEpoch == sourceEpoch && s.servingIVFPQ().compatibleProfile(profile) {
		idx := s.servingIVFPQ()
		s.ivfpqMu.RUnlock()
		return idx, nil
	}
	s.ivfpqMu.RUnlock()

	s.ivfpqMu.Lock()
	defer s.ivfpqMu.Unlock()
	if s.servingIVFPQ() != nil && s.ivfpqEpoch == sourceEpoch && s.servingIVFPQ().compatibleProfile(profile) {
		return s.servingIVFPQ(), nil
	}
	current := func() bool { return s.vectorSourceEpoch.Load() == sourceEpoch }

	s.mu.RLock()
	vectorPath := s.vectorIndexPath
	hnswPath := s.hnswIndexPath
	fulltext := s.fulltext()
	s.mu.RUnlock()
	basePath := s.ivfpqPersistenceBasePath(vectorPath, hnswPath)

	if loaded, mutations, err := loadIVFPQBundle(basePath); err == nil && loaded != nil &&
		loaded.compatibleProfile(profile) && mutations.matchesVectorStore(vfs) {
		// Query-only knobs do not require rebuilding the compressed codes.
		loaded.profile.NProbe = profile.NProbe
		loaded.profile.RerankTopK = profile.RerankTopK
		if !current() {
			return loaded, nil
		}
		// The caller is creating the pipeline under pipelineMu: publish
		// without retiring it.
		s.publishVectorServing(func() {
			s.ivfpqOverlay.restore(mutations)
			s.ivfpqIndex = loaded
			s.ivfpqEpoch = sourceEpoch
		}, false)
		return s.servingIVFPQ(), nil
	}

	if vfs == nil {
		return nil, localizedError(localization.SearchIVFPQVectorStoreUnavailable(), nil)
	}
	seedIDs := bm25SeedDocIDs(fulltext)
	built, stats, err := BuildIVFPQFromVectorStore(ctx, vfs, profile, seedIDs)
	if err != nil {
		return nil, err
	}
	logSearchPrintf("[IVFPQ] ✅ built | vectors=%d sample=%d lists=%d avg_list=%.1f max_list=%d bytes_per_vector=%.2f duration=%v",
		stats.VectorCount, stats.TrainingSampleCount, stats.ListCount, stats.AvgListSize, stats.MaxListSize, stats.BytesPerVector, stats.BuildDuration)
	if !current() {
		return built, nil
	}
	s.publishVectorServing(func() {
		s.ivfpqIndex = built
		s.ivfpqEpoch = sourceEpoch
		s.ivfpqOverlay.Reset()
	}, false)
	return s.servingIVFPQ(), nil
}
