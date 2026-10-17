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
		"meta":      ivfpqMetaSnapshot{FormatVersion: ivfpqBundleFormatVersion, Profile: idx.profile, BuiltAtUnixNano: idx.builtAtUnixNano},
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
	idx := s.ivfpqIndex
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

func (s *Service) getOrBuildIVFPQIndex(ctx context.Context, profile IVFPQProfile, vfs *VectorFileStore) (*IVFPQIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.ivfpqMu.RLock()
	if s.ivfpqIndex != nil && s.ivfpqIndex.compatibleProfile(profile) {
		idx := s.ivfpqIndex
		s.ivfpqMu.RUnlock()
		return idx, nil
	}
	s.ivfpqMu.RUnlock()

	s.ivfpqMu.Lock()
	defer s.ivfpqMu.Unlock()
	if s.ivfpqIndex != nil && s.ivfpqIndex.compatibleProfile(profile) {
		return s.ivfpqIndex, nil
	}

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
		s.ivfpqOverlay.restore(mutations)
		s.ivfpqIndex = loaded
		return s.ivfpqIndex, nil
	}

	if vfs == nil {
		return nil, localizedError(localization.SearchIVFPQVectorStoreUnavailable(), nil)
	}
	seedIDs := s.bm25SeedEmbeddingIDs(bm25SeedDocIDs(fulltext))
	built, stats, err := BuildIVFPQFromVectorStore(ctx, vfs, profile, seedIDs)
	if err != nil {
		return nil, err
	}
	logSearchPrintf("[IVFPQ] ✅ built | vectors=%d sample=%d lists=%d avg_list=%.1f max_list=%d bytes_per_vector=%.2f duration=%v",
		stats.VectorCount, stats.TrainingSampleCount, stats.ListCount, stats.AvgListSize, stats.MaxListSize, stats.BytesPerVector, stats.BuildDuration)
	s.ivfpqIndex = built
	s.ivfpqOverlay.Reset()
	return s.ivfpqIndex, nil
}
