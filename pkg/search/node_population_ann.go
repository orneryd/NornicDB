package search

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/pkg/security"
)

// restoreNodePopulationANN consumes the NODE admission result only for an
// explicit build, before NODE waiters are released. Caller owns indexMu.
func (s *Service) restoreNodePopulationANN(ctx context.Context, plan nodePopulationPlan) {
	skipIteration, hnswPath := plan.skipIteration, plan.hnswPath
	vectorCount, forceHNSWRebuild := plan.vectorCount, plan.forceHNSWRebuild
	hnswLoadedFromDisk := false
	hnswWarmupReason := ""
	// When skipping iteration and HNSW is active, load HNSW from disk so warmup does not rebuild it.
	cpuBruteMaxN := cpuBruteForceMaxN()
	if skipIteration && hnswPath != "" && vectorCount > 0 && vectorCount >= cpuBruteMaxN {
		vectorLookup := s.getVectorLookup()
		dimensions := s.VectorIndexDimensions()
		var loaded *HNSWIndex
		var err error
		// Graph-only HNSW loads in lookup mode to avoid duplicating vectors in RAM
		// (canonical vectors remain in vector index / file store).
		loaded, err = LoadHNSWIndex(hnswPath, vectorLookup)
		if forceHNSWRebuild {
			hnswWarmupReason = "settings changed"
			s.logPrintf("📇 HNSW on-disk index ignored due to settings change; rebuilding")
		} else if err == nil && loaded != nil && dimensions > 0 && loaded.GetDimensions() == dimensions {
			want := s.hnswConfig()
			have := loaded.Config()
			if have.M != want.M || have.EfConstruction != want.EfConstruction || have.EfSearch != want.EfSearch {
				hnswWarmupReason = "config changed"
				s.logPrintf("📇 HNSW config changed (old m=%d efc=%d efs=%d, new m=%d efc=%d efs=%d); rebuilding",
					have.M, have.EfConstruction, have.EfSearch, want.M, want.EfConstruction, want.EfSearch)
			} else {
				// Beam factor is query-only tuning and does not affect the persisted
				// graph, so apply it without paying for an index rebuild.
				loaded.setSearchBeamFactor(want.SearchBeamFactor)
				loaded.SetBuildLexicalHints(lexicalHintValues(s.hnswBuildLexicalSeedHintsLocked(s.fulltext(), want.M)))
				if ctx.Err() != nil {
					return
				}
				s.hnswMu.Lock()
				s.hnswIndex = loaded
				s.hnswMu.Unlock()
				s.invalidateAnswersForPublishedGraph()
				hnswLoadedFromDisk = true
				s.logPrintf("📇 HNSW index loaded from disk: vectors=%d tombstone_ratio=%.2f", loaded.Size(), loaded.TombstoneRatio())
			}
		} else if err != nil {
			hnswWarmupReason = fmt.Sprintf("load failed: %v", err)
		} else if loaded == nil {
			if info, statErr := security.RootedStat(hnswPath); statErr == nil {
				if info.Size() == 0 {
					hnswWarmupReason = "index file exists but is empty"
				} else {
					hnswWarmupReason = fmt.Sprintf("index file exists (%d bytes) but could not be decoded", info.Size())
				}
			} else if errors.Is(statErr, os.ErrNotExist) {
				hnswWarmupReason = "index file missing"
			} else {
				hnswWarmupReason = fmt.Sprintf("index file stat failed: %v", statErr)
			}
		} else if dimensions > 0 && loaded.GetDimensions() != dimensions {
			hnswWarmupReason = fmt.Sprintf("dimension mismatch (disk=%d current=%d)", loaded.GetDimensions(), dimensions)
		}
	}
	if skipIteration && vectorCount > 0 && vectorCount >= cpuBruteMaxN && !hnswLoadedFromDisk {
		if hnswWarmupReason == "" {
			if hnswPath == "" {
				hnswWarmupReason = "hnsw path not configured"
			} else {
				hnswWarmupReason = "no reusable on-disk HNSW"
			}
		}
		s.logPrintf("📇 BuildIndexes: base indexes loaded; warmup will build HNSW (%s)", hnswWarmupReason)
	}

}
