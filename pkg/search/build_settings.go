package search

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	searchBuildSettingsFormatVersion = 1
	bm25SettingsSchemaVersion        = "1"
	vectorSettingsSchemaVersion      = "1"
	hnswSettingsSchemaVersion        = "1"
	routingSettingsSchemaVersion     = "1"
	strategySettingsSchemaVersion    = "1"
)

// searchBuildSettingsSnapshot tracks the index-build settings that influence
// persisted index compatibility beyond file format versions.
type searchBuildSettingsSnapshot struct {
	FormatVersion int    `msgpack:"format_version"`
	SavedAtUnix   int64  `msgpack:"saved_at_unix"`
	BM25          string `msgpack:"bm25"`
	Vector        string `msgpack:"vector"`
	HNSW          string `msgpack:"hnsw"`
	Routing       string `msgpack:"routing,omitempty"`
	Strategy      string `msgpack:"strategy,omitempty"`
}

func searchBuildSettingsPath(fulltextPath, vectorPath, hnswPath string) string {
	basePath := ""
	switch {
	case fulltextPath != "":
		basePath = fulltextPath
	case vectorPath != "":
		basePath = vectorPath
	case hnswPath != "":
		basePath = hnswPath
	default:
		return ""
	}
	return filepath.Join(filepath.Dir(basePath), "build_settings")
}

func loadSearchBuildSettings(path string) (*searchBuildSettingsSnapshot, error) {
	if path == "" {
		return nil, nil
	}
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var snap searchBuildSettingsSnapshot
	if err := util.DecodeMsgpackFile(file.File, &snap); err != nil {
		return nil, nil
	}
	if snap.FormatVersion != searchBuildSettingsFormatVersion {
		return nil, nil
	}
	return &snap, nil
}

func saveSearchBuildSettings(path string, snap searchBuildSettingsSnapshot) error {
	if path == "" {
		return nil
	}
	// lgtm[go/path-injection] -- path is derived from the configured search persistence root.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := security.CreateRootedFile(path, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return msgpack.NewEncoder(f).Encode(&snap)
}

func (s *Service) currentSearchBuildSettings() searchBuildSettingsSnapshot {
	return searchBuildSettingsSnapshot{
		FormatVersion: searchBuildSettingsFormatVersion,
		SavedAtUnix:   time.Now().Unix(),
		BM25:          s.composeBM25BuildSettings(),
		Vector:        s.composeVectorBuildSettings(),
		HNSW:          s.composeHNSWBuildSettings(),
		Routing:       s.composeRoutingBuildSettings(),
		Strategy:      s.composeStrategyBuildSettings(),
	}
}

func (s *Service) composeBM25BuildSettings() string {
	return fmt.Sprintf("schema=%s;format=%s;props=%s",
		bm25SettingsSchemaVersion,
		s.currentBM25FormatVersion(),
		strings.Join(SearchableProperties, ","))
}

func (s *Service) composeVectorBuildSettings() string {
	dimensions := s.VectorIndexDimensions()
	return fmt.Sprintf("schema=%s;format=%s;dimensions=%d",
		vectorSettingsSchemaVersion,
		vectorIndexFormatVersion,
		dimensions)
}

func (s *Service) composeHNSWBuildSettings() string {
	hcfg := HNSWConfigFromEnv()
	return fmt.Sprintf("schema=%s;format=%s;m=%d;efc=%d;efs=%d",
		hnswSettingsSchemaVersion,
		hnswIndexFormatVersionGraphOnly,
		hcfg.M, hcfg.EfConstruction, hcfg.EfSearch)
}

func (s *Service) composeRoutingBuildSettings() string {
	maxIter := envutil.GetInt("NORNICDB_KMEANS_MAX_ITERATIONS", 5)
	if maxIter < 5 {
		maxIter = 5
	}
	if maxIter > 500 {
		maxIter = 500
	}
	routingMode := strings.TrimSpace(strings.ToLower(os.Getenv("NORNICDB_VECTOR_ROUTING_MODE")))
	if routingMode == "" {
		routingMode = "hybrid"
	}
	wSem := envFloat("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", 0.7)
	wLex := envFloat("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", 0.3)
	return fmt.Sprintf("schema=%s;mode=%s;w_sem=%.4f;w_lex=%.4f;lex_profile=%s;kmeans_max_iter=%d",
		routingSettingsSchemaVersion,
		routingMode, wSem, wLex, routingSettingsSchemaVersion, maxIter)
}

func (s *Service) composeStrategyBuildSettings() string {
	if ANNQualityFromEnv() != ANNQualityCompressed {
		return ""
	}
	s.mu.RLock()
	dimensions := s.VectorIndexDimensions()
	vectorCount := s.embeddingCountLocked()
	vfsReady := s.vectorFileStore != nil && s.vectorFileStore.Count() > 0
	s.mu.RUnlock()
	profile := ResolveCompressedANNProfile(vectorCount, dimensions, vfsReady)
	return fmt.Sprintf("schema=%s;quality=%s;active=%t;dims=%d;lists=%d;segments=%d;bits=%d;nprobe=%d;rerank_topk=%d;train_max=%d;kmeans_max_iter=%d;seed_max_terms=%d;seed_docs_per_term=%d;routing=%s",
		strategySettingsSchemaVersion,
		profile.Quality,
		profile.Active,
		profile.Dimensions,
		profile.IVFLists,
		profile.PQSegments,
		profile.PQBits,
		profile.NProbe,
		profile.RerankTopK,
		profile.TrainingSampleMax,
		profile.KMeansMaxIterations,
		profile.SeedMaxTerms,
		profile.SeedDocsPerTerm,
		profile.RoutingMode)
}

func (s *Service) currentBM25FormatVersion() string {
	if normalizeBM25Engine(s.bm25Engine) == BM25EngineV2 {
		return bm25V2FormatVersion
	}
	return fulltextIndexFormatVersion
}

func (s *Service) persistSearchBuildSettings(fulltextPath, vectorPath, hnswPath string) {
	path := searchBuildSettingsPath(fulltextPath, vectorPath, hnswPath)
	if path == "" {
		return
	}
	if err := saveSearchBuildSettings(path, s.currentSearchBuildSettings()); err != nil {
		log.Printf("⚠️ Background persist: failed to save build settings metadata to %s: %v", path, err)
	}
}
