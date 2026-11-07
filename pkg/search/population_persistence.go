package search

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/util"
)

const (
	populationUnopened uint32 = iota // retain the existing pre-build live-ingestion contract
	populationIncomplete
	populationComplete
	populationFailed
)

// populationPersistence owns source completeness and checkpoint publication.
// A positive count, readiness, or a successful individual replay is not proof
// that a population may replace the saved sources. BuildMu owns work; the
// persistence mutex serializes path changes, checkpoint I/O and Close.
type populationPersistence struct {
	state   atomic.Uint32
	active  atomic.Bool
	paths   [3]string // private writer destinations, guarded by Service.mu
	work    *populationArtifacts
	retired []string
}

// Reads retain the configured paths, including relationship and ANN restores.
// Only population writers and a new vector store use private destinations.
// Caller holds Service.mu.
func (s *Service) persistencePathsLocked() (string, string, string) {
	if p := s.population.paths; p != [3]string{} {
		return p[0], p[1], p[2]
	}
	return s.fulltextIndexPath, s.vectorIndexPath, s.hnswIndexPath
}

// populationRecord is the saved population's source certificate. Complete
// says the artifact set was published; SourceStamp, VectorSettings and
// VectorDigest say which database source and which vector tuple it belongs to.
// Payload integrity belongs to each existing loader, not this certificate.
type populationRecord struct {
	Complete       bool
	SourceStamp    string
	VectorSettings string
	VectorDigest   string
}

// admits is the sole database-source equality decision for saved populations.
func (record populationRecord) admits(stamp string) bool {
	return record.Complete && stamp != "" && record.SourceStamp == stamp
}

type populationArtifacts struct {
	canonical, working [3]string // BM25, vectors, HNSW
	dirs               []string
	record             string
	restart            bool
	prepared           bool
	previous           populationRecord
	baseline           populationRecord
	certificate        populationRecord
}

// readPopulationRecord is shared by first live admission, build admission and
// background persistence. A missing record supplies no source authority.
func readPopulationRecord(path string) (populationRecord, bool, error) {
	var record populationRecord
	if path == "" {
		return record, false, nil
	}
	f, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	err = util.DecodeMsgpackFile(f.File, &record)
	closeErr := f.Close()
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return record, true, errors.Join(err, closeErr)
		}
		return record, true, rejectVectorCheckpoint(err)
	}
	return record, true, closeErr
}

func (s *Service) savedPopulationSource() (populationRecord, error) {
	if work := s.population.work; work != nil {
		return work.previous, nil
	}
	s.mu.RLock()
	path := populationRecordPath([3]string{s.fulltextIndexPath, s.vectorIndexPath, s.hnswIndexPath})
	s.mu.RUnlock()
	record, _, err := readPopulationRecord(path)
	if isRejectedCheckpoint(err) {
		return populationRecord{}, nil
	}
	return record, err
}

func vectorCheckpointMatches(store *VectorFileStore, digest string) bool {
	if store == nil || digest == "" {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.checkpointDigest == digest
}

// vectorStoreSourceAdmitted is the one admission decision for a loaded vector
// tuple: its own integrity is established by the loader; this decides whether
// it is this database's projection under this producer policy.
func (s *Service) vectorStoreSourceAdmitted(store *VectorFileStore, stamp string) (bool, error) {
	record, err := s.savedPopulationSource()
	if err != nil {
		return false, err
	}
	if !record.admits(stamp) || record.VectorSettings != s.vectorSourcePolicy() || !vectorCheckpointMatches(store, record.VectorDigest) {
		return false, nil
	}
	store.mu.RLock()
	metadata := store.nodeMetadataPersisted && store.nodeTypesPersisted
	store.mu.RUnlock()
	return metadata, nil
}

// bindPopulationSource binds a completed baseline and replay only when its
// source and producer policies still equal the captured admission. Changed or
// unsupported sources finish without a restart certificate; there is no scan
// retry and no partial prefix is called complete.
func (s *Service) bindPopulationSource(ctx context.Context) error {
	w := s.population.work
	if w == nil || w.baseline.SourceStamp == "" {
		return nil
	}
	current, err := s.vectorSourceStamp(ctx, nil)
	if err != nil {
		return err
	}
	if current != w.baseline.SourceStamp || s.vectorSourcePolicy() != w.baseline.VectorSettings {
		return nil
	}
	w.certificate = w.baseline
	return nil
}

func (s *Service) populationCanPersist() bool {
	if s.lifecycleCtx.Err() != nil {
		return false
	}
	state := s.population.state.Load()
	return state == populationUnopened || state == populationComplete
}

func (s *Service) populationContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.lifecycleCtx.Err()
}

// populationMaySave runs with persistRunMu held. A new service may receive
// live writes before BuildIndexes; those writes cannot make an interrupted
// on-disk population complete or replace its last committed checkpoint.
func (s *Service) populationMaySave() bool {
	if s.population.state.Load() == populationUnopened {
		s.mu.RLock()
		path := populationRecordPath([3]string{s.fulltextIndexPath, s.vectorIndexPath, s.hnswIndexPath})
		s.mu.RUnlock()
		if path != "" {
			record, exists, err := readPopulationRecord(path)
			if err != nil || (exists && !record.Complete) {
				s.population.state.Store(populationFailed)
			}
		}
	}
	return s.populationCanPersist()
}

func populationRecordPath(paths [3]string) string {
	p := searchBuildSettingsPath(paths[0], paths[1], paths[2])
	if p == "" {
		return ""
	}
	return p + ".population"
}

func (s *Service) beginPopulationPersistence() error {
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	s.mu.RLock()
	paths := [3]string{s.fulltextIndexPath, s.vectorIndexPath, s.hnswIndexPath}
	s.mu.RUnlock()
	w := &populationArtifacts{canonical: paths, restart: s.population.state.Load() == populationFailed}
	s.population.state.Store(populationIncomplete)
	s.population.active.Store(true)
	s.population.work = w
	if !s.persistEnabled.Load() {
		return nil
	}
	w.record = populationRecordPath(paths)
	if w.record == "" {
		return nil
	}
	record, exists, err := readPopulationRecord(w.record)
	if err != nil {
		if !isRejectedCheckpoint(err) {
			return err
		}
		w.restart = true
	} else {
		w.restart = w.restart || (exists && !record.Complete)
		w.previous = record
	}
	// The previous files remain untouched. This completion record makes an
	// interrupted replacement inadmissible even when its files are nonempty.
	return writeMsgpackSnapshot(w.record, &populationRecord{})
}

func (s *Service) resetIncompletePopulation() {
	w := s.population.work
	if w == nil || !w.restart {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.Lock()
	s.closeVectorFileStoreLocked()
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	clear(s.nodeLabels)
	clear(s.nodeTypes)
	clear(s.nodeNamedVector)
	clear(s.nodePropVector)
	clear(s.nodeChunkVectors)
	clear(s.nodeChunkSpans)
	s.mu.Unlock()
	s.fulltext().Clear()
}

// preparePopulationArtifacts runs after saved-source admission and before any
// replacement scan/save. The tuple owner copies vectors and metadata into a
// private destination; scans and intermediate saves do not replace the saved
// population. Final publication binds the resulting tuple to its certificate.
func (s *Service) preparePopulationArtifacts() error {
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	w := s.population.work
	if w == nil || w.record == "" || !s.persistEnabled.Load() {
		return nil
	}
	parents := map[string]string{}
	for i, p := range w.canonical {
		if p == "" {
			continue
		}
		parent := filepath.Dir(p)
		dir := parents[parent]
		if dir == "" {
			if err := security.EnsureRootedParent(p, 0o755); err != nil {
				return err
			}
			var err error
			dir, err = os.MkdirTemp(parent, ".search-population-")
			if err != nil {
				return err
			}
			parents[parent] = dir
			w.dirs = append(w.dirs, dir)
		}
		w.working[i] = filepath.Join(dir, filepath.Base(p))
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.RLock()
	vfs := s.vectorFileStore
	s.mu.RUnlock()
	// The vector owner relocates its complete tuple, instead of redirecting
	// metadata while continuing to write the committed payload elsewhere.
	if vfs != nil && w.working[1] != "" {
		if err := vfs.relocateCheckpoint(w.working[1]); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.population.paths = w.working
	s.mu.Unlock()
	w.prepared = true
	return nil
}

func (s *Service) finishPopulationPersistence(ctx context.Context, buildErr error, pending, tailChanged bool) error {
	buildErr = errors.Join(buildErr, s.populationContextError(ctx))
	w := s.population.work
	if w == nil {
		return buildErr
	}
	if buildErr == nil && !pending && tailChanged {
		// The final replay may follow the earlier base checkpoint. Save that
		// complete sequence to its private destinations before publication.
		s.population.state.Store(populationComplete)
		buildErr = s.persistBaseIndexes()
		if buildErr == nil {
			s.runPersistWithContext(ctx)
		}
	}
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	buildErr = errors.Join(buildErr, s.populationContextError(ctx))
	defer func() {
		s.mu.Lock()
		s.population.paths = [3]string{}
		s.mu.Unlock()
		s.population.retired = append(s.population.retired, w.dirs...)
		s.population.work = nil
		s.population.active.Store(false)
	}()
	if buildErr == nil && !pending && s.persistEnabled.Load() && w.prepared {
		// The completion record rolls back with the non-vector artifacts. The
		// vector tuple publishes atomically through its own recovery owner; if
		// later publication fails, its digest mismatch forces a complete rebuild.
		stagedRecord := populationRecordPath(w.working)
		// Source equality was checked while the population snapshot was pinned,
		// before persistence. No replay can silently extend that authority:
		// applyBuildWrites clears the certificate. Bind the tuple actually saved
		// by the completed build only after all successful saves.
		if w.certificate.SourceStamp != "" {
			s.indexMu.RLock()
			s.mu.RLock()
			if vfs := s.vectorFileStore; vfs != nil {
				vfs.mu.RLock()
				w.certificate.VectorDigest = vfs.checkpointDigest
				vfs.mu.RUnlock()
			}
			s.mu.RUnlock()
			s.indexMu.RUnlock()
		}
		w.certificate.Complete = true
		buildErr = writeMsgpackSnapshot(stagedRecord, &w.certificate)
		if buildErr == nil {
			buildErr = s.publishPopulationArtifacts(ctx, w)
		}
	}
	if buildErr != nil || pending {
		s.population.state.Store(populationFailed)
	} else {
		s.population.state.Store(populationComplete)
	}
	return buildErr
}

type populationFileMove struct {
	root                   *os.Root
	staged, target, backup string
	hadPrevious            bool
}

func (s *Service) publishPopulationArtifacts(ctx context.Context, w *populationArtifacts) (resultErr error) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.RLock()
	vfs := s.vectorFileStore
	s.mu.RUnlock()
	if vfs != nil && w.working[1] != "" {
		if err := vfs.relocateCheckpoint(w.canonical[1]); err != nil {
			return err
		}
	}
	moves := []populationFileMove{}
	defer func() {
		if resultErr != nil {
			for i := len(moves) - 1; i >= 0; i-- {
				m := moves[i]
				resultErr = errors.Join(resultErr, m.root.Rename(m.target, m.staged))
				if m.hadPrevious {
					resultErr = errors.Join(resultErr, m.root.Rename(m.backup, m.target))
				}
			}
		}
		for _, m := range moves {
			if resultErr == nil && m.hadPrevious {
				_ = m.root.RemoveAll(m.backup)
			}
			_ = m.root.Close()
		}
	}()
	move := func(staged, target string) error {
		if staged == "" || target == "" {
			return nil
		}
		if _, err := security.RootedStat(staged); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		parent := filepath.Dir(target)
		root, err := os.OpenRoot(parent)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(parent, staged)
		if err != nil {
			_ = root.Close()
			return err
		}
		m := populationFileMove{root: root, staged: rel, target: filepath.Base(target), backup: rel + ".previous"}
		if _, err := root.Stat(m.target); err == nil {
			if err = root.Rename(m.target, m.backup); err != nil {
				_ = root.Close()
				return err
			}
			m.hadPrevious = true
		} else if !os.IsNotExist(err) {
			_ = root.Close()
			return err
		}
		if err := root.Rename(m.staged, m.target); err != nil {
			if m.hadPrevious {
				_ = root.Rename(m.backup, m.target)
			}
			_ = root.Close()
			return err
		}
		moves = append(moves, m)
		return nil
	}
	for i, p := range w.working {
		if p == "" {
			continue
		}
		if i == 1 {
			continue // The tuple owner published vectors and metadata together above.
		}
		if err := move(p, w.canonical[i]); err != nil {
			return err
		}
	}
	for _, bundle := range []string{"hnsw_ivf", "ivfpq"} {
		base := s.ivfpqPersistenceBasePath(w.working[1], w.working[2])
		target := s.ivfpqPersistenceBasePath(w.canonical[1], w.canonical[2])
		if base != "" {
			if err := move(filepath.Join(filepath.Dir(base), bundle), filepath.Join(filepath.Dir(target), bundle)); err != nil {
				return err
			}
		}
	}
	if err := move(searchBuildSettingsPath(w.working[0], w.working[1], w.working[2]), searchBuildSettingsPath(w.canonical[0], w.canonical[1], w.canonical[2])); err != nil {
		return err
	}
	if err := s.populationContextError(ctx); err != nil {
		return err
	}
	if err := move(populationRecordPath(w.working), w.record); err != nil {
		return err
	}
	return s.populationContextError(ctx)
}

func (s *Service) cleanupPopulationArtifacts() {
	for _, dir := range s.population.retired {
		if err := security.RemoveAllRootedPath(dir); err != nil {
			s.logPrintf("Search: remove unpublished population %s: %v", dir, err)
		}
	}
	s.population.retired = nil
}
