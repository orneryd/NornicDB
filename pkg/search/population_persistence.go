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

type populationRecord struct {
	Complete bool
}

type populationArtifacts struct {
	canonical, working [3]string // BM25, vectors, HNSW
	dirs               []string
	record             string
	restart            bool
	prepared           bool
}

func (s *Service) populationCanPersist() bool {
	if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
		return false
	}
	state := s.population.state.Load()
	return state == populationUnopened || state == populationComplete
}

func (s *Service) populationContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.lifecycleCtx != nil {
		return s.lifecycleCtx.Err()
	}
	return nil
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
			f, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
			if err == nil {
				var record populationRecord
				err = util.DecodeMsgpackFile(f.File, &record)
				_ = f.Close()
				if err != nil || !record.Complete {
					s.population.state.Store(populationFailed)
				}
			} else if !os.IsNotExist(err) {
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
	f, err := security.OpenRootedFile(w.record, os.O_RDONLY, 0)
	if err == nil {
		var record populationRecord
		err = util.DecodeMsgpackFile(f.File, &record)
		_ = f.Close()
		w.restart = w.restart || err != nil || !record.Complete
	} else if !os.IsNotExist(err) {
		return err
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
	if s.vectorFileStore != nil {
		_ = s.vectorFileStore.Close()
		s.vectorFileStore = nil
	}
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
// replacement scan/save. Append-only vectors can retain their committed file:
// its old metadata still selects the old ordinals on failure. A restarted
// vector population uses a new file instead of deleting the committed one.
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
	s.mu.Lock()
	if s.vectorFileStore != nil && w.working[1] != "" {
		s.vectorFileStore.mu.Lock()
		s.vectorFileStore.metaPath = w.working[1] + ".meta"
		s.vectorFileStore.mu.Unlock()
	}
	s.population.paths = w.working
	s.mu.Unlock()
	s.indexMu.Unlock()
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
		// The completion record participates in the same rollback as the files.
		stagedRecord := populationRecordPath(w.working)
		buildErr = writeMsgpackSnapshot(stagedRecord, &populationRecord{Complete: true})
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
	newVectorFile := false
	if vfs != nil {
		vfs.appendMu.Lock()
		defer vfs.appendMu.Unlock()
		vfs.mu.Lock()
		defer vfs.mu.Unlock()
		newVectorFile = vfs.vecPath == w.working[1]+".vec"
		if newVectorFile && vfs.file != nil {
			if err := vfs.releaseQueryReadMapLocked(); err != nil {
				return err
			}
			if err := vfs.file.Close(); err != nil {
				return err
			}
			vfs.file = nil
		}
	}
	moves := []populationFileMove{}
	defer func() {
		if resultErr == nil && vfs != nil && newVectorFile {
			var err error
			vfs.file, err = security.OpenRootedFile(w.canonical[1]+".vec", os.O_RDWR|os.O_APPEND, 0o644)
			resultErr = errors.Join(resultErr, err)
		}
		if resultErr != nil {
			for i := len(moves) - 1; i >= 0; i-- {
				m := moves[i]
				resultErr = errors.Join(resultErr, m.root.Rename(m.target, m.staged))
				if m.hadPrevious {
					resultErr = errors.Join(resultErr, m.root.Rename(m.backup, m.target))
				}
			}
		}
		if vfs != nil {
			if newVectorFile {
				path := w.canonical[1] + ".vec"
				if resultErr != nil {
					path = w.working[1] + ".vec"
				}
				if vfs.file == nil {
					var err error
					vfs.file, err = security.OpenRootedFile(path, os.O_RDWR|os.O_APPEND, 0o644)
					resultErr = errors.Join(resultErr, err)
				}
				vfs.vecPath = path
			}
			if resultErr == nil {
				vfs.metaPath = w.canonical[1] + ".meta"
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
			if newVectorFile {
				if err := move(p+".vec", w.canonical[i]+".vec"); err != nil {
					return err
				}
			}
			if err := move(p+".meta", w.canonical[i]+".meta"); err != nil {
				return err
			}
		} else if err := move(p, w.canonical[i]); err != nil {
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
