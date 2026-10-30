package nornicdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// The search state of a database has one owner, the DB: the service (the
// entry in db.searchServices), its persisted files, its pending and retried
// refreshes, and a generation. Every reset of the state, whatever asks for it
// (a DeleteByPrefix of the database, a restore, a rebuild or configuration
// change), goes through invalidateSearchState, which covers the databases with
// a loaded service and those with only persisted files alike. Consumers
// resolve the service on each use (currentSearchService), so none keeps a
// replaced one.
//
// Persisted files of one generation live in their own directory,
// <DataDir>/search/<db>/<token>/, named by <DataDir>/search/<db>/CURRENT.
// Invalidation removes <db>/ whole; a retired service that still writes a file
// late writes it under its own token, which no CURRENT names again, so it is
// never loaded.

const (
	searchStateCurrentFile = "CURRENT"
	// searchRefreshRetryLimit bounds the retries of one refresh whose storage
	// read keeps failing; the delay doubles from the flush delay each time.
	searchRefreshRetryLimit = 8
	searchRefreshRetryMax   = 30 * time.Second
)

func (db *DB) searchStateRoot() string {
	if db.config == nil || db.config.Database.DataDir == "" {
		return ""
	}
	return filepath.Join(db.config.Database.DataDir, "search")
}

// upgradeRelationshipSnapshots runs while the Badger owner has the data
// directory locked, before any search service can load or overwrite a file.
// Search snapshots live outside the Badger schema and can also be restored
// independently, so an existing V4 store must be checked on every open.
func (db *DB) upgradeRelationshipSnapshots(allowUpgrade bool, syncDirectory func(string) error) error {
	root := db.searchStateRoot()
	if root == "" {
		return nil
	}
	entries, err := security.ReadRootedDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, err := searchPersistenceBasePath(db.config.Database.DataDir, entry.Name())
		if err != nil {
			return err
		}
		dir := base
		token, err := readSearchStateToken(filepath.Join(base, searchStateCurrentFile))
		if err == nil {
			dir = filepath.Join(base, token)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		path := filepath.Join(dir, "bm25.v2.relationships")
		if err := search.UpgradeRelationshipSnapshot(path, allowUpgrade, syncDirectory); err != nil {
			return fmt.Errorf("database %s relationship snapshot: %w", entry.Name(), err)
		}
	}
	return nil
}

// activeSearchStateDir returns the directory the database's current generation
// persists to, creating the generation's token when there is none. Files left
// directly in <db>/ by an earlier layout (or by the offline importer) become
// the current generation's. Other token directories are leftovers of retired
// generations and are removed. Caller holds searchServiceCreationMu.
func (db *DB) activeSearchStateDir(dbName string) (string, error) {
	base, err := searchPersistenceBasePath(db.config.Database.DataDir, dbName)
	if err != nil {
		return "", err
	}
	currentPath := filepath.Join(base, searchStateCurrentFile)
	if token, err := readSearchStateToken(currentPath); err == nil {
		db.removeRetiredSearchStateDirs(base, token)
		return filepath.Join(base, token), nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	token, err := newSearchStateToken()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(base, token)
	entries, err := security.ReadRootedDir(base)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if len(entries) > 0 {
		if err := adoptLegacySearchState(base, token, entries); err != nil {
			return "", err
		}
	}
	if err := writeSearchStateToken(currentPath, token); err != nil {
		return "", err
	}
	return dir, nil
}

// adoptLegacySearchState moves files left directly in base into the new
// generation's directory token; a token directory there without CURRENT is a
// retired generation's and is removed.
func adoptLegacySearchState(base, token string, entries []fs.DirEntry) error {
	root, err := os.OpenRoot(base)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.Mkdir(token, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == token {
			continue
		}
		if entry.IsDir() && isSearchStateToken(name) {
			if err := root.RemoveAll(name); err != nil {
				return err
			}
			continue
		}
		if err := root.Rename(name, filepath.Join(token, name)); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) removeRetiredSearchStateDirs(base, current string) {
	entries, err := security.ReadRootedDir(base)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != current && isSearchStateToken(entry.Name()) {
			_ = security.RemoveAllRootedPath(filepath.Join(base, entry.Name()))
		}
	}
}

func newSearchStateToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "g-" + hex.EncodeToString(b[:]), nil
}

func isSearchStateToken(name string) bool {
	return strings.HasPrefix(name, "g-") && len(name) == 18
}

func readSearchStateToken(path string) (string, error) {
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 64))
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if !isSearchStateToken(token) {
		return "", fs.ErrNotExist
	}
	return token, nil
}

func writeSearchStateToken(path, token string) error {
	if err := security.EnsureRootedParent(path, 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := security.WriteRootedFile(tmp, []byte(token+"\n"), 0o600); err != nil {
		return err
	}
	return security.RenameRootedFile(tmp, path)
}

// invalidateSearchState ends the current generation of every database
// matching match: the loaded service is removed and closed (its build, queue
// and persistence stop), its pending and retried refreshes are dropped, and,
// with discard, its persisted files are removed, loaded service or not. The
// next use creates the next generation, from storage when discarded.
func (db *DB) invalidateSearchState(match func(name string) bool, discard bool) {
	if !db.work.Enter() {
		return
	}
	defer db.work.Leave()
	for {
		db.searchServiceCreationMu.Lock()
		db.searchServicesMu.RLock()
		running := db.searchRetirement
		db.searchServicesMu.RUnlock()
		if running == nil {
			break
		}
		db.searchServiceCreationMu.Unlock()
		<-running.Done()
	}
	// This is a lifetime state, not a generation or origin transition.
	retirement := &lifecycle.OnceCompletion{}
	db.searchServicesMu.Lock()
	db.searchRetirement = retirement
	epoch := db.searchConstruction
	db.searchServicesMu.Unlock()
	// Preserve the creation fence's original order: admitted preparation
	// publishes before Reset retires that generation. Foreign resolvers run
	// without the native fence, and the phase refuses later constructors.
	db.searchServiceCreationMu.Unlock()
	if epoch != nil {
		db.recordBackgroundError(epoch.Shutdown(context.Background(), nil))
	}
	db.searchServiceCreationMu.Lock()
	var retired []*dbSearchService
	db.searchServicesMu.Lock()
	if db.searchGenerations == nil {
		db.searchGenerations = make(map[string]uint64)
	}
	names := make([]string, 0)
	for _, name := range db.searchStateNamesLocked() {
		if !match(name) {
			continue
		}
		names = append(names, name)
		db.searchGenerations[name]++
		if entry, ok := db.searchServices[name]; ok {
			delete(db.searchServices, name)
			retired = append(retired, entry)
		}
	}
	db.searchServicesMu.Unlock()
	// All native generation publication remains fenced by the retiring phase.
	// No host fence spans cancellation callbacks or the actual lifetime join.
	db.searchServiceCreationMu.Unlock()
	for _, entry := range retired {
		if entry.svc != nil {
			entry.svc.BeginShutdown()
		}
	}
	var retirementErr error
	for _, entry := range retired {
		retirementErr = compose.Outcomes(retirementErr, entry.retire())
	}
	// Creation binds a service to its registry epoch before publication.
	// Keep epoch retirement under the same creation fence as state retirement,
	// including resets that preserve the files (restore/configuration paths).
	db.searchContinuationMu.Lock()
	registry := db.searchContinuation
	db.searchContinuationMu.Unlock()
	if registry != nil {
		for _, name := range names {
			registry.DiscardDatabase(name)
		}
	}
	db.searchServiceCreationMu.Lock()
	defer func() {
		db.searchServicesMu.Lock()
		if epoch != nil {
			// The sealed epoch stopped above; later constructors use a new one.
			db.searchConstruction = &lifecycle.WorkLifetime{}
		}
		db.searchRetirement = nil
		db.searchServicesMu.Unlock()
		retirement.Do(func() error { return nil })
		db.searchServiceCreationMu.Unlock()
		db.recordBackgroundError(retirementErr)
	}()

	if !discard || db.searchStateRoot() == "" {
		return
	}
	for _, name := range names {
		path, err := searchPersistenceBasePath(db.config.Database.DataDir, name)
		if err != nil {
			retirementErr = compose.Outcomes(retirementErr, err)
			continue
		}
		retirementErr = compose.Outcomes(retirementErr, security.RemoveAllRootedPath(path))
	}
}

// searchStateNamesLocked lists every database with search state, a loaded
// service, persisted files or both, for a caller holding
// searchServicesMu.
func (db *DB) searchStateNamesLocked() []string {
	seen := make(map[string]struct{}, len(db.searchServices))
	for name := range db.searchServices {
		seen[name] = struct{}{}
	}
	if root := db.searchStateRoot(); root != "" {
		if entries, err := security.ReadRootedDir(root); err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					seen[entry.Name()] = struct{}{}
				}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	return names
}

// retire ends an entry that is no longer its database's current one: its
// service is closed (its build, persistence and maintenance stop), and no
// refresh is retried or flushed for it again.
func (e *dbSearchService) retire() error {
	e.pendingMu.Lock()
	e.retired = true
	for key, retry := range e.refreshRetries {
		e.forgetRetryLocked(key, retry, true)
	}
	for _, mutation := range e.pendingOps {
		(search.RefreshRetry{Node: mutation}).Discard()
	}
	for _, mutation := range e.pendingEdgeOps {
		(search.RefreshRetry{Edge: mutation}).Discard()
	}
	clear(e.pendingOps)
	clear(e.pendingEdgeOps)
	e.pendingMu.Unlock()
	if e.svc != nil {
		return e.svc.Close()
	}
	return nil
}

func (e *dbSearchService) isRetired() bool {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return e.retired
}

// currentSearchService resolves a database's current service, creating it
// when there is none: consumers call it on each use rather than keeping a
// service, so a reset never leaves one with a replaced service. It returns nil
// when the database has no search (the system database, a closed DB).
func (db *DB) currentSearchService(dbName string) *search.Service {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry != nil {
		return entry.svc
	}
	var engine storage.Engine // the database's namespaced view of base storage
	if dbName == db.defaultDatabaseName() {
		engine = db.storage
	}
	svc, err := db.GetOrCreateSearchService(dbName, engine)
	if err != nil {
		return nil
	}
	return svc
}

// SearchServiceResolver resolves a database's current search service on each
// use (currentSearchService). Executors are given it instead of a service, so
// a reset or restore of the database's search never leaves them one it
// replaced.
func (db *DB) SearchServiceResolver() cypher.SearchServiceResolver {
	return db.currentSearchService
}

// AttachDefaultSearchMetrics attaches metrics to the default database's search
// service, now and to each one that later replaces it.
func (db *DB) AttachDefaultSearchMetrics(metrics *observability.SearchMetrics) {
	db.searchServicesMu.Lock()
	db.defaultSearchMetrics = metrics
	entry := db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.Unlock()
	if entry != nil && entry.svc != nil {
		entry.svc.AttachMetrics(metrics)
	}
}
