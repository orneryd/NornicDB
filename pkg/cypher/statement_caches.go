package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// StatementCaches owns bounded statement-text caches for one database. Selection
// generations on entries have a shorter lifetime than this canonical owner.
type StatementCaches struct {
	syntax   *syntaxValidationCache
	upper    *upperQueryCache
	scope    *multidb.StatementCacheScope
	disposed atomic.Bool
}

func NewStatementCaches() *StatementCaches {
	return &StatementCaches{syntax: newSyntaxValidationCache(), upper: newUpperQueryCache()}
}

func (e *StorageExecutor) StatementCaches() *StatementCaches {
	e.statementCachesOnce.Do(func() {
		if e.statementCaches == nil {
			e.statementCaches = &StatementCaches{syntax: e.ensureSyntaxValidationCache(), upper: e.ensureUpperQueryCache()}
		}
	})
	return e.statementCaches
}

// UseStatementCaches configures a fresh executor before its first execution.
func (e *StorageExecutor) UseStatementCaches(caches *StatementCaches) {
	if caches == nil {
		return
	}
	e.statementCaches = caches
	e.statementCacheScope = caches.scope
	e.syntaxValidationCache = caches.syntax
	e.upperQueryCache = caches.upper
}

func (c *StatementCaches) Disposed() bool { return c.disposed.Load() }

// Dispose retires the canonical database owner. Removed aliases and routed
// targets instead invalidate their selection tokens and dependent entries.
func (c *StatementCaches) Dispose() {
	c.disposed.Store(true)
	c.upper.disposed.Store(true)
	c.syntax.disposed.Store(true)
	c.upper.mu.Lock()
	c.upper.cache = nil
	c.upper.dependencies = nil
	c.upper.mu.Unlock()
	for i := range c.syntax.shards {
		sh := &c.syntax.shards[i]
		sh.mu.Lock()
		sh.cache = nil
		sh.dependencies = nil
		sh.mu.Unlock()
	}
}

// InvalidateScopes purges only entries depending on removed catalog selections.
// Scope retirement precedes this sweep, and writers recheck tokens while locked.
func (c *StatementCaches) InvalidateScopes() {
	c.upper.mu.Lock()
	for key, scopes := range c.upper.dependencies {
		if !statementScopesValid(scopes) {
			delete(c.upper.cache, key)
			delete(c.upper.dependencies, key)
		}
	}
	c.upper.mu.Unlock()
	for i := range c.syntax.shards {
		sh := &c.syntax.shards[i]
		sh.mu.Lock()
		for key, scopes := range sh.dependencies {
			if !statementScopesValid(scopes) {
				delete(sh.cache, key)
				delete(sh.dependencies, key)
			}
		}
		sh.mu.Unlock()
	}
}

func newDatabaseStatementCaches(scope *multidb.StatementCacheScope) multidb.StatementCacheResource {
	caches := NewStatementCaches()
	caches.scope = scope
	return caches
}

func DatabaseStatementCaches(manager *multidb.DatabaseManager, name string) (*StatementCaches, error) {
	resource, _, err := manager.DatabaseStatementCaches(name, newDatabaseStatementCaches)
	if err != nil {
		return nil, err
	}
	return resource.(*StatementCaches), nil
}

// UseDatabaseStatementCaches binds both the canonical owner and selected alias
// or constituent generation before publishing a fresh protocol executor.
func (e *StorageExecutor) UseDatabaseStatementCaches(manager *multidb.DatabaseManager, name string) error {
	resource, scope, err := manager.DatabaseStatementCaches(name, newDatabaseStatementCaches)
	if err != nil {
		return err
	}
	e.UseStatementCaches(resource.(*StatementCaches))
	e.statementCacheScope = scope
	return nil
}

// StatementCacheSelectionValid lets protocol caches reject removed selections
// without retiring a surviving canonical owner or mutating a shared executor.
func (e *StorageExecutor) StatementCacheSelectionValid() bool {
	return e.statementCacheScope.Valid() && !e.StatementCaches().Disposed()
}

type statementCacheScopeResolver interface {
	StatementCacheScope(string) (*multidb.StatementCacheScope, error)
}

func (e *StorageExecutor) bindStatementCacheScope(name string) error {
	resolver, ok := e.dbManager.(statementCacheScopeResolver)
	if !ok {
		return nil
	}
	scope, err := resolver.StatementCacheScope(name)
	if err != nil {
		return err
	}
	e.statementCacheScope = scope
	return nil
}

func statementScopesValid(scopes []*multidb.StatementCacheScope) bool {
	for _, scope := range scopes {
		if !scope.Valid() {
			return false
		}
	}
	return true
}

func containsStatementScope(scopes []*multidb.StatementCacheScope, scope *multidb.StatementCacheScope) bool {
	for _, existing := range scopes {
		if existing == scope {
			return true
		}
	}
	return false
}

// Called only under a cache's write lock; metadata shares its entry's bound and
// eviction lifecycle. Default database entries need no dependency allocation.
func mergeStatementScopes(scopes, incoming []*multidb.StatementCacheScope) []*multidb.StatementCacheScope {
	for _, scope := range incoming {
		if !containsStatementScope(scopes, scope) {
			scopes = append(scopes, scope)
		}
	}
	return scopes
}

func (e *StorageExecutor) routedStatementScope() *multidb.StatementCacheScope {
	if e.statementCacheScope != nil && e.statementCacheScope != e.StatementCaches().scope {
		return e.statementCacheScope
	}
	return nil
}

// cacheDependencyScopes runs on misses, outside cache locks. Reuse the existing
// USE parser/scanner for full texts; routed bodies carry their resolved token.
func (e *StorageExecutor) cacheDependencyScopes(query string) ([]*multidb.StatementCacheScope, bool) {
	if !e.statementCacheScope.Valid() {
		return nil, false
	}
	var scopes []*multidb.StatementCacheScope
	if scope := e.routedStatementScope(); scope != nil {
		scopes = append(scopes, scope)
	}
	// Dynamic Fabric selection can have recorded the outer text before its
	// grammar verdict (for example before slow-query redaction) is stored.
	c := e.ensureSyntaxValidationCache()
	sh := &c.shards[boundedCacheStringShardIndex(query)]
	sh.mu.RLock()
	scopes = append(scopes, sh.dependencies[query]...)
	sh.mu.RUnlock()
	resolver, ok := e.dbManager.(statementCacheScopeResolver)
	if !ok {
		return scopes, statementScopesValid(scopes)
	}
	// The answer is cached by text, so it reads the text as a new input.
	opts := defaultKeywordScanOpts(inputBraceView(context.Background(), query, cyphertext.AcceptedSource))
	opts.SkipParens, opts.SkipBrackets = false, false
	for from := 0; ; {
		at := useKeywordIndexFrom(query, from, opts)
		if at < 0 {
			break
		}
		use, _, hasUse, err := fabric.ParseUseClause(context.Background(), query[at:], true, cyphertext.AcceptedSource)
		if hasUse && err == nil && !use.IsDynamic() {
			scope, err := resolver.StatementCacheScope(use.Name)
			if err != nil {
				return nil, false
			}
			scopes = append(scopes, scope)
		}
		from = at + len("USE")
	}
	return scopes, statementScopesValid(scopes)
}

func (e *StorageExecutor) cachedSyntaxVerdict(query string) uint8 {
	c := e.ensureSyntaxValidationCache()
	if c.disposed.Load() || !e.statementCacheScope.Valid() {
		return 0
	}
	sh := &c.shards[boundedCacheStringShardIndex(query)]
	scope := e.routedStatementScope()
	sh.mu.RLock()
	verdict := sh.cache[query]
	valid := statementScopesValid(sh.dependencies[query])
	needsScope := verdict != 0 && scope != nil && !containsStatementScope(sh.dependencies[query], scope)
	sh.mu.RUnlock()
	if !valid {
		return 0
	}
	if needsScope {
		sh.mu.Lock()
		if !c.disposed.Load() && scope.Valid() && statementScopesValid(sh.dependencies[query]) {
			verdict = sh.cache[query]
			if verdict != 0 {
				if sh.dependencies == nil {
					sh.dependencies = make(map[string][]*multidb.StatementCacheScope)
				}
				sh.dependencies[query] = mergeStatementScopes(sh.dependencies[query], []*multidb.StatementCacheScope{scope})
			}
		} else {
			verdict = 0
		}
		sh.mu.Unlock()
	}
	return verdict
}

func (e *StorageExecutor) addSyntaxCacheEntry(query string, verdict uint8, scopes []*multidb.StatementCacheScope) {
	c := e.ensureSyntaxValidationCache()
	sh := &c.shards[boundedCacheStringShardIndex(query)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if c.disposed.Load() || !e.statementCacheScope.Valid() || !statementScopesValid(scopes) {
		return
	}
	if !statementScopesValid(sh.dependencies[query]) {
		delete(sh.cache, query)
		delete(sh.dependencies, query)
	}
	if _, cached := sh.cache[query]; !cached && len(sh.cache) >= c.max/boundedCacheShardCount {
		for key := range sh.cache {
			delete(sh.cache, key)
			delete(sh.dependencies, key)
			break
		}
	}
	sh.cache[query] |= verdict
	if len(scopes) != 0 {
		if sh.dependencies == nil {
			sh.dependencies = make(map[string][]*multidb.StatementCacheScope)
		}
		sh.dependencies[query] = mergeStatementScopes(sh.dependencies[query], scopes)
	}
}

// recordStatementReference covers resolved dynamic Fabric selections as well
// as remote fragments. A bounded zero-verdict entry holds dependencies until
// validation or redaction supplies a verdict; it never grants acceptance.
func (e *StorageExecutor) recordStatementReference(query, name string) error {
	resolver, ok := e.dbManager.(statementCacheScopeResolver)
	if !ok {
		return nil
	}
	scope, err := resolver.StatementCacheScope(name)
	if err != nil {
		return err
	}
	e.addSyntaxCacheEntry(query, 0, []*multidb.StatementCacheScope{scope})
	upper := e.ensureUpperQueryCache()
	upper.mu.Lock()
	if _, ok := upper.cache[query]; ok && !upper.disposed.Load() && e.statementCacheScope.Valid() && scope.Valid() {
		if upper.dependencies == nil {
			upper.dependencies = make(map[string][]*multidb.StatementCacheScope)
		}
		upper.dependencies[query] = mergeStatementScopes(upper.dependencies[query], []*multidb.StatementCacheScope{scope})
	}
	upper.mu.Unlock()
	return nil
}

func (e *StorageExecutor) cachedUpperQuery(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}
	c := e.ensureUpperQueryCache()
	if c.disposed.Load() || !e.statementCacheScope.Valid() {
		return upperASCII(query)
	}
	scope := e.routedStatementScope()
	c.mu.RLock()
	upper, found := c.cache[query]
	valid := statementScopesValid(c.dependencies[query])
	needsScope := found && scope != nil && !containsStatementScope(c.dependencies[query], scope)
	c.mu.RUnlock()
	if found && valid && !needsScope {
		return upper
	}
	if found && valid && needsScope {
		c.mu.Lock()
		if current, ok := c.cache[query]; ok && !c.disposed.Load() && scope.Valid() && statementScopesValid(c.dependencies[query]) {
			if c.dependencies == nil {
				c.dependencies = make(map[string][]*multidb.StatementCacheScope)
			}
			c.dependencies[query] = mergeStatementScopes(c.dependencies[query], []*multidb.StatementCacheScope{scope})
			c.mu.Unlock()
			return current
		}
		c.mu.Unlock()
	}
	upper = upperASCII(query)
	scopes, cacheable := e.cacheDependencyScopes(query)
	if !cacheable {
		return upper
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disposed.Load() || !e.statementCacheScope.Valid() || !statementScopesValid(scopes) {
		return upper
	}
	if _, cached := c.cache[query]; !cached && len(c.cache) >= c.max {
		for key := range c.cache {
			delete(c.cache, key)
			delete(c.dependencies, key)
			break
		}
	}
	if !statementScopesValid(c.dependencies[query]) {
		delete(c.cache, query)
		delete(c.dependencies, query)
	}
	c.cache[query] = upper
	if len(scopes) != 0 {
		if c.dependencies == nil {
			c.dependencies = make(map[string][]*multidb.StatementCacheScope)
		}
		c.dependencies[query] = mergeStatementScopes(c.dependencies[query], scopes)
	}
	return upper
}

func newUpperQueryCache() *upperQueryCache {
	return &upperQueryCache{cache: make(map[string]string, 1024), max: 4096}
}
func newSyntaxValidationCache() *syntaxValidationCache {
	c := &syntaxValidationCache{max: 4096}
	for i := range c.shards {
		c.shards[i].cache = make(map[string]uint8, 4096/boundedCacheShardCount/4+1)
	}
	return c
}

func (e *StorageExecutor) bindFabricStatementCacheScope(ctx context.Context, database string) error {
	if selection := fabric.GraphSelectionFromContext(ctx); selection != "" {
		database = selection
	}
	return e.bindStatementCacheScope(database)
}
