package search

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	visibleCorpusMaxBytes   = 256 << 20
	visibleCorpusMaxEntries = 64
	visibleCorpusReadBatch  = 256
)

var errMissingFieldProvenance = errors.New("search: field provenance unavailable")

func searchAccessHash(request *auth.FineGrainedRequest) ([32]byte, error) {
	rules, err := json.Marshal(request.Access.Rules())
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(rules), nil
}

// searchVisibilityCacheSafe requires a known local storage owner without
// decay or manual suppression. The clock and query-local reveal scopes can
// change those readers without a graph mutation, so graph-version keys alone
// cannot validate a retained admission decision.
func searchVisibilityCacheSafe(engine storage.Engine) bool {
	owner, ok := storage.UnwrapEngine(engine).(interface {
		IsDecayEnabled() bool
		HasManualSuppressions() bool
	})
	return ok && !owner.IsDecayEnabled() && !owner.HasManualSuppressions()
}

type visibleCorpusKey struct {
	index         *FulltextIndexV2
	version       uint64
	graphVersion  uint64
	fingerprint   string
	accessHash    [32]byte
	database      string
	relationships bool
}

// visibleCorpusCache retains only derived field masks and statistics. It is
// bounded independently of the shared index and never owns text or postings.
type visibleCorpusCache struct {
	mu      sync.Mutex
	entries map[visibleCorpusKey]*visibleFieldCorpus
	order   []visibleCorpusKey
	bytes   int64
}

func (c *visibleCorpusCache) get(key visibleCorpusKey) *visibleFieldCorpus {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil
	}
	for i, old := range c.order {
		if old == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.order = append(c.order, key)
	return entry
}

func (c *visibleCorpusCache) put(key visibleCorpusKey, entry *visibleFieldCorpus) {
	bytes := entry.retainedBytes()
	if bytes > visibleCorpusMaxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[visibleCorpusKey]*visibleFieldCorpus)
	}
	if _, exists := c.entries[key]; exists {
		return
	}
	for len(c.entries) >= visibleCorpusMaxEntries || c.bytes+bytes > visibleCorpusMaxBytes {
		old := c.order[0]
		c.order = c.order[1:]
		c.bytes -= c.entries[old].retainedBytes()
		delete(c.entries, old)
	}
	c.entries[key] = entry
	c.order = append(c.order, key)
	c.bytes += bytes
}

func (v *visibleFieldCorpus) retainedBytes() int64 {
	bytes := int64(cap(v.masks)) * 8
	for _, words := range v.extraMasks {
		bytes += int64(cap(words))*8 + 16
	}
	return bytes
}

func (s *Service) visibleCorpus(ctx context.Context, scope *indexedSearchScope, index *FulltextIndexV2, relationships bool) (*visibleFieldCorpus, error) {
	index.mu.RLock()
	version := index.version
	slots := len(index.docNumToID)
	index.mu.RUnlock()
	if int64(slots)*8 > visibleCorpusMaxBytes {
		return nil, errMissingFieldProvenance
	}
	database := storage.DatabaseOf(s.engine)
	if database == "" {
		database = s.continuationDatabase()
	}
	key := visibleCorpusKey{index: index, version: version, graphVersion: scope.version,
		fingerprint: scope.principal.Fingerprint, database: scope.principal.DatabaseID(database), relationships: relationships}
	cacheable := scope.versions != nil && key.fingerprint != "" && key.database != "" && searchVisibilityCacheSafe(s.engine)
	if cacheable {
		// The runtime fingerprint binds the principal and policy revision.
		// Include the effective rules as well so manually assembled requests
		// cannot share a cache entry by reusing an arbitrary fingerprint.
		accessHash, err := searchAccessHash(scope.principal)
		if err != nil {
			return nil, err
		}
		key.accessHash = accessHash
		if cached := s.visibleCorpora.get(key); cached != nil {
			return cached, scope.check(ctx)
		}
	}
	index.mu.RLock()
	if index.version != version {
		index.mu.RUnlock()
		return nil, errMissingFieldProvenance
	}
	ids := append([]string(nil), index.docNumToID...)
	index.mu.RUnlock()
	view := &visibleFieldCorpus{index: index, version: version, masks: make([]uint64, len(ids))}
	for start := 0; start < len(ids); start += visibleCorpusReadBatch {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+visibleCorpusReadBatch, len(ids))
		batch := ids[start:end]
		var nodes map[storage.NodeID]*storage.Node
		if !relationships {
			nodeIDs := make([]storage.NodeID, 0, len(batch))
			for _, id := range batch {
				if id != "" {
					nodeIDs = append(nodeIDs, storage.NodeID(id))
				}
			}
			var err error
			nodes, err = scope.engine.BatchGetNodes(ctx, nodeIDs)
			if err != nil {
				return nil, err
			}
		}
		for offset, id := range batch {
			if id == "" {
				continue
			}
			var properties map[string]any
			if relationships {
				edge, err := scope.engine.GetEdge(ctx, storage.EdgeID(id))
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if err != nil {
					return nil, err
				}
				properties = edge.Properties
			} else {
				node := nodes[storage.NodeID(id)]
				if node == nil {
					continue
				}
				properties = node.Properties
			}
			index.mu.RLock()
			provenance, exists := index.fieldDocuments[id]
			index.mu.RUnlock()
			if !exists {
				return nil, errMissingFieldProvenance
			}
			docNum := uint32(start + offset)
			var length uint32
			for field, part := range provenance.Fields {
				allowed := strings.HasPrefix(part.Name, "\x00label:")
				if strings.HasPrefix(part.Name, "\x01property:") {
					_, allowed = properties[strings.TrimPrefix(part.Name, "\x01property:")]
				}
				if !allowed {
					continue
				}
				if field < 64 {
					view.masks[docNum] |= uint64(1) << uint(field)
				} else {
					word := (field - 64) / 64
					words := view.extraMasks[docNum]
					if len(words) <= word {
						grown := make([]uint64, word+1)
						copy(grown, words)
						words = grown
						if view.extraMasks == nil {
							view.extraMasks = make(map[uint32][]uint64)
						}
						view.extraMasks[docNum] = words
					}
					words[word] |= uint64(1) << uint(field%64)
				}
				length += part.Length
			}
			if length > 0 {
				view.docCount++
				view.totalLength += int64(length)
			}
		}
	}
	if err := scope.check(ctx); err != nil {
		return nil, err
	}
	index.mu.RLock()
	stable := index.version == version
	index.mu.RUnlock()
	if !stable {
		return nil, errMissingFieldProvenance
	}
	if cacheable {
		s.visibleCorpora.put(key, view)
	}
	return view, nil
}

func (scope *indexedSearchScope) relationshipBM25(ctx context.Context, index bm25Index, read func(context.Context, []string) ([]FulltextBatchEntry, error)) (bm25Ranker, error) {
	if shared, ok := index.(*FulltextIndexV2); ok {
		view, err := scope.service.visibleCorpus(ctx, scope, shared, true)
		if err == nil {
			scope.edgeView.Store(view)
			return visibleBM25Index{corpus: view}, nil
		}
		if !errors.Is(err, errMissingFieldProvenance) {
			return nil, err
		}
	}
	return visibleBM25(ctx, index, read)
}
