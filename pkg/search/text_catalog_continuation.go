package search

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type graphMutationVersionProvider interface {
	GraphMutationVersion() (uint64, bool)
}

// CompleteContinuationPolicy bounds exact population materialization. A zero
// field uses its default so callers can override one limit independently.
type CompleteContinuationPolicy struct {
	MaxScannedNodes     int
	MaxMembers          int
	MaxPassages         int
	MaxBuildBytes       int64
	MaxBuildDuration    time.Duration
	MaxConcurrentBuilds int64
}

func defaultCompleteContinuationPolicy() CompleteContinuationPolicy {
	return CompleteContinuationPolicy{
		MaxScannedNodes:     1_000_000,
		MaxMembers:          100_000,
		MaxPassages:         1_000_000,
		MaxBuildBytes:       256 << 20,
		MaxBuildDuration:    30 * time.Second,
		MaxConcurrentBuilds: 1,
	}
}

// SetCompleteContinuationPolicy replaces the limits used by future complete
// continuation builds. Zero fields retain their defaults.
func (s *Service) SetCompleteContinuationPolicy(policy CompleteContinuationPolicy) {
	defaults := defaultCompleteContinuationPolicy()
	if policy.MaxScannedNodes <= 0 {
		policy.MaxScannedNodes = defaults.MaxScannedNodes
	}
	if policy.MaxMembers <= 0 {
		policy.MaxMembers = defaults.MaxMembers
	}
	if policy.MaxPassages <= 0 {
		policy.MaxPassages = defaults.MaxPassages
	}
	if policy.MaxBuildBytes <= 0 {
		policy.MaxBuildBytes = defaults.MaxBuildBytes
	}
	if policy.MaxBuildDuration <= 0 {
		policy.MaxBuildDuration = defaults.MaxBuildDuration
	}
	if policy.MaxConcurrentBuilds <= 0 {
		policy.MaxConcurrentBuilds = defaults.MaxConcurrentBuilds
	}
	s.completePolicyMu.Lock()
	s.completePolicy.Store(policy)
	s.completePolicyGen.Add(1)
	s.completePolicyMu.Unlock()
}

func (s *Service) acquireCompleteContinuationBuild() (CompleteContinuationPolicy, uint64, bool) {
	s.completePolicyMu.RLock()
	defer s.completePolicyMu.RUnlock()
	policy, ok := s.completePolicy.Load().(CompleteContinuationPolicy)
	if !ok {
		policy = defaultCompleteContinuationPolicy()
	}
	policyID := s.completePolicyGen.Load()
	for {
		active := s.completeBuilds.Load()
		if active >= policy.MaxConcurrentBuilds {
			return policy, policyID, false
		}
		if s.completeBuilds.CompareAndSwap(active, active+1) {
			return policy, policyID, true
		}
	}
}

type catalogContinuationStream struct {
	mu            sync.RWMutex
	results       []SearchResult
	engine        storage.Engine
	authorizeNode NodeAuthorizationFunc
	version       uint64
	policy        *atomic.Uint64
	policyID      uint64
	metadata      map[string]any
	retainedBytes int64
	closed        bool
}

type continuationGroup struct {
	ranked  []SearchResult
	catalog []SearchResult
}

func (s *Service) newIDContinuationStream(ctx context.Context, options SearchOptions, request SearchContinuationRequest) (resultstream.Stream, error) {
	return s.newCompleteContinuationStream(ctx, options, request, nil)
}

func (s *Service) streamCompleteContinuationNodes(ctx context.Context, options *SearchOptions, request SearchContinuationRequest, visit storage.NodeVisitor) error {
	if request.AuthorizeNode == nil {
		if reader, ok := s.engine.(storage.ProjectedPrefixNodeReader); ok {
			return reader.StreamNodesByPrefixProjected(ctx, "", s.completeContinuationProjectionProperties(options, request), visit)
		}
	}
	return storage.StreamNodesWithFallback(ctx, s.engine, 1000, visit)
}

func (s *Service) completeContinuationProjectionProperties(options *SearchOptions, request SearchContinuationRequest) []string {
	properties := make(map[string]struct{}, len(options.Filters)+4)
	if len(options.Types) > 0 {
		properties["type"] = struct{}{}
	}
	for property := range options.Filters {
		properties[property] = struct{}{}
	}
	if request.GroupBy != "" {
		properties[request.GroupBy] = struct{}{}
	}
	if schema := s.engine.GetSchema(); schema != nil {
		for _, constraint := range schema.GetAllConstraints() {
			if constraint.Type != storage.ConstraintTemporal {
				continue
			}
			for _, property := range constraint.Properties {
				properties[property] = struct{}{}
			}
		}
	}
	projected := make([]string, 0, len(properties))
	for property := range properties {
		projected = append(projected, property)
	}
	sort.Strings(projected)
	return projected
}

func (s *Service) newCompleteContinuationStream(ctx context.Context, options SearchOptions, request SearchContinuationRequest, ranked *SearchResponse) (resultstream.Stream, error) {
	started := time.Now()
	policy, policyID, admitted := s.acquireCompleteContinuationBuild()
	if !admitted {
		return nil, resultstream.ErrCapacity
	}
	defer s.completeBuilds.Add(-1)

	provider, ok := s.engine.(graphMutationVersionProvider)
	if !ok {
		return nil, fmt.Errorf("complete continuation requires graph mutation revisions: %w", resultstream.ErrInvalidated)
	}
	version, supported := provider.GraphMutationVersion()
	if !supported {
		return nil, fmt.Errorf("complete continuation requires graph mutation revisions: %w", resultstream.ErrInvalidated)
	}

	s.mu.RLock()
	decayFilter := s.nodeDecayFilter
	s.mu.RUnlock()
	rankedByID := make(map[string]SearchResult)
	searchMethod := "id"
	fallbackTriggered := false
	fallbackReason := SearchFallbackNone
	rankedPoolExhausted := true
	if ranked != nil {
		searchMethod = ranked.SearchMethod
		fallbackTriggered = ranked.FallbackTriggered
		fallbackReason = ranked.FallbackReason
		rankedPoolExhausted = ranked.RetrievalExhausted
		for _, result := range ranked.Results {
			result.Phase = SearchContinuationRankedPhase
			rankedByID[searchResultID(result)] = result
		}
	}
	var members map[string]*continuationGroup
	var ungroupedChunks [][]SearchResult
	if request.GroupBy != "" {
		members = make(map[string]*continuationGroup)
	}
	scannedNodes := 0
	passageCount := 0
	var retainedBytes int64
	err := s.streamCompleteContinuationNodes(ctx, &options, request, func(node *storage.Node) error {
		if time.Since(started) > policy.MaxBuildDuration {
			return resultstream.ErrCapacity
		}
		scannedNodes++
		if scannedNodes > policy.MaxScannedNodes {
			return resultstream.ErrCapacity
		}
		eligible, err := s.continuationNodeEligible(node, &options, decayFilter, request.AuthorizeNode)
		if err != nil || !eligible {
			return err
		}
		groupKey := ""
		if request.GroupBy != "" {
			value, valid := node.Properties[request.GroupBy].(string)
			if !valid || value == "" || !utf8.ValidString(value) {
				return fmt.Errorf("group property %q must be a nonempty UTF-8 string", request.GroupBy)
			}
			groupKey = value
		}
		id := string(node.ID)
		logicalID := id
		if groupKey != "" {
			logicalID = groupKey
		}
		candidate := SearchResult{ID: id, NodeID: node.ID}
		if selected, exists := rankedByID[id]; exists {
			candidate = compactContinuationResult(selected)
			candidate.ID = id
			candidate.NodeID = node.ID
			candidate.Phase = SearchContinuationRankedPhase
		} else {
			candidate.Phase = SearchContinuationCatalogPhase
		}
		candidate.GroupKey = groupKey
		retainedBytes += compactContinuationResultBytes(candidate)
		if retainedBytes > policy.MaxBuildBytes {
			return resultstream.ErrCapacity
		}
		if request.GroupBy == "" {
			if passageCount >= policy.MaxMembers {
				return resultstream.ErrCapacity
			}
			passageCount++
			if passageCount > policy.MaxPassages {
				return resultstream.ErrCapacity
			}
			if len(ungroupedChunks) == 0 || len(ungroupedChunks[len(ungroupedChunks)-1]) == cap(ungroupedChunks[len(ungroupedChunks)-1]) {
				ungroupedChunks = append(ungroupedChunks, make([]SearchResult, 0, 1024))
			}
			last := len(ungroupedChunks) - 1
			ungroupedChunks[last] = append(ungroupedChunks[last], candidate)
			return nil
		}
		group := members[logicalID]
		if group == nil {
			if len(members) >= policy.MaxMembers {
				return resultstream.ErrCapacity
			}
			group = &continuationGroup{}
			members[logicalID] = group
		}
		passageCount++
		if passageCount > policy.MaxPassages {
			return resultstream.ErrCapacity
		}
		if candidate.Phase == SearchContinuationRankedPhase {
			group.ranked = append(group.ranked, candidate)
		} else {
			group.catalog = append(group.catalog, candidate)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if time.Since(started) > policy.MaxBuildDuration {
		return nil, resultstream.ErrCapacity
	}
	after, supported := provider.GraphMutationVersion()
	if !supported || after != version {
		return nil, resultstream.ErrInvalidated
	}
	memberCount := len(members)
	if request.GroupBy == "" {
		memberCount = passageCount
	}
	var results []SearchResult
	if request.GroupBy == "" {
		results = make([]SearchResult, 0, memberCount)
		for _, chunk := range ungroupedChunks {
			results = append(results, chunk...)
		}
	}
	if request.GroupBy != "" {
		results = make([]SearchResult, 0, len(members))
		for _, group := range members {
			passages := group.catalog
			if len(group.ranked) > 0 {
				passages = group.ranked
				sort.Slice(passages, func(i, j int) bool {
					return betterContinuationRepresentative(passages[i], passages[j])
				})
			} else {
				sort.Slice(passages, func(i, j int) bool { return passages[i].ID < passages[j].ID })
			}
			representative := passages[0]
			representative.Passages = make([]SearchPassage, len(passages))
			for index := range passages {
				representative.Passages[index] = searchPassageFromResult(passages[index])
			}
			results = append(results, representative)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Phase != results[j].Phase {
			return results[i].Phase == SearchContinuationRankedPhase
		}
		if results[i].Phase == SearchContinuationRankedPhase && results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		left, right := results[i].ID, results[j].ID
		if request.GroupBy != "" {
			left, right = results[i].GroupKey, results[j].GroupKey
		}
		if left != right {
			return left < right
		}
		return results[i].ID < results[j].ID
	})
	eligibleCount := len(results)
	maxResultsReached := request.MaxResults > 0 && eligibleCount > request.MaxResults
	if maxResultsReached {
		results = results[:request.MaxResults]
	}
	rankedCount := 0
	for index := range results {
		if results[index].Phase == SearchContinuationRankedPhase {
			rankedCount++
		}
	}
	if s.completePolicyGen.Load() != policyID {
		return nil, resultstream.ErrInvalidated
	}
	return &catalogContinuationStream{
		results:       results,
		engine:        s.engine,
		authorizeNode: request.AuthorizeNode,
		version:       version,
		policy:        &s.completePolicyGen,
		policyID:      policyID,
		metadata: map[string]any{
			"search_method":         searchMethod,
			"response":              searchResponseMetadata(ranked),
			"fallback_triggered":    fallbackTriggered,
			"fallback_reason":       fallbackReason,
			"discovered":            eligibleCount,
			"mode":                  request.Mode,
			"ranked_count":          rankedCount,
			"eligible_count":        &eligibleCount,
			"ranked_pool_exhausted": rankedPoolExhausted,
			"max_results_reached":   maxResultsReached,
		},
		retainedBytes: retainedBytes,
	}, nil
}

func compactContinuationResultBytes(result SearchResult) int64 {
	return int64(unsafe.Sizeof(result)) + int64(len(result.ID)+len(result.NodeID)+len(result.GroupKey)+len(result.Phase))
}

func compactContinuationResult(result SearchResult) SearchResult {
	result.Type = ""
	result.Labels = nil
	result.Title = ""
	result.Description = ""
	result.ContentPreview = ""
	result.Properties = nil
	result.Passages = nil
	return result
}

func hydrateContinuationResult(ranked, stored SearchResult) SearchResult {
	ranked.NodeID = stored.NodeID
	ranked.Type = stored.Type
	ranked.Labels = stored.Labels
	ranked.Title = stored.Title
	ranked.Description = stored.Description
	ranked.ContentPreview = stored.ContentPreview
	ranked.Properties = stored.Properties
	return ranked
}

func searchPassageFromResult(result SearchResult) SearchPassage {
	result.GroupKey = ""
	result.Passages = nil
	return result
}

func betterContinuationRepresentative(candidate, current SearchResult) bool {
	if candidate.Score != current.Score {
		return candidate.Score > current.Score
	}
	if candidate.ID != current.ID {
		return candidate.ID < current.ID
	}
	if candidate.RRFScore != current.RRFScore {
		return candidate.RRFScore > current.RRFScore
	}
	if candidate.Similarity != current.Similarity {
		return candidate.Similarity > current.Similarity
	}
	if candidate.VectorRank != current.VectorRank {
		return candidate.VectorRank < current.VectorRank
	}
	return candidate.BM25Rank < current.BM25Rank
}

func (s *Service) continuationNodeEligible(node *storage.Node, options *SearchOptions, decayFilter NodeDecayFilterFunc, authorizeNode NodeAuthorizationFunc) (bool, error) {
	if node == nil || node.VisibilitySuppressed || (decayFilter != nil && decayFilter(string(node.ID))) {
		return false, nil
	}
	if authorizeNode != nil {
		authorized, err := authorizeNode(node)
		if err != nil || !authorized {
			return false, err
		}
	}
	if len(options.Types) > 0 {
		matched := false
		for _, wanted := range options.Types {
			wanted = strings.ToLower(wanted)
			for _, label := range node.Labels {
				if wanted == strings.ToLower(label) {
					matched = true
					break
				}
			}
			if nodeType, ok := node.Properties["type"].(string); ok && wanted == strings.ToLower(nodeType) {
				matched = true
			}
			if matched {
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	if !nodeMatchesFilters(node, options.Filters) {
		return false, nil
	}
	return s.shouldIndexNode(node)
}

func searchResultFromContinuationNode(node *storage.Node) SearchResult {
	result := SearchResult{
		ID:         string(node.ID),
		NodeID:     node.ID,
		Labels:     append([]string(nil), node.Labels...),
		Properties: node.Properties,
	}
	if t, ok := node.Properties["type"].(string); ok {
		result.Type = t
	} else if len(node.Labels) > 0 {
		result.Type = node.Labels[0]
	}
	if title, ok := node.Properties["title"].(string); ok {
		result.Title = title
	}
	if desc, ok := node.Properties["description"].(string); ok {
		result.Description = desc
	}
	if content, ok := node.Properties["content"].(string); ok {
		result.ContentPreview = truncate(content, 200)
	} else if text, ok := node.Properties["text"].(string); ok {
		result.ContentPreview = truncate(text, 200)
	}
	return result
}

func (s *catalogContinuationStream) Pull(ctx context.Context, position uint64, n int) (*resultstream.Page, error) {
	if n <= 0 {
		return nil, resultstream.ErrInvalidPageSize
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provider, ok := s.engine.(graphMutationVersionProvider)
	if !ok {
		return nil, resultstream.ErrInvalidated
	}
	version, supported := provider.GraphMutationVersion()
	if !supported || version != s.version {
		return nil, resultstream.ErrInvalidated
	}
	if s.policy == nil || s.policy.Load() != s.policyID {
		return nil, resultstream.ErrInvalidated
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, resultstream.ErrClosed
	}
	if position > uint64(len(s.results)) {
		s.mu.RUnlock()
		return nil, resultstream.ErrInvalidPosition
	}
	end := min(position+uint64(n), uint64(len(s.results)))
	hasMore := end < uint64(len(s.results))
	total := uint64(len(s.results))
	window := s.results[position:end]
	metadataSource := s.metadata
	s.mu.RUnlock()

	compact := append([]SearchResult(nil), window...)
	metadata := make(map[string]any, len(metadataSource)+2)
	for key, value := range metadataSource {
		metadata[key] = value
	}
	maxResultsReached, _ := metadata["max_results_reached"].(bool)
	results, err := hydrateContinuationResults(s.engine, compact, s.authorizeNode)
	if err != nil {
		return nil, err
	}
	if hasMore {
		metadata["collection_exhausted"] = false
		metadata["completion"] = SearchContinuationMoreResults
	} else if maxResultsReached {
		metadata["collection_exhausted"] = false
		metadata["completion"] = SearchContinuationMaxResultsComplete
	} else {
		metadata["collection_exhausted"] = true
		metadata["completion"] = SearchContinuationCollectionComplete
	}
	return &resultstream.Page{
		Rows:     continuationRows(results),
		Position: position,
		Next:     end,
		HasMore:  hasMore,
		Total:    &total,
		Metadata: metadata,
	}, nil
}

func hydrateContinuationResults(engine storage.Engine, compact []SearchResult, authorizeNode NodeAuthorizationFunc) ([]SearchResult, error) {
	idCount := 0
	for index := range compact {
		if len(compact[index].Passages) > 0 {
			idCount += len(compact[index].Passages)
		} else {
			idCount++
		}
	}
	ids := make([]storage.NodeID, 0, idCount)
	for index := range compact {
		if len(compact[index].Passages) == 0 {
			ids = append(ids, compact[index].NodeID)
			continue
		}
		for passageIndex := range compact[index].Passages {
			ids = append(ids, compact[index].Passages[passageIndex].NodeID)
		}
	}
	nodes, err := batchContinuationNodesWithoutEmbeddings(engine, ids)
	if err != nil {
		return nil, err
	}
	if authorizeNode != nil {
		for _, node := range nodes {
			authorized, authorizeErr := authorizeNode(node)
			if authorizeErr != nil {
				return nil, authorizeErr
			}
			if !authorized {
				return nil, resultstream.ErrInvalidated
			}
		}
	}
	results := make([]SearchResult, len(compact))
	for index := range compact {
		node := nodes[compact[index].NodeID]
		if node == nil {
			return nil, resultstream.ErrInvalidated
		}
		results[index] = hydrateContinuationResult(compact[index], searchResultFromContinuationNode(node))
		if len(compact[index].Passages) == 0 {
			continue
		}
		results[index].Passages = make([]SearchPassage, len(compact[index].Passages))
		for passageIndex := range compact[index].Passages {
			passage := compact[index].Passages[passageIndex]
			passageNode := nodes[passage.NodeID]
			if passageNode == nil {
				return nil, resultstream.ErrInvalidated
			}
			passageResult := passage
			passageResult = hydrateContinuationResult(passageResult, searchResultFromContinuationNode(passageNode))
			results[index].Passages[passageIndex] = searchPassageFromResult(passageResult)
		}
	}
	return results, nil
}

func batchContinuationNodesWithoutEmbeddings(engine storage.Engine, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	if reader, ok := engine.(storage.BatchNodeWithoutEmbeddingsReader); ok {
		nodes, err := reader.BatchGetNodesWithoutEmbeddings(ids)
		if err == nil {
			return nodes, nil
		}
		if !errors.Is(err, storage.ErrNotImplemented) {
			return nil, err
		}
	}
	return engine.BatchGetNodes(ids)
}

func (s *catalogContinuationStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.results = nil
	s.mu.Unlock()
	return nil
}

func (s *catalogContinuationStream) RetainedBytes() int64 {
	return s.retainedBytes
}
