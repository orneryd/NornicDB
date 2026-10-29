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
	completion    continuationStatus
	retainedBytes int64
	closed        bool
	projection    resultPropertyProjection
}

type continuationGroup struct {
	ranked  []SearchResult
	catalog []SearchResult
}

func (s *Service) newIDContinuationStream(ctx context.Context, options SearchOptions, request SearchContinuationRequest) (resultstream.Stream, error) {
	return s.newCompleteContinuationStream(ctx, options, request, nil, newContinuationExecution(options, request))
}

func (s *Service) streamCompleteContinuationNodes(ctx context.Context, options *SearchOptions, request SearchContinuationRequest, visit storage.NodeVisitor) error {
	// A Where may read any property, so it gets whole nodes.
	if request.AuthorizeNode == nil && options.Where == nil {
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
		schema.VisitConstraints(nil, func(constraint storage.ConstraintFact) bool {
			if constraint.Type() == storage.ConstraintTemporal {
				for index := 0; index < constraint.PropertyCount(); index++ {
					properties[constraint.Property(index)] = struct{}{}
				}
			}
			return true
		})
	}
	projected := make([]string, 0, len(properties))
	for property := range properties {
		projected = append(projected, property)
	}
	sort.Strings(projected)
	return projected
}

func (s *Service) newCompleteContinuationStream(ctx context.Context, options SearchOptions, request SearchContinuationRequest, ranked *SearchResponse, execution *continuationExecution) (resultstream.Stream, error) {
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
	rankedIDs := make([]string, 0)
	searchMethod := "id"
	fallbackTriggered := false
	fallbackReason := SearchFallbackNone
	if ranked != nil {
		searchMethod = ranked.SearchMethod
		fallbackTriggered = ranked.FallbackTriggered
		fallbackReason = ranked.FallbackReason
		for _, result := range ranked.Results {
			result.Phase = SearchContinuationRankedPhase
			id := searchResultID(result)
			rankedByID[id] = result
			rankedIDs = append(rankedIDs, id)
		}
	}
	rankedSeen := make(map[string]struct{}, len(rankedByID))
	var members map[string]*continuationGroup
	var ungroupedChunks [][]SearchResult
	if request.GroupBy != "" {
		members = make(map[string]*continuationGroup)
	}
	scannedNodes := 0
	passageCount := 0
	var retainedBytes int64
	// Foreign authorization/Where callbacks run before projection and outside
	// newly acquired scopes, so they may safely execute their own queries.
	var projectNode func(*storage.Node) *storage.Node
	if options.Where == nil && request.AuthorizeNode == nil {
		scoped, view, release := storage.BeginQueryResultProjectionContext(ctx, s.engine)
		defer release()
		ctx = scoped
		projectNode = view.Node
	} else {
		projectNode = func(n *storage.Node) *storage.Node {
			_, view, release := storage.BeginQueryResultProjectionContext(ctx, s.engine)
			defer release()
			return view.Node(n)
		}
	}
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
		node = projectNode(node)
		groupKey := ""
		if request.GroupBy != "" {
			value, valid := node.Properties[request.GroupBy].(string)
			if !valid || value == "" || !utf8.ValidString(value) {
				return fmt.Errorf("group property %q must be a nonempty UTF-8 string", request.GroupBy)
			}
			groupKey = value
		}
		id := string(node.ID)
		if ranked != nil {
			if _, excluded := ranked.rerankExcluded[id]; excluded {
				return nil
			}
		}
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
		if candidate.Phase == SearchContinuationRankedPhase {
			rankedByID[id] = candidate
			rankedSeen[id] = struct{}{}
		}
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
			if candidate.Phase == SearchContinuationRankedPhase {
				return nil
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
		// The ranked response is the canonical ordered list. Catalogue scanning
		// decides membership and hydrates results, but never derives a new rank
		// from provider or retrieval scores.
		for _, id := range rankedIDs {
			if candidate, ok := rankedByID[id]; ok {
				if _, eligible := rankedSeen[id]; !eligible {
					continue
				}
				results = append(results, candidate)
			}
		}
		catalogStart := len(results)
		for _, chunk := range ungroupedChunks {
			results = append(results, chunk...)
		}
		sort.Slice(results[catalogStart:], func(i, j int) bool {
			return results[catalogStart+i].ID < results[catalogStart+j].ID
		})
	}
	if request.GroupBy != "" {
		results = make([]SearchResult, 0, len(members))
		for _, group := range members {
			group.ranked = nil
		}
		rankedGroups := make([]string, 0, len(members))
		for _, id := range rankedIDs {
			if _, eligible := rankedSeen[id]; !eligible {
				continue
			}
			passage, ok := rankedByID[id]
			if !ok {
				continue
			}
			group := members[passage.GroupKey]
			if len(group.ranked) == 0 {
				rankedGroups = append(rankedGroups, passage.GroupKey)
			}
			group.ranked = append(group.ranked, passage)
		}
		appendGroup := func(passages []SearchResult) {
			representative := passages[0]
			representative.Passages = make([]SearchPassage, len(passages))
			for index := range passages {
				representative.Passages[index] = searchPassageFromResult(passages[index])
			}
			results = append(results, representative)
		}
		for _, groupKey := range rankedGroups {
			appendGroup(members[groupKey].ranked)
		}
		catalogGroups := make([]string, 0, len(members)-len(rankedGroups))
		for groupKey, group := range members {
			if len(group.ranked) == 0 {
				catalogGroups = append(catalogGroups, groupKey)
			}
		}
		sort.Strings(catalogGroups)
		for _, groupKey := range catalogGroups {
			passages := members[groupKey].catalog
			sort.Slice(passages, func(i, j int) bool { return passages[i].ID < passages[j].ID })
			appendGroup(passages)
		}
	}
	eligibleCount := len(results)
	execution.catalog(eligibleCount)
	results = results[:execution.resultCount]
	rankedCount := 0
	for index := range results {
		if results[index].Phase == SearchContinuationRankedPhase {
			rankedCount++
		}
	}
	if s.completePolicyGen.Load() != policyID {
		return nil, resultstream.ErrInvalidated
	}
	resultProjection := newResultPropertyProjection(options.IncludeProperties, options.ExcludeProperties)
	resultProjection.groupBy = request.GroupBy
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
			"ranked_pool_exhausted": execution.status.rankedExhausted,
		},
		retainedBytes: retainedBytes,
		completion:    execution.status,
		projection:    resultProjection,
	}, nil
}

func compactContinuationResultBytes(result SearchResult) int64 {
	return int64(unsafe.Sizeof(result)) + int64(len(result.ID)+len(result.NodeID)+len(result.GroupKey)+len(result.Phase)) + searchMatchesBytes(result.Matches)
}

func compactContinuationResult(result SearchResult) SearchResult {
	result.Type = ""
	result.Labels = nil
	result.Title = ""
	result.Description = ""
	result.ContentPreview = ""
	result.Properties = nil
	result.sourceNode = nil
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
	ranked.sourceNode = stored.sourceNode
	return ranked
}

func searchPassageFromResult(result SearchResult) SearchPassage {
	result.GroupKey = ""
	result.Passages = nil
	return result
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
	if !options.admitsNode(node) {
		return false, nil
	}
	return s.shouldIndexNode(node)
}

func searchResultFromContinuationNode(node *storage.Node, projection resultPropertyProjection) SearchResult {
	result := searchResultFromNode(string(node.ID), node, projection)
	result.Labels = append([]string(nil), node.Labels...)
	if _, typed := node.Properties["type"].(string); !typed && len(node.Labels) > 0 {
		result.Type = node.Labels[0]
	}
	return result
}

// CheckValid is shared by pulls and registry cleanup of abandoned cursors.
func (s *catalogContinuationStream) CheckValid() error {
	provider, ok := s.engine.(graphMutationVersionProvider)
	if !ok {
		return resultstream.ErrInvalidated
	}
	version, supported := provider.GraphMutationVersion()
	if !supported || version != s.version {
		return resultstream.ErrInvalidated
	}
	if s.policy == nil || s.policy.Load() != s.policyID {
		return resultstream.ErrInvalidated
	}
	return nil
}

func (s *catalogContinuationStream) Pull(ctx context.Context, position uint64, n int) (*resultstream.Page, error) {
	if n <= 0 {
		return nil, resultstream.ErrInvalidPageSize
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.CheckValid(); err != nil {
		return nil, err
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

	results, err := hydrateContinuationResultsContext(ctx, s.engine, compact, s.authorizeNode, s.projection)
	if err != nil {
		return nil, err
	}
	_, collectionExhausted, completion := s.completion.page(hasMore)
	metadata["collection_exhausted"], metadata["completion"] = collectionExhausted, completion
	return &resultstream.Page{
		Rows:     continuationRows(results),
		Position: position,
		Next:     end,
		HasMore:  hasMore,
		Total:    &total,
		Metadata: metadata,
	}, nil
}

func hydrateContinuationResults(engine storage.Engine, compact []SearchResult, authorizeNode NodeAuthorizationFunc, projection resultPropertyProjection) ([]SearchResult, error) {
	return hydrateContinuationResultsContext(context.Background(), engine, compact, authorizeNode, projection)
}

func hydrateContinuationResultsContext(ctx context.Context, engine storage.Engine, compact []SearchResult, authorizeNode NodeAuthorizationFunc, projection resultPropertyProjection) ([]SearchResult, error) {
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
	_, visible, release := storage.BeginQueryResultProjectionContext(ctx, engine)
	defer release()
	results := make([]SearchResult, len(compact))
	for index := range compact {
		node := nodes[compact[index].NodeID]
		if node == nil {
			return nil, resultstream.ErrInvalidated
		}
		node = visible.Node(node)
		if projection.groupBy != "" {
			if _, ok := node.Properties[projection.groupBy]; !ok {
				return nil, resultstream.ErrInvalidated
			}
		}
		results[index] = hydrateContinuationResult(compact[index], searchResultFromContinuationNode(node, projection))
		if visible.Enabled() {
			results[index].Matches = visibleSearchMatches(results[index].Matches, node.Properties)
		}
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
			passageNode = visible.Node(passageNode)
			if projection.groupBy != "" {
				if _, ok := passageNode.Properties[projection.groupBy]; !ok {
					return nil, resultstream.ErrInvalidated
				}
			}
			passageResult := passage
			passageResult = hydrateContinuationResult(passageResult, searchResultFromContinuationNode(passageNode, projection))
			if visible.Enabled() {
				passageResult.Matches = visibleSearchMatches(passageResult.Matches, passageNode.Properties)
			}
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
