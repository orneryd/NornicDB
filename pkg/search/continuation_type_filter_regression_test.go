// Package search – regression tests for the canonical label-or-type-property
// predicate shared by the candidate filters and the complete-continuation
// catalog build.
//
// nodeMatchesType owns the Type decision. The continuation build used to carry
// its own copy of the membership rule, case-folding the wanted types per
// candidate; it now compiles wantedTypesLower once and calls nodeMatchesType,
// preserving strings.ToLower (not EqualFold) semantics, nil/empty Types and the
// suppression/decay → authorization/error → type → property → shouldIndexNode
// order. The allocation test stays indexed against the compiled set, so a
// per-node wanted-map rebuild fails it.
package search

import (
	"context"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newContinuationCatalogService(t *testing.T) (*Service, storage.Engine) {
	t.Helper()
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	service := NewService(engine, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	return service, engine
}

func createContinuationCatalogNode(t *testing.T, engine storage.Engine, node *storage.Node) {
	t.Helper()
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
}

// pullContinuationCatalog drains every page of one ID-mode stream and returns
// the sorted union of the IDs it returned.
func pullContinuationCatalog(t *testing.T, service *Service, options *SearchOptions, request SearchContinuationRequest) []string {
	t.Helper()
	page, err := service.SearchTextContinuation(context.Background(), "", options, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	ids := make([]string, 0, len(page.Results))
	for _, result := range page.Results {
		ids = append(ids, result.ID)
	}
	for page.HasMore {
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		for _, result := range page.Results {
			ids = append(ids, result.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// TestContinuationCatalogTypeFilterMatchesMixedCaseLabelAndProperty is the
// canonical-predicate regression: a label and a type property match the
// requested type in any case mix.
func TestContinuationCatalogTypeFilterMatchesMixedCaseLabelAndProperty(t *testing.T) {
	service, engine := newContinuationCatalogService(t)
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "label-upper", Labels: []string{"Document"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "label-mixed", Labels: []string{"DoCuMeNt"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "property-upper", Labels: []string{"Legacy"}, Properties: map[string]any{"type": "DOCUMENT"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "property-mixed", Labels: []string{"Legacy"}, Properties: map[string]any{"type": "DoCuMeNt"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "other-label", Labels: []string{"Other"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "other-property", Labels: []string{"Other"}, Properties: map[string]any{"type": "note"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "non-string-type", Labels: []string{"Other"}, Properties: map[string]any{"type": 42}})

	want := []string{"label-mixed", "label-upper", "property-mixed", "property-upper"}
	for _, requested := range []string{"document", "DOCUMENT", "DoCuMeNt"} {
		t.Run(requested, func(t *testing.T) {
			ids := pullContinuationCatalog(t, service, &SearchOptions{Types: []string{requested}}, SearchContinuationRequest{
				Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 100,
			})
			require.Equal(t, want, ids)
		})
	}
}

// TestContinuationCatalogMissingTypesAdmitsEveryEligibleNode pins nil/empty
// Types behavior: no Type constraint, so the property filter alone decides.
func TestContinuationCatalogMissingTypesAdmitsEveryEligibleNode(t *testing.T) {
	service, engine := newContinuationCatalogService(t)
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "keep-a", Labels: []string{"Document"}, Properties: map[string]any{"state": "ready"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "keep-b", Labels: []string{"Other"}, Properties: map[string]any{"type": "note", "state": "ready"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "drop-state", Labels: []string{"Document"}, Properties: map[string]any{"state": "draft"}})

	for name, options := range map[string]*SearchOptions{
		"nil types":   {Filters: map[string][]string{"state": {"ready"}}},
		"empty types": {Types: []string{}, Filters: map[string][]string{"state": {"ready"}}},
	} {
		t.Run(name, func(t *testing.T) {
			ids := pullContinuationCatalog(t, service, options, SearchContinuationRequest{
				Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 100,
			})
			require.Equal(t, []string{"keep-a", "keep-b"}, ids)
		})
	}
}

// TestContinuationCatalogAuthorizationPrecedesTypeFilter records the check
// order: authorization and its error, then the Type predicate, then property
// filters.
func TestContinuationCatalogAuthorizationPrecedesTypeFilter(t *testing.T) {
	service, engine := newContinuationCatalogService(t)
	var mu sync.Mutex
	var authorizations []string
	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 100,
		AuthorizeNode: func(node *storage.Node) (bool, error) {
			mu.Lock()
			authorizations = append(authorizations, string(node.ID))
			mu.Unlock()
			if node.ID == "denied-error" {
				return false, errors.New("authorization backend unavailable")
			}
			return node.ID != "denied", nil
		},
	}
	options := &SearchOptions{Types: []string{"document"}, Filters: map[string][]string{"state": {"ready"}}}

	// Its type does not match, yet the authorization error surfaces: the
	// authorization step runs before the Type predicate.
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "denied-error", Labels: []string{"Other"}})
	_, err := service.SearchTextContinuation(context.Background(), "", options, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorContains(t, err, "authorization backend unavailable")
	mu.Lock()
	require.Equal(t, []string{"denied-error"}, authorizations)
	authorizations = nil
	mu.Unlock()

	// The same rule without a failing callback: denials and Type mismatches are
	// plain exclusions after the node is authorized.
	request.AuthorizeNode = func(node *storage.Node) (bool, error) {
		mu.Lock()
		authorizations = append(authorizations, string(node.ID))
		mu.Unlock()
		return node.ID != "denied", nil
	}
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "denied", Labels: []string{"Document"}, Properties: map[string]any{"state": "ready"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "admitted", Labels: []string{"Document"}, Properties: map[string]any{"state": "ready"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "wrong-type", Labels: []string{"Other"}, Properties: map[string]any{"state": "ready"}})
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "wrong-property", Labels: []string{"Document"}, Properties: map[string]any{"state": "draft"}})

	ids := pullContinuationCatalog(t, service, options, request)
	require.Equal(t, []string{"admitted"}, ids)
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, authorizations, "wrong-type", "authorization precedes the Type predicate")
	require.Contains(t, authorizations, "wrong-property", "authorization precedes property filters")
	require.Contains(t, authorizations, "admitted")
	require.Contains(t, authorizations, "denied")
}

// TestContinuationCatalogSuppressionPrecedesAuthorization keeps the first step
// of the order intact: a visibility-suppressed node is never authorized.
func TestContinuationCatalogSuppressionPrecedesAuthorization(t *testing.T) {
	service, engine := newContinuationCatalogService(t)
	suppressed := &storage.Node{ID: "suppressed", Labels: []string{"Document"}}
	suppressed.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: true})
	createContinuationCatalogNode(t, engine, suppressed)
	createContinuationCatalogNode(t, engine, &storage.Node{ID: "visible", Labels: []string{"Document"}})

	var mu sync.Mutex
	var authorized []string
	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 100,
		AuthorizeNode: func(node *storage.Node) (bool, error) {
			mu.Lock()
			authorized = append(authorized, string(node.ID))
			mu.Unlock()
			return true, nil
		},
	}
	ids := pullContinuationCatalog(t, service, &SearchOptions{Types: []string{"document"}}, request)
	require.Equal(t, []string{"visible"}, ids)
	mu.Lock()
	defer mu.Unlock()
	require.NotContains(t, authorized, "suppressed")
}

// TestContinuationNodeEligibleTypePredicateContract pins the eligibility
// function to the canonical predicate: its Type decision must equal
// nodeMatchesType over the compiled wanted set.
func TestContinuationNodeEligibleTypePredicateContract(t *testing.T) {
	service, _ := newContinuationCatalogService(t)
	options := &SearchOptions{Types: []string{"DoCuMeNt"}}
	wanted := wantedTypesLower(options.Types)
	nodes := map[string]*storage.Node{
		"label-upper":     {ID: "label-upper", Labels: []string{"Document"}},
		"label-mixed":     {ID: "label-mixed", Labels: []string{"DoCuMeNt"}},
		"property-upper":  {ID: "property-upper", Labels: []string{"Legacy"}, Properties: map[string]any{"type": "DOCUMENT"}},
		"property-mixed":  {ID: "property-mixed", Labels: []string{"Legacy"}, Properties: map[string]any{"type": "DoCuMeNt"}},
		"other":           {ID: "other", Labels: []string{"Other"}},
		"empty-labels":    {ID: "empty-labels"},
		"non-string-type": {ID: "non-string-type", Labels: []string{"Other"}, Properties: map[string]any{"type": 7}},
	}
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		node := nodes[key]
		eligible, err := service.continuationNodeEligible(node, options, wanted, nil, nil)
		require.NoError(t, err)
		require.Equal(t, nodeMatchesType(node, wanted), eligible, "node %s", key)
	}

	// Empty Types leaves the wanted set empty, so no Type rejection applies.
	unconstrained := &SearchOptions{}
	eligible, err := service.continuationNodeEligible(nodes["other"], unconstrained, wantedTypesLower(unconstrained.Types), nil, nil)
	require.NoError(t, err)
	require.True(t, eligible)
}

// A precompiled wanted set is reused across candidate eligibility checks.
// Lowercase labels avoid pre-existing string normalization allocations, so a
// per-candidate map construction would be visible in the allocation assertion.
func TestContinuationCatalogCompilesWantedTypesOncePerBuild(t *testing.T) {
	service, engine := newContinuationCatalogService(t)
	const nodeCount = 64
	nodes := make([]*storage.Node, nodeCount)
	for index := range nodes {
		nodes[index] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%02d", index)), Labels: []string{"document"}}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	options := &SearchOptions{Types: []string{"DOCUMENT"}}
	ids := pullContinuationCatalog(t, service, options, SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: nodeCount,
	})
	require.Len(t, ids, nodeCount)
	wanted := wantedTypesLower([]string{"DOCUMENT", "document"})
	require.Len(t, wanted, 1)
	matched := 0
	allocations := testing.AllocsPerRun(10, func() {
		matched = 0
		for _, node := range nodes {
			admitted, err := service.continuationNodeEligible(node, options, wanted, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if admitted {
				matched++
			}
		}
	})
	require.Equal(t, nodeCount, matched)
	require.Zero(t, allocations, "eligible candidates reuse the prepared type set")
}

func TestWantedTypesLowerRetainsSimpleLowercaseContract(t *testing.T) {
	wanted := wantedTypesLower([]string{"S"})
	require.True(t, nodeMatchesType(&storage.Node{Labels: []string{"s"}}, wanted))
	// EqualFold would admit long-s here; the existing loaded-node rule is
	// equality of strings.ToLower results and must remain so.
	require.False(t, nodeMatchesType(&storage.Node{Labels: []string{"ſ"}}, wanted))
}
