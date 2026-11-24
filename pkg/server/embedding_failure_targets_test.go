package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// embeddingSubmissionRecorder records every text that reaches the external
// provider. A refused caller operation must leave it empty.
type embeddingSubmissionRecorder struct {
	fixedSearchEmbedder
	mu    sync.Mutex
	calls []string
}

func (e *embeddingSubmissionRecorder) record(texts ...string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, texts...)
}

func (e *embeddingSubmissionRecorder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.record(text)
	return e.fixedSearchEmbedder.Embed(ctx, text)
}

func (e *embeddingSubmissionRecorder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.record(texts...)
	return e.fixedSearchEmbedder.EmbedBatch(ctx, texts)
}

// parkEmbeddingFailure writes the durable terminal-failure marker the manual
// retry administers. It uses the same storage capability the worker's accepted
// outcome uses, so the parked failure is the production record.
func parkEmbeddingFailure(t *testing.T, server *Server, database, id string) storage.NodeID {
	t.Helper()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	wrapper, ok := engine.(storage.EngineUnwrapper)
	require.True(t, ok, "the catalog view must expose its scoped storage")
	namespace, ok := wrapper.GetInnerEngine().(*storage.NamespacedEngine)
	require.True(t, ok, "the catalog view must retain its database scope")
	_, err = engine.CreateNode(t.Context(), &storage.Node{
		ID: storage.NodeID(id), Labels: []string{"Doc"},
		Properties: map[string]any{"content": "parked " + id},
	})
	require.NoError(t, err)
	node, err := engine.GetNode(t.Context(), storage.NodeID(id))
	require.NoError(t, err)
	node.EmbedMeta = map[string]any{
		"has_embedding":       false,
		"embedding_failed":    true,
		"embedding_error":     "provider failed",
		"embedding_failed_at": "2026-01-01T00:00:00Z",
	}
	require.NoError(t, namespace.UpdateNodeEmbeddingSidecar(t.Context(), node))
	// The retry request uses the physical ID returned by the failure scan.
	return namespace.PrefixNodeID(node.ID)
}

// editFineGrainedCatalog stages one catalog edit through the authorized HTTP
// owner, preserving the committed revision contract.
func editFineGrainedCatalog(t *testing.T, server *Server, adminToken string, edit func(*auth.FineGrainedCatalog)) {
	t.Helper()
	read := func() fineGrainedCatalogResponse {
		t.Helper()
		response := makeRequest(t, server, http.MethodGet, "/auth/fgac/catalog", nil, "Bearer "+adminToken)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var document fineGrainedCatalogResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &document))
		return document
	}
	before := read()
	require.True(t, before.Enforced, "the fine-grained runtime must own the request decision")
	catalog := before.Catalog
	edit(&catalog)
	body, err := json.Marshal(map[string]any{"catalog": catalog, "revision": before.Revision})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPut, "/auth/fgac/catalog", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+adminToken)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
}

func grantFineGrainedEmbeddingRetry(t *testing.T, server *Server, adminToken, role, database string) string {
	t.Helper()
	databaseID, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, databaseID)
	definition := "retry-" + database
	editFineGrainedCatalog(t, server, adminToken, func(catalog *auth.FineGrainedCatalog) {
		catalog.Definitions = append(catalog.Definitions, auth.FineGrainedEntitlement{
			ID: definition, Action: "embedding.failures.retry",
			Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: databaseID},
		})
		if catalog.Assignments == nil {
			catalog.Assignments = map[string][]string{}
		}
		normalized := auth.NormalizeRoleName(role)
		catalog.Assignments[normalized] = append(catalog.Assignments[normalized], definition)
	})
	return definition
}

func revokeFineGrainedEmbeddingRetry(t *testing.T, server *Server, adminToken, role, definition string) {
	t.Helper()
	editFineGrainedCatalog(t, server, adminToken, func(catalog *auth.FineGrainedCatalog) {
		definitions := catalog.Definitions[:0]
		for _, entitlement := range catalog.Definitions {
			if entitlement.ID != definition {
				definitions = append(definitions, entitlement)
			}
		}
		catalog.Definitions = definitions
		normalized := auth.NormalizeRoleName(role)
		assigned := catalog.Assignments[normalized][:0]
		for _, id := range catalog.Assignments[normalized] {
			if id != definition {
				assigned = append(assigned, id)
			}
		}
		catalog.Assignments[normalized] = assigned
	})
}

func embeddingFailureNodeIDs(t *testing.T, server *Server) []storage.NodeID {
	t.Helper()
	failures, err := server.db.EmbeddingFailures(t.Context(), 0)
	require.NoError(t, err)
	ids := make([]storage.NodeID, 0, len(failures))
	for _, failure := range failures {
		ids = append(ids, failure.NodeID)
	}
	return ids
}

// TestFineGrainedEmbeddingRetryAuthorizesNamedTargets is the product proof for
// manual retry ownership: a caller grant on one database admits a retry of that
// database's parked failures, refuses a retry of another database's failure
// before anything is requeued, and keeps the population-wide decision for a
// retry that names no targets.
func TestFineGrainedEmbeddingRetryAuthorizesNamedTargets(t *testing.T) {
	server, users := setupTestServer(t)
	require.True(t, server.db.SetEmbedder(fixedSearchEmbedder{}), "the queue must exist for durable failure administration")
	server.db.SetEmbedQueueShouldYield(func() bool { return true })
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))

	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	defaultDatabase := server.dbManager.DefaultDatabaseName()
	response := makeRequest(t, server, http.MethodPost, "/db/system/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE DATABASE tenant_b OPTIONS {storage: 'isolated'}"}},
	}, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.True(t, server.dbManager.ExistsOrIsConstituent("tenant_b"))

	homeTarget := parkEmbeddingFailure(t, server, defaultDatabase, "doc")
	tenantTarget := parkEmbeddingFailure(t, server, "tenant_b", "doc")
	require.ElementsMatch(t, []storage.NodeID{homeTarget, tenantTarget}, embeddingFailureNodeIDs(t, server))

	grantFineGrainedEmbeddingRetry(t, server, admin, string(auth.RoleViewer), defaultDatabase)

	// The granted database is admitted for the named entity it owns.
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/embed/retry-failures",
		map[string]any{"node_ids": []string{string(homeTarget)}}, "Bearer "+reader)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"retried":1`)
	require.Equal(t, []storage.NodeID{tenantTarget}, embeddingFailureNodeIDs(t, server))

	// The other database's named entity is refused before any requeue.
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/embed/retry-failures",
		map[string]any{"node_ids": []string{string(tenantTarget)}}, "Bearer "+reader)
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.Equal(t, []storage.NodeID{tenantTarget}, embeddingFailureNodeIDs(t, server), "a refused retry requeues nothing")

	// An unnamed retry still targets the whole parked population.
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/embed/retry-failures", nil, "Bearer "+reader)
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.Equal(t, []storage.NodeID{tenantTarget}, embeddingFailureNodeIDs(t, server))

	// The installation administrator holds the population-wide grant, so the
	// same named entity is admitted for it: the denial above is scope, not a
	// broken path.
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/embed/retry-failures",
		map[string]any{"node_ids": []string{string(tenantTarget)}}, "Bearer "+admin)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"retried":1`)
	require.Empty(t, embeddingFailureNodeIDs(t, server))
}

// TestEmbeddingJobAuthorityRevalidatesRecordedCaller proves the caller-authority
// binding that manual retry records: the admitted operation revalidates against
// its own database scope and against the committed policy, and a policy change
// while the work is queued refuses the batch. Without a fine-grained principal
// there is no caller authority to record and the existing contract is kept.
func TestEmbeddingJobAuthorityRevalidatesRecordedCaller(t *testing.T) {
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	admin := getAuthToken(t, users, "admin")
	defaultDatabase := server.dbManager.DefaultDatabaseName()
	response := makeRequest(t, server, http.MethodPost, "/db/system/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE DATABASE tenant_b"}},
	}, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	definition := grantFineGrainedEmbeddingRetry(t, server, admin, string(auth.RoleViewer), defaultDatabase)

	claims, err := users.ValidateToken(getAuthToken(t, users, "reader"))
	require.NoError(t, err)
	ctx := context.WithValue(t.Context(), contextKeyClaims, claims)
	ctx = auth.WithRequestIdentity(ctx, auth.RequestIdentity{ID: auth.PrincipalID(claims), Username: claims.Username})
	principal, err := server.fineGrainedRuntime.Admit(ctx, claims.Roles)
	require.NoError(t, err)
	ctx = auth.WithFineGrainedRequest(ctx, principal)
	request := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/retry-failures", nil).WithContext(ctx)

	authority := server.embeddingJobAuthority(request, "embedding.failures.retry")
	require.NotNil(t, authority, "a fine-grained caller retry must record its authority")
	home := &storage.Node{ID: storage.NodeID(defaultDatabase + ":doc")}
	require.NoError(t, authority.RevalidateEmbeddingJob(ctx, []*storage.Node{home}))

	// The recorded operation's scope does not cover another database.
	require.ErrorIs(t,
		authority.RevalidateEmbeddingJob(ctx, []*storage.Node{{ID: "tenant_b:doc"}}),
		nornicdb.ErrEmbeddingSubmissionRefused)

	// A policy change while the caller's work is queued refuses the batch.
	revokeFineGrainedEmbeddingRetry(t, server, admin, string(auth.RoleViewer), definition)
	require.ErrorIs(t, authority.RevalidateEmbeddingJob(ctx, []*storage.Node{home}), nornicdb.ErrEmbeddingSubmissionRefused)

	// Without a fine-grained principal there is no caller authority to record, so
	// the legacy and managed (service) contracts are unchanged.
	require.Nil(t, server.embeddingJobAuthority(
		httptest.NewRequest(http.MethodPost, "/nornicdb/embed/retry-failures", nil), "embedding.failures.retry"))
}

func TestFineGrainedEmbeddingRegenerationUsesCallerSourceAndTarget(t *testing.T) {
	server, users := setupTestServer(t)
	provider := &embeddingSubmissionRecorder{}
	require.True(t, server.db.SetEmbedder(provider))
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	database := server.dbManager.DefaultDatabaseName()
	base := server.db.GetBaseStorageForManager().(*storage.WALEngine).GetInnerEngine().(*storage.BadgerEngine)
	base.SetEmbeddingsEnabled(true)
	server.db.SetDatabaseEmbeddingsEnabled(database, true)
	identifier, _ := server.dbManager.DatabaseIdentity(database)
	require.NotEmpty(t, identifier)
	engine := storage.NewNamespacedEngine(server.db.GetBaseStorageForManager(), database)
	for id, props := range map[string]map[string]any{
		"visible-doc": {"content": "allowed synthetic text"},
		"private-doc": {"internal_note": "restricted synthetic text"},
	} {
		_, err := engine.CreateNode(t.Context(), &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: props,
			ChunkEmbeddings: [][]float32{{0, 1}}, EmbedMeta: map[string]any{"has_embedding": true},
		})
		require.NoError(t, err)
	}
	editFineGrainedCatalog(t, server, admin, func(catalog *auth.FineGrainedCatalog) {
		for _, grant := range []auth.FineGrainedEntitlement{
			{ID: "regenerate-documents", Action: "embedding.regenerate",
				Scope: auth.FineGrainedScope{Kind: auth.FineGrainedDatabase, DatabaseID: identifier}},
			{ID: "traverse-documents", Action: "graph.node.traverse",
				Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, DatabaseID: identifier, Types: []string{"Doc"}}},
			{ID: "read-content", Action: "graph.node.property.read",
				Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, DatabaseID: identifier, Types: []string{"Doc"}, Properties: []string{"content"}}},
			{ID: "replace-document-vectors", Action: "graph.node.embedding.set",
				Scope: auth.FineGrainedScope{Kind: auth.FineGrainedNode, DatabaseID: identifier, Types: []string{"Doc"}}},
		} {
			catalog.Definitions = append(catalog.Definitions, grant)
			catalog.Assignments["viewer"] = append(catalog.Assignments["viewer"], grant.ID)
		}
	})

	response := makeRequest(t, server, http.MethodPost, "/nornicdb/embed/trigger?regenerate=true&database="+database, nil, "Bearer "+reader)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		node, err := engine.GetNode(t.Context(), "visible-doc")
		if assert.NoError(c, err) {
			assert.Equal(c, [][]float32{{1, 0}}, node.ChunkEmbeddings)
		}
	}, 8*time.Second, 20*time.Millisecond)
	private, err := engine.GetNode(t.Context(), "private-doc")
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0, 1}}, private.ChunkEmbeddings)
	provider.mu.Lock()
	defer provider.mu.Unlock()
	require.Len(t, provider.calls, 1)
	require.Contains(t, provider.calls[0], "allowed synthetic text")
}
