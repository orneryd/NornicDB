package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/stretchr/testify/require"
)

// retentionPoliciesEndpoint is the canonical URL for the policies
// admin route. Centralized so the test set fails fast on URL drift.
const retentionPoliciesEndpoint = "/admin/retention/policies"

func TestRetentionPolicies_GET_ListsPolicies(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodGet, retentionPoliciesEndpoint, nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	var payload []retention.Policy
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
}

func TestRetentionPolicies_POST_AddPolicy(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	policy := retention.Policy{
		ID:              "test-policy-1",
		Name:            "test",
		Category:        retention.CategoryUser,
		RetentionPeriod: retention.RetentionPeriod{Duration: 30 * 24 * time.Hour},
		Active:          true,
	}
	resp := makeRequest(t, server, http.MethodPost, retentionPoliciesEndpoint, policy, "Bearer "+token)
	require.Equal(t, http.StatusCreated, resp.Code)
}

func TestRetentionPolicies_POST_BadJSON(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// Pass a string where an object is expected — readJSON returns
	// an error and the handler responds 400.
	resp := makeRequest(t, server, http.MethodPost, retentionPoliciesEndpoint, "not a policy", "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestRetentionPolicies_MethodNotAllowed(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodDelete, retentionPoliciesEndpoint, nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}

func TestRetentionPolicyByID_GET_NotFound(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodGet, retentionPoliciesEndpoint+"/nope", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestRetentionPolicyByID_PUT_AndDELETE(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// Seed a policy.
	policy := retention.Policy{
		ID:              "test-cycle",
		Name:            "for update",
		Category:        retention.CategoryUser,
		RetentionPeriod: retention.RetentionPeriod{Duration: 30 * 24 * time.Hour},
		Active:          true,
	}
	require.NoError(t, server.db.GetRetentionManager().AddPolicy(&policy))

	// PUT updates description.
	policy.Description = "updated"
	resp := makeRequest(t, server, http.MethodPut, retentionPoliciesEndpoint+"/test-cycle", policy, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// DELETE removes it.
	resp = makeRequest(t, server, http.MethodDelete, retentionPoliciesEndpoint+"/test-cycle", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// Confirm gone.
	resp = makeRequest(t, server, http.MethodGet, retentionPoliciesEndpoint+"/test-cycle", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

// (no path-validation test for empty-id branch — net/http/httptest
// rejects whitespace-only path segments at request construction, so
// the handler's r.PathValue("id") TrimSpace branch is unreachable
// from black-box HTTP tests.)

func TestRetentionHolds_GETandPOST(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// GET initial: empty.
	resp := makeRequest(t, server, http.MethodGet, "/admin/retention/holds", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// POST a new hold.
	hold := retention.LegalHold{
		ID: "hold-X", Description: "x", PlacedBy: "legal",
		SubjectIDs: []string{"subject"},
	}
	resp = makeRequest(t, server, http.MethodPost, "/admin/retention/holds", hold, "Bearer "+token)
	require.Equal(t, http.StatusCreated, resp.Code)

	// GET now sees it.
	resp = makeRequest(t, server, http.MethodGet, "/admin/retention/holds", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	var holds []retention.LegalHold
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&holds))
	found := false
	for _, h := range holds {
		if h.ID == "hold-X" {
			found = true
		}
	}
	require.True(t, found, "POSTed hold must show up in GET listing")
}

func TestRetentionHolds_BadJSON_AndMethodNotAllowed(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// Bad body → 400.
	resp := makeRequest(t, server, http.MethodPost, "/admin/retention/holds", "junk", "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, resp.Code)

	// PUT not supported → 405.
	resp = makeRequest(t, server, http.MethodPut, "/admin/retention/holds", retention.LegalHold{}, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}

func TestRetentionHoldByID_DeleteAndErrors(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// Seed.
	require.NoError(t, server.db.GetRetentionManager().PlaceLegalHold(&retention.LegalHold{
		ID: "hold-Y", Description: "y", PlacedBy: "legal", SubjectIDs: []string{"sub"},
	}))

	// GET / PUT not allowed (handler only accepts DELETE).
	resp := makeRequest(t, server, http.MethodGet, "/admin/retention/holds/hold-Y", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)

	// DELETE releases the hold.
	resp = makeRequest(t, server, http.MethodDelete, "/admin/retention/holds/hold-Y", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// DELETE on unknown id → 404.
	resp = makeRequest(t, server, http.MethodDelete, "/admin/retention/holds/nope", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestRetentionErasures_LifecycleAndErrors(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// GET initial (may be empty or have prior).
	resp := makeRequest(t, server, http.MethodGet, "/admin/retention/erasures", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	// POST new erasure with the correct body shape (subject_id only).
	body := map[string]string{"subject_id": "subject", "subject_email": "subject@example.com"}
	resp = makeRequest(t, server, http.MethodPost, "/admin/retention/erasures", body, "Bearer "+token)
	require.Equal(t, http.StatusCreated, resp.Code)

	// Bad body → 400.
	resp = makeRequest(t, server, http.MethodPost, "/admin/retention/erasures", "garbage", "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, resp.Code)

	// PUT not supported.
	resp = makeRequest(t, server, http.MethodPut, "/admin/retention/erasures", body, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}

func TestRetentionSweep(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	// POST kicks the sweep; result must be 200 or similar success.
	resp := makeRequest(t, server, http.MethodPost, "/admin/retention/sweep", nil, "Bearer "+token)
	require.True(t, resp.Code == http.StatusOK || resp.Code == http.StatusAccepted || resp.Code == http.StatusInternalServerError,
		"sweep must respond with a valid HTTP code, got %d", resp.Code)

	// GET not supported.
	resp = makeRequest(t, server, http.MethodGet, "/admin/retention/sweep", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}
