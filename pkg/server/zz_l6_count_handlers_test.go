package server

// Lane L6 (closeout): G-composite-get-constituent, HTTP part of P-N152-69.
// With the native count read failing, the handler's own call
// (s.dbManager.GetStorage(name).NodeCount / EdgeCount) returns the error;
// the response still carries 200 and a zero. Decisive value logged at the
// handler's input and output: server_admin.go:42-43 (`_`), server_db.go:412-419
// (error -> 0).

import (
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
)

func TestL6CountHandlersDropError(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	manager, setFailure := nor287FaultedDatabaseManager(t, server)
	nor287AcceptanceGraph(t, manager)
	ctx := t.Context()

	for _, op := range []storagefault.ReadOperation{"", storagefault.NodeCountRead, storagefault.EdgeCountRead} {
		setFailure(op)
		for _, name := range []string{"nor287_acc_a", "nor287_acc_b"} {
			engine, gErr := server.dbManager.GetStorage(name)
			var n, e int64
			var nErr, eErr error
			if gErr == nil {
				n, nErr = engine.NodeCount(ctx)
				e, eErr = engine.EdgeCount(ctx)
			}
			info := makeRequest(t, server, http.MethodGet, "/db/"+name, nil, token)
			var nodeCount, edgeCount any
			if info.Code == http.StatusOK {
				payload := nor287DecodeJSON(t, info.Body.Bytes())
				nodeCount, edgeCount = payload["nodeCount"], payload["edgeCount"]
			}
			t.Logf("L6_HANDLER failure=%q db=%s handler-input NodeCount=%d err=%v EdgeCount=%d err=%v | GET /db/%s code=%d nodeCount=%v edgeCount=%v",
				op, name, n, nErr, e, eErr, name, info.Code, nodeCount, edgeCount)
			if op != "" && info.Code == http.StatusOK && (nErr != nil && nodeCount == float64(0) || eErr != nil && edgeCount == float64(0)) {
				t.Errorf("CAUSE server_db.go:412-419: the handler's count call returned an error and the answer is HTTP 200 with a zero")
			}
		}
		stats := makeRequest(t, server, http.MethodGet, "/admin/stats", nil, token)
		var db any
		if stats.Code == http.StatusOK {
			db = nor287DecodeJSON(t, stats.Body.Bytes())["database"]
		}
		t.Logf("L6_HANDLER failure=%q GET /admin/stats code=%d database=%v", op, stats.Code, db)
		if op != "" && stats.Code == http.StatusOK {
			t.Errorf("CAUSE server_admin.go:42-43: count errors discarded with _, answer HTTP 200: %v", db)
		}
	}
	setFailure("")
}
