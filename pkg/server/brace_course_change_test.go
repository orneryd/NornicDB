package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/testing/cypher/querytextmatrix"
)

func TestBraceCourseChangeHTTP(t *testing.T) {
	service, authenticator := setupTestServer(t)
	local := httptest.NewServer(service.buildRouter())
	defer local.Close()
	executor := httpDifferentialExecutor{client: &http.Client{Timeout: 30 * time.Second}, endpoint: local.URL + "/db/nornic/tx/commit", authorization: "Bearer " + getAuthToken(t, authenticator, "admin")}
	querytextmatrix.BraceCourseChangeCases(func(item querytextmatrix.MapBraceCase) {
		t.Run(item.Name, func(t *testing.T) {
			actual := executor.Execute(t.Context(), item.Query)
			if item.SyntaxError {
				if actual.Code != "Neo.ClientError.Statement.SyntaxError" {
					t.Fatalf("wanted SyntaxError, got %+v", actual)
				}
			} else if actual.Failed() {
				t.Fatalf("valid projection: %+v", actual)
			}
		})
	})
}
