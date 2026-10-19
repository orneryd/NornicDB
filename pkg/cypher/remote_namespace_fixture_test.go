package cypher

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

// Remote transaction fixtures expose the same catalog identity handshake as
// the real server. Their existing counters continue to count business queries.
func respondRemoteNamespaceFixture(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost || r.Body == nil {
		return false
	}
	raw, err := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if err != nil {
		return false
	}
	var request struct {
		Statements []struct {
			Statement string `json:"statement"`
		} `json:"statements"`
	}
	if json.Unmarshal(raw, &request) != nil || len(request.Statements) != 1 || request.Statements[0].Statement != "CALL db.info()" {
		return false
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"id"}, "data": []any{map[string]any{"row": []any{"fixture-database-lifetime"}}}}}, "errors": []any{}})
	return true
}
