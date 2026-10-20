// Package remotenamespace supplies the catalog handshake for stable HTTP peer fixtures.
package remotenamespace

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

// Respond handles the database identity probe for a fixture whose catalog lifetime
// is stable. It restores every unhandled request body for the fixture's normal
// transaction handler. Peers that test recreation supply their own changing IDs.
func Respond(w http.ResponseWriter, r *http.Request) bool {
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"id"}, "data": []any{map[string]any{"row": []any{"fixture-database-lifetime"}}}}}, "errors": []any{}})
	return true
}
