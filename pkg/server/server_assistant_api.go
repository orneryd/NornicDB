package server

import (
	"errors"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"net/http"
	"strings"
)

func writeConversationError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, heimdall.ErrChatAuthenticationRequired) {
		status = http.StatusUnauthorized
	}
	if errors.Is(err, heimdall.ErrChatWorkspaceMissing) || errors.Is(err, multidb.ErrDatabaseExists) {
		status = http.StatusConflict
	}
	httpresponse.WritePropertyJSON(w, status, map[string]string{"message": observability.RedactError(err).Error()})
}
func (s *Server) handleAssistantWorkspace(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	r = s.withBifrostRBAC(r)
	var name *string
	switch r.Method {
	case http.MethodGet:
	case http.MethodPost:
		var request struct {
			Database string `json:"database"`
		}
		if err := valueconvert.CypherInputJSON.NewDecoder(r.Body).Decode(&request); err != nil {
			httpresponse.WritePropertyJSON(w, http.StatusBadRequest, map[string]string{"message": "Invalid workspace request"})
			return
		}
		name = &request.Database
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	router := newHeimdallDBRouter(s.db, s.dbManager, nil)
	workspace, err := router.ChatWorkspace(r.Context(), name)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, workspace)
}
func (s *Server) handleAssistantConversations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	r = s.withBifrostRBAC(r)
	suffix := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/bifrost/conversations"), "/")
	if strings.Contains(suffix, "/attachments") {
		s.handleAssistantAttachment(w, r, suffix)
		return
	}
	if strings.HasSuffix(suffix, "/compact") {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		result, err := heimdall.CallConversationOperation(r.Context(), heimdall.ConversationRequest{Operation: "compact", ID: strings.TrimSuffix(suffix, "/compact")})
		if err != nil {
			writeConversationError(w, err)
			return
		}
		httpresponse.WritePropertyJSON(w, http.StatusAccepted, result)
		return
	}
	if strings.HasSuffix(suffix, "/context") {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		result, err := heimdall.CallConversationOperation(r.Context(), heimdall.ConversationRequest{Operation: "context", ID: strings.TrimSuffix(suffix, "/context")})
		if err != nil {
			writeConversationError(w, err)
			return
		}
		httpresponse.WritePropertyJSON(w, http.StatusOK, result)
		return
	}
	archived := r.URL.Query().Get("archived") == "true"
	request := heimdall.ConversationRequest{ID: suffix, Query: r.URL.Query().Get("q"), Archived: &archived}
	switch r.Method {
	case http.MethodGet:
		request.Operation = "list"
		if suffix == "search" {
			request.Operation = "search"
			request.ID = ""
		} else if suffix != "" {
			request.Operation = "get"
		}
	case http.MethodPost:
		if suffix != "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := valueconvert.CypherInputJSON.NewDecoder(r.Body).Decode(&request); err != nil {
			writeConversationError(w, err)
			return
		}
		request.Operation = "create"
	case http.MethodPatch:
		if suffix == "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := valueconvert.CypherInputJSON.NewDecoder(r.Body).Decode(&request); err != nil {
			writeConversationError(w, err)
			return
		}
		request.Operation = "update"
		request.ID = suffix
	case http.MethodDelete:
		request.Operation = "delete"
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	result, err := heimdall.CallConversationOperation(r.Context(), request)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, result)
}
