package server

import (
	"fmt"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// =============================================================================
// GDPR Compliance Handlers
// =============================================================================

func (s *Server) handleGDPRExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		UserID string `json:"user_id"`
		Format string `json:"format"` // "json" or "csv"
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// User can only export own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(claims.Roles, auth.PermAdmin) {
		s.writeError(w, http.StatusForbidden, "can only export own data", ErrForbidden)
		return
	}

	data, err := s.db.ExportUserData(r.Context(), req.UserID, req.Format)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.logAudit(r, req.UserID, "gdpr_export", true, fmt.Sprintf("format: %s", req.Format))

	if req.Format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=user_data.csv")
		w.Write(data)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=user_data.json")
		w.Write(data)
	}
}

func (s *Server) handleGDPRDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	var req struct {
		UserID    string `json:"user_id"`
		Anonymize bool   `json:"anonymize"` // Anonymize instead of hard delete
		Confirm   bool   `json:"confirm"`   // Confirmation required
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if !req.Confirm {
		s.writeError(w, http.StatusBadRequest, "confirmation required", ErrBadRequest)
		return
	}

	// User can only delete own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(claims.Roles, auth.PermAdmin) {
		s.writeError(w, http.StatusForbidden, "can only delete own data", ErrForbidden)
		return
	}

	var err error
	if req.Anonymize {
		err = s.db.AnonymizeUserData(r.Context(), req.UserID)
	} else {
		err = s.db.DeleteUserData(r.Context(), req.UserID)
	}

	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	action := "deleted"
	if req.Anonymize {
		action = "anonymized"
	}

	s.logAudit(r, req.UserID, "gdpr_delete", true, fmt.Sprintf("action: %s", action))

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status":  action,
		"user_id": req.UserID,
	})
}
