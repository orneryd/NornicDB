package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/retention"
)

// =============================================================================
// GDPR Compliance Handlers
// =============================================================================

func (s *Server) handleGDPRExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req struct {
		UserID string `json:"user_id"`
		Format string `json:"format"` // "json" or "csv"
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}

	// User can only export own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(s, claims.Roles, auth.PermAdmin) {
		s.writeGDPROwnDataExportOnly(w, r)
		return
	}

	data, err := s.db.ExportUserData(r.Context(), req.UserID, req.Format)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
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
		s.writePostRequired(w, r)
		return
	}

	var req struct {
		UserID    string `json:"user_id"`
		Anonymize bool   `json:"anonymize"` // Anonymize instead of hard delete
		Confirm   bool   `json:"confirm"`   // Confirmation required
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}

	if !req.Confirm {
		s.writeGDPRConfirmationRequired(w, r)
		return
	}

	// User can only delete own data unless admin
	claims := getClaims(r)
	if claims != nil && claims.Sub != req.UserID && !hasPermission(s, claims.Roles, auth.PermAdmin) {
		s.writeGDPROwnDataDeleteOnly(w, r)
		return
	}

	if rm := s.db.GetRetentionManager(); rm != nil {
		if rm.IsUnderLegalHold(req.UserID, "") {
			s.writeGDPRLegalHoldPreventsDeletion(w, r)
			return
		}

		erasureReq, err := rm.CreateErasureRequest(req.UserID, "")
		if err != nil && !errors.Is(err, retention.ErrErasureInProgress) {
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}
		if erasureReq != nil {
			s.logAudit(r, req.UserID, "gdpr_erasure_created", true,
				fmt.Sprintf("request_id: %s, deadline: %s", erasureReq.ID, erasureReq.Deadline.Format(time.RFC3339)))
		}
	}

	var err error
	if req.Anonymize {
		err = s.db.AnonymizeUserData(r.Context(), req.UserID)
	} else {
		err = s.db.DeleteUserData(r.Context(), req.UserID)
	}

	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
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
