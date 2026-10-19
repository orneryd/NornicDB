package server

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/retention"
)

func (s *Server) registerRetentionRoutes(mux *http.ServeMux) {
	s.registerRouteTable(mux, []routeSpec{
		{"/admin/retention/policies", auth.PermAdmin, s.handleRetentionPolicies},
		{"/admin/retention/policies/{id}", auth.PermAdmin, s.handleRetentionPolicyByID},
		{"/admin/retention/policies/defaults", auth.PermAdmin, s.handleRetentionPolicyDefaults},
		{"/admin/retention/holds", auth.PermAdmin, s.handleRetentionHolds},
		{"/admin/retention/holds/{id}", auth.PermAdmin, s.handleRetentionHoldByID},
		{"/admin/retention/erasures", auth.PermAdmin, s.handleRetentionErasures},
		{"/admin/retention/erasures/{id}/process", auth.PermAdmin, s.handleRetentionProcessErasure},
		{"/admin/retention/sweep", auth.PermAdmin, s.handleRetentionSweep},
		{"/admin/retention/status", auth.PermAdmin, s.handleRetentionStatus},
	})
}

func (s *Server) retentionManagerOr503(w http.ResponseWriter, r *http.Request) *retention.Manager {
	permissions := []auth.Permission{auth.PermRead, auth.PermAdmin}
	if r.Method != http.MethodGet {
		permissions = append(permissions, auth.PermWrite)
	}
	// Retention records and sweeps use DB.storage, the default namespace.
	if !s.authorizeDatabaseOperation(w, r, s.dbManager.DefaultDatabaseName(), permissions...) {
		return nil
	}
	rm := s.db.GetRetentionManager()
	if rm == nil {
		s.writeRetentionManagerDisabled(w, r)
		return nil
	}
	return rm
}

func (s *Server) handleRetentionPolicies(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, rm.ListPolicies())
	case http.MethodPost:
		var policy retention.Policy
		if err := s.readJSON(r, &policy); err != nil {
			s.writeInvalidRequestBody(w, r)
			return
		}
		if err := rm.AddPolicy(&policy); err != nil {
			s.writeBoundaryError(w, r, http.StatusBadRequest, err, err)
			return
		}
		s.writeJSON(w, http.StatusCreated, policy)
	default:
		s.writeGetOrPostRequired(w, r)
	}
}

func (s *Server) handleRetentionPolicyByID(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeRetentionPolicyIDRequired(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		policy, err := rm.GetPolicy(id)
		if err != nil {
			s.writeBoundaryError(w, r, http.StatusNotFound, err, err)
			return
		}
		s.writeJSON(w, http.StatusOK, policy)
	case http.MethodPut:
		var policy retention.Policy
		if err := s.readJSON(r, &policy); err != nil {
			s.writeInvalidRequestBody(w, r)
			return
		}
		policy.ID = id
		if err := rm.UpdatePolicy(&policy); err != nil {
			s.writeBoundaryError(w, r, http.StatusBadRequest, err, err)
			return
		}
		s.writeJSON(w, http.StatusOK, policy)
	case http.MethodDelete:
		if err := rm.DeletePolicy(id); err != nil {
			s.writeBoundaryError(w, r, http.StatusNotFound, err, err)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionPolicyDeleted()), "id": id})
	default:
		s.writeGetPutOrDeleteRequired(w, r)
	}
}

func (s *Server) handleRetentionPolicyDefaults(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	loaded := 0
	skipped := 0
	var loadErrs []string
	for _, policy := range retention.DefaultPolicies() {
		if err := rm.AddPolicy(policy); err == nil {
			loaded++
		} else if errors.Is(err, retention.ErrAlreadyExists) {
			skipped++
		} else {
			loadErrs = append(loadErrs, policy.ID+": "+err.Error())
			s.logEvent(r.Context(), slog.LevelWarn, localization.ServerRetentionDefaultPolicyAddFailedEvent(policy.ID, err))
		}
	}
	status := http.StatusOK
	if len(loadErrs) > 0 {
		status = http.StatusInternalServerError
	}
	s.writeJSON(w, status, map[string]any{
		"loaded":  loaded,
		"skipped": skipped,
		"errors":  loadErrs,
		"total":   len(rm.ListPolicies()),
	})
}

func (s *Server) handleRetentionHolds(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, rm.ListLegalHolds())
	case http.MethodPost:
		var hold retention.LegalHold
		if err := s.readJSON(r, &hold); err != nil {
			s.writeInvalidRequestBody(w, r)
			return
		}
		if err := rm.PlaceLegalHold(&hold); err != nil {
			s.writeBoundaryError(w, r, http.StatusBadRequest, err, err)
			return
		}
		s.writeJSON(w, http.StatusCreated, hold)
	default:
		s.writeGetOrPostRequired(w, r)
	}
}

func (s *Server) handleRetentionHoldByID(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeRetentionHoldIDRequired(w, r)
		return
	}
	if r.Method != http.MethodDelete {
		s.writeDeleteRequired(w, r)
		return
	}
	if err := rm.ReleaseLegalHold(id); err != nil {
		s.writeBoundaryError(w, r, http.StatusNotFound, err, err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionHoldReleased()), "id": id})
}

func (s *Server) handleRetentionErasures(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, rm.ListErasureRequests())
	case http.MethodPost:
		var req struct {
			SubjectID    string `json:"subject_id"`
			SubjectEmail string `json:"subject_email"`
		}
		if err := s.readJSON(r, &req); err != nil {
			s.writeInvalidRequestBody(w, r)
			return
		}
		erasureReq, err := rm.CreateErasureRequest(req.SubjectID, req.SubjectEmail)
		if err != nil {
			s.writeBoundaryError(w, r, http.StatusBadRequest, err, err)
			return
		}
		s.writeJSON(w, http.StatusCreated, erasureReq)
	default:
		s.writeGetOrPostRequired(w, r)
	}
}

func (s *Server) handleRetentionProcessErasure(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeRetentionErasureIDRequired(w, r)
		return
	}
	request, err := rm.GetErasureRequest(id)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusNotFound, err, err)
		return
	}
	records, err := s.db.CollectSubjectRetentionRecords(r.Context(), request.SubjectID)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, err)
		return
	}
	if err := rm.ProcessErasure(r.Context(), id, records); err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, err)
		return
	}
	updated, err := rm.GetErasureRequest(id)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, err)
		return
	}
	s.writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleRetentionSweep(w http.ResponseWriter, r *http.Request) {
	if s.retentionManagerOr503(w, r) == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	s.db.RunRetentionSweep(r.Context())
	s.writeJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionSweepTriggered())})
}

func (s *Server) handleRetentionStatus(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	if r.Method != http.MethodGet {
		s.writeGetRequired(w, r)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"enabled":       true,
		"policy_count":  len(rm.ListPolicies()),
		"hold_count":    len(rm.ListLegalHolds()),
		"erasure_count": len(rm.ListErasureRequests()),
		"timestamp":     time.Now().UTC(),
	})
}
