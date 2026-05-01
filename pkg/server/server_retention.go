package server

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/retention"
)

func (s *Server) registerRetentionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/retention/policies", s.withAuth(s.handleRetentionPolicies, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/policies/{id}", s.withAuth(s.handleRetentionPolicyByID, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/policies/defaults", s.withAuth(s.handleRetentionPolicyDefaults, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/holds", s.withAuth(s.handleRetentionHolds, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/holds/{id}", s.withAuth(s.handleRetentionHoldByID, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/erasures", s.withAuth(s.handleRetentionErasures, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/erasures/{id}/process", s.withAuth(s.handleRetentionProcessErasure, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/sweep", s.withAuth(s.handleRetentionSweep, auth.PermAdmin))
	mux.HandleFunc("/admin/retention/status", s.withAuth(s.handleRetentionStatus, auth.PermAdmin))
}

func (s *Server) retentionManagerOr503(w http.ResponseWriter) *retention.Manager {
	rm := s.db.GetRetentionManager()
	if rm == nil {
		s.writeError(w, http.StatusServiceUnavailable, "retention manager is disabled", ErrServiceUnavailable)
		return nil
	}
	return rm
}

func (s *Server) handleRetentionPolicies(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, rm.ListPolicies())
	case http.MethodPost:
		var policy retention.Policy
		if err := s.readJSON(r, &policy); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}
		if err := rm.AddPolicy(&policy); err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusCreated, policy)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleRetentionPolicyByID(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeError(w, http.StatusBadRequest, "policy id required", ErrBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		policy, err := rm.GetPolicy(id)
		if err != nil {
			s.writeError(w, http.StatusNotFound, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusOK, policy)
	case http.MethodPut:
		var policy retention.Policy
		if err := s.readJSON(r, &policy); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}
		policy.ID = id
		if err := rm.UpdatePolicy(&policy); err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusOK, policy)
	case http.MethodDelete:
		if err := rm.DeletePolicy(id); err != nil {
			s.writeError(w, http.StatusNotFound, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET, PUT, or DELETE required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleRetentionPolicyDefaults(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
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
			s.log.Warn("retention defaults: failed to add policy", "policy_id", policy.ID, "error", err)
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
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, rm.ListLegalHolds())
	case http.MethodPost:
		var hold retention.LegalHold
		if err := s.readJSON(r, &hold); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}
		if err := rm.PlaceLegalHold(&hold); err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusCreated, hold)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleRetentionHoldByID(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeError(w, http.StatusBadRequest, "hold id required", ErrBadRequest)
		return
	}
	if r.Method != http.MethodDelete {
		s.writeError(w, http.StatusMethodNotAllowed, "DELETE required", ErrMethodNotAllowed)
		return
	}
	if err := rm.ReleaseLegalHold(id); err != nil {
		s.writeError(w, http.StatusNotFound, err.Error(), err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "released", "id": id})
}

func (s *Server) handleRetentionErasures(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
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
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}
		erasureReq, err := rm.CreateErasureRequest(req.SubjectID, req.SubjectEmail)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), err)
			return
		}
		s.writeJSON(w, http.StatusCreated, erasureReq)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleRetentionProcessErasure(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		s.writeError(w, http.StatusBadRequest, "erasure id required", ErrBadRequest)
		return
	}
	request, err := rm.GetErasureRequest(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error(), err)
		return
	}
	records, err := s.db.CollectSubjectRetentionRecords(r.Context(), request.SubjectID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), err)
		return
	}
	if err := rm.ProcessErasure(r.Context(), id, records); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), err)
		return
	}
	updated, err := rm.GetErasureRequest(id)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), err)
		return
	}
	s.writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleRetentionSweep(w http.ResponseWriter, r *http.Request) {
	if s.retentionManagerOr503(w) == nil {
		return
	}
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}
	s.db.RunRetentionSweep(r.Context())
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "sweep triggered"})
}

func (s *Server) handleRetentionStatus(w http.ResponseWriter, r *http.Request) {
	rm := s.retentionManagerOr503(w)
	if rm == nil {
		return
	}
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
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
