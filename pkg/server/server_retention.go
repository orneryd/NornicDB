package server

import (
	"errors"
	"github.com/orneryd/nornicdb/internal/httpresponse"
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
		{"/admin/retention/policies", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionPoliciesRoute) }},
		{"/admin/retention/policies/{id}", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionPolicyRoute) }},
		{"/admin/retention/policies/defaults", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionDefaultsRoute) }},
		{"/admin/retention/holds", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionHoldsRoute) }},
		{"/admin/retention/holds/{id}", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionHoldRoute) }},
		{"/admin/retention/erasures", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionErasuresRoute) }},
		{"/admin/retention/erasures/{id}/process", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionProcessRoute) }},
		{"/admin/retention/sweep", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionSweepRoute) }},
		{"/admin/retention/status", auth.PermAdmin, func(w http.ResponseWriter, r *http.Request) { s.handleRetention(w, r, retentionStatusRoute) }},
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

type retentionOperation uint8

const (
	retentionPoliciesRoute retentionOperation = iota
	retentionPolicyRoute
	retentionDefaultsRoute
	retentionHoldsRoute
	retentionHoldRoute
	retentionErasuresRoute
	retentionProcessRoute
	retentionSweepRoute
	retentionStatusRoute
)

// handleRetention owns the administrative retention request boundary. The
// original method, ID-validation order and manager transitions remain per route.
func (s *Server) handleRetention(w http.ResponseWriter, r *http.Request, operation retentionOperation) {
	rm := s.retentionManagerOr503(w, r)
	if rm == nil {
		return
	}
	switch operation {
	case retentionPoliciesRoute:
		switch r.Method {
		case http.MethodGet:
			httpresponse.WriteJSON(w, http.StatusOK, rm.ListPolicies())
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
			httpresponse.WriteJSON(w, http.StatusCreated, policy)
		default:
			s.writeGetOrPostRequired(w, r)
		}
	case retentionPolicyRoute:
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
			httpresponse.WriteJSON(w, http.StatusOK, policy)
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
			httpresponse.WriteJSON(w, http.StatusOK, policy)
		case http.MethodDelete:
			if err := rm.DeletePolicy(id); err != nil {
				s.writeBoundaryError(w, r, http.StatusNotFound, err, err)
				return
			}
			httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionPolicyDeleted()), "id": id})
		default:
			s.writeGetPutOrDeleteRequired(w, r)
		}
	case retentionDefaultsRoute:
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
		httpresponse.WriteJSON(w, status, map[string]any{
			"loaded":  loaded,
			"skipped": skipped,
			"errors":  loadErrs,
			"total":   len(rm.ListPolicies()),
		})
	case retentionHoldsRoute:
		switch r.Method {
		case http.MethodGet:
			httpresponse.WriteJSON(w, http.StatusOK, rm.ListLegalHolds())
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
			httpresponse.WriteJSON(w, http.StatusCreated, hold)
		default:
			s.writeGetOrPostRequired(w, r)
		}
	case retentionHoldRoute:
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
		httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionHoldReleased()), "id": id})
	case retentionErasuresRoute:
		switch r.Method {
		case http.MethodGet:
			httpresponse.WriteJSON(w, http.StatusOK, rm.ListErasureRequests())
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
			httpresponse.WriteJSON(w, http.StatusCreated, erasureReq)
		default:
			s.writeGetOrPostRequired(w, r)
		}
	case retentionProcessRoute:
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
		httpresponse.WriteJSON(w, http.StatusOK, updated)
	case retentionSweepRoute:
		if r.Method != http.MethodPost {
			s.writePostRequired(w, r)
			return
		}
		s.db.RunRetentionSweep(r.Context())
		httpresponse.WriteJSON(w, http.StatusOK, map[string]string{"status": s.localizedText(w, r, localization.RetentionSweepTriggered())})
	case retentionStatusRoute:
		if r.Method != http.MethodGet {
			s.writeGetRequired(w, r)
			return
		}
		httpresponse.WriteJSON(w, http.StatusOK, map[string]any{
			"enabled":       true,
			"policy_count":  len(rm.ListPolicies()),
			"hold_count":    len(rm.ListLegalHolds()),
			"erasure_count": len(rm.ListErasureRequests()),
			"timestamp":     time.Now().UTC(),
		})
	default:
		s.writeBoundaryError(w, r, http.StatusInternalServerError, errors.New("invalid retention operation"), ErrInternalError)
	}
}
