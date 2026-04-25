package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (s *Server) registerKnowledgePolicyRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/knowledge-policies/profiles", s.withAuth(s.handleKPProfiles, auth.PermAdmin))
	mux.HandleFunc("/admin/knowledge-policies/policies", s.withAuth(s.handleKPPolicies, auth.PermAdmin))
	mux.HandleFunc("/admin/knowledge-policies/resolve", s.withAuth(s.handleKPResolve, auth.PermAdmin))
	mux.HandleFunc("/admin/knowledge-policies/deindex/status", s.withAuth(s.handleKPDeindexStatus, auth.PermAdmin))
}

func (s *Server) kpDatabaseName(r *http.Request) string {
	if db := r.URL.Query().Get("database"); db != "" {
		return db
	}
	if s.dbManager != nil {
		return s.dbManager.DefaultDatabaseName()
	}
	return "nornic"
}

func (s *Server) kpSchemaOr503(w http.ResponseWriter, r *http.Request) *storage.SchemaManager {
	dbName := s.kpDatabaseName(r)
	if s.dbManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "database manager unavailable", ErrServiceUnavailable)
		return nil
	}
	eng, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "database not found: "+dbName, err)
		return nil
	}
	sm := eng.GetSchema()
	if sm == nil {
		s.writeError(w, http.StatusServiceUnavailable, "schema manager unavailable", ErrServiceUnavailable)
		return nil
	}
	return sm
}

func (s *Server) handleKPProfiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}

	sm := s.kpSchemaOr503(w, r)
	if sm == nil {
		return
	}

	bundles, bindings := sm.ShowDecayProfiles()
	decayInfo := s.db.GetDecayInfo()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"bundles":       bundles,
		"bindings":      bindings,
		"decay_enabled": decayInfo != nil && decayInfo.Enabled,
	})
}

func (s *Server) handleKPPolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}

	sm := s.kpSchemaOr503(w, r)
	if sm == nil {
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"promotion_profiles": sm.ShowPromotionProfiles(),
		"promotion_policies": sm.ShowPromotionPolicies(),
	})
}

func (s *Server) handleKPResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}

	sm := s.kpSchemaOr503(w, r)
	if sm == nil {
		return
	}

	q := r.URL.Query()
	entityID := q.Get("entityId")
	labelsParam := q.Get("labels")
	edgeType := q.Get("edgeType")

	if entityID == "" && labelsParam == "" && edgeType == "" {
		s.writeError(w, http.StatusBadRequest, "entityId, labels, or edgeType required", ErrBadRequest)
		return
	}

	bt := sm.GetBindingTable()
	decayInfo := s.db.GetDecayInfo()
	decayEnabled := decayInfo != nil && decayInfo.Enabled

	resolver := knowledgepolicy.NewResolver(bt, nil)
	scorer := knowledgepolicy.NewScorer(resolver, decayEnabled)

	now := time.Now().UnixNano()

	var resolution knowledgepolicy.ScoringResolution

	if entityID != "" {
		dbName := s.kpDatabaseName(r)
		eng, err := s.dbManager.GetStorage(dbName)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "database not found", err)
			return
		}

		node, err := eng.GetNode(storage.NodeID(entityID))
		if err != nil || node == nil {
			edge, eerr := eng.GetEdge(storage.EdgeID(entityID))
			if eerr != nil || edge == nil {
				s.writeError(w, http.StatusNotFound, "entity not found: "+entityID, ErrNotFound)
				return
			}
			resolution = scorer.ScoreEdge(entityID, edge.Type, nil, edge.CreatedAt.UnixNano(), edge.CreatedAt.UnixNano(), now)
		} else {
			createdNanos := node.CreatedAt.UnixNano()
			versionNanos := createdNanos
			if !node.UpdatedAt.IsZero() {
				versionNanos = node.UpdatedAt.UnixNano()
			}
			resolution = scorer.ScoreNode(entityID, node.Labels, nil, createdNanos, versionNanos, now)
		}
	} else if edgeType != "" {
		resolution = scorer.ScoreEdge("dry-run", edgeType, nil, now, now, now)
	} else {
		labels := strings.Split(labelsParam, ",")
		for i := range labels {
			labels[i] = strings.TrimSpace(labels[i])
		}
		resolution = scorer.ScoreNode("dry-run", labels, nil, now, now, now)
	}

	s.writeJSON(w, http.StatusOK, resolution)
}

func (s *Server) handleKPDeindexStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}

	dbName := s.kpDatabaseName(r)
	if s.dbManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "database manager unavailable", ErrServiceUnavailable)
		return
	}

	eng, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "database not found: "+dbName, err)
		return
	}

	be := unwrapToBadgerEngine(eng)
	if be == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"pending_count": 0,
			"items":         []interface{}{},
			"supported":     false,
			"message":       "deindex status requires BadgerDB storage backend",
		})
		return
	}

	items, err := be.ScanPendingDeindexWorkItems()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to scan deindex items", err)
		return
	}
	if items == nil {
		items = []*storage.DeindexWorkItem{}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"pending_count": len(items),
		"items":         items,
		"supported":     true,
	})
}

func unwrapToBadgerEngine(eng storage.Engine) *storage.BadgerEngine {
	for {
		switch e := eng.(type) {
		case *storage.BadgerEngine:
			return e
		case interface{ GetInnerEngine() storage.Engine }:
			eng = e.GetInnerEngine()
		case interface{ UnwrapEngine() storage.Engine }:
			eng = e.UnwrapEngine()
		default:
			return nil
		}
	}
}
