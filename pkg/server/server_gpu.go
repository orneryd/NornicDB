package server

import (
	"errors"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// gpuManagerLookup distinguishes an absent interface from a typed nil manager.
type gpuManagerLookup struct {
	manager *gpu.Manager
	missing bool
}

// resolveGPUManager writes the existing invalid-type response on failure.
func (s *Server) resolveGPUManager(w http.ResponseWriter, r *http.Request) (gpuManagerLookup, bool) {
	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		return gpuManagerLookup{missing: true}, true
	}
	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeInvalidGPUManagerType(w, r)
		return gpuManagerLookup{}, false
	}
	return gpuManagerLookup{manager: gpuManager}, true
}

// =============================================================================
// GPU Control Handlers
// =============================================================================

func (s *Server) handleGPUStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeGetRequired(w, r)
		return
	}

	lookup, valid := s.resolveGPUManager(w, r)
	if !valid {
		return
	}
	if lookup.missing {
		httpresponse.WriteJSON(w, http.StatusOK, map[string]interface{}{
			"available": false,
			"enabled":   false,
			"message":   s.localizedText(w, r, localization.GPUManagerNotInitialized()),
		})
		return
	}

	gpuManager := lookup.manager
	enabled := gpuManager.IsEnabled()
	device := gpuManager.Device()
	stats := gpuManager.Stats()

	response := map[string]interface{}{
		"available":      device != nil,
		"enabled":        enabled,
		"operations_gpu": stats.OperationsGPU,
		"operations_cpu": stats.OperationsCPU,
		"fallback_count": stats.FallbackCount,
		"allocated_mb":   gpuManager.AllocatedMemoryMB(),
	}

	if device != nil {
		response["device"] = map[string]interface{}{
			"id":            device.ID,
			"name":          device.Name,
			"vendor":        device.Vendor,
			"backend":       device.Backend,
			"memory_mb":     device.MemoryMB,
			"compute_units": device.ComputeUnits,
		}
	}

	httpresponse.WriteJSON(w, http.StatusOK, response)
}

type gpuControlAction uint8

const (
	gpuControlEnable gpuControlAction = iota
	gpuControlDisable
)

// handleGPUControl owns admission and mutation for the two GPU control routes.
// Router authorization remains outside this handler, as for the original routes.
func (s *Server) handleGPUControl(w http.ResponseWriter, r *http.Request, action gpuControlAction) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	lookup, valid := s.resolveGPUManager(w, r)
	if !valid {
		return
	}
	if lookup.missing {
		s.writeGPUManagerNotInitialized(w, r)
		return
	}
	gpuManager := lookup.manager
	var status string
	var message localization.Message
	switch action {
	case gpuControlEnable:
		if err := gpuManager.Enable(); err != nil {
			s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
			return
		}
		status, message = "enabled", localization.GPUAccelerationEnabled()
	case gpuControlDisable:
		gpuManager.Disable()
		status, message = "disabled", localization.GPUAccelerationDisabled()
	default:
		s.writeBoundaryError(w, r, http.StatusInternalServerError, errors.New("invalid GPU control action"), ErrInternalError)
		return
	}
	httpresponse.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"status":  status,
		"message": s.localizedText(w, r, message),
	})
}

func (s *Server) handleGPUTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
		Limit  int    `json:"limit,omitempty"`
		Mode   string `json:"mode,omitempty"` // "auto", "cpu", "gpu"
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeInvalidRequestBody(w, r)
		return
	}

	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.Mode == "" {
		req.Mode = "auto"
	}

	lookup, valid := s.resolveGPUManager(w, r)
	if !valid {
		return
	}
	if lookup.missing {
		s.writeGPUManagerNotInitialized(w, r)
		return
	}

	gpuManager := lookup.manager

	// Store original state
	originallyEnabled := gpuManager.IsEnabled()

	// Configure mode for this test
	switch req.Mode {
	case "cpu":
		gpuManager.Disable()
		defer func() {
			if originallyEnabled {
				gpuManager.Enable()
			}
		}()
	case "gpu":
		if err := gpuManager.Enable(); err != nil {
			s.writeLocalizedError(w, r, http.StatusInternalServerError, localization.GPUUnavailable(err), ErrInternalError)
			return
		}
		defer func() {
			if !originallyEnabled {
				gpuManager.Disable()
			}
		}()
	case "auto":
		// Use current state
	}

	// Measure search performance
	startTime := time.Now()
	results, err := s.db.FindSimilar(r.Context(), req.NodeID, req.Limit)
	if err != nil {
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
		return
	}
	elapsedMs := time.Since(startTime).Milliseconds()

	// Get stats
	stats := gpuManager.Stats()
	usedMode := "cpu"
	if gpuManager.IsEnabled() {
		usedMode = "gpu"
	}

	httpresponse.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
		"performance": map[string]interface{}{
			"elapsed_ms":     elapsedMs,
			"mode":           usedMode,
			"operations_gpu": stats.OperationsGPU,
			"operations_cpu": stats.OperationsCPU,
		},
	})
}
