package server

import (
	"net/http"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

// =============================================================================
// GPU Control Handlers
// =============================================================================

func (s *Server) handleGPUStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeGetRequired(w, r)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"available": false,
			"enabled":   false,
			"message":   "GPU manager not initialized",
		})
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeInvalidGPUManagerType(w, r)
		return
	}

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

	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleGPUEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeGPUManagerNotInitialized(w, r)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeInvalidGPUManagerType(w, r)
		return
	}

	if err := gpuManager.Enable(); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "enabled",
		"message": "GPU acceleration enabled",
	})
}

func (s *Server) handleGPUDisable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeGPUManagerNotInitialized(w, r)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeInvalidGPUManagerType(w, r)
		return
	}

	gpuManager.Disable()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "disabled",
		"message": "GPU acceleration disabled (CPU fallback active)",
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

	gpuManagerIface := s.db.GetGPUManager()
	if gpuManagerIface == nil {
		s.writeGPUManagerNotInitialized(w, r)
		return
	}

	gpuManager, ok := gpuManagerIface.(*gpu.Manager)
	if !ok {
		s.writeInvalidGPUManagerType(w, r)
		return
	}

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
			s.writeError(w, http.StatusInternalServerError, "GPU unavailable: "+err.Error(), ErrInternalError)
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
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}
	elapsedMs := time.Since(startTime).Milliseconds()

	// Get stats
	stats := gpuManager.Stats()
	usedMode := "cpu"
	if gpuManager.IsEnabled() {
		usedMode = "gpu"
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"results": results,
		"performance": map[string]interface{}{
			"elapsed_ms":     elapsedMs,
			"mode":           usedMode,
			"operations_gpu": stats.OperationsGPU,
			"operations_cpu": stats.OperationsCPU,
		},
	})
}
