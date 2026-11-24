package server

import (
	"errors"
	"net/http"
	"os"

	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (s *Server) handlePublishPreparedDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writePostRequired(w, r)
		return
	}
	if !s.authorizeAllDatabaseOperation(w, r) {
		return
	}
	var req struct {
		Database string `json:"database"`
		ImportID string `json:"import_id"`
	}
	if err := s.readJSON(r, &req); err != nil || req.Database == "" || req.ImportID == "" {
		s.writeInvalidRequestBody(w, r)
		return
	}
	err := s.dbManager.PublishPreparedDatabase(r.Context(), req.Database, req.ImportID)
	switch {
	case err == nil:
		httpresponse.WritePropertyJSON(w, http.StatusOK, map[string]string{"database": req.Database, "status": "online"})
	case errors.Is(err, multidb.ErrDatabaseExists):
		s.writeBoundaryError(w, r, http.StatusConflict, err, err)
	case errors.Is(err, storage.ErrInvalidData), errors.Is(err, multidb.ErrInvalidDatabaseName), errors.Is(err, os.ErrNotExist):
		s.writeBoundaryError(w, r, http.StatusBadRequest, err, ErrBadRequest)
	case errors.Is(err, multidb.ErrPreparedImportDisabled), errors.Is(err, storage.ErrNotImplemented):
		s.writeBoundaryError(w, r, http.StatusNotImplemented, err, err)
	default:
		s.writeBoundaryError(w, r, http.StatusInternalServerError, err, ErrInternalError)
	}
}
