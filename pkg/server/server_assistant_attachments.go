package server

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/pkg/heimdall"
)

func (s *Server) handleAssistantAttachment(w http.ResponseWriter, r *http.Request, suffix string) {
	parts := strings.Split(suffix, "/")
	if len(parts) < 2 || len(parts) > 3 || parts[1] != "attachments" || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	request := heimdall.ConversationRequest{ID: parts[0]}
	switch r.Method {
	case http.MethodPost:
		if len(parts) != 2 {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, heimdall.MaxConversationAttachmentBytes+(1<<20))
		reader, err := r.MultipartReader()
		if err != nil {
			writeConversationError(w, fmt.Errorf("select a file to upload"))
			return
		}
		part, err := reader.NextPart()
		if err != nil || part.FormName() != "file" {
			writeConversationError(w, fmt.Errorf("select a file to upload"))
			return
		}
		data, err := io.ReadAll(io.LimitReader(part, heimdall.MaxConversationAttachmentBytes+1))
		_ = part.Close()
		if err != nil {
			writeConversationError(w, err)
			return
		}
		if len(data) > heimdall.MaxConversationAttachmentBytes {
			httpresponse.WritePropertyJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"message": "This file exceeds the 8 MiB upload limit. Choose a smaller file or split it into parts."})
			return
		}
		request.Operation = "attachment_upload"
		request.Attachment = &heimdall.ConversationAttachmentUpload{Name: part.FileName(), ContentType: http.DetectContentType(data), Data: data}
	case http.MethodGet:
		request.Operation = "attachment_list"
		if len(parts) == 3 {
			request.Operation = "attachment_download"
			request.AttachmentID = parts[2]
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	result, err := heimdall.CallConversationOperation(r.Context(), request)
	if err != nil {
		writeConversationError(w, err)
		return
	}
	if download, ok := result.(heimdall.ConversationAttachmentDownload); ok {
		disposition := "attachment"
		if download.Metadata.ContentType == "image/png" || download.Metadata.ContentType == "image/jpeg" || download.Metadata.ContentType == "image/gif" || download.Metadata.ContentType == "image/webp" {
			disposition = "inline"
		}
		w.Header().Set("Content-Type", download.Metadata.ContentType)
		w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": download.Metadata.Name}))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(download.Data)
		return
	}
	httpresponse.WritePropertyJSON(w, http.StatusOK, result)
}
