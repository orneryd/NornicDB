package server

import (
	"context"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
)

func (s *Server) localizationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.localizer == nil {
			next.ServeHTTP(w, r)
			return
		}
		preferences, _, err := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
		if err == nil && len(preferences) > 0 {
			match := s.localizer.Resolve("http", preferences...)
			r = r.WithContext(localization.WithPreferences(r.Context(), match.Tag))
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) renderMessage(ctx context.Context, message localization.Message) (string, language.Tag) {
	if s.localizer == nil {
		if message.Fallback != "" {
			return message.Fallback, language.AmericanEnglish
		}
		return string(message.ID), language.AmericanEnglish
	}
	text, tag, err := s.localizer.Render(ctx, message)
	if err != nil {
		return s.localizer.MustRenderEnglish(message), language.AmericanEnglish
	}
	return text, tag
}

func (s *Server) writeLocalizedError(w http.ResponseWriter, r *http.Request, status int, message localization.Message, err error) {
	text := s.localizedText(w, r, message)
	s.writeError(w, status, text, err)
}

func (s *Server) localizedText(w http.ResponseWriter, r *http.Request, message localization.Message) string {
	text, tag := s.renderMessage(r.Context(), message)
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	return text
}

func (s *Server) writeLocalizedNeo4jError(w http.ResponseWriter, r *http.Request, status int, code string, message localization.Message) {
	s.writeNeo4jError(w, status, code, s.localizedText(w, r, message))
}

func (s *Server) writeInvalidRequestBody(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.InvalidRequestBody(), ErrBadRequest)
}

func (s *Server) writeNeo4jInvalidRequestBody(w http.ResponseWriter, r *http.Request, code string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, code, localization.InvalidRequestBody())
}

func (s *Server) writeNeo4jInvalidJSONBody(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", localization.InvalidJSONBody())
}

func (s *Server) writePostRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.PostRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeGetRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.GetRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeNeo4jGetRequired(w http.ResponseWriter, r *http.Request, code string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusMethodNotAllowed, code, localization.GetRequired())
}

func (s *Server) writeGetOrPostRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.GetOrPostRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeGetOrPutRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.GetOrPutRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeGetPutOrDeleteRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.GetPutOrDeleteRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeNeo4jPostOrDeleteRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedNeo4jError(w, r, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", localization.PostOrDeleteRequired())
}

func (s *Server) writeMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.MethodNotAllowed(), ErrMethodNotAllowed)
}

func (s *Server) writeNeo4jMethodNotAllowed(w http.ResponseWriter, r *http.Request, code string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusMethodNotAllowed, code, localization.MethodNotAllowed())
}

func (s *Server) writeNeo4jPostRequired(w http.ResponseWriter, r *http.Request, code string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusMethodNotAllowed, code, localization.PostRequired())
}

func (s *Server) writeDatabaseNotFound(w http.ResponseWriter, r *http.Request, status int, err error, name string) {
	s.writeLocalizedError(w, r, status, localization.HTTPDatabaseNotFound(name), err)

}

func (s *Server) writeNeo4jDatabaseNotFound(w http.ResponseWriter, r *http.Request, name string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound", localization.HTTPDatabaseNotFound(name))
}

func (s *Server) writeNeo4jDatabaseAccessDenied(w http.ResponseWriter, r *http.Request, name string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", localization.DatabaseAccessDenied(name))
}

func (s *Server) writeNeo4jDatabaseWriteDenied(w http.ResponseWriter, r *http.Request, name string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", localization.DatabaseWriteDenied(name))
}

func (s *Server) writeAuthenticationNotConfigured(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusServiceUnavailable, localization.AuthenticationNotConfigured(), nil)
}

func (s *Server) writeOAuthNotConfigured(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.OAuthNotConfigured(), ErrBadRequest)
}

func (s *Server) writeNotAuthenticated(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusUnauthorized, localization.HTTPNotAuthenticated(), ErrUnauthorized)
}

func (s *Server) writeUserNotFound(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusNotFound, localization.UserNotFound(), ErrNotFound)
}

func (s *Server) writeNeo4jTransactionNotFound(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedNeo4jError(w, r, http.StatusNotFound, "Neo.ClientError.Request.Invalid", localization.TransactionNotFound())
}

func (s *Server) writeRequestFieldRequired(w http.ResponseWriter, r *http.Request, field string) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.RequestFieldRequired(field), ErrBadRequest)
}

func (s *Server) writeNeo4jRequestFieldRequired(w http.ResponseWriter, r *http.Request, code, field string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, code, localization.RequestFieldRequired(field))
}

func (s *Server) writeNeo4jNotFound(w http.ResponseWriter, r *http.Request, code string) {
	s.writeLocalizedNeo4jError(w, r, http.StatusNotFound, code, localization.NotFound())
}

func (s *Server) writeInvalidGPUManagerType(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusInternalServerError, localization.InvalidGPUManagerType(), ErrInternalError)
}

func (s *Server) writeGPUManagerNotInitialized(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusServiceUnavailable, localization.GPUManagerNotInitialized(), ErrInternalError)
}

func (s *Server) writeTemporalGraphReconstructionUnsupported(w http.ResponseWriter, r *http.Request, err error) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.TemporalGraphReconstructionUnsupported(), err)
}

func (s *Server) writeTemporalGraphDiffUnsupported(w http.ResponseWriter, r *http.Request, err error) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.TemporalGraphDiffUnsupported(), err)
}

func (s *Server) writeNeo4jNoAuthenticationProvided(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedNeo4jError(w, r, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", localization.NoAuthenticationProvided())
}

func (s *Server) writeNeo4jInsufficientPermissions(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedNeo4jError(w, r, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", localization.InsufficientPermissions())
}

func (s *Server) writeInternalServerError(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusInternalServerError, localization.InternalServerError(), ErrInternalError)
}

func (s *Server) writeRetentionManagerDisabled(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusServiceUnavailable, localization.RetentionManagerDisabled(), ErrServiceUnavailable)
}

func (s *Server) writeRetentionPolicyIDRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.RetentionPolicyIDRequired(), ErrBadRequest)
}

func (s *Server) writeRetentionHoldIDRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.RetentionHoldIDRequired(), ErrBadRequest)
}

func (s *Server) writeRetentionErasureIDRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.RetentionErasureIDRequired(), ErrBadRequest)
}

func (s *Server) writeDeleteRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.DeleteRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeGDPROwnDataExportOnly(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusForbidden, localization.GDPROwnDataExportOnly(), ErrForbidden)
}

func (s *Server) writeGDPRConfirmationRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusBadRequest, localization.GDPRConfirmationRequired(), ErrBadRequest)
}

func (s *Server) writeGDPROwnDataDeleteOnly(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusForbidden, localization.GDPROwnDataDeleteOnly(), ErrForbidden)
}

func (s *Server) writeGDPRLegalHoldPreventsDeletion(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusConflict, localization.GDPRLegalHoldPreventsDeletion(), ErrForbidden)
}
