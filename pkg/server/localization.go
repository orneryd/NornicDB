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

func (s *Server) writePostRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.PostRequired(), ErrMethodNotAllowed)
}

func (s *Server) writeGetOrPostRequired(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusMethodNotAllowed, localization.GetOrPostRequired(), ErrMethodNotAllowed)
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

func (s *Server) writeNotAuthenticated(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusUnauthorized, localization.HTTPNotAuthenticated(), ErrUnauthorized)
}

func (s *Server) writeUserNotFound(w http.ResponseWriter, r *http.Request) {
	s.writeLocalizedError(w, r, http.StatusNotFound, localization.UserNotFound(), ErrNotFound)
}
