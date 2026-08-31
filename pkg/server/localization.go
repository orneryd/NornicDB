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
	text, tag := s.renderMessage(r.Context(), message)
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	s.writeError(w, status, text, err)
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
	text, tag := s.renderMessage(r.Context(), localization.PostRequired())
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	s.writeNeo4jError(w, http.StatusMethodNotAllowed, code, text)
}
