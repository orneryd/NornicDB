package search

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nicksnyder/go-i18n/v2/i18n"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

type rerankTestCause struct{}

func (*rerankTestCause) Error() string { return "backend unavailable" }

func TestSearchRerankLocalizationDescriptors(t *testing.T) {
	cause := &rerankTestCause{}
	tests := []struct {
		name    string
		message localization.Message
		id      localization.MessageID
		english string
		cause   error
	}{
		{name: "marshal request", message: localization.SearchRerankRequestMarshalFailed(cause), id: localization.MessageSearchRerankRequestMarshalFailed, english: "failed to marshal request: backend unavailable", cause: cause},
		{name: "create request", message: localization.SearchRerankRequestCreationFailed(cause), id: localization.MessageSearchRerankRequestCreationFailed, english: "failed to create request: backend unavailable", cause: cause},
		{name: "send request", message: localization.SearchRerankRequestFailed(cause), id: localization.MessageSearchRerankRequestFailed, english: "rerank request failed: backend unavailable", cause: cause},
		{name: "API status", message: localization.SearchRerankAPIStatus(503), id: localization.MessageSearchRerankAPIStatus, english: "rerank API returned status 503"},
		{name: "parse response", message: localization.SearchRerankResponseParseFailed(cause), id: localization.MessageSearchRerankResponseParseFailed, english: "failed to parse response: backend unavailable", cause: cause},
		{name: "unrecognized response", message: localization.SearchRerankResponseUnrecognized(), id: localization.MessageSearchRerankResponseUnrecognized, english: "unable to parse rerank response"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := localizedError(tt.message, tt.cause)
			require.EqualError(t, err, tt.english)
			var localizedErr *nornicerrors.Localized
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, tt.id, localizedErr.Message.ID)
			if tt.cause == nil {
				require.NoError(t, errors.Unwrap(err))
				return
			}
			require.ErrorIs(t, err, cause)
			var typedCause *rerankTestCause
			require.ErrorAs(t, err, &typedCause)
		})
	}
}

type rerankRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn rerankRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestCallRerankAPILocalizedErrors(t *testing.T) {
	candidates := []RerankCandidate{{ID: "1", Content: "document", Score: 0.5}}
	requestCause := &rerankTestCause{}
	tests := []struct {
		name      string
		apiURL    string
		transport rerankRoundTripFunc
		id        localization.MessageID
		english   string
		cause     error
	}{
		{name: "create request", apiURL: "://bad-url", id: localization.MessageSearchRerankRequestCreationFailed, english: `failed to create request: parse "://bad-url": missing protocol scheme`},
		{name: "send request", apiURL: "http://rerank.test", cause: requestCause, transport: func(*http.Request) (*http.Response, error) { return nil, requestCause }, id: localization.MessageSearchRerankRequestFailed, english: "rerank request failed: Post \"http://rerank.test\": backend unavailable"},
		{name: "API status", apiURL: "http://rerank.test", transport: rerankResponse(http.StatusServiceUnavailable, `{}`), id: localization.MessageSearchRerankAPIStatus, english: "rerank API returned status 503"},
		{name: "parse response", apiURL: "http://rerank.test", transport: rerankResponse(http.StatusOK, `{`), id: localization.MessageSearchRerankResponseParseFailed, english: "failed to parse response: unexpected EOF"},
		{name: "unrecognized response", apiURL: "http://rerank.test", transport: rerankResponse(http.StatusOK, `{}`), id: localization.MessageSearchRerankResponseUnrecognized, english: "unable to parse rerank response"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := NewCrossEncoder(&CrossEncoderConfig{Enabled: true, APIURL: tt.apiURL})
			if tt.transport != nil {
				encoder.client.Transport = tt.transport
			}
			_, err := encoder.callRerankAPI(context.Background(), "query", candidates)
			require.EqualError(t, err, tt.english)
			var localizedErr *nornicerrors.Localized
			require.ErrorAs(t, err, &localizedErr)
			require.Equal(t, tt.id, localizedErr.Message.ID)
			if tt.cause != nil {
				require.ErrorIs(t, err, tt.cause)
				var typedCause *rerankTestCause
				require.ErrorAs(t, err, &typedCause)
			}
		})
	}
}

func rerankResponse(status int, body string) rerankRoundTripFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	}
}

func TestSearchRerankLocalizationLocaleRendering(t *testing.T) {
	bundle := i18n.NewBundle(language.AmericanEnglish)
	bundle.RegisterUnmarshalFunc("yaml", yaml.Unmarshal)
	for _, path := range []string{
		"../localization/catalog/active.searchrerank.en-US.yaml",
		"../localization/catalog/active.searchrerank.es-ES.yaml",
		"../localization/catalog/active.searchrerank.en-XA.yaml",
	} {
		_, err := bundle.LoadMessageFile(path)
		require.NoError(t, err)
	}

	tests := []struct {
		name     string
		locale   language.Tag
		message  localization.Message
		expected string
	}{
		{name: "Spanish cause", locale: language.EuropeanSpanish, message: localization.SearchRerankRequestFailed(errors.New("backend unavailable")), expected: "error en la solicitud de reclasificación: backend unavailable"},
		{name: "Spanish status", locale: language.EuropeanSpanish, message: localization.SearchRerankAPIStatus(503), expected: "la API de reclasificación devolvió el estado 503"},
		{name: "pseudo cause", locale: language.MustParse("en-XA"), message: localization.SearchRerankResponseParseFailed(errors.New("unexpected EOF")), expected: "[!! failed to parse response: unexpected EOF !!]"},
		{name: "pseudo response", locale: language.MustParse("en-XA"), message: localization.SearchRerankResponseUnrecognized(), expected: "[!! unable to parse rerank response !!]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localizer := i18n.NewLocalizer(bundle, tt.locale.String())
			text, err := localizer.Localize(&i18n.LocalizeConfig{
				MessageID:    string(tt.message.ID),
				TemplateData: tt.message.Data,
			})
			require.NoError(t, err)
			require.Equal(t, tt.expected, text)
		})
	}
}
