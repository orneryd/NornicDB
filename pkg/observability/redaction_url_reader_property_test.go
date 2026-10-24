package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type generatedCredentialURL struct {
	seed      uint16
	delimiter uint8
	form      uint8
	scheme    uint8
}

func (generatedCredentialURL) Generate(random *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(generatedCredentialURL{
		seed:      uint16(random.Uint32()),
		delimiter: uint8(random.Intn(len(urlUserinfoSubdelimiters))),
		form:      uint8(random.Intn(3)),
		scheme:    uint8(random.Intn(4)),
	})
}

const urlUserinfoSubdelimiters = "!$&'()*+,;="

func generatedCredentialURLParts(input generatedCredentialURL) (string, []string, string, bool, bool) {
	marker := fmt.Sprintf("%04x", input.seed)
	delimiter := urlUserinfoSubdelimiters[int(input.delimiter)%len(urlUserinfoSubdelimiters)]
	user := "r73-user-" + marker + "-" + urlUserinfoSubdelimiters + string(delimiter)
	password := "r73-pass-" + marker + "-" + urlUserinfoSubdelimiters + string(delimiter) + "-%2B"
	queryValue := "r73-query-" + marker + "-" + urlUserinfoSubdelimiters + string(delimiter) + "-%2F"
	hasUserInfo, hasQuery := input.form != 1, input.form != 0
	userinfo := ""
	fragments := make([]string, 0, 5)
	if hasUserInfo {
		userinfo = user + ":" + password + "@"
		fragments = append(fragments, user, password, user+":"+password)
	}
	query := ""
	if hasQuery {
		query = "?api_key=" + url.QueryEscape(queryValue)
		fragments = append(fragments, queryValue, url.QueryEscape(queryValue))
	}
	schemes := [...]string{"http", "https", "probe+7.test", "x-2"}
	endpoint := schemes[input.scheme%uint8(len(schemes))] + "://" + userinfo + "reader.example.invalid/path" + query
	if input.scheme%uint8(len(schemes)) == 3 && !hasUserInfo {
		endpoint = "urn:probe" + query
	}
	return endpoint, fragments, queryValue, hasUserInfo, hasQuery
}

func containsEveryCredential(text string, credentials []string) bool {
	for _, credential := range credentials {
		if credential != "" && strings.Contains(text, credential) {
			return true
		}
	}
	return false
}

func containsCredentialValue(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func TestGeneratedCredentialURLsAreRedactedAcrossTextConsumers(t *testing.T) {
	var logBuffer bytes.Buffer
	logger := slog.New(newRedactingHandler(slog.NewTextHandler(&logBuffer, nil), defaultRedactSet()))
	stage := "generation"
	property := func(input generatedCredentialURL) bool {
		endpoint, fragments, queryValue, hasUserInfo, hasQuery := generatedCredentialURLParts(input)
		stage = "url parsing"
		parsed, err := url.Parse(endpoint)
		if err != nil || (parsed.User != nil) != hasUserInfo || (parsed.Query().Has("api_key")) != hasQuery ||
			(hasQuery && parsed.Query().Get("api_key") != queryValue) {
			return false
		}
		parsedUsername := ""
		parsedPassword := ""
		if hasUserInfo {
			parsedUsername = parsed.User.Username()
			var ok bool
			parsedPassword, ok = parsed.User.Password()
			if !ok {
				stage = "URL parser password presence"
				return false
			}
			if !containsCredentialValue(ClientCredentials(endpoint), parsedPassword) {
				stage = "client URL password extraction"
				return false
			}
		}

		stage = fmt.Sprintf("credential collection userinfo=%t query=%t", hasUserInfo, hasQuery)
		owned := ContextCredentials(endpoint)
		if hasUserInfo && !containsCredentialValue(owned, parsedUsername) {
			stage = "userinfo username extraction"
			return false
		}
		if hasUserInfo && !containsCredentialValue(owned, parsedPassword) {
			stage = "userinfo password extraction"
			return false
		}
		if hasQuery && !containsCredentialValue(owned, queryValue) {
			stage = "query credential extraction"
			return false
		}
		variants := []string{
			"request failed for " + endpoint + " after retry",
			"request failed for '" + endpoint + "' after retry",
			"request failed for \"" + endpoint + "\" after retry",
			`{"endpoint":` + string(mustJSONQuote(t, endpoint)) + `}`,
		}
		for _, source := range variants {
			stage = "text redaction"
			if containsEveryCredential(RedactText(source), fragments) {
				return false
			}
			stage = "context extraction"
			if len(ContextCredentials(source)) == 0 {
				return false
			}
		}

		stage = "JSON handling"
		var decoded any
		if jsonErr := json.Unmarshal([]byte(RedactText(variants[3])), &decoded); jsonErr != nil {
			return false
		}

		stage = "nested errors"
		nested := fmt.Errorf("outer diagnostic: %w", errors.Join(
			fmt.Errorf("provider rejected %s", variants[1]),
			fmt.Errorf("request details: %s", variants[3]),
		))
		if containsEveryCredential(RedactError(nested).Error(), fragments) {
			return false
		}

		stage = "localized errors"
		localized := localization.NewLocalizedError("test.generated_url", localization.Message{
			Fallback: variants[2],
			Data: map[string]any{
				"endpoint": endpoint,
				"cause":    errors.New(variants[0]),
			},
		}, nested)
		safeLocalized := RedactError(localized)
		var rendered *localization.LocalizedError
		if !errors.As(safeLocalized, &rendered) || containsEveryCredential(safeLocalized.Error(), fragments) ||
			containsEveryCredential(rendered.Message.Fallback, fragments) {
			return false
		}
		if cause, ok := rendered.Message.Data["cause"].(error); !ok || containsEveryCredential(cause.Error(), fragments) {
			return false
		}
		if value, ok := rendered.Message.Data["endpoint"].(string); !ok || containsEveryCredential(value, fragments) {
			return false
		}

		stage = "logs"
		logger.Error(variants[0], "endpoint", endpoint, "details", variants[3])
		if containsEveryCredential(logBuffer.String(), fragments) {
			return false
		}
		return true
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 128}); err != nil {
		t.Fatalf("generated credential URL text consumer property failed at %s", stage)
	}
}

func mustJSONQuote(t *testing.T, value string) []byte {
	t.Helper()
	quoted, err := json.Marshal(value)
	if err != nil {
		t.Fatal("could not encode generated JSON string")
	}
	return quoted
}
