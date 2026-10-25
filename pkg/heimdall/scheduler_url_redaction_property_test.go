package heimdall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/orneryd/nornicdb/pkg/observability"
)

type generatedManagerURL struct {
	seed      uint16
	delimiter uint8
	streamCut uint8
	form      uint8
	scheme    uint8
}

func (generatedManagerURL) Generate(random *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(generatedManagerURL{
		seed:      uint16(random.Uint32()),
		delimiter: uint8(random.Intn(len(managerURLSubdelimiters))),
		streamCut: uint8(random.Intn(255)),
		form:      uint8(random.Intn(3)),
		scheme:    uint8(random.Intn(4)),
	})
}

const managerURLSubdelimiters = "!$&'()*+,;="

func generatedManagerURLParts(input generatedManagerURL) (string, []string, string, bool, bool) {
	marker := fmt.Sprintf("%04x", input.seed)
	delimiter := managerURLSubdelimiters[int(input.delimiter)%len(managerURLSubdelimiters)]
	user := "r73-manager-user-" + marker + "-" + managerURLSubdelimiters + string(delimiter)
	password := "r73-manager-pass-" + marker + "-" + managerURLSubdelimiters + string(delimiter) + "-%2B"
	query := "r73-manager-query-" + marker + "-" + managerURLSubdelimiters + string(delimiter) + "-%2F"
	hasUserInfo, hasQuery := input.form != 1, input.form != 0
	userinfo := ""
	fragments := make([]string, 0, 5)
	if hasUserInfo {
		userinfo = user + ":" + password + "@"
		fragments = append(fragments, user, password, user+":"+password)
	}
	queryString := ""
	if hasQuery {
		queryString = "?api_key=" + url.QueryEscape(query)
		fragments = append(fragments, query, url.QueryEscape(query))
	}
	schemes := [...]string{"http", "https", "probe+7.test", "x-2"}
	endpoint := schemes[input.scheme%uint8(len(schemes))] + "://" + userinfo + "manager.example.invalid/path" + queryString
	if input.scheme%uint8(len(schemes)) == 3 && !hasUserInfo {
		endpoint = "urn:probe" + queryString
	}
	return endpoint, fragments, query, hasUserInfo, hasQuery
}

func managerOutputContainsCredential(text string, fragments []string) bool {
	for _, fragment := range fragments {
		if fragment != "" && strings.Contains(text, fragment) {
			return true
		}
	}
	return false
}

type urlPropertyGenerator struct {
	generatePrompt string
	generateOutput string
	generateErr    error
	streamPrompt   string
	streamOutput   string
	streamCut      int
	toolMessages   []ToolRoundMessage
	toolDefs       []MCPTool
	toolContent    string
	toolCalls      []ParsedToolCall
}

func (g *urlPropertyGenerator) Generate(_ context.Context, prompt string, _ GenerateParams) (string, error) {
	g.generatePrompt = prompt
	return g.generateOutput, g.generateErr
}

func (g *urlPropertyGenerator) GenerateStream(_ context.Context, prompt string, _ GenerateParams, callback func(string) error) error {
	g.streamPrompt = prompt
	cut := g.streamCut
	if cut < 1 || cut >= len(g.streamOutput) {
		cut = len(g.streamOutput) / 2
	}
	if err := callback(g.streamOutput[:cut]); err != nil {
		return err
	}
	return callback(g.streamOutput[cut:])
}

func (g *urlPropertyGenerator) Close() error      { return nil }
func (g *urlPropertyGenerator) ModelPath() string { return "property-generator" }

func (g *urlPropertyGenerator) GenerateWithTools(_ context.Context, messages []ToolRoundMessage, tools []MCPTool, _ GenerateParams) (string, []ParsedToolCall, error) {
	g.toolMessages = messages
	g.toolDefs = tools
	return g.toolContent, g.toolCalls, nil
}

func TestGeneratedCredentialURLsAreRedactedAcrossManagerConsumers(t *testing.T) {
	stage := "generation"
	property := func(input generatedManagerURL) bool {
		endpoint, fragments, queryValue, hasUserInfo, hasQuery := generatedManagerURLParts(input)
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
			if !containsGeneratedManagerCredential(observability.ClientCredentials(endpoint), parsedPassword) {
				stage = "client URL password extraction"
				return false
			}
		}
		stage = fmt.Sprintf("context extraction userinfo=%t query=%t", hasUserInfo, hasQuery)
		owned := observability.ContextCredentials("request: " + endpoint)
		if hasUserInfo && !containsGeneratedManagerCredential(owned, parsedUsername) {
			stage = "userinfo username extraction"
			return false
		}
		if hasUserInfo && !containsGeneratedManagerCredential(owned, parsedPassword) {
			stage = "userinfo password extraction"
			return false
		}
		if hasQuery && !containsGeneratedManagerCredential(owned, queryValue) {
			stage = "query credential extraction"
			return false
		}
		stage = "text redaction"
		if managerOutputContainsCredential(observability.RedactText(`{"endpoint":`+managerJSONQuote(endpoint)+`}`), fragments) {
			return false
		}

		prompt := "Use this endpoint: '" + endpoint + "'"
		generator := &urlPropertyGenerator{generateOutput: "response includes " + endpoint}
		manager := &Manager{generator: generator}
		stage = "prompt and generated text"
		generated, err := manager.Generate(context.Background(), prompt, DefaultGenerateParams())
		if err != nil || managerOutputContainsCredential(generator.generatePrompt, fragments) ||
			managerOutputContainsCredential(generated, fragments) {
			return false
		}

		generator.streamOutput = "streamed response " + endpoint + " finished"
		generator.streamCut = int(input.streamCut) % (len(generator.streamOutput) - 1)
		var streamed strings.Builder
		streamCallbacks := 0
		stage = "streamed text"
		if err := manager.GenerateStream(context.Background(), "summarize the generated response", DefaultGenerateParams(), func(token string) error {
			streamCallbacks++
			streamed.WriteString(token)
			return nil
		}); err != nil || managerOutputContainsCredential(streamed.String(), fragments) ||
			managerOutputContainsCredential(generator.streamPrompt, fragments) || streamCallbacks == 0 {
			return false
		}

		arguments, err := json.Marshal(map[string]string{"endpoint": endpoint})
		if err != nil {
			return false
		}
		schema, err := json.Marshal(map[string]string{"description": "Endpoint " + endpoint})
		if err != nil {
			return false
		}
		messages := []ToolRoundMessage{{
			Role:       "assistant",
			Content:    "tool context " + endpoint,
			ToolCallID: "result-" + endpoint,
			ToolCalls:  []ParsedToolCall{{Id: "call-" + endpoint, Name: "lookup", Arguments: string(arguments)}},
		}}
		tools := []MCPTool{{Name: "lookup", Description: "Reads " + endpoint, InputSchema: schema}}
		generator.toolContent = "tool response " + endpoint
		generator.toolCalls = []ParsedToolCall{{Id: "out-" + endpoint, Name: "lookup", Arguments: string(arguments)}}
		stage = "tool arguments and descriptions"
		content, calls, err := manager.GenerateWithTools(context.Background(), messages, tools, DefaultGenerateParams())
		if err != nil || len(generator.toolMessages) != 1 || len(generator.toolMessages[0].ToolCalls) != 1 ||
			len(generator.toolDefs) != 1 || len(calls) != 1 {
			return false
		}
		if managerOutputContainsCredential(generator.toolMessages[0].Content, fragments) ||
			managerOutputContainsCredential(generator.toolMessages[0].ToolCallID, fragments) ||
			managerOutputContainsCredential(generator.toolMessages[0].ToolCalls[0].Arguments, fragments) ||
			managerOutputContainsCredential(generator.toolDefs[0].Description, fragments) ||
			managerOutputContainsCredential(string(generator.toolDefs[0].InputSchema), fragments) ||
			managerOutputContainsCredential(content, fragments) || managerOutputContainsCredential(calls[0].Arguments, fragments) {
			return false
		}

		stage = "nested errors"
		generator.generateErr = fmt.Errorf("outer: %w", errors.Join(errors.New("cause: "+endpoint), errors.New("nested: "+endpoint)))
		_, err = manager.Generate(context.Background(), "summarize this failure", DefaultGenerateParams())
		return err != nil && !managerOutputContainsCredential(err.Error(), fragments)
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 128}); err != nil {
		t.Fatalf("generated manager credential URL property failed at %s", stage)
	}
}

func containsGeneratedManagerCredential(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func managerJSONQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
