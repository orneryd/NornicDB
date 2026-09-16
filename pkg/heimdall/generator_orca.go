package heimdall

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	defaultOrcaBaseURL = "https://api.orcarouter.ai"
	defaultOrcaModel   = "orcarouter/auto"
)

func init() {
	RegisterHeimdallProvider("orca", newOrcaGenerator)
}

// orcaGenerator reuses the OpenAI-compatible transport while retaining an
// OrcaRouter provider identity in logs and diagnostics.
type orcaGenerator struct {
	*openAIGenerator
}

// ModelPath implements Generator.
func (g *orcaGenerator) ModelPath() string {
	return "orca:" + g.model
}

func newOrcaGenerator(cfg Config) (Generator, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("orca provider requires NORNICDB_HEIMDALL_API_KEY")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/")
	if baseURL == "" {
		baseURL = defaultOrcaBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/v1")
	model := strings.TrimSpace(cfg.Model)
	if model == "" || looksLikeLocalModel(model) {
		model = defaultOrcaModel
	}
	return &orcaGenerator{openAIGenerator: &openAIGenerator{
		baseURL: baseURL,
		apiKey:  cfg.APIKey,
		model:   model,
		client:  &http.Client{Timeout: 120 * time.Second},
	}}, nil
}
