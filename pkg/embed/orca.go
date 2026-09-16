package embed

import (
	"strings"
	"time"
)

const (
	orcaAPIURL     = "https://api.orcarouter.ai"
	orcaEmbedModel = "openai/text-embedding-3-small"
	orcaDimensions = 1536
)

// DefaultOrcaConfig returns an OrcaRouter embedding configuration using its
// OpenAI-compatible embeddings endpoint and a generally available model.
//
// Example:
//
//	config := embed.DefaultOrcaConfig(apiKey)
//	embedder, err := embed.NewEmbedder(config)
func DefaultOrcaConfig(apiKey string) *Config {
	return &Config{
		Provider:   "orca",
		APIURL:     orcaAPIURL,
		APIPath:    "/v1/embeddings",
		APIKey:     apiKey,
		Model:      orcaEmbedModel,
		Dimensions: orcaDimensions,
		Timeout:    30 * time.Second,
	}
}

func resolveOrcaConfig(config *Config) *Config {
	defaults := DefaultOrcaConfig("")
	if config == nil {
		return defaults
	}
	cfg := *config
	cfg.Provider = "orca"
	if strings.TrimSpace(cfg.APIURL) == "" {
		cfg.APIURL = defaults.APIURL
	}
	cfg.APIURL = openAICompatibleAPIRoot(cfg.APIURL)
	if strings.TrimSpace(cfg.APIPath) == "" {
		cfg.APIPath = defaults.APIPath
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaults.Model
	}
	if cfg.Dimensions <= 0 {
		cfg.Dimensions = defaults.Dimensions
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaults.Timeout
	}
	return &cfg
}

func openAICompatibleAPIRoot(apiURL string) string {
	apiURL = strings.TrimRight(strings.TrimSpace(apiURL), "/")
	return strings.TrimSuffix(apiURL, "/v1")
}
