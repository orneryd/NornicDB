package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func TestApplyServeEmbeddingOverrides(t *testing.T) {
	for _, key := range []string{"PROVIDER", "MODEL", "API_URL", "API_KEY", "DIMENSIONS", "CACHE_SIZE", "GPU_LAYERS", "ENABLED"} {
		t.Setenv("NORNICDB_EMBEDDING_"+key, "")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	// API-key strings below are inert fixtures, never provider credentials.
	err := os.WriteFile(path, []byte("embedding:\n  enabled: true\n  provider: ollama\n  model: yaml-model\n  url: http://127.0.0.1:11435\n  api_key: yaml-fixture\n  dimensions: 384\n  cache_size: 250\n"), 0600)
	if err != nil {
		t.Fatal(err)
	}
	type settings struct {
		provider, model, url, key string
		dim, cache, gpu           int
		enabled                   bool
	}
	for _, tc := range []struct {
		name  string
		env   map[string]string
		flags []string
		want  settings
	}{
		{name: "omitted flags preserve YAML", want: settings{"ollama", "yaml-model", "http://127.0.0.1:11435", "yaml-fixture", 384, 250, -1, true}},
		{name: "environment overrides YAML", env: map[string]string{"PROVIDER": "openai", "MODEL": "env-model", "API_URL": "http://127.0.0.1:11436", "API_KEY": "env-fixture", "DIMENSIONS": "512", "CACHE_SIZE": "500", "GPU_LAYERS": "4", "ENABLED": "false"}, want: settings{"openai", "env-model", "http://127.0.0.1:11436", "env-fixture", 512, 500, 4, false}},
		{name: "CLI overrides environment and YAML including zero false and empty", env: map[string]string{"PROVIDER": "openai", "API_KEY": "env-fixture", "GPU_LAYERS": "4"}, flags: []string{"--embedding-provider=local", "--embedding-model=bge-m3", "--embedding-url=http://localhost:11434", "--embedding-key=", "--embedding-dim=1024", "--embedding-cache=0", "--embedding-gpu-layers=0", "--embedding-enabled=false"}, want: settings{"local", "bge-m3", "http://localhost:11434", "", 1024, 0, 0, false}},
		{name: "CLI credentials and enablement", env: map[string]string{"ENABLED": "false"}, flags: []string{"--embedding-key=cli-fixture", "--embedding-enabled=true"}, want: settings{"ollama", "yaml-model", "http://127.0.0.1:11435", "cli-fixture", 384, 250, -1, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv("NORNICDB_EMBEDDING_"+key, value)
			}
			cfg, err := config.LoadFromFile(path)
			if err != nil {
				t.Fatal(err)
			}
			root := newRootCommand(nil)
			cmd, _, err := root.Find([]string{"serve"})
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.ParseFlags(tc.flags); err != nil {
				t.Fatal(err)
			}
			applyServeEmbeddingOverrides(cmd, cfg)
			m := cfg.Memory
			got := settings{m.EmbeddingProvider, m.EmbeddingModel, m.EmbeddingAPIURL, m.EmbeddingAPIKey, m.EmbeddingDimensions, m.EmbeddingCacheSize, m.EmbeddingGPULayers, m.EmbeddingEnabled}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("resolved settings = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestApplyEmbeddingProviderDefaultsOrca(t *testing.T) {
	t.Run("normalizes provider name", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Memory.EmbeddingProvider = " ORCA "

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingProvider != "orca" {
			t.Fatalf("provider = %q", cfg.Memory.EmbeddingProvider)
		}
		if cfg.Memory.EmbeddingDimensions != 1536 {
			t.Fatalf("dimensions = %d", cfg.Memory.EmbeddingDimensions)
		}
	})

	t.Run("replaces built-in defaults", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Memory.EmbeddingProvider = "orca"

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingAPIURL != "https://api.orcarouter.ai" {
			t.Fatalf("API URL = %q", cfg.Memory.EmbeddingAPIURL)
		}
		if cfg.Memory.EmbeddingModel != "openai/text-embedding-3-small" {
			t.Fatalf("model = %q", cfg.Memory.EmbeddingModel)
		}
		if cfg.Memory.EmbeddingDimensions != 1536 {
			t.Fatalf("dimensions = %d", cfg.Memory.EmbeddingDimensions)
		}
	})

	t.Run("preserves generic overrides", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Memory.EmbeddingProvider = "orca"
		cfg.Memory.EmbeddingAPIURL = "https://gateway.example/v1"
		cfg.Memory.EmbeddingModel = "google/gemini-embedding-001"
		cfg.Memory.EmbeddingDimensions = 3072

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingAPIURL != "https://gateway.example" {
			t.Fatalf("API URL = %q", cfg.Memory.EmbeddingAPIURL)
		}
		if cfg.Memory.EmbeddingModel != "google/gemini-embedding-001" {
			t.Fatalf("model = %q", cfg.Memory.EmbeddingModel)
		}
		if cfg.Memory.EmbeddingDimensions != 3072 {
			t.Fatalf("dimensions = %d", cfg.Memory.EmbeddingDimensions)
		}
	})

	t.Run("preserves explicit values equal to generic defaults", func(t *testing.T) {
		cfg := config.LoadDefaults()
		cfg.Memory.EmbeddingProvider = "orca"
		cfg.EmbeddingExplicit.APIURL = true
		cfg.EmbeddingExplicit.Model = true
		cfg.EmbeddingExplicit.Dimensions = true

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingAPIURL != "http://localhost:11434" {
			t.Fatalf("API URL = %q", cfg.Memory.EmbeddingAPIURL)
		}
		if cfg.Memory.EmbeddingModel != "bge-m3" {
			t.Fatalf("model = %q", cfg.Memory.EmbeddingModel)
		}
		if cfg.Memory.EmbeddingDimensions != 1024 {
			t.Fatalf("dimensions = %d", cfg.Memory.EmbeddingDimensions)
		}
	})

	t.Run("preserves explicit environment values equal to generic defaults", func(t *testing.T) {
		t.Setenv("NORNICDB_EMBEDDING_PROVIDER", "ORCA")
		t.Setenv("NORNICDB_EMBEDDING_API_URL", "http://localhost:11434")
		t.Setenv("NORNICDB_EMBEDDING_MODEL", "bge-m3")
		t.Setenv("NORNICDB_EMBEDDING_DIMENSIONS", "1024")
		cfg := config.LoadFromEnv()

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingProvider != "orca" ||
			cfg.Memory.EmbeddingAPIURL != "http://localhost:11434" ||
			cfg.Memory.EmbeddingModel != "bge-m3" ||
			cfg.Memory.EmbeddingDimensions != 1024 {
			t.Fatalf("resolved environment settings = %+v", cfg.Memory)
		}
	})

	t.Run("preserves explicit YAML values equal to generic defaults", func(t *testing.T) {
		for _, key := range []string{"PROVIDER", "API_URL", "MODEL", "DIMENSIONS"} {
			t.Setenv("NORNICDB_EMBEDDING_"+key, "")
		}
		path := filepath.Join(t.TempDir(), "orca.yaml")
		err := os.WriteFile(path, []byte("embedding:\n  provider: ORCA\n  url: http://localhost:11434\n  model: bge-m3\n  dimensions: 1024\n"), 0600)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := config.LoadFromFile(path)
		if err != nil {
			t.Fatal(err)
		}

		applyEmbeddingProviderDefaults(cfg)

		if cfg.Memory.EmbeddingProvider != "orca" ||
			cfg.Memory.EmbeddingAPIURL != "http://localhost:11434" ||
			cfg.Memory.EmbeddingModel != "bge-m3" ||
			cfg.Memory.EmbeddingDimensions != 1024 {
			t.Fatalf("resolved YAML settings = %+v", cfg.Memory)
		}
	})
}
