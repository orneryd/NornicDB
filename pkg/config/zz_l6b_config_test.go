package config

// Lane L6b (closeout):
//   P03 L-H9-006  the documented memory.search_* block: the YAML decode step
//                 (config.go:3371, non-strict yaml.Unmarshal into YAMLConfig, whose
//                 Memory struct has no search_* fields) versus a strict decode.
//   X03 L-H9-004  the loader's environment names (applyEnvVars, config.go:2293):
//                 the documented NORNICDB_AUTH_ENABLED / _USER / _PASSWORD versus
//                 NORNICDB_AUTH.

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestL6bMemorySearchBlockDecoder(t *testing.T) {
	doc := []byte("memory:\n  search_bm25_enabled: false\n  search_bm25_warming: lazy\n  search_vector_enabled: false\n  search_vector_warming: lazy\n")
	control := []byte("search:\n  bm25_enabled: false\n  bm25_warming: lazy\n  vector_enabled: false\n  vector_warming: lazy\n")
	var memoryTags []string
	mt, _ := reflect.TypeOf(YAMLConfig{}).FieldByName("Memory")
	for i := 0; i < mt.Type.NumField(); i++ {
		memoryTags = append(memoryTags, mt.Type.Field(i).Tag.Get("yaml"))
	}
	hasSearch := strings.Contains(strings.Join(memoryTags, ","), "search_")
	for _, tc := range []struct {
		name string
		data []byte
	}{{"memory-block", doc}, {"search-block-control", control}} {
		var plain YAMLConfig
		plainErr := yaml.Unmarshal(tc.data, &plain)
		var strict YAMLConfig
		dec := yaml.NewDecoder(bytes.NewReader(tc.data))
		dec.KnownFields(true)
		strictErr := dec.Decode(&strict)
		t.Logf("L6B_P03 %s yaml.Unmarshal(config.go:3371) err=%v -> Search.BM25Enabled=%v BM25Warming=%q VectorEnabled=%v VectorWarming=%q | strict KnownFields err=%v | YAMLConfig.Memory has search_* tag=%v",
			tc.name, plainErr, plain.Search.BM25Enabled, plain.Search.BM25Warming, plain.Search.VectorEnabled, plain.Search.VectorWarming, strictErr, hasSearch)
		if tc.name == "memory-block" && plainErr == nil && plain.Search.BM25Enabled == nil && strictErr != nil && !hasSearch {
			t.Errorf("CAUSE P03: YAMLConfig.Memory has no search_* fields and the non-strict decode drops the keys silently; a strict decode names them: %v", strictErr)
		}
	}
}

func TestL6bAuthEnvironmentNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"documented-docker-table", map[string]string{"NORNICDB_AUTH_ENABLED": "true", "NORNICDB_AUTH_USER": "admin", "NORNICDB_AUTH_PASSWORD": "verify-l6b-long-password"}},
		{"control-NORNICDB_AUTH", map[string]string{"NORNICDB_AUTH": "admin:verify-l6b-long-password"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{"NORNICDB_AUTH", "NEO4J_AUTH", "NORNICDB_AUTH_ENABLED", "NORNICDB_AUTH_USER", "NORNICDB_AUTH_PASSWORD", "NORNICDB_NO_AUTH"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg := LoadFromEnv()
			t.Logf("L6B_X03 %s env=%v -> Auth.Enabled=%v InitialUsername=%q InitialPassword=%q", tc.name, tc.env, cfg.Auth.Enabled, cfg.Auth.InitialUsername, cfg.Auth.InitialPassword)
			if tc.name == "documented-docker-table" && cfg.Auth.InitialPassword != "verify-l6b-long-password" {
				t.Errorf("CAUSE X03: applyEnvVars reads NORNICDB_AUTH / NEO4J_AUTH only; the documented NORNICDB_AUTH_PASSWORD leaves the password %q", cfg.Auth.InitialPassword)
			}
		})
	}
}
