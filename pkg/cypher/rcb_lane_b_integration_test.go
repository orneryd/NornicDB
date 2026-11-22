//go:build integration
// +build integration

package cypher

// Root-cause variant, v1.5.2 verification campaign, lane B (test-only).
// G-test-integration-tag-compile: QueryEmbedder (executor.go:723-726) gained
// ChunkText in 57131dcef; the integration-tagged stub
// staticBenchmarkEmbedder (fabric_realdata_e2e_bench_test.go:19-25) has only
// Embed. This method alone is the variant: with it the tagged test build is
// expected to type-check.

func (s staticBenchmarkEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}
