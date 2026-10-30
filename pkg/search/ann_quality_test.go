package search

import "testing"

func TestANNQualityFromEnv(t *testing.T) {
	t.Run("default fast", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "")
		if got := ANNQualityFromEnv(); got != ANNQualityFast {
			t.Fatalf("expected %q, got %q", ANNQualityFast, got)
		}
	})

	t.Run("compressed accepted", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
		if got := ANNQualityFromEnv(); got != ANNQualityCompressed {
			t.Fatalf("expected %q, got %q", ANNQualityCompressed, got)
		}
	})

	t.Run("invalid defaults to fast", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "nope")
		if got := ANNQualityFromEnv(); got != ANNQualityFast {
			t.Fatalf("expected %q, got %q", ANNQualityFast, got)
		}
	})
}

func TestHNSWPresetFromANNQuality(t *testing.T) {
	if got := hnswPresetFromANNQuality(ANNQualityCompressed); got != QualityFast {
		t.Fatalf("expected compressed to map to %q, got %q", QualityFast, got)
	}
}
