package replication

import "testing"

// BenchmarkWALCompletionChannelAllocation measures the only allocation the typed
// completion owner changed in the async path: the per-record outcome channel now
// carries a position and an error instead of an error alone. The former barrier
// position/error pair is replaced by the same single channel.
func BenchmarkWALCompletionChannelAllocation(b *testing.B) {
	b.Run("seed-error-channel", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			outcome := make(chan error, 1)
			outcome <- nil
			<-outcome
		}
	})
	b.Run("typed-completion-channel", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			outcome := make(chan walCompletion, 1)
			outcome <- walCompletion{position: 1}
			<-outcome
		}
	})
}
