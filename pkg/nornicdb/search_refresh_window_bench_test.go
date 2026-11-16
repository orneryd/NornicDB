package nornicdb

import (
	"testing"
	"time"
)

// BenchmarkQueuedSearchRefreshScheduling measures scheduling requests joining
// one armed window. It excludes graph writes and refresh execution; the public
// sustained-write regression separately proves that the window makes progress.
func BenchmarkQueuedSearchRefreshScheduling(b *testing.B) {
	db, entry := openProducerBenchDB(b, false)
	entry.pendingFlushDelay = time.Hour
	db.ensurePendingFlush(entry)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.ensurePendingFlush(entry)
	}
}
