package nornicdb

import "testing"

// closeTestDBOnCleanup closes a hand-built test DB before the storage
// cleanups the test registered earlier (t.Cleanup runs last-in, first-out).
// DB.Close waits for the background tasks the test started (search index
// build after Restore, embed queue and clustering timer after SetEmbedder)
// before the storage engine closes; closing only the engine leaves those
// tasks running against it (#589).
func closeTestDBOnCleanup(t *testing.T, db *DB) {
	t.Helper()
	t.Cleanup(func() { _ = db.Close() })
}
