package storage

// HoldIndexReadForTesting takes idx's read lock so that a writer of idx parks
// on the lock. writerWaiting reports true once a writer is parked; release
// drops the hold. It adds no product seam: it only uses the lock the index
// itself owns, the contextRWMutex every composite index carries.
func HoldIndexReadForTesting(idx *CompositeIndex) (release func(), writerWaiting func() bool) {
	return holdIndexReadForTesting(&idx.mu)
}

func holdIndexReadForTesting(mu *contextRWMutex) (release func(), writerWaiting func() bool) {
	mu.RLock()
	release = mu.RUnlock
	writerWaiting = func() bool {
		if mu.TryRLock() {
			mu.RUnlock()
			return false
		}
		return true
	}
	return release, writerWaiting
}
