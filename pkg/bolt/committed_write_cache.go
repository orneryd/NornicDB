package bolt

type committedWriteCacheInvalidator interface {
	InvalidateCommittedWriteCaches()
}

type pendingTransactionWriteReporter interface {
	HasPendingTransactionWrites() bool
}

func (s *Session) invalidateCommittedWriteCaches(databaseName string) {
	if s == nil {
		return
	}
	if invalidator, ok := s.baseExec.(committedWriteCacheInvalidator); ok {
		invalidator.InvalidateCommittedWriteCaches()
	} else if provider, ok := s.baseExec.(baseCypherExecutorProvider); ok {
		if executor := provider.BaseCypherExecutor(); executor != nil {
			executor.InvalidateCommittedWriteCaches()
		}
	}
	if s.server == nil {
		return
	}

	s.server.executorsMu.RLock()
	databaseExecutor := s.server.executors[databaseName]
	s.server.executorsMu.RUnlock()
	if invalidator, ok := databaseExecutor.(committedWriteCacheInvalidator); ok {
		invalidator.InvalidateCommittedWriteCaches()
	}
}

func (a *boltQueryExecutorAdapter) InvalidateCommittedWriteCaches() {
	if a != nil && a.executor != nil {
		a.executor.InvalidateCommittedWriteCaches()
	}
}
