package cypher

import (
	"runtime"
	"sync"
)

func lockQueryTextCache(mutex *sync.RWMutex, read bool, probe *pipelineCancellation) bool {
	if probe == nil || probe.ctx.Done() == nil {
		if read {
			mutex.RLock()
		} else {
			mutex.Lock()
		}
		return true
	}
	for {
		if !probe.check() {
			return false
		}
		if read {
			if mutex.TryRLock() {
				return true
			}
		} else if mutex.TryLock() {
			return true
		}
		runtime.Gosched()
	}
}

func putQueryTextCache[T any](index *pipelineStringMap[T], limit int, key string, value T, probe *pipelineCancellation) bool {
	if limit <= 0 {
		return true
	}
	if index.Len() >= limit {
		_, exists := index.GetWithCancellation(key, probe)
		if probe != nil && probe.err != nil {
			return false
		}
		if !exists {
			old, found := index.FirstKeyWithCancellation(probe)
			if !found || !index.DeleteWithCancellation(old, probe) {
				return false
			}
		}
	}
	return index.SetWithCancellation(key, value, probe)
}

func getQueryTextCache[T any](index *pipelineStringMap[T], mutex *sync.RWMutex, key string, probe *pipelineCancellation) (value T, found bool) {
	if !lockQueryTextCache(mutex, true, probe) {
		return value, false
	}
	defer mutex.RUnlock()
	return index.GetWithCancellation(key, probe)
}
