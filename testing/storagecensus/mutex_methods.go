package storagecensus

// Author: #1667 implementation lane (Codex). Domain data for retained mutex
// owners; generic checked-call extraction lives in workcensus. These contracts
// describe acquisitions, never exempt an effect or grant callback coverage.

type retainedMutexMethod struct {
	errorIndex int // -1: infallible exclusive acquisition; otherwise nil-success result.
	release    bool
}

var retainedMutexMethods = map[string]retainedMutexMethod{
	"sync.Mutex.Lock":                        {errorIndex: -1},
	"sync.Mutex.Unlock":                      {release: true},
	"sync.RWMutex.Lock":                      {errorIndex: -1},
	"sync.RWMutex.Unlock":                    {release: true},
	"pkg/storage.trackedRWMutex.Lock":        {errorIndex: -1},
	"pkg/storage.trackedRWMutex.LockContext": {errorIndex: 0},
	"pkg/storage.trackedRWMutex.Unlock":      {release: true},
}
