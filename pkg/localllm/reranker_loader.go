package localllm

import (
	"context"
	"sync"
)

// RerankerBackend supplies scoring and disposal for an installed backend.
// LoadRerankerModel uses native GGUF loading unless a loader is supplied.
type RerankerBackend interface {
	Score(context.Context, string, string) (float32, error)
	Close() error
}

// RerankerLoader constructs a backend. The caller owns the returned backend.
type RerankerLoader func(Options) (RerankerBackend, error)

var rerankerLoaderMu sync.RWMutex
var rerankerLoader RerankerLoader

// SetRerankerLoader replaces construction, returning the previous loader. Set
// this before constructing servers; nil restores the platform's native loader.
func SetRerankerLoader(loader RerankerLoader) RerankerLoader {
	rerankerLoaderMu.Lock()
	previous := rerankerLoader
	rerankerLoader = loader
	rerankerLoaderMu.Unlock()
	return previous
}
func loadRerankerBackend(opts Options) (*RerankerModel, bool, error) {
	rerankerLoaderMu.RLock()
	loader := rerankerLoader
	rerankerLoaderMu.RUnlock()
	if loader == nil {
		return nil, false, nil
	}
	backend, err := loader(opts)
	if err != nil {
		return nil, true, err
	}
	return &RerankerModel{backend: backend}, true, nil
}
