package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/vectorspace"
)

// registerVectorSpace maps a Cypher vector index to a vector space in the registry.
func (e *StorageExecutor) registerVectorSpace(indexName, label, property string, dims int, similarity string) {
	if e.vectorRegistry == nil || dims <= 0 {
		return
	}
	if e.vectorIndexSpaces == nil {
		e.vectorIndexSpaces = make(map[string]vectorspace.VectorSpaceKey)
	}

	vectorName := strings.TrimSpace(property)
	if vectorName == "" {
		vectorName = vectorspace.DefaultVectorName
	}

	distance, err := toDistanceMetric(similarity)
	if err != nil {
		return
	}

	key := vectorspace.VectorSpaceKey{
		DB:         e.databaseName(),
		Type:       label,
		VectorName: vectorName,
		Dims:       dims,
		Distance:   distance,
	}

	if canonical, err := key.Canonical(); err == nil {
		e.vectorIndexSpaces[indexName] = canonical
		_, _ = e.vectorRegistry.CreateSpace(canonical, vectorspace.BackendAuto)
	}
}

func (e *StorageExecutor) unregisterVectorSpace(indexName string) {
	if e.vectorRegistry == nil {
		return
	}
	if key, ok := e.vectorIndexSpaces[indexName]; ok {
		e.vectorRegistry.DeleteSpace(key)
		delete(e.vectorIndexSpaces, indexName)
	}
}

func (e *StorageExecutor) databaseName() string {
	if nsEngine, ok := e.storage.(*storage.NamespacedEngine); ok {
		return nsEngine.Namespace()
	}

	conf := config.LoadFromEnv()
	if conf != nil && conf.Database.DefaultDatabase != "" {
		return conf.Database.DefaultDatabase
	}
	return "nornic"
}

func toDistanceMetric(similarity string) (vectorspace.DistanceMetric, error) {
	switch strings.ToLower(strings.TrimSpace(similarity)) {
	case "", "cosine":
		return vectorspace.DistanceCosine, nil
	case "dot":
		return vectorspace.DistanceDot, nil
	case "euclidean":
		return vectorspace.DistanceEuclidean, nil
	default:
		return "", fmt.Errorf("unsupported similarity %q", similarity)
	}
}
