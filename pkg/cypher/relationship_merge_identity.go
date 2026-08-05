package cypher

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const relationshipMergeEdgeIDDomain = "nornicdb:cypher:relationship-merge:v1\x00"

// relationshipMergeIdentityKey returns the full Cypher MERGE identity for a
// bound relationship pattern. Relationship properties in the pattern are
// identity fields; later SET assignments are not.
func relationshipMergeIdentityKey(
	startID storage.NodeID,
	endID storage.NodeID,
	relType string,
	matchProps map[string]interface{},
) string {
	normalized := make(map[string]interface{}, len(matchProps))
	for key, value := range matchProps {
		normalized[key] = normalizeRelationshipMergeIdentityValue(value)
	}
	return string(startID) + "\x00" + relType + "\x00" + string(endID) + "\x00" + unwindMergeKey("", normalized)
}

// normalizeRelationshipMergeIdentityValue canonicalizes values for identity
// comparison and hashing without changing the property representation stored
// on the edge. Badger restores homogeneous arrays as typed slices, so treating
// the concrete Go slice type as identity would make MERGE non-idempotent after
// a restart.
func normalizeRelationshipMergeIdentityValue(value interface{}) interface{} {
	normalized := normalizePropValue(value)
	if number, ok := normalized.(float64); ok &&
		!math.IsNaN(number) && !math.IsInf(number, 0) &&
		math.Trunc(number) == number && number >= -(1<<63) && number < (1<<63) {
		return int64(number)
	}
	if props, ok := normalized.(map[string]interface{}); ok {
		out := make(map[string]interface{}, len(props))
		for key, item := range props {
			out[key] = normalizeRelationshipMergeIdentityValue(item)
		}
		return out
	}

	rv := reflect.ValueOf(normalized)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return normalized
	}
	out := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = normalizeRelationshipMergeIdentityValue(rv.Index(i).Interface())
	}
	return out
}

func relationshipMergeIdentityContainsNaN(value interface{}) bool {
	switch typed := value.(type) {
	case float32:
		return math.IsNaN(float64(typed))
	case float64:
		return math.IsNaN(typed)
	case map[string]interface{}:
		for _, item := range typed {
			if relationshipMergeIdentityContainsNaN(item) {
				return true
			}
		}
		return false
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || (rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array) {
		return false
	}
	for i := 0; i < rv.Len(); i++ {
		if relationshipMergeIdentityContainsNaN(rv.Index(i).Interface()) {
			return true
		}
	}
	return false
}

// deterministicRelationshipMergeEdgeID makes concurrent creation of the same MERGE
// identity contend on one storage key. Different property identities keep
// independent keys and therefore do not serialize each other.
func deterministicRelationshipMergeEdgeID(
	startID storage.NodeID,
	endID storage.NodeID,
	relType string,
	matchProps map[string]interface{},
	collisionOrdinal int,
) storage.EdgeID {
	identity := relationshipMergeIdentityKey(startID, endID, relType, matchProps)
	sum := sha256.Sum256([]byte(
		relationshipMergeEdgeIDDomain + identity + "\x00" + strconv.Itoa(collisionOrdinal),
	))
	return storage.EdgeID("merge-" + hex.EncodeToString(sum[:]))
}

func (e *StorageExecutor) newRelationshipMergeEdgeID(
	startID storage.NodeID,
	endID storage.NodeID,
	relType string,
	matchProps map[string]interface{},
) storage.EdgeID {
	if len(matchProps) == 0 || relationshipMergeIdentityContainsNaN(matchProps) {
		return storage.EdgeID(e.generateID())
	}
	return deterministicRelationshipMergeEdgeID(startID, endID, relType, matchProps, 0)
}

func createRelationshipForMerge(
	e *StorageExecutor,
	store storage.Engine,
	edge *storage.Edge,
	matchProps map[string]interface{},
) (*storage.Edge, bool, error) {
	const maxBareCreateAttempts = 3
	bareAttempts := 0

	for {
		nonReflexiveIdentity := relationshipMergeIdentityContainsNaN(matchProps)
		if len(matchProps) > 0 && !nonReflexiveIdentity {
			existing, err := selectRelationshipMergeCreateID(store, edge, matchProps)
			if err != nil {
				return nil, false, err
			}
			if existing != nil {
				return existing, false, nil
			}
		}
		if err := store.CreateEdge(edge); err != nil {
			if err != storage.ErrAlreadyExists {
				return nil, false, err
			}
			existing, lookupErr := findRelationshipForMerge(
				store,
				edge.StartNode,
				edge.EndNode,
				edge.Type,
				matchProps,
			)
			if lookupErr != nil {
				return nil, false, lookupErr
			}
			if existing != nil {
				return existing, false, nil
			}

			if len(matchProps) == 0 || nonReflexiveIdentity {
				bareAttempts++
				if bareAttempts >= maxBareCreateAttempts {
					return nil, false, fmt.Errorf(
						"relationship MERGE create failed after %d edge ID collisions",
						maxBareCreateAttempts,
					)
				}
				edge.ID = storage.EdgeID(e.generateID())
				continue
			}

			continue
		}
		return edge, true, nil
	}
}

// selectRelationshipMergeCreateID avoids overwriting a deterministic edge ID
// whose original pattern properties were later changed by SET. The point read
// is required before transactional CreateEdge because a transaction can buffer
// a snapshot-visible committed ID without returning ErrAlreadyExists.
func selectRelationshipMergeCreateID(
	store storage.Engine,
	edge *storage.Edge,
	matchProps map[string]interface{},
) (*storage.Edge, error) {
	if len(matchProps) == 0 {
		return nil, nil
	}

	candidate, err := store.GetEdge(edge.ID)
	if err == nil {
		if candidate.StartNode == edge.StartNode &&
			candidate.EndNode == edge.EndNode &&
			relationshipMatchesMergePattern(candidate, edge.Type, matchProps) {
			return candidate, nil
		}
	} else if err != storage.ErrNotFound {
		return nil, err
	} else {
		return nil, nil
	}

	edges, err := store.GetEdgesBetween(edge.StartNode, edge.EndNode)
	if err != nil {
		return nil, err
	}
	firstOrdinal := len(edges)
	for offset := 0; offset <= len(edges); offset++ {
		edge.ID = deterministicRelationshipMergeEdgeID(
			edge.StartNode,
			edge.EndNode,
			edge.Type,
			matchProps,
			firstOrdinal+offset,
		)
		candidate, err = store.GetEdge(edge.ID)
		switch {
		case err == storage.ErrNotFound:
			return nil, nil
		case err != nil:
			return nil, err
		case candidate.StartNode == edge.StartNode &&
			candidate.EndNode == edge.EndNode &&
			relationshipMatchesMergePattern(candidate, edge.Type, matchProps):
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("relationship MERGE property identity has no free storage key")
}

func relationshipMatchesMergePattern(
	edge *storage.Edge,
	relType string,
	matchProps map[string]interface{},
) bool {
	if edge == nil || edge.Type != relType {
		return false
	}
	for key, want := range matchProps {
		got, ok := edge.Properties[key]
		if !ok || !relationshipMergeValuesEqual(got, want) {
			return false
		}
	}
	return true
}

func relationshipMergeValuesEqual(got, want interface{}) bool {
	got = normalizeRelationshipMergeIdentityValue(got)
	want = normalizeRelationshipMergeIdentityValue(want)
	if relationshipMergeIdentityContainsNaN(got) || relationshipMergeIdentityContainsNaN(want) {
		return false
	}
	if reflect.DeepEqual(got, want) {
		return true
	}
	return reflect.DeepEqual(canonicalUnwindMergeValue(got), canonicalUnwindMergeValue(want))
}

func findRelationshipForMerge(
	store storage.Engine,
	startID storage.NodeID,
	endID storage.NodeID,
	relType string,
	matchProps map[string]interface{},
) (*storage.Edge, error) {
	if len(matchProps) == 0 {
		return store.GetEdgeBetween(startID, endID, relType), nil
	}
	if relationshipMergeIdentityContainsNaN(matchProps) {
		return nil, nil
	}
	candidateID := deterministicRelationshipMergeEdgeID(startID, endID, relType, matchProps, 0)
	candidate, err := store.GetEdge(candidateID)
	if err == nil && candidate.StartNode == startID && candidate.EndNode == endID &&
		relationshipMatchesMergePattern(candidate, relType, matchProps) {
		return candidate, nil
	}
	if err != nil && err != storage.ErrNotFound {
		return nil, err
	}

	edges, err := store.GetEdgesBetween(startID, endID)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if relationshipMatchesMergePattern(edge, relType, matchProps) {
			return edge, nil
		}
	}
	return nil, nil
}
