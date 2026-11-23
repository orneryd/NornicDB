package storage

import "context"

type pendingRelationshipUniqueScope struct {
	edgeType   string
	properties string
}

type pendingRelationshipUniqueBuckets struct {
	scalar map[any][]*Edge
	other  []*Edge
}

// pendingRelationshipUniqueRead selects candidates by the canonical scalar key tuple
// over the immutable pending set. Exact constraint equality still decides
// collisions for the entire tuple. Values without scalar keys stay candidates
// for every comparable key.
type pendingRelationshipUniqueRead map[pendingRelationshipUniqueScope]pendingRelationshipUniqueBuckets

func (r *pendingRelationshipUniqueRead) get(
	ctx context.Context, tx *BadgerTransaction, scope pendingRelationshipUniqueScope, properties []string,
) (pendingRelationshipUniqueBuckets, error) {
	if err := ctx.Err(); err != nil {
		return pendingRelationshipUniqueBuckets{}, err
	}
	if buckets, found := (*r)[scope]; found {
		return buckets, nil
	}
	buckets := pendingRelationshipUniqueBuckets{scalar: make(map[any][]*Edge)}
	var cancel bulkReadCancellation
	for _, edge := range tx.pendingEdges {
		if err := cancel.check(ctx); err != nil {
			return pendingRelationshipUniqueBuckets{}, err
		}
		if edge.Type != scope.edgeType {
			continue
		}
		key, supported, err := pendingRelationshipUniqueKey(ctx, edge.Properties, properties)
		if err != nil {
			return pendingRelationshipUniqueBuckets{}, err
		}
		if supported {
			buckets.scalar[key] = append(buckets.scalar[key], edge)
		} else {
			buckets.other = append(buckets.other, edge)
		}
	}
	if err := ctx.Err(); err != nil {
		return pendingRelationshipUniqueBuckets{}, err
	}
	if *r == nil {
		*r = make(pendingRelationshipUniqueRead)
	}
	(*r)[scope] = buckets
	return buckets, nil
}

func pendingRelationshipUniqueKey(ctx context.Context, values map[string]interface{}, properties []string) (any, bool, error) {
	if len(properties) == 1 {
		key, ok := transactionEqualityKey(values[properties[0]])
		return key, ok, nil
	}
	parts := make([]any, len(properties))
	for index, property := range properties {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		value := values[property]
		if _, supported := transactionEqualityKey(value); !supported {
			return nil, false, nil
		}
		parts[index] = value
	}
	return compositeIndexKeyOf(ctx, parts)
}
