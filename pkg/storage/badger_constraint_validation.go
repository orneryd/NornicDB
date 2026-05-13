package storage

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// compareValues compares two property values for equality.
// This mirrors Cypher semantics for numeric comparisons across int/float types.
func compareValues(a, b interface{}) bool {
	if aNum, ok := numericConstraintValue(a); ok {
		if bNum, ok := numericConstraintValue(b); ok {
			return aNum == bNum
		}
	}

	// Handle different numeric types
	switch v1 := a.(type) {
	case int:
		switch v2 := b.(type) {
		case int:
			return v1 == v2
		case int64:
			return int64(v1) == v2
		case float64:
			return float64(v1) == v2
		}
	case int64:
		switch v2 := b.(type) {
		case int:
			return v1 == int64(v2)
		case int64:
			return v1 == v2
		case float64:
			return float64(v1) == v2
		}
	case float64:
		switch v2 := b.(type) {
		case int:
			return v1 == float64(v2)
		case int64:
			return v1 == float64(v2)
		case float64:
			return v1 == v2
		}
	case string:
		if v2, ok := b.(string); ok {
			return v1 == v2
		}
	case bool:
		if v2, ok := b.(bool); ok {
			return v1 == v2
		}
	}

	// Default comparison
	if a == nil || b == nil {
		return a == b
	}
	if !reflect.TypeOf(a).Comparable() || !reflect.TypeOf(b).Comparable() {
		return reflect.DeepEqual(a, b)
	}
	return a == b
}

func numericConstraintValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func (b *BadgerEngine) validateNodeConstraintsInTxn(txn *badger.Txn, node *Node, schema *SchemaManager, namespace string, excludeNodeID NodeID) error {
	if node == nil || schema == nil {
		return nil
	}

	constraints := schema.GetConstraintsForLabels(node.Labels)
	for _, c := range constraints {
		switch c.Type {
		case ConstraintUnique:
			if len(c.Properties) != 1 {
				continue
			}
			prop := c.Properties[0]
			value := node.Properties[prop]
			if value == nil {
				continue
			}
			if existingNode, found, cacheComplete, constrained := schema.lookupUniqueConstraintValueForValidation(c.Label, prop, value); constrained && cacheComplete {
				if found && existingNode != excludeNodeID {
					return uniqueConstraintViolation(c.Label, prop, value, existingNode)
				}
				continue
			}
			if err := b.scanForUniqueViolationInTxn(txn, namespace, c.Label, prop, value, excludeNodeID); err != nil {
				return err
			}
		case ConstraintNodeKey:
			values := make([]interface{}, len(c.Properties))
			for i, prop := range c.Properties {
				values[i] = node.Properties[prop]
				if values[i] == nil {
					return &ConstraintViolationError{
						Type:       ConstraintNodeKey,
						Label:      c.Label,
						Properties: c.Properties,
						Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
					}
				}
			}
			if err := b.scanForNodeKeyViolationInTxn(txn, namespace, c.Label, c.Properties, values, excludeNodeID); err != nil {
				return err
			}
		case ConstraintExists:
			if len(c.Properties) != 1 {
				continue
			}
			prop := c.Properties[0]
			if node.Properties == nil {
				return &ConstraintViolationError{
					Type:       ConstraintExists,
					Label:      c.Label,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Required property %s is missing", prop),
				}
			}
			if val, ok := node.Properties[prop]; !ok || val == nil {
				return &ConstraintViolationError{
					Type:       ConstraintExists,
					Label:      c.Label,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Required property %s is missing", prop),
				}
			}
		case ConstraintTemporal:
			if len(c.Properties) != 3 {
				return fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
			}
			keyProp := c.Properties[0]
			startProp := c.Properties[1]
			endProp := c.Properties[2]

			keyVal := node.Properties[keyProp]
			if keyVal == nil {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", keyProp),
				}
			}
			start, ok := coerceTemporalTime(node.Properties[startProp])
			if !ok {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
				}
			}
			end, hasEnd := coerceTemporalTime(node.Properties[endProp])

			if err := b.scanForTemporalOverlapInTxn(txn, namespace, c.Label, keyProp, startProp, endProp, keyVal, start, end, hasEnd, excludeNodeID); err != nil {
				return err
			}
		case ConstraintDomain:
			if len(c.Properties) == 1 && len(c.AllowedValues) > 0 {
				prop := c.Properties[0]
				value := node.Properties[prop]
				if value != nil && !isValueInAllowedList(value, c.AllowedValues) {
					return &ConstraintViolationError{
						Type:       ConstraintDomain,
						Label:      c.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Property %s value %v is not in allowed values %v", prop, value, c.AllowedValues),
					}
				}
			}
		}
	}

	typeConstraints := schema.GetPropertyTypeConstraintsForLabels(node.Labels)
	for _, c := range typeConstraints {
		value := node.Properties[c.Property]
		if err := ValidatePropertyType(value, c.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      c.Label,
				Properties: []string{c.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", c.Property, c.ExpectedType, err),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForUniqueViolationInTxn(txn *badger.Txn, namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}

	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		nodeNum, ok := extractNodeNumIDFromLabelIndex(key, labelLen)
		if !ok {
			continue
		}
		nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
		if !ok || nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		if existingValue, ok := existingNode.Properties[property]; ok {
			if compareValues(existingValue, value) {
				return &ConstraintViolationError{
					Type:       ConstraintUnique,
					Label:      label,
					Properties: []string{property},
					Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, existingNode.ID),
				}
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForNodeKeyViolationInTxn(txn *badger.Txn, namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		nodeNum, ok := extractNodeNumIDFromLabelIndex(key, labelLen)
		if !ok {
			continue
		}
		nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
		if !ok || nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}
		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      label,
				Properties: properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists (nodeID: %s)", properties, values, existingNode.ID),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) legacyScanForTemporalOverlapInTxn(txn *badger.Txn, namespace, label, keyProp, startProp, endProp string, keyValue interface{}, start time.Time, end time.Time, hasEnd bool, excludeNodeID NodeID) error {
	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		nodeNum, ok := extractNodeNumIDFromLabelIndex(key, labelLen)
		if !ok {
			continue
		}
		nodeID, ok := b.idDict.lookupNodeIDByNum(nodeNum)
		if !ok || nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		existingKey, ok := existingNode.Properties[keyProp]
		if !ok || existingKey == nil {
			continue
		}
		if !compareValues(existingKey, keyValue) {
			continue
		}

		existingStart, ok := coerceTemporalTime(existingNode.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, existingNode.ID),
			}
		}
		existingEnd, existingHasEnd := coerceTemporalTime(existingNode.Properties[endProp])

		if intervalsOverlap(temporalInterval{
			start:  start,
			end:    end,
			hasEnd: hasEnd,
		}, temporalInterval{
			start:  existingStart,
			end:    existingEnd,
			hasEnd: existingHasEnd,
		}) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					existingNode.ID, keyProp, keyValue),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForTemporalOverlapInTxn(txn *badger.Txn, namespace, label, keyProp, startProp, endProp string, keyValue interface{}, start time.Time, end time.Time, hasEnd bool, excludeNodeID NodeID) error {
	constraint := Constraint{
		Type:       ConstraintTemporal,
		Label:      label,
		Properties: []string{keyProp, startProp, endProp},
	}
	target := temporalRefreshTarget{
		constraint: constraint,
		desc:       makeTemporalDescriptor(namespace, constraint, keyValue),
		keyValue:   keyValue,
	}
	prefix := temporalHistoryPrefix(target.desc)
	hasIndexedEntries := false
	it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
	for it.Rewind(); it.Valid(); it.Next() {
		hasIndexedEntries = true
		break
	}
	it.Close()
	if !hasIndexedEntries {
		return b.legacyScanForTemporalOverlapInTxn(txn, namespace, label, keyProp, startProp, endProp, keyValue, start, end, hasEnd, excludeNodeID)
	}

	prevNode, nextNode, err := b.temporalAdjacentNodesInTxn(txn, target, start, excludeNodeID)
	if err != nil {
		return err
	}
	if prevNode != nil {
		_, prevStart, prevEnd, prevHasEnd, ok := temporalNodeState(prevNode, constraint)
		if ok && intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: prevStart, end: prevEnd, hasEnd: prevHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					prevNode.ID, keyProp, keyValue),
			}
		}
	}
	if nextNode != nil {
		_, nextStart, nextEnd, nextHasEnd, ok := temporalNodeState(nextNode, constraint)
		if ok && intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: nextStart, end: nextEnd, hasEnd: nextHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					nextNode.ID, keyProp, keyValue),
			}
		}
	}

	return nil
}

func constraintValueKey(value interface{}) string {
	return NewCompositeKey(value).Hash
}

func constraintCompositeKey(values []interface{}) string {
	return NewCompositeKey(values...).Hash
}

// validateEdgeConstraintsInTxn checks all relationship constraints for an edge within a transaction.
// The excludeEdgeID allows excluding the edge being updated from uniqueness checks.
func (b *BadgerEngine) validateEdgeConstraintsInTxn(txn *badger.Txn, edge *Edge, schema *SchemaManager, namespace string, excludeEdgeID EdgeID) error {
	if edge == nil || schema == nil || edge.Type == "" {
		return nil
	}

	constraints := schema.GetConstraintsForLabels([]string{edge.Type})
	for _, c := range constraints {
		if c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		switch c.Type {
		case ConstraintUnique:
			if err := b.checkEdgeUniquenessInTxn(txn, edge, c, namespace, excludeEdgeID); err != nil {
				return err
			}
		case ConstraintExists:
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
		case ConstraintRelationshipKey:
			// Relationship key = existence + uniqueness
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
			if err := b.checkEdgeUniquenessInTxn(txn, edge, c, namespace, excludeEdgeID); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := b.checkEdgeTemporalInTxn(txn, edge, c, namespace, excludeEdgeID); err != nil {
				return err
			}
		case ConstraintDomain:
			if len(c.Properties) == 1 && len(c.AllowedValues) > 0 {
				prop := c.Properties[0]
				value := edge.Properties[prop]
				if value != nil && !isValueInAllowedList(value, c.AllowedValues) {
					return &ConstraintViolationError{
						Type:       ConstraintDomain,
						Label:      edge.Type,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Property %s value %v is not in allowed values %v", prop, value, c.AllowedValues),
					}
				}
			}
		case ConstraintCardinality:
			if err := b.checkEdgeCardinalityInTxn(txn, edge, c, namespace, excludeEdgeID); err != nil {
				return err
			}
		}
	}

	// Policy constraints must be evaluated as a set (all ALLOWED policies form a union).
	if err := b.checkEdgePolicyInTxn(txn, edge, schema, namespace); err != nil {
		return err
	}

	ptConstraints := schema.GetPropertyTypeConstraintsForLabels([]string{edge.Type})
	for _, ptc := range ptConstraints {
		if ptc.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		value := edge.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      edge.Type,
				Properties: []string{ptc.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", ptc.Property, ptc.ExpectedType, err),
			}
		}
	}

	return nil
}

// checkEdgeExistence validates that all required properties exist on the edge.
func checkEdgeExistence(edge *Edge, c Constraint) error {
	for _, prop := range c.Properties {
		if edge.Properties == nil {
			return &ConstraintViolationError{
				Type:       c.Type,
				Label:      edge.Type,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Required property %s is missing on relationship", prop),
			}
		}
		if val, ok := edge.Properties[prop]; !ok || val == nil {
			return &ConstraintViolationError{
				Type:       c.Type,
				Label:      edge.Type,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Required property %s is missing on relationship", prop),
			}
		}
	}
	return nil
}

// checkEdgeUniquenessInTxn scans all edges of the same type to check for uniqueness violations.
func (b *BadgerEngine) checkEdgeUniquenessInTxn(txn *badger.Txn, edge *Edge, c Constraint, namespace string, excludeEdgeID EdgeID) error {
	// Scan via edge type index
	prefix := edgeTypeIndexPrefix(edge.Type)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	nsPrefix := namespace + ":"

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		edgeNum, ok := extractEdgeNumIDFromEdgeTypeKey(key)
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok || edgeID == "" || edgeID == excludeEdgeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(edgeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			continue
		}

		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingEdge, err := b.decodeEdgeBodyWithID(edgeBytes, edgeID)
		if err != nil {
			continue
		}

		if len(c.Properties) == 1 {
			prop := c.Properties[0]
			newVal := edge.Properties[prop]
			if newVal == nil {
				return nil // NULL doesn't violate uniqueness
			}
			existVal := existingEdge.Properties[prop]
			if existVal != nil && compareValues(existVal, newVal) {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Relationship with %s=%v already exists (edgeID: %s)", prop, newVal, existingEdge.ID),
				}
			}
		} else {
			// Composite uniqueness
			allMatch := true
			allPresent := true
			for _, prop := range c.Properties {
				newVal := edge.Properties[prop]
				if newVal == nil {
					allPresent = false
					break
				}
				existVal := existingEdge.Properties[prop]
				if existVal == nil || !compareValues(existVal, newVal) {
					allMatch = false
					break
				}
			}
			if !allPresent {
				return nil // NULL in composite key doesn't violate uniqueness
			}
			if allMatch {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: c.Properties,
					Message:    fmt.Sprintf("Relationship with duplicate composite key already exists (edgeID: %s)", existingEdge.ID),
				}
			}
		}
	}
	return nil
}

// edgeTemporalCompositeKeyMatch checks whether the existing edge has the same composite key values as keyVals.
func edgeTemporalCompositeKeyMatch(existingEdge *Edge, keyProps []string, keyVals []interface{}) bool {
	for i, prop := range keyProps {
		existingVal := existingEdge.Properties[prop]
		if existingVal == nil || !compareValues(existingVal, keyVals[i]) {
			return false
		}
	}
	return true
}

// checkEdgeTemporalInTxn scans all edges of the same type to check for temporal overlap violations.
// Supports composite key properties: all properties except the last 2 form the key.
func (b *BadgerEngine) checkEdgeTemporalInTxn(txn *badger.Txn, edge *Edge, c Constraint, namespace string, excludeEdgeID EdgeID) error {
	if len(c.Properties) < 3 {
		return fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
	}

	keyProps := c.Properties[:len(c.Properties)-2]
	startProp := c.Properties[len(c.Properties)-2]
	endProp := c.Properties[len(c.Properties)-1]

	// Validate all key properties are non-null
	keyVals := make([]interface{}, len(keyProps))
	for i, prop := range keyProps {
		keyVals[i] = edge.Properties[prop]
		if keyVals[i] == nil {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      edge.Type,
				Properties: c.Properties,
				Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", prop),
			}
		}
	}

	start, ok := coerceTemporalTime(edge.Properties[startProp])
	if !ok {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      edge.Type,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
		}
	}
	end, hasEnd := coerceTemporalTime(edge.Properties[endProp])

	newInterval := temporalInterval{start: start, end: end, hasEnd: hasEnd}

	// Scan via edge type index
	prefix := edgeTypeIndexPrefix(edge.Type)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	nsPrefix := namespace + ":"

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().KeyCopy(nil)
		edgeNum, ok := extractEdgeNumIDFromEdgeTypeKey(key)
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok || edgeID == "" || edgeID == excludeEdgeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(edgeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			continue
		}

		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingEdge, err := b.decodeEdgeBodyWithID(edgeBytes, edgeID)
		if err != nil {
			continue
		}

		if !edgeTemporalCompositeKeyMatch(existingEdge, keyProps, keyVals) {
			continue
		}

		existingStart, ok := coerceTemporalTime(existingEdge.Properties[startProp])
		if !ok {
			continue
		}
		existingEnd, existingHasEnd := coerceTemporalTime(existingEdge.Properties[endProp])

		existingInterval := temporalInterval{start: existingStart, end: existingEnd, hasEnd: existingHasEnd}

		if intervalsOverlap(newInterval, existingInterval) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      edge.Type,
				Properties: c.Properties,
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with edge %s for key=%v",
					existingEdge.ID, keyVals),
			}
		}
	}

	return nil
}

// checkEdgeCardinalityInTxn counts edges of the given type connected to the anchor
// node (determined by constraint direction) and rejects the new edge if adding it
// would exceed MaxCount.
func (b *BadgerEngine) checkEdgeCardinalityInTxn(txn *badger.Txn, edge *Edge, c Constraint, namespace string, excludeEdgeID EdgeID) error {
	// Determine anchor node based on direction.
	var anchorNode NodeID
	if c.Direction == "OUTGOING" {
		anchorNode = NodeID(edge.StartNode)
	} else {
		anchorNode = NodeID(edge.EndNode)
	}

	// Choose the index prefix based on direction.
	var prefix []byte
	if c.Direction == "OUTGOING" {
		prefix = b.outgoingIndexPrefixString(anchorNode)
	} else {
		prefix = b.incomingIndexPrefixString(anchorNode)
	}
	if prefix == nil {
		// Anchor node has no numID → no adjacent edges indexed.
		return nil
	}

	count := 0
	nsPrefix := namespace + ":"

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(iter.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok || edgeID == "" || edgeID == excludeEdgeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(edgeID), nsPrefix) {
			continue
		}

		// Read the edge to check its type.
		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			continue
		}
		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}
		existingEdge, err := b.decodeEdgeBodyWithID(edgeBytes, edgeID)
		if err != nil {
			continue
		}
		if existingEdge.Type == c.Label {
			count++
			if count >= c.MaxCount {
				dir := strings.ToLower(c.Direction)
				return &ConstraintViolationError{
					Type:  ConstraintCardinality,
					Label: c.Label,
					Message: fmt.Sprintf("Adding this edge would exceed max %s count of %d for relationship type %s on node %s (current: %d)",
						dir, c.MaxCount, c.Label, anchorNode, count),
				}
			}
		}
	}

	return nil
}

// checkEdgePolicyInTxn validates DISALLOWED and ALLOWED endpoint policies for an edge.
// All policies for the edge type are evaluated as a set: DISALLOWED policies are checked
// individually, while ALLOWED policies form a union (at least one must match).
func (b *BadgerEngine) checkEdgePolicyInTxn(txn *badger.Txn, edge *Edge, schema *SchemaManager, namespace string) error {
	constraints := schema.GetConstraintsForLabels([]string{edge.Type})

	var allowedPolicies []Constraint
	var disallowedPolicies []Constraint
	for _, c := range constraints {
		if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		if c.PolicyMode == "ALLOWED" {
			allowedPolicies = append(allowedPolicies, c)
		} else if c.PolicyMode == "DISALLOWED" {
			disallowedPolicies = append(disallowedPolicies, c)
		}
	}

	if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
		return nil
	}

	// Read source node labels.
	srcLabels, err := b.readNodeLabelsInTxn(txn, NodeID(edge.StartNode))
	if err != nil {
		return nil // If node can't be read, skip policy check (other validation catches missing nodes)
	}

	// Read target node labels.
	tgtLabels, err := b.readNodeLabelsInTxn(txn, NodeID(edge.EndNode))
	if err != nil {
		return nil
	}

	// Check DISALLOWED policies first (they take precedence).
	for _, c := range disallowedPolicies {
		if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
			return &ConstraintViolationError{
				Type:  ConstraintPolicy,
				Label: c.Label,
				Message: fmt.Sprintf("DISALLOWED policy violation: (:%s)-[:%s]->(:%s) is forbidden (constraint %q)",
					c.SourceLabel, c.Label, c.TargetLabel, c.Name),
			}
		}
	}

	// Check ALLOWED policies: if any exist, at least one must match.
	if len(allowedPolicies) > 0 {
		matched := false
		for _, c := range allowedPolicies {
			if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
				matched = true
				break
			}
		}
		if !matched {
			return &ConstraintViolationError{
				Type:  ConstraintPolicy,
				Label: edge.Type,
				Message: fmt.Sprintf("ALLOWED policy violation: no ALLOWED policy permits (:%s)-[:%s]->(:%s)",
					strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":")),
			}
		}
	}

	return nil
}

// readNodeLabelsInTxn reads a node's labels from within a Badger transaction.
func (b *BadgerEngine) readNodeLabelsInTxn(txn *badger.Txn, nodeID NodeID) ([]string, error) {
	item, err := txn.Get(nodeKey(nodeID))
	if err != nil {
		return nil, err
	}
	var nodeBytes []byte
	if err := item.Value(func(val []byte) error {
		nodeBytes = append([]byte{}, val...)
		return nil
	}); err != nil {
		return nil, err
	}
	node, err := decodeNode(nodeBytes)
	if err != nil {
		return nil, err
	}
	return node.Labels, nil
}

// validatePolicyOnNodeLabelChangeInTxn checks all policy constraints on edges connected
// to a node whose labels are changing. This enforces that label mutations (SET n:Label,
// REMOVE n:Label) don't violate endpoint policies.
func (b *BadgerEngine) validatePolicyOnNodeLabelChangeInTxn(txn *badger.Txn, node *Node, oldNode *Node, schema *SchemaManager, namespace string) error {
	if schema == nil || node == nil || oldNode == nil {
		return nil
	}
	// Quick check: did labels actually change?
	if labelsEqual(node.Labels, oldNode.Labels) {
		return nil
	}

	// Check all edges connected to this node.
	return b.validatePolicyForAdjacentEdgesInTxn(txn, node, schema, namespace)
}

// validatePolicyForAdjacentEdgesInTxn scans all outgoing and incoming edges for a node
// and re-validates policy constraints with the node's current labels.
func (b *BadgerEngine) validatePolicyForAdjacentEdgesInTxn(txn *badger.Txn, node *Node, schema *SchemaManager, namespace string) error {
	if outPrefix := b.outgoingIndexPrefixString(node.ID); outPrefix != nil {
		if err := b.validatePolicyForEdgesWithPrefixInTxn(txn, outPrefix, node, true, schema, namespace); err != nil {
			return err
		}
	}
	if inPrefix := b.incomingIndexPrefixString(node.ID); inPrefix != nil {
		return b.validatePolicyForEdgesWithPrefixInTxn(txn, inPrefix, node, false, schema, namespace)
	}
	return nil
}

// validatePolicyForEdgesWithPrefixInTxn scans edges with the given index prefix and
// validates policy constraints. isOutgoing indicates whether the node is the source (true)
// or target (false) of the edges.
func (b *BadgerEngine) validatePolicyForEdgesWithPrefixInTxn(txn *badger.Txn, prefix []byte, node *Node, isOutgoing bool, schema *SchemaManager, namespace string) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(iter.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := b.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok || edgeID == "" {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(edgeID), nsPrefix) {
			continue
		}

		// Read edge.
		item, err := txn.Get(edgeKey(edgeID))
		if err != nil {
			continue
		}
		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}
		edge, err := b.decodeEdgeBodyWithID(edgeBytes, edgeID)
		if err != nil {
			continue
		}

		// Get the other node's labels.
		var otherNodeID NodeID
		if isOutgoing {
			otherNodeID = NodeID(edge.EndNode)
		} else {
			otherNodeID = NodeID(edge.StartNode)
		}
		otherLabels, err := b.readNodeLabelsInTxn(txn, otherNodeID)
		if err != nil {
			continue
		}

		// Determine source/target labels based on edge direction.
		var srcLabels, tgtLabels []string
		if isOutgoing {
			srcLabels = node.Labels
			tgtLabels = otherLabels
		} else {
			srcLabels = otherLabels
			tgtLabels = node.Labels
		}

		// Check policy constraints for this edge type.
		constraints := schema.GetConstraintsForLabels([]string{edge.Type})
		var allowedPolicies []Constraint
		var disallowedPolicies []Constraint
		for _, c := range constraints {
			if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
				continue
			}
			if c.PolicyMode == "ALLOWED" {
				allowedPolicies = append(allowedPolicies, c)
			} else if c.PolicyMode == "DISALLOWED" {
				disallowedPolicies = append(disallowedPolicies, c)
			}
		}

		if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
			continue
		}

		// DISALLOWED check.
		for _, c := range disallowedPolicies {
			if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
				return &ConstraintViolationError{
					Type:  ConstraintPolicy,
					Label: c.Label,
					Message: fmt.Sprintf("Label change would violate DISALLOWED policy: (:%s)-[:%s]->(:%s) (constraint %q)",
						c.SourceLabel, c.Label, c.TargetLabel, c.Name),
				}
			}
		}

		// ALLOWED check.
		if len(allowedPolicies) > 0 {
			matched := false
			for _, c := range allowedPolicies {
				if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
					matched = true
					break
				}
			}
			if !matched {
				return &ConstraintViolationError{
					Type:  ConstraintPolicy,
					Label: edge.Type,
					Message: fmt.Sprintf("Label change would violate ALLOWED policy: no ALLOWED policy permits (:%s)-[:%s]->(:%s)",
						strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":")),
				}
			}
		}
	}

	return nil
}

// labelsEqual checks whether two label slices have the same content (order-independent).
func labelsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, l := range a {
		set[l] = struct{}{}
	}
	for _, l := range b {
		if _, ok := set[l]; !ok {
			return false
		}
	}
	return true
}
