package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Creation-time validation of relationship constraints.
//
// Each constraint kind is an incremental check: visit sees one edge at a time
// and keeps only the state the kind needs (seen values, per-key interval
// lists, per-node counts), and finish reports violations that need the whole
// type (overlaps, counts). The edges come from StreamEdgesByType for the
// constrained relationship type only, so creating a constraint in one
// database never reads, decodes or holds the edges of other types or other
// databases.

// relEdgeCheck is one incremental relationship-constraint check. A nil visit
// means the constraint needs no scan.
type relEdgeCheck struct {
	visit  func(*Edge) error
	finish func() error
}

// errRelEdgeCheckStopped stops an edge stream after a check reported a
// violation; the violation itself is returned to the caller.
var errRelEdgeCheckStopped = errors.New("relationship constraint check stopped")

// runRelEdgeCheck streams the edges of edgeType through engine into chk.
func runRelEdgeCheck(engine Engine, edgeType string, chk relEdgeCheck) error {
	if chk.visit == nil {
		return nil
	}
	var violation error
	err := StreamEdgesByType(context.Background(), engine, edgeType, func(edge *Edge) error {
		if err := chk.visit(edge); err != nil {
			violation = err
			return errRelEdgeCheckStopped
		}
		return nil
	})
	if violation != nil {
		return violation
	}
	if err != nil {
		return localizedError(localization.StorageValidationScanEdgesFailed(err), err)
	}
	if chk.finish != nil {
		return chk.finish()
	}
	return nil
}

// checkEdgeSlice runs chk over an in-memory slice of edges.
func checkEdgeSlice(chk relEdgeCheck, edges []*Edge) error {
	if chk.visit == nil {
		return nil
	}
	for _, edge := range edges {
		if err := chk.visit(edge); err != nil {
			return err
		}
	}
	if chk.finish != nil {
		return chk.finish()
	}
	return nil
}

// ofType wraps visit so it only sees non-nil edges of edgeType. Streams are
// already restricted to the type; the guard keeps slice callers and the
// GetEdgesByType("") fallback correct.
func ofType(edgeType string, visit func(*Edge) error) func(*Edge) error {
	return func(edge *Edge) error {
		if edge == nil || edge.Type != edgeType {
			return nil
		}
		return visit(edge)
	}
}

// validateRelationshipConstraintOnCreationForEngine validates relationship
// constraints using the Engine interface. It streams only the edges of the
// constrained relationship type (c.Label).
func validateRelationshipConstraintOnCreationForEngine(engine Engine, c Constraint) error {
	chk, err := newRelationshipConstraintCheck(engine, c)
	if err != nil {
		return err
	}
	return runRelEdgeCheck(engine, c.Label, chk)
}

func newRelationshipConstraintCheck(engine Engine, c Constraint) (relEdgeCheck, error) {
	switch c.Type {
	case ConstraintUnique:
		return newRelUniquenessCheck(c), nil
	case ConstraintExists:
		return newRelExistenceCheck(c), nil
	case ConstraintPropertyType:
		// Handled separately via PropertyTypeConstraint path
		return relEdgeCheck{}, nil
	case ConstraintRelationshipKey:
		return newRelKeyCheck(c), nil
	case ConstraintTemporal:
		return newRelTemporalCheck(c)
	case ConstraintDomain:
		return newRelDomainCheck(c)
	case ConstraintCardinality:
		return newRelCardinalityCheck(c), nil
	case ConstraintPolicy:
		return newRelPolicyCheck(engine, c), nil
	default:
		return relEdgeCheck{}, localizedError(localization.StorageValidationRelationshipConstraintTypeUnsupported(string(c.Type)), nil)
	}
}

// newRelUniquenessCheck checks uniqueness for relationship properties.
func newRelUniquenessCheck(c Constraint) relEdgeCheck {
	if len(c.Properties) != 1 {
		return newRelCompositeUniquenessCheck(c)
	}
	property := c.Properties[0]
	seen := make(map[interface{}]EdgeID)
	return relEdgeCheck{visit: ofType(c.Label, func(edge *Edge) error {
		value := edge.Properties[property]
		if value == nil {
			return nil
		}
		if existingID, found := seen[value]; found {
			message := localization.StorageValidationRelationshipUniqueDuplicate(string(existingID), string(edge.ID), property, value)
			return newLocalizedConstraintViolation(ConstraintUnique, c.Label, []string{property}, message, nil)
		}
		seen[value] = edge.ID
		return nil
	})}
}

// newRelCompositeUniquenessCheck checks composite uniqueness for relationship properties.
func newRelCompositeUniquenessCheck(c Constraint) relEdgeCheck {
	seen := make(map[string]EdgeID)
	return relEdgeCheck{visit: ofType(c.Label, func(edge *Edge) error {
		// Build composite key — skip if any property is nil
		parts := make([]string, len(c.Properties))
		for i, prop := range c.Properties {
			val := edge.Properties[prop]
			if val == nil {
				return nil
			}
			parts[i] = fmt.Sprintf("%v", val)
		}
		key := strings.Join(parts, "\x00")
		if existingID, found := seen[key]; found {
			message := localization.StorageValidationRelationshipCompositeDuplicate(string(existingID), string(edge.ID), parts)
			return newLocalizedConstraintViolation(ConstraintUnique, c.Label, c.Properties, message, nil)
		}
		seen[key] = edge.ID
		return nil
	})}
}

// newRelExistenceCheck checks existence for relationship properties.
func newRelExistenceCheck(c Constraint) relEdgeCheck {
	return relEdgeCheck{visit: ofType(c.Label, func(edge *Edge) error {
		for _, prop := range c.Properties {
			if edge.Properties[prop] == nil {
				message := localization.StorageValidationRelationshipPropertyMissing(string(edge.ID), prop)
				return newLocalizedConstraintViolation(ConstraintExists, c.Label, []string{prop}, message, nil)
			}
		}
		return nil
	})}
}

// newRelKeyCheck is existence + composite uniqueness on the key properties.
// A missing property is reported in preference to a duplicate key anywhere in
// the type (the order the two separate scans used to report them), so a
// duplicate is remembered and reported only after the whole type passed the
// existence check.
func newRelKeyCheck(c Constraint) relEdgeCheck {
	exists := newRelExistenceCheck(c)
	unique := newRelCompositeUniquenessCheck(c)
	var duplicate error
	return relEdgeCheck{
		visit: func(edge *Edge) error {
			if err := exists.visit(edge); err != nil {
				return err
			}
			if duplicate == nil {
				duplicate = unique.visit(edge)
			}
			return nil
		},
		finish: func() error { return duplicate },
	}
}

// temporalCompositeKey builds a composite key string from multiple key property values on an edge.
func temporalCompositeKey(edge *Edge, keyProps []string) (string, error) {
	parts := make([]string, len(keyProps))
	for i, prop := range keyProps {
		val := edge.Properties[prop]
		if val == nil {
			return "", localizedError(localization.StorageValidationEdgeKeyNull(string(edge.ID), prop), nil)
		}
		parts[i] = fmt.Sprint(val)
	}
	return strings.Join(parts, "\x00"), nil
}

// newRelTemporalCheck checks temporal no-overlap for relationship properties.
// Supports 3+ properties: the last 2 are always (valid_from, valid_to),
// everything before that forms a composite key (e.g., from_id, to_id,
// valid_from, valid_to). Intervals are kept per key; overlaps are reported
// once the whole type has been read.
func newRelTemporalCheck(c Constraint) (relEdgeCheck, error) {
	if len(c.Properties) < 3 {
		return relEdgeCheck{}, localizedError(localization.StorageValidationTemporalPropertiesAtLeastThree(), nil)
	}
	keyProps := c.Properties[:len(c.Properties)-2]
	startProp := c.Properties[len(c.Properties)-2]
	endProp := c.Properties[len(c.Properties)-1]

	type edgeInterval struct {
		temporalInterval
		edgeID EdgeID
	}
	byKey := make(map[string][]edgeInterval)
	visit := ofType(c.Label, func(edge *Edge) error {
		key, err := temporalCompositeKey(edge, keyProps)
		if err != nil {
			message := localization.StorageValidationTemporalCreationFailed(err.Error())
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, err)
		}
		start, ok := coerceTemporalTime(edge.Properties[startProp])
		if !ok {
			message := localization.StorageValidationTemporalEdgeInvalid(string(edge.ID), startProp)
			return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
		}
		end, hasEnd := coerceTemporalTime(edge.Properties[endProp])
		byKey[key] = append(byKey[key], edgeInterval{
			temporalInterval: temporalInterval{start: start, end: end, hasEnd: hasEnd},
			edgeID:           edge.ID,
		})
		return nil
	})
	finish := func() error {
		for _, intervals := range byKey {
			sort.Slice(intervals, func(i, j int) bool {
				return intervals[i].start.Before(intervals[j].start)
			})
			for i := 1; i < len(intervals); i++ {
				prev := intervals[i-1]
				curr := intervals[i]
				if intervalsOverlap(prev.temporalInterval, curr.temporalInterval) {
					message := localization.StorageValidationTemporalEdgesOverlap(string(prev.edgeID), string(curr.edgeID))
					return newLocalizedConstraintViolation(ConstraintTemporal, c.Label, c.Properties, message, nil)
				}
			}
		}
		return nil
	}
	return relEdgeCheck{visit: visit, finish: finish}, nil
}

// newRelDomainCheck checks that every edge's property is in the allowed list.
func newRelDomainCheck(c Constraint) (relEdgeCheck, error) {
	if len(c.Properties) != 1 {
		return relEdgeCheck{}, localizedError(localization.StorageValidationDomainPropertyCount(len(c.Properties)), nil)
	}
	if len(c.AllowedValues) == 0 {
		return relEdgeCheck{}, localizedError(localization.StorageValidationDomainAllowedValuesRequired(), nil)
	}
	property := c.Properties[0]
	return relEdgeCheck{visit: ofType(c.Label, func(edge *Edge) error {
		value := edge.Properties[property]
		if value == nil {
			return nil // NULL is valid for domain constraints
		}
		if !isValueInAllowedList(value, c.AllowedValues) {
			message := localization.StorageValidationDomainEdgeInvalid(string(edge.ID), property, value, c.AllowedValues)
			return newLocalizedConstraintViolation(ConstraintDomain, c.Label, []string{property}, message, nil)
		}
		return nil
	})}, nil
}

// newRelCardinalityCheck counts edges per anchor node (start node for
// OUTGOING, end node otherwise) and reports a node above c.MaxCount.
func newRelCardinalityCheck(c Constraint) relEdgeCheck {
	counts := make(map[NodeID]int)
	visit := ofType(c.Label, func(edge *Edge) error {
		if c.Direction == "OUTGOING" {
			counts[edge.StartNode]++
		} else {
			counts[edge.EndNode]++
		}
		return nil
	})
	finish := func() error {
		for nodeID, count := range counts {
			if count > c.MaxCount {
				message := localization.StorageValidationCardinalityCreationExceeded(string(nodeID), count, strings.ToLower(c.Direction), c.Label, c.MaxCount)
				return newLocalizedConstraintViolation(ConstraintCardinality, c.Label, nil, message, nil)
			}
		}
		return nil
	}
	return relEdgeCheck{visit: visit, finish: finish}
}

// newRelPolicyCheck checks that every existing edge satisfies the policy.
// For ALLOWED policies, the full set of ALLOWED policies for this
// relationship type (existing ones from the schema plus the new one being
// created) must cover every edge of the type.
func newRelPolicyCheck(engine Engine, c Constraint) relEdgeCheck {
	var allowedSet []Constraint
	if c.PolicyMode == "ALLOWED" {
		if schema := engine.GetSchema(); schema != nil {
			for _, existing := range schema.GetAllConstraints() {
				if existing.Type == ConstraintPolicy && existing.Label == c.Label && existing.PolicyMode == "ALLOWED" {
					allowedSet = append(allowedSet, existing)
				}
			}
		}
		// Add the new constraint being created (not yet in schema).
		allowedSet = append(allowedSet, c)
	}

	return relEdgeCheck{visit: ofType(c.Label, func(edge *Edge) error {
		srcNode, err := engine.GetNode(edge.StartNode)
		if err != nil || srcNode == nil {
			return nil
		}
		tgtNode, err := engine.GetNode(edge.EndNode)
		if err != nil || tgtNode == nil {
			return nil
		}

		switch c.PolicyMode {
		case "DISALLOWED":
			if hasLabel(srcNode.Labels, c.SourceLabel) && hasLabel(tgtNode.Labels, c.TargetLabel) {
				message := localization.StorageValidationDisallowedPolicyCreation(string(edge.ID), string(edge.StartNode), c.SourceLabel, string(edge.EndNode), c.TargetLabel, c.Label)
				return newLocalizedConstraintViolation(ConstraintPolicy, c.Label, nil, message, nil)
			}
		case "ALLOWED":
			for _, ap := range allowedSet {
				if hasLabel(srcNode.Labels, ap.SourceLabel) && hasLabel(tgtNode.Labels, ap.TargetLabel) {
					return nil
				}
			}
			message := localization.StorageValidationAllowedPolicyCreation(string(edge.ID), string(edge.StartNode), string(edge.EndNode), c.Label)
			return newLocalizedConstraintViolation(ConstraintPolicy, c.Label, nil, message, nil)
		}
		return nil
	})}
}

// newRelPropertyTypeCheck checks a relationship property type constraint.
func newRelPropertyTypeCheck(ptc PropertyTypeConstraint) relEdgeCheck {
	return relEdgeCheck{visit: ofType(ptc.Label, func(edge *Edge) error {
		value := edge.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return localizedError(localization.StorageValidationRelationshipPropertyInvalid(string(edge.ID), ptc.Property, err), err)
		}
		return nil
	})}
}

// Slice forms of the checks, for callers that already hold the edges.

func validateRelUniquenessOnEdges(edges []*Edge, c Constraint) error {
	return checkEdgeSlice(newRelUniquenessCheck(c), edges)
}

func validateRelCompositeUniquenessOnEdges(edges []*Edge, c Constraint) error {
	return checkEdgeSlice(newRelCompositeUniquenessCheck(c), edges)
}

func validateRelExistenceOnEdges(edges []*Edge, c Constraint) error {
	return checkEdgeSlice(newRelExistenceCheck(c), edges)
}

func validateRelTemporalOnCreationForEngine(edges []*Edge, c Constraint) error {
	chk, err := newRelTemporalCheck(c)
	if err != nil {
		return err
	}
	return checkEdgeSlice(chk, edges)
}

func validateRelDomainOnCreationForEngine(edges []*Edge, c Constraint) error {
	chk, err := newRelDomainCheck(c)
	if err != nil {
		return err
	}
	return checkEdgeSlice(chk, edges)
}

func validateCardinalityOnCreationForEngine(edges []*Edge, c Constraint) error {
	return checkEdgeSlice(newRelCardinalityCheck(c), edges)
}

func validatePolicyOnCreationForEngine(engine Engine, edges []*Edge, c Constraint) error {
	return checkEdgeSlice(newRelPolicyCheck(engine, c), edges)
}
