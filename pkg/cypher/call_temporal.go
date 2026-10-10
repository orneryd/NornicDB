package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Temporal Helper Procedures
// ========================================

// callDbTemporalAssertNoOverlap implements db.temporal.assertNoOverlap
// Syntax:
//
//	CALL db.temporal.assertNoOverlap(label, keyProp, validFromProp, validToProp, keyValue, newValidFrom, newValidTo [, systemTime [, systemSequence]])
//
// This returns ok=true if no overlaps are detected, otherwise returns an error.
// newValidTo can be null to indicate an open-ended interval.
//
// args are the call's evaluated arguments, so a bound is any temporal value
// (datetime(), date(), localdatetime()), an ISO string, Unix seconds, a
// parameter or an expression, read as the TEMPORAL NO OVERLAP constraint
// reads it.
func (e *StorageExecutor) callDbTemporalAssertNoOverlap(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	if len(args) < 7 || len(args) > 9 {
		return nil, localizedError(localization.CypherSpecializedCallsTemporalAssertArgumentCount(), nil)
	}

	label, err := coerceStringArg(args[0], "label")
	if err != nil {
		return nil, err
	}
	keyProp, err := coerceStringArg(args[1], "keyProp")
	if err != nil {
		return nil, err
	}
	validFromProp, err := coerceStringArg(args[2], "validFromProp")
	if err != nil {
		return nil, err
	}
	validToProp, err := coerceStringArg(args[3], "validToProp")
	if err != nil {
		return nil, err
	}

	keyValue := args[4]
	newStart, ok := storage.CoerceTemporalTime(args[5])
	if !ok {
		return nil, localizedError(localization.CypherSpecializedCallsDateTimeRequired("newValidFrom"), nil)
	}
	newEnd, newHasEnd := coerceDateTimeOptional(args[6])
	snapshotVersion, hasSnapshot, err := coerceOptionalMVCCVersion(args[7:])
	if err != nil {
		return nil, err
	}

	nodes, err := temporalNodesByLabel(e.storage, label, snapshotVersion, hasSnapshot)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTemporalReadNodesFailed(label, err), err)
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !valuesEqual(node.Properties[keyProp], keyValue) {
			continue
		}

		existingStart, ok := storage.CoerceTemporalTime(node.Properties[validFromProp])
		if !ok {
			continue
		}
		existingEnd, existingHasEnd := coerceDateTimeOptional(node.Properties[validToProp])

		if storage.TemporalIntervalsOverlap(newStart, newEnd, newHasEnd, existingStart, existingEnd, existingHasEnd) {
			return nil, localizedError(localization.CypherSpecializedCallsTemporalOverlap(keyProp, keyValue), nil)
		}
	}

	return &ExecuteResult{
		Columns: []string{"ok"},
		Rows:    [][]interface{}{{true}},
	}, nil
}

// callDbTemporalAsOf implements db.temporal.asOf
// Syntax:
//
//	CALL db.temporal.asOf(label, keyProp, keyValue, validFromProp, validToProp, asOf [, systemTime [, systemSequence]]) YIELD node
//
// Returns the most recent node whose [valid_from, valid_to) covers asOf.
//
// args are the call's evaluated arguments: asOf and the stored bounds are
// read as the TEMPORAL NO OVERLAP constraint reads them
// (storage.CoerceTemporalTime), so datetime(), date() and localdatetime()
// values match as ISO strings do.
func (e *StorageExecutor) callDbTemporalAsOf(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	if len(args) < 6 || len(args) > 8 {
		return nil, localizedError(localization.CypherSpecializedCallsTemporalAsOfArgumentCount(), nil)
	}

	label, err := coerceStringArg(args[0], "label")
	if err != nil {
		return nil, err
	}
	keyProp, err := coerceStringArg(args[1], "keyProp")
	if err != nil {
		return nil, err
	}
	keyValue := args[2]
	validFromProp, err := coerceStringArg(args[3], "validFromProp")
	if err != nil {
		return nil, err
	}
	validToProp, err := coerceStringArg(args[4], "validToProp")
	if err != nil {
		return nil, err
	}
	asOf, ok := storage.CoerceTemporalTime(args[5])
	if !ok {
		return nil, localizedError(localization.CypherSpecializedCallsDateTimeRequired("asOf"), nil)
	}
	snapshotVersion, hasSnapshot, err := coerceOptionalMVCCVersion(args[6:])
	if err != nil {
		return nil, err
	}

	if temporalLookup, ok := e.storage.(storage.TemporalLookupEngine); ok && !hasSnapshot {
		node, err := temporalLookup.GetTemporalNodeAsOf(label, keyProp, keyValue, validFromProp, validToProp, asOf)
		if err != nil {
			return nil, localizedError(localization.CypherSpecializedCallsTemporalLookupFailed(label, err), err)
		}
		if node != nil {
			return &ExecuteResult{
				Columns: []string{"node"},
				Rows:    [][]interface{}{{node}},
			}, nil
		}
	}

	nodes, err := temporalNodesByLabel(e.storage, label, snapshotVersion, hasSnapshot)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTemporalReadNodesFailed(label, err), err)
	}

	var bestNode interface{}
	var bestStart time.Time
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !valuesEqual(node.Properties[keyProp], keyValue) {
			continue
		}

		start, ok := storage.CoerceTemporalTime(node.Properties[validFromProp])
		if !ok {
			continue
		}
		end, hasEnd := coerceDateTimeOptional(node.Properties[validToProp])

		if asOf.Before(start) {
			continue
		}
		if hasEnd && !asOf.Before(end) {
			continue
		}

		if bestNode == nil || start.After(bestStart) {
			bestNode = node
			bestStart = start
		}
	}

	if bestNode == nil {
		return &ExecuteResult{
			Columns: []string{"node"},
			Rows:    [][]interface{}{},
		}, nil
	}

	return &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{{bestNode}},
	}, nil
}

func coerceStringArg(val interface{}, name string) (string, error) {
	if val == nil {
		return "", localizedError(localization.CypherSpecializedCallsArgumentRequired(name), nil)
	}
	switch v := val.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return "", localizedError(localization.CypherSpecializedCallsArgumentEmpty(name), nil)
		}
		return v, nil
	default:
		return fmt.Sprint(val), nil
	}
}

func coerceDateTimeOptional(val interface{}) (time.Time, bool) {
	if val == nil {
		return time.Time{}, false
	}
	return storage.CoerceTemporalTime(val)
}

func coerceOptionalMVCCVersion(args []interface{}) (storage.MVCCVersion, bool, error) {
	if len(args) == 0 {
		return storage.MVCCVersion{}, false, nil
	}
	commitTime, ok := storage.CoerceTemporalTime(args[0])
	if !ok {
		return storage.MVCCVersion{}, false, localizedError(localization.CypherSpecializedCallsDateTimeRequired("systemTime"), nil)
	}
	version := storage.MVCCVersion{CommitTimestamp: commitTime.UTC(), CommitSequence: ^uint64(0)}
	if len(args) > 1 {
		seq, err := coerceUint64Arg(args[1], "systemSequence")
		if err != nil {
			return storage.MVCCVersion{}, false, err
		}
		version.CommitSequence = seq
	}
	return version, true, nil
}

func coerceUint64Arg(val interface{}, name string) (uint64, error) {
	switch v := val.(type) {
	case int:
		if v < 0 {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedNonNegative(name), nil)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedNonNegative(name), nil)
		}
		return uint64(v), nil
	case float64:
		if v < 0 || v != float64(uint64(v)) {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedWholeNonNegative(name), nil)
		}
		return uint64(v), nil
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedValid(name), nil)
		}
		return parsed, nil
	default:
		return 0, localizedError(localization.CypherSpecializedCallsUnsignedValid(name), nil)
	}
}

func temporalNodesByLabel(engine storage.Engine, label string, version storage.MVCCVersion, hasSnapshot bool) ([]*storage.Node, error) {
	if !hasSnapshot {
		return engine.GetNodesByLabel(label)
	}
	if provider, ok := engine.(storage.MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, storage.ErrNotImplemented
}

func valuesEqual(a, b interface{}) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
