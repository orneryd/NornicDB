package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

import (
	"context"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"reflect"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Temporal Helper Procedures
// ========================================

// The reads the temporal procedures perform, and the record PROFILE keeps of
// each. A read is recorded where it handed its population over, before the
// procedure filters that population by key value or validity interval: the one
// row a procedure admits is not the population it read.
const (
	// profileKindNodeByLabelVisibleAtScan names a snapshot-visible label read:
	// the nodes of one label as they were at the statement's system time and
	// sequence. It is a different request from profileKindNodeByLabelScan (the
	// label's current nodes), so a statement reading the historical view names
	// that request instead of claiming the current one it did not make. Rows is
	// the label population the snapshot returned, before key/interval
	// filtering.
	profileKindNodeByLabelVisibleAtScan = "NodeByLabelVisibleAtScan"
	// profileKindTemporalNodeAsOfSeek names the one temporal index lookup
	// db.temporal.asOf performs for a label, key property and key value as of
	// one instant. Rows is 1 when the lookup returned the version covering that
	// instant and 0 when it returned none; Hits is the one request, whether or
	// not it found a version. The label read a lookup that found nothing falls
	// back to is a separate request and is recorded as its own.
	profileKindTemporalNodeAsOfSeek = "TemporalNodeAsOfSeek"
)

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
//
// The registry checks the argument count (7 to 9) before the call.
func (e *StorageExecutor) callDbTemporalAssertNoOverlap(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
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

	nodes, err := temporalNodesByLabel(ctx, e.storage, label, snapshotVersion, hasSnapshot)
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
		Columns: columnNames(dbTemporalAssertNoOverlapColumns),
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
//
// The registry checks the argument count (6 to 8) before the call.
func (e *StorageExecutor) callDbTemporalAsOf(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	if len(args) == 2 || len(args) == 3 {
		return e.callHistoricalQuery(ctx, args)
	}
	if len(args) < 6 || len(args) > 8 {
		return nil, newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "TemporalArguments", "db.temporal.asOf expects (at, query [, parameters]); the deprecated interval signature accepts six to eight arguments")
	}

	// The deprecated positional overload retains its STRING argument contract.
	for _, parameter := range [...]struct {
		index int
		name  string
	}{{0, "label"}, {1, "keyProp"}, {3, "validFromProp"}, {4, "validToProp"}} {
		if args[parameter.index] != nil {
			if _, ok := args[parameter.index].(string); !ok {
				return nil, procedureArgumentTypeError("db.temporal.asOf", parameter.name, "STRING", args[parameter.index])
			}
		}
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
		// The lookup is one point request whatever it answers: the version that
		// covers asOf, or nothing, when the caller's engine has no temporal
		// index for the request and the label read below answers it instead.
		var seek *queryprofile.ReadCounter
		if queryprofile.FromContext(ctx) != nil {
			seek = queryprofile.Access(ctx, profileKindTemporalNodeAsOfSeek, label+"("+keyProp+")")
		}
		node, err := temporalLookup.GetTemporalNodeAsOf(ctx, label, keyProp, keyValue, validFromProp, validToProp, asOf)
		queryprofile.EntityReadResult(seek, node, err)
		if err != nil {
			return nil, localizedError(localization.CypherSpecializedCallsTemporalLookupFailed(label, err), err)
		}
		if node != nil {
			return &ExecuteResult{
				Columns: columnNames(nodeColumns),
				Rows:    [][]interface{}{{node}},
			}, nil
		}
	}

	nodes, err := temporalNodesByLabel(ctx, e.storage, label, snapshotVersion, hasSnapshot)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsTemporalReadNodesFailed(label, err), err)
	}

	var bestNode interface{}
	var bestStart time.Time
	for _, node := range nodes {
		// The label read honors this caller context too; checking here keeps a
		// canceled caller from walking the nodes it already materialized.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
			Columns: columnNames(nodeColumns),
			Rows:    [][]interface{}{},
		}, nil
	}

	return &ExecuteResult{
		Columns: columnNames(nodeColumns),
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
	if unsigned, ok := valueconvert.ToUint64(val, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat|valueconvert.IntegerStrings|valueconvert.IntegerTrimStrings); ok {
		return unsigned, nil
	}
	if number, numeric := valueconvert.ReadNumber(val); numeric {
		if number.Kind() == valueconvert.FloatNumber {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedWholeNonNegative(name), nil)
		}
		if signed, ok := number.Signed(); ok && signed < 0 {
			return 0, localizedError(localization.CypherSpecializedCallsUnsignedNonNegative(name), nil)
		}
	}
	return 0, localizedError(localization.CypherSpecializedCallsUnsignedValid(name), nil)
}

// temporalNodesByLabel reads the label population both temporal procedures
// filter. It is the one owner of those reads, so it is the one owner of their
// records: the current nodes of the label, or, for a statement that names a
// system time and sequence, the nodes the snapshot view held then. A record
// holds the population the read returned, before the caller narrows it by key
// value and validity interval, and an empty population the read did return is
// recorded as an observed zero rather than as no measurement.
func temporalNodesByLabel(ctx context.Context, engine storage.Engine, label string, version storage.MVCCVersion, hasSnapshot bool) ([]*storage.Node, error) {
	if !hasSnapshot {
		nodes, err := engine.GetNodesByLabel(ctx, label)
		if err != nil {
			return nodes, err
		}
		queryprofile.EntityPopulation(ctx, profileKindNodeByLabelScan, label, nodes)
		return nodes, nil
	}
	if provider, ok := engine.(storage.MVCCIndexedVisibilityEngine); ok {
		nodes, err := provider.GetNodesByLabelVisibleAt(ctx, label, version)
		if err != nil {
			return nodes, err
		}
		queryprofile.EntityPopulation(ctx, profileKindNodeByLabelVisibleAtScan, label, nodes)
		return nodes, nil
	}
	return nil, storage.ErrNotImplemented
}

func valuesEqual(a, b interface{}) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
