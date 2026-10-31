package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

import (
	"encoding/base64"
	"encoding/binary"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"hash/fnv"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Change Data Capture procedures (#937): db.cdc.current, db.cdc.earliest and
// db.cdc.query read the change events storage writes in the commit of every
// change of a database whose txLogEnrichment is DIFF or FULL, and give them
// in Neo4j's change event format.

// capturesChanges reports whether the executor's database captures changes.
func (e *StorageExecutor) capturesChanges() bool {
	capture, ok := storage.ChangeCaptureOf(e.storage)
	return ok && capture.ChangeCapture(e.databaseName()) != storage.ChangeCaptureOff
}

// changeCapture returns the executor's database and its change-capturing
// engine; Neo4j's Disabled error when the database doesn't capture changes.
func (e *StorageExecutor) changeCapture() (storage.ChangeCaptureEngine, string, error) {
	database := e.databaseName()
	capture, ok := storage.ChangeCaptureOf(e.storage)
	if !ok || capture.ChangeCapture(database) == storage.ChangeCaptureOff {
		return nil, database, localizedStatusError("Neo.DatabaseError.ChangeDataCapture.Disabled", "Disabled",
			localization.CypherSpecializedCallsCDCDisabled(database))
	}
	return capture, database, nil
}

// changePosition is a place in a database's change order: just after the
// change seq of transaction commitSeq. (commitSeq, MaxUint32) is after the
// whole transaction.
type changePosition struct {
	commitSeq uint64
	seq       uint32
}

func (p changePosition) before(other changePosition) bool {
	return p.commitSeq < other.commitSeq || p.commitSeq == other.commitSeq && p.seq < other.seq
}

const changeIDVersion = byte(1)

// encodeChangeID is a position's opaque change identifier, bound to its
// database (tag, databaseChangeTag): Neo4j's identifiers are local to the
// database they come from.
func encodeChangeID(tag uint64, position changePosition) string {
	raw := make([]byte, 0, 21)
	raw = append(raw, changeIDVersion)
	raw = binary.BigEndian.AppendUint64(raw, tag)
	raw = binary.BigEndian.AppendUint64(raw, position.commitSeq)
	raw = binary.BigEndian.AppendUint32(raw, position.seq)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeChangeID reads a change identifier of database; Neo4j's
// InvalidIdentifier error for anything else.
func decodeChangeID(tag uint64, id string) (changePosition, error) {
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil || len(raw) != 21 || raw[0] != changeIDVersion || binary.BigEndian.Uint64(raw[1:9]) != tag {
		return changePosition{}, localizedStatusError("Neo.ClientError.ChangeDataCapture.InvalidIdentifier", "InvalidIdentifier",
			localization.CypherSpecializedCallsCDCInvalidIdentifier(id))
	}
	return changePosition{commitSeq: binary.BigEndian.Uint64(raw[9:17]), seq: binary.BigEndian.Uint32(raw[17:21])}, nil
}

// databaseChangeTag identifies the database a change identifier belongs
// to: its name and, where the database manager records it, its identity, so
// that a database dropped and created again under the same name rejects
// the identifiers of the one before.
func (e *StorageExecutor) databaseChangeTag(database string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(database))
	if provider, ok := e.dbManager.(DatabaseIdentityProvider); ok {
		id, _ := provider.DatabaseIdentity(database)
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(id))
	}
	return hash.Sum64()
}

// changeBounds returns the positions just before a database's earliest
// retained change and just after its last committed one.
func changeBounds(capture storage.ChangeCaptureEngine, database string) (earliest, current changePosition, err error) {
	first, last, err := capture.ChangeBounds(database)
	if err != nil {
		return changePosition{}, changePosition{}, localizedStatusError("Neo.DatabaseError.ChangeDataCapture.ScanFailure", "ScanFailure",
			localization.CypherSpecializedCallsCDCScanFailed(err))
	}
	current = changePosition{commitSeq: last, seq: math.MaxUint32}
	earliest = current
	if first > 0 {
		earliest = changePosition{commitSeq: first - 1, seq: math.MaxUint32}
	}
	return earliest, current, nil
}

// callDbCdcCurrent is db.cdc.current(): the identifier just after the last
// committed change, which db.cdc.query takes to return later changes.
func (e *StorageExecutor) callDbCdcCurrent() (*ExecuteResult, error) {
	capture, database, err := e.changeCapture()
	if err != nil {
		return nil, err
	}
	_, current, err := changeBounds(capture, database)
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: columnNames(cdcPositionColumns), Rows: [][]interface{}{{encodeChangeID(e.databaseChangeTag(database), current)}}}, nil
}

// callDbCdcEarliest is db.cdc.earliest(): the identifier just before the
// earliest retained change.
func (e *StorageExecutor) callDbCdcEarliest() (*ExecuteResult, error) {
	capture, database, err := e.changeCapture()
	if err != nil {
		return nil, err
	}
	earliest, _, err := changeBounds(capture, database)
	if err != nil {
		return nil, err
	}
	return &ExecuteResult{Columns: columnNames(cdcPositionColumns), Rows: [][]interface{}{{encodeChangeID(e.databaseChangeTag(database), earliest)}}}, nil
}

// callDbCdcQuery is db.cdc.query(from = "", selectors = [], limit = null):
// the changes after from (exclusive; "" is db.cdc.current()), in commit
// order, that match any selector (all when there is none), at most limit of
// them (a NornicDB extension for reading a large backlog in parts: pass the
// last id as the next from). An identifier older than the earliest retained
// change is Neo4j's InvalidIdentifier error, one past the last committed
// change its FutureIdentifier error.
func (e *StorageExecutor) callDbCdcQuery(args []interface{}) (*ExecuteResult, error) {
	capture, database, err := e.changeCapture()
	if err != nil {
		return nil, err
	}
	var fromArg, selectorsArg interface{}
	if len(args) > 0 {
		fromArg = args[0]
	}
	if len(args) > 1 {
		selectorsArg = args[1]
	}
	selectors, err := parseChangeSelectors(selectorsArg)
	if err != nil {
		return nil, err
	}
	limit, hasLimit, err := txlogInteger(args, 2, "limit")
	if err != nil {
		return nil, err
	}
	if hasLimit && limit < 0 {
		return nil, localizedError(localization.CypherSpecializedCallsTxlogArgumentType("limit", "a non-negative INTEGER", neo4jProvidedValue(args[2])), nil)
	}
	earliest, current, err := changeBounds(capture, database)
	if err != nil {
		return nil, err
	}
	tag := e.databaseChangeTag(database)
	from := current
	if id, _ := fromArg.(string); id != "" {
		if from, err = decodeChangeID(tag, id); err != nil {
			return nil, err
		}
		if from.before(earliest) {
			return nil, localizedStatusError("Neo.ClientError.ChangeDataCapture.InvalidIdentifier", "InvalidIdentifier",
				localization.CypherSpecializedCallsCDCExpiredIdentifier(id))
		}
		if current.before(from) {
			return nil, localizedStatusError("Neo.TransientError.ChangeDataCapture.FutureIdentifier", "FutureIdentifier",
				localization.CypherSpecializedCallsCDCFutureIdentifier(id))
		}
	}
	databaseID := ""
	if provider, ok := e.dbManager.(DatabaseIdentityProvider); ok {
		databaseID, _ = provider.DatabaseIdentity(database)
	}
	result := &ExecuteResult{Columns: columnNames(dbCdcQueryColumns), Rows: [][]interface{}{}}
	// Events are read a page at a time, so a large backlog isn't decoded at
	// once, until the limit is reached or the events end.
	const page = 1000
	for position := from; !hasLimit || int64(len(result.Rows)) < limit; {
		events, err := capture.ChangeEvents(database, position.commitSeq, position.seq, page)
		if err != nil {
			return nil, localizedStatusError("Neo.DatabaseError.ChangeDataCapture.ScanFailure", "ScanFailure",
				localization.CypherSpecializedCallsCDCScanFailed(err))
		}
		for index := range events {
			event := &events[index]
			if !matchesAnyChangeSelector(selectors, database, event) {
				continue
			}
			result.Rows = append(result.Rows, []interface{}{
				encodeChangeID(tag, changePosition{commitSeq: event.CommitSeq, seq: event.Seq}),
				int64(event.CommitSeq), int64(event.Seq),
				changeMetadataValue(event, databaseID), changeEventValue(database, event),
			})
			if hasLimit && int64(len(result.Rows)) == limit {
				break
			}
		}
		if len(events) < page {
			break
		}
		last := events[len(events)-1]
		position = changePosition{commitSeq: last.CommitSeq, seq: last.Seq}
	}
	return result, nil
}

// changeMetadataValue is an event's metadata as Neo4j gives it. The users
// and connection are those NornicDB records (none yet).
func changeMetadataValue(event *storage.ChangeEvent, databaseID string) map[string]interface{} {
	txMetadata := map[string]interface{}{}
	for key, value := range event.TxMetadata {
		txMetadata[key] = cdcValue(value)
	}
	return map[string]interface{}{
		"executingUser": "", "authenticatedUser": "",
		"captureMode":    event.Mode.String(),
		"connectionType": "", "connectionClient": "", "connectionServer": "",
		"serverId": "", "databaseId": databaseID,
		"txStartTime":  time.Unix(0, event.StartTime).UTC(),
		"txCommitTime": time.Unix(0, event.CommitTime).UTC(),
		"txMetadata":   txMetadata,
	}
}

// changeEventValue is an event as Neo4j gives it.
func changeEventValue(database string, event *storage.ChangeEvent) map[string]interface{} {
	state := map[string]interface{}{
		"before": changeStateValue(event.Before, event.Node),
		"after":  changeStateValue(event.After, event.Node),
	}
	if event.Node {
		return map[string]interface{}{
			"elementId": storage.NodeElementID(database, storage.NodeID(event.ID)),
			"eventType": "n", "operation": string(event.Operation),
			"labels": nonNilStrings(event.Labels), "keys": nodeKeysValue(event.Keys),
			"state": state,
		}
	}
	endpoint := func(id string, labels []string, keys map[string][]map[string]any) map[string]interface{} {
		return map[string]interface{}{
			"elementId": storage.NodeElementID(database, storage.NodeID(id)),
			"keys":      nodeKeysValue(keys), "labels": nonNilStrings(labels),
		}
	}
	return map[string]interface{}{
		"elementId": storage.RelationshipElementID(database, storage.EdgeID(event.ID)),
		"eventType": "r", "operation": string(event.Operation), "type": event.Type,
		"keys":  keyMapsValue(event.EdgeKeys),
		"start": endpoint(event.Start, event.StartLabels, event.StartKeys), "end": endpoint(event.End, event.EndLabels, event.EndKeys),
		"state": state,
	}
}

// nodeKeysValue is a node's key values per label as a Cypher map of lists
// of maps; an empty map when it has none.
func nodeKeysValue(keys map[string][]map[string]any) map[string]interface{} {
	value := make(map[string]interface{}, len(keys))
	for label, maps := range keys {
		value[label] = keyMapsValue(maps)
	}
	return value
}

// keyMapsValue is a list of key-value maps as Cypher values.
func keyMapsValue(maps []map[string]any) []interface{} {
	value := make([]interface{}, len(maps))
	for index, keys := range maps {
		converted := make(map[string]interface{}, len(keys))
		for name, key := range keys {
			converted[name] = cdcValue(key)
		}
		value[index] = converted
	}
	return value
}

func changeStateValue(state *storage.ChangeState, node bool) interface{} {
	if state == nil {
		return nil
	}
	properties := make(map[string]interface{}, len(state.Properties))
	for key, value := range state.Properties {
		properties[key] = cdcValue(value)
	}
	value := map[string]interface{}{"properties": properties}
	if node {
		value["labels"] = nonNilStrings(state.Labels)
	}
	return value
}

func nonNilStrings(values []string) []interface{} {
	out := make([]interface{}, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

// cdcValue is a stored property value as a Cypher value: the stored form
// decodes integers into their smallest Go type and floats into float32 when
// they fit; Cypher has INTEGER and FLOAT.
func cdcValue(value interface{}) interface{} {
	if number, ok := valueconvert.ReadNumber(value); ok {
		if integer, ok := valueconvert.IntegerValue(value); ok {
			return integer
		}
		if unsigned, ok := number.Unsigned(); ok {
			return unsigned // Accepted native uint64 values must remain exact.
		}
		return number.Float64()
	}
	switch number := value.(type) {
	case []interface{}:
		out := make([]interface{}, len(number))
		for index, item := range number {
			out[index] = cdcValue(item)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(number))
		for key, item := range number {
			out[key] = cdcValue(item)
		}
		return out
	}
	return value
}

// changeSelector is one db.cdc.query selector: an entity ("e"), node ("n")
// or relationship ("r") selector, its fields ANDed.
type changeSelector struct {
	kind              string
	operation         string
	changesTo         []string
	elementID         string
	labels            []string
	key               map[string]interface{}
	relType           string
	start, end        *changeSelector
	executingUser     *string
	authenticatedUser *string
	txMetadata        map[string]interface{}
}

// parseChangeSelectors reads db.cdc.query's selectors: a list of maps, each
// with Neo4j's fields for its kind. key matches property values (Neo4j's
// needs key constraints); start and end take node selectors without
// operation, changesTo or key.
func parseChangeSelectors(raw interface{}) ([]changeSelector, error) {
	if raw == nil {
		return nil, nil
	}
	list, ok := raw.([]interface{})
	if !ok {
		return nil, invalidChangeSelector("selectors must be a list of maps")
	}
	selectors := make([]changeSelector, 0, len(list))
	for _, item := range list {
		fields, ok := item.(map[string]interface{})
		if !ok {
			return nil, invalidChangeSelector("each selector must be a map")
		}
		selector, err := parseChangeSelector(fields, false)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}
	return selectors, nil
}

func parseChangeSelector(fields map[string]interface{}, endpoint bool) (changeSelector, error) {
	var selector changeSelector
	kind, _ := fields["select"].(string)
	switch {
	case kind == "e" && !endpoint, kind == "n", kind == "r" && !endpoint:
		selector.kind = kind
	default:
		return selector, invalidChangeSelector("select must be 'e', 'n' or 'r' ('n' for start and end)")
	}
	allowed := map[string]bool{"select": true, "operation": !endpoint, "changesTo": !endpoint,
		"executingUser": !endpoint, "authenticatedUser": !endpoint, "txMetadata": !endpoint,
		"elementId": kind != "e", "key": kind != "e" && !endpoint, "labels": kind == "n",
		"type": kind == "r", "start": kind == "r", "end": kind == "r"}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := fields[name]
		if !allowed[name] {
			return selector, invalidChangeSelector("'" + name + "' is not a field of a '" + kind + "' selector")
		}
		var ok bool
		switch name {
		case "operation":
			selector.operation, ok = value.(string)
			ok = ok && (selector.operation == "c" || selector.operation == "u" || selector.operation == "d")
		case "changesTo":
			selector.changesTo, ok = stringList(value)
		case "elementId":
			selector.elementID, ok = value.(string)
		case "labels":
			selector.labels, ok = stringList(value)
		case "key":
			selector.key, ok = value.(map[string]interface{})
		case "type":
			selector.relType, ok = value.(string)
		case "start", "end":
			var nested map[string]interface{}
			if nested, ok = value.(map[string]interface{}); ok {
				parsed, err := parseChangeSelector(nested, true)
				if err != nil {
					return selector, err
				}
				if name == "start" {
					selector.start = &parsed
				} else {
					selector.end = &parsed
				}
			}
		case "executingUser", "authenticatedUser":
			var user string
			if user, ok = value.(string); ok {
				if name == "executingUser" {
					selector.executingUser = &user
				} else {
					selector.authenticatedUser = &user
				}
			}
		case "txMetadata":
			selector.txMetadata, ok = value.(map[string]interface{})
		default:
			ok = true
		}
		if !ok {
			return selector, invalidChangeSelector("'" + name + "' has a value of the wrong type")
		}
	}
	return selector, nil
}

func stringList(value interface{}) ([]string, bool) {
	list, ok := value.([]interface{})
	if !ok {
		return nil, false
	}
	out := make([]string, len(list))
	for index, item := range list {
		if out[index], ok = item.(string); !ok {
			return nil, false
		}
	}
	return out, true
}

func invalidChangeSelector(reason string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherSpecializedCallsCDCSelectorInvalid(reason))
}

func matchesAnyChangeSelector(selectors []changeSelector, database string, event *storage.ChangeEvent) bool {
	if len(selectors) == 0 {
		return true
	}
	for index := range selectors {
		if selectors[index].matches(database, event) {
			return true
		}
	}
	return false
}

func (s *changeSelector) matches(database string, event *storage.ChangeEvent) bool {
	switch {
	case s.kind == "n" && !event.Node, s.kind == "r" && event.Node:
		return false
	case s.operation != "" && s.operation != string(event.Operation):
		return false
	case s.executingUser != nil && *s.executingUser != "", s.authenticatedUser != nil && *s.authenticatedUser != "":
		return false // NornicDB doesn't record users yet: they are ""
	}
	elementID := storage.NodeElementID(database, storage.NodeID(event.ID))
	if !event.Node {
		elementID = storage.RelationshipElementID(database, storage.EdgeID(event.ID))
	}
	if s.elementID != "" && s.elementID != elementID {
		return false
	}
	if s.relType != "" && s.relType != event.Type {
		return false
	}
	if len(s.labels) > 0 {
		labels := map[string]bool{}
		for _, state := range []*storage.ChangeState{event.Before, event.After} {
			if state != nil {
				for _, label := range state.Labels {
					labels[label] = true
				}
			}
		}
		for _, label := range event.Labels {
			labels[label] = true
		}
		for _, label := range s.labels {
			if !labels[label] {
				return false
			}
		}
	}
	if len(s.changesTo) > 0 && !changedAll(event, s.changesTo) {
		return false
	}
	if len(s.key) > 0 && !keyMatches(event, s.key) {
		return false
	}
	for key, want := range s.txMetadata {
		if got, ok := event.TxMetadata[key]; !ok || !cdcSelectorValuesEqual(got, want) {
			return false
		}
	}
	if s.start != nil && !s.start.matchesEndpoint(database, event.Start, event.StartLabels) {
		return false
	}
	if s.end != nil && !s.end.matchesEndpoint(database, event.End, event.EndLabels) {
		return false
	}
	return true
}

func (s *changeSelector) matchesEndpoint(database, id string, labels []string) bool {
	if s.elementID != "" && s.elementID != storage.NodeElementID(database, storage.NodeID(id)) {
		return false
	}
	has := map[string]bool{}
	for _, label := range labels {
		has[label] = true
	}
	for _, label := range s.labels {
		if !has[label] {
			return false
		}
	}
	return true
}

// changedAll reports whether the change affected every one of keys.
func changedAll(event *storage.ChangeEvent, keys []string) bool {
	var before, after map[string]interface{}
	if event.Before != nil {
		before = event.Before.Properties
	}
	if event.After != nil {
		after = event.After.Properties
	}
	for _, key := range keys {
		old, hadOld := before[key]
		updated, hasNew := after[key]
		if hadOld == hasNew && reflect.DeepEqual(old, updated) {
			return false
		}
	}
	return true
}

// keyMatches reports whether one of the entity's key-constraint value maps
// (ChangeEvent.Keys), or else its properties after the change or before a
// delete, hold every key value. The key maps answer for a DIFF update,
// whose state carries only the changed properties.
func keyMatches(event *storage.ChangeEvent, key map[string]interface{}) bool {
	keyMaps := append([]map[string]any(nil), event.EdgeKeys...)
	for _, maps := range event.Keys {
		keyMaps = append(keyMaps, maps...)
	}
	for _, keys := range keyMaps {
		if holdsValues(keys, key) {
			return true
		}
	}
	state := event.After
	if state == nil {
		state = event.Before
	}
	if state == nil {
		return false
	}
	return holdsValues(state.Properties, key)
}

// holdsValues reports whether properties hold every one of values.
func holdsValues(properties map[string]any, values map[string]interface{}) bool {
	for name, want := range values {
		if got, ok := properties[name]; !ok || !cdcSelectorValuesEqual(got, want) {
			return false
		}
	}
	return true
}

// CDC selectors are two-valued filters. Delegate numeric and structural
// equality to Cypher, retaining the existing match for identical nullable
// metadata when Cypher's three-valued equality is unknown.
func cdcSelectorValuesEqual(stored, selected interface{}) bool {
	actual := cdcValue(stored)
	equal := cypherEquality(actual, selected)
	if equal == nil {
		return reflect.DeepEqual(actual, selected)
	}
	return equal == true
}
