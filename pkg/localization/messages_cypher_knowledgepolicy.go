package localization

import "strconv"

const (
	MessageCypherKnowledgePolicyExpectedAfter              MessageID = "cypherknowledgepolicy.expected_after"
	MessageCypherKnowledgePolicyProfileNameExpectedAfter   MessageID = "cypherknowledgepolicy.profile_name_expected_after"
	MessageCypherKnowledgePolicyExpectedAfterProfileName   MessageID = "cypherknowledgepolicy.expected_after_profile_name"
	MessageCypherKnowledgePolicyPolicyNameExpectedAfter    MessageID = "cypherknowledgepolicy.policy_name_expected_after"
	MessageCypherKnowledgePolicyLabelExpectedAfterColon    MessageID = "cypherknowledgepolicy.label_expected_after_colon"
	MessageCypherKnowledgePolicyEdgeTypeExpectedAfterColon MessageID = "cypherknowledgepolicy.edge_type_expected_after_colon"
	MessageCypherKnowledgePolicyEdgePatternExpectedFor     MessageID = "cypherknowledgepolicy.edge_pattern_expected_for"
	MessageCypherKnowledgePolicyEdgePatternExpectedIn      MessageID = "cypherknowledgepolicy.edge_pattern_expected_in"
	MessageCypherKnowledgePolicyNumberExpectedAfter        MessageID = "cypherknowledgepolicy.number_expected_after"
	MessageCypherKnowledgePolicySecondsExpectedAfter       MessageID = "cypherknowledgepolicy.seconds_expected_after"
	MessageCypherKnowledgePolicyKalmanClosingBraceExpected MessageID = "cypherknowledgepolicy.kalman_closing_brace_expected"
	MessageCypherKnowledgePolicyExpressionExpectedAfterSet MessageID = "cypherknowledgepolicy.expression_expected_after_set"
	MessageCypherKnowledgePolicyInvalidValue               MessageID = "cypherknowledgepolicy.invalid_value"
	MessageCypherKnowledgePolicyUnknownOption              MessageID = "cypherknowledgepolicy.unknown_option"
	MessageCypherKnowledgePolicyUnknownKalmanConfigKey     MessageID = "cypherknowledgepolicy.unknown_kalman_config_key"
	MessageCypherKnowledgePolicyUnsupportedCommand         MessageID = "cypherknowledgepolicy.unsupported_command"
	MessageCypherKnowledgePolicySchemaManagerUnavailable   MessageID = "cypherknowledgepolicy.schema_manager_unavailable"
	MessageCypherKnowledgePolicyUnsupportedCommandType     MessageID = "cypherknowledgepolicy.unsupported_command_type"
	MessageCypherKnowledgePolicyOperationFailed            MessageID = "cypherknowledgepolicy.operation_failed"
	MessageCypherKnowledgePolicyResolveTargetRequired      MessageID = "cypherknowledgepolicy.resolve_target_required"
	MessageCypherKnowledgePolicyBindingTableUnavailable    MessageID = "cypherknowledgepolicy.binding_table_unavailable"
	MessageCypherKnowledgePolicyEntityNotFound             MessageID = "cypherknowledgepolicy.entity_not_found"
	MessageCypherKnowledgePolicyArgumentStringRequired     MessageID = "cypherknowledgepolicy.argument_string_required"
	MessageCypherKnowledgePolicyDeindexBadgerRequired      MessageID = "cypherknowledgepolicy.deindex_badger_required"
	MessageCypherKnowledgePolicyDecayOptionsMapRequired    MessageID = "cypherknowledgepolicy.decay_options_map_required"
	MessageCypherKnowledgePolicyUnknownDecayOption         MessageID = "cypherknowledgepolicy.unknown_decay_option"
	MessageCypherKnowledgePolicyReasonNoEntityArgument     MessageID = "cypherknowledgepolicy.reason_no_entity_argument"
	MessageCypherKnowledgePolicyReasonEntityNotFound       MessageID = "cypherknowledgepolicy.reason_entity_not_found"
	MessageCypherKnowledgePolicyReasonNoBadgerEngine       MessageID = "cypherknowledgepolicy.reason_no_badger_engine"
	MessageCypherKnowledgePolicyReasonDecayDisabled        MessageID = "cypherknowledgepolicy.reason_decay_disabled"
	MessageCypherKnowledgePolicyReasonNoDecayProfile       MessageID = "cypherknowledgepolicy.reason_no_decay_profile"
	MessageCypherKnowledgePolicyReasonNoDecay              MessageID = "cypherknowledgepolicy.reason_no_decay"
)

// CypherKnowledgePolicyExpectedAfter identifies a missing parser term after Cypher syntax.
func CypherKnowledgePolicyExpectedAfter(expected, after string) Message {
	return Message{ID: MessageCypherKnowledgePolicyExpectedAfter, Fallback: "expected " + expected + " after " + after, Data: map[string]any{"Expected": expected, "After": after}}
}

// CypherKnowledgePolicyProfileNameExpectedAfter identifies a missing profile name.
func CypherKnowledgePolicyProfileNameExpectedAfter(after string) Message {
	return Message{ID: MessageCypherKnowledgePolicyProfileNameExpectedAfter, Fallback: "expected profile name after " + after, Data: map[string]any{"After": after}}
}

// CypherKnowledgePolicyExpectedAfterProfileName identifies missing syntax after a profile name.
func CypherKnowledgePolicyExpectedAfterProfileName(expected, profileName string) Message {
	displayName := strconv.Quote(profileName)
	return Message{ID: MessageCypherKnowledgePolicyExpectedAfterProfileName, Fallback: "expected " + expected + " after profile name " + displayName, Data: map[string]any{"Expected": expected, "ProfileName": profileName, "DisplayName": displayName}}
}

// CypherKnowledgePolicyPolicyNameExpectedAfter identifies a missing policy name.
func CypherKnowledgePolicyPolicyNameExpectedAfter(after string) Message {
	return Message{ID: MessageCypherKnowledgePolicyPolicyNameExpectedAfter, Fallback: "expected policy name after " + after, Data: map[string]any{"After": after}}
}

// CypherKnowledgePolicyLabelExpectedAfterColon identifies a missing node label.
func CypherKnowledgePolicyLabelExpectedAfterColon() Message {
	return Message{ID: MessageCypherKnowledgePolicyLabelExpectedAfterColon, Fallback: "expected label after ':'"}
}

// CypherKnowledgePolicyEdgeTypeExpectedAfterColon identifies a missing edge type.
func CypherKnowledgePolicyEdgeTypeExpectedAfterColon() Message {
	return Message{ID: MessageCypherKnowledgePolicyEdgeTypeExpectedAfterColon, Fallback: "expected edge type after ':'"}
}

// CypherKnowledgePolicyEdgePatternExpectedFor identifies a missing opening edge-pattern symbol.
func CypherKnowledgePolicyEdgePatternExpectedFor(expected string) Message {
	return Message{ID: MessageCypherKnowledgePolicyEdgePatternExpectedFor, Fallback: "expected " + expected + " for edge pattern", Data: map[string]any{"Expected": expected}}
}

// CypherKnowledgePolicyEdgePatternExpectedIn identifies a missing edge-pattern symbol.
func CypherKnowledgePolicyEdgePatternExpectedIn(expected string) Message {
	return Message{ID: MessageCypherKnowledgePolicyEdgePatternExpectedIn, Fallback: "expected " + expected + " in edge pattern", Data: map[string]any{"Expected": expected}}
}

// CypherKnowledgePolicyNumberExpectedAfter identifies a missing numeric value.
func CypherKnowledgePolicyNumberExpectedAfter(after string) Message {
	return Message{ID: MessageCypherKnowledgePolicyNumberExpectedAfter, Fallback: "expected number after " + after, Data: map[string]any{"After": after}}
}

// CypherKnowledgePolicySecondsExpectedAfter identifies a missing duration value.
func CypherKnowledgePolicySecondsExpectedAfter(after string) Message {
	return Message{ID: MessageCypherKnowledgePolicySecondsExpectedAfter, Fallback: "expected seconds after " + after, Data: map[string]any{"After": after}}
}

// CypherKnowledgePolicyKalmanClosingBraceExpected identifies an unterminated Kalman block.
func CypherKnowledgePolicyKalmanClosingBraceExpected() Message {
	return Message{ID: MessageCypherKnowledgePolicyKalmanClosingBraceExpected, Fallback: "expected } in KALMAN config block"}
}

// CypherKnowledgePolicyExpressionExpectedAfterSet identifies a missing SET expression.
func CypherKnowledgePolicyExpressionExpectedAfterSet() Message {
	return Message{ID: MessageCypherKnowledgePolicyExpressionExpectedAfterSet, Fallback: "expected expression after SET"}
}

// CypherKnowledgePolicyInvalidValue identifies an invalid configuration value.
func CypherKnowledgePolicyInvalidValue(field, value string, quoted bool) Message {
	displayValue := value
	if quoted {
		displayValue = strconv.Quote(value)
	}
	return Message{ID: MessageCypherKnowledgePolicyInvalidValue, Fallback: "invalid " + field + ": " + displayValue, Data: map[string]any{"Field": field, "Value": value, "DisplayValue": displayValue}}
}

// CypherKnowledgePolicyUnknownOption identifies an unsupported profile option.
func CypherKnowledgePolicyUnknownOption(option string) Message {
	return Message{ID: MessageCypherKnowledgePolicyUnknownOption, Fallback: "unknown option: " + strconv.Quote(option), Data: map[string]any{"Option": option, "DisplayOption": strconv.Quote(option)}}
}

// CypherKnowledgePolicyUnknownKalmanConfigKey identifies an unsupported Kalman option.
func CypherKnowledgePolicyUnknownKalmanConfigKey(key string) Message {
	return Message{ID: MessageCypherKnowledgePolicyUnknownKalmanConfigKey, Fallback: "unknown Kalman config key: " + strconv.Quote(key), Data: map[string]any{"Key": key, "DisplayKey": strconv.Quote(key)}}
}

// CypherKnowledgePolicyUnsupportedCommand identifies Cypher that is not knowledge-policy DDL.
func CypherKnowledgePolicyUnsupportedCommand(command string) Message {
	return Message{ID: MessageCypherKnowledgePolicyUnsupportedCommand, Fallback: "unsupported knowledge policy command: " + command, Data: map[string]any{"Command": command}}
}

// CypherKnowledgePolicySchemaManagerUnavailable identifies unavailable schema support.
func CypherKnowledgePolicySchemaManagerUnavailable() Message {
	return Message{ID: MessageCypherKnowledgePolicySchemaManagerUnavailable, Fallback: "schema manager unavailable"}
}

// CypherKnowledgePolicyUnsupportedCommandType identifies an unexpected parsed command type.
func CypherKnowledgePolicyUnsupportedCommandType(commandType string) Message {
	return Message{ID: MessageCypherKnowledgePolicyUnsupportedCommandType, Fallback: "unsupported knowledge policy command type " + commandType, Data: map[string]any{"CommandType": commandType}}
}

// CypherKnowledgePolicyOperationFailed gives a schema operation typed identity without changing its text.
func CypherKnowledgePolicyOperationFailed(operation string, cause error) Message {
	return Message{ID: MessageCypherKnowledgePolicyOperationFailed, Fallback: cause.Error(), Data: map[string]any{"Operation": operation, "Cause": cause.Error()}}
}

// CypherKnowledgePolicyResolveTargetRequired identifies a resolve call without a target selector.
func CypherKnowledgePolicyResolveTargetRequired() Message {
	return Message{ID: MessageCypherKnowledgePolicyResolveTargetRequired, Fallback: "nornicdb.knowledgepolicy.resolve requires entityId, labels, or edgeType"}
}

// CypherKnowledgePolicyBindingTableUnavailable identifies unavailable compiled policy bindings.
func CypherKnowledgePolicyBindingTableUnavailable() Message {
	return Message{ID: MessageCypherKnowledgePolicyBindingTableUnavailable, Fallback: "knowledge policy binding table unavailable"}
}

// CypherKnowledgePolicyEntityNotFound identifies an unknown entity ID.
func CypherKnowledgePolicyEntityNotFound(entityID string) Message {
	return Message{ID: MessageCypherKnowledgePolicyEntityNotFound, Fallback: "entity not found: " + entityID, Data: map[string]any{"EntityID": entityID}}
}

// CypherKnowledgePolicyArgumentStringRequired identifies a non-string procedure argument.
func CypherKnowledgePolicyArgumentStringRequired(position int) Message {
	textPosition := strconv.Itoa(position)
	return Message{ID: MessageCypherKnowledgePolicyArgumentStringRequired, Fallback: "argument " + textPosition + " must be a string", Data: map[string]any{"Position": position}}
}

// CypherKnowledgePolicyDeindexBadgerRequired identifies unsupported deindex status storage.
func CypherKnowledgePolicyDeindexBadgerRequired() Message {
	return Message{ID: MessageCypherKnowledgePolicyDeindexBadgerRequired, Fallback: "deindex status requires BadgerDB storage backend"}
}

// CypherKnowledgePolicyDecayOptionsMapRequired identifies a non-map decay options argument.
func CypherKnowledgePolicyDecayOptionsMapRequired(valueType string) Message {
	return Message{ID: MessageCypherKnowledgePolicyDecayOptionsMapRequired, Fallback: "decayScore/decay options must be a map, got " + valueType, Data: map[string]any{"ValueType": valueType}}
}

// CypherKnowledgePolicyUnknownDecayOption identifies an unsupported decay function option.
func CypherKnowledgePolicyUnknownDecayOption(option string) Message {
	return Message{ID: MessageCypherKnowledgePolicyUnknownDecayOption, Fallback: "unknown decay option key: " + strconv.Quote(option), Data: map[string]any{"Option": option, "DisplayOption": strconv.Quote(option)}}
}
func CypherKnowledgePolicyReasonNoEntityArgument() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonNoEntityArgument, Fallback: "no entity argument"}
}

func CypherKnowledgePolicyReasonEntityNotFound() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonEntityNotFound, Fallback: "entity not found"}
}

func CypherKnowledgePolicyReasonNoBadgerEngine() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonNoBadgerEngine, Fallback: "no BadgerEngine"}
}

func CypherKnowledgePolicyReasonDecayDisabled() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonDecayDisabled, Fallback: "decay subsystem disabled"}
}

func CypherKnowledgePolicyReasonNoDecayProfile() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonNoDecayProfile, Fallback: "no decay profile"}
}

func CypherKnowledgePolicyReasonNoDecay() Message {
	return Message{ID: MessageCypherKnowledgePolicyReasonNoDecay, Fallback: "no decay"}
}
