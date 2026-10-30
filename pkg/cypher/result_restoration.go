package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// resultRestoration owns presentation traversal for exactly one source policy.
// Source diagnostics and quoted names select explicit policies in one tree
// owner, preserving container contracts without callbacks or a second type switch.
type resultRestoration struct {
	source *queryRewrite
	names  *quotedVariableNames
}

// restore owns result copying, metadata and EXPLAIN row presentation for both
// policies. Ordinary rows remain data; quoted RETURN columns use the projection
// owner, and each error policy keeps its original classification and cause.
func (restoration resultRestoration) restore(result *ExecuteResult, err error, parseItems func(braceView) []returnItem) (*ExecuteResult, error) {
	if restoration.source == nil && restoration.names == nil {
		return result, err
	}
	if result != nil {
		var columns []string
		if restoration.names != nil && len(result.Columns) > 0 {
			columns = make([]string, len(result.Columns))
		}
		for index, column := range result.Columns {
			var original string
			if restoration.names != nil {
				original = restoration.names.restoreColumn(column)
			} else {
				original = restoration.source.originalText(column)
			}
			if original == column && restoration.names == nil {
				continue
			}
			if columns == nil {
				columns = append([]string(nil), result.Columns...)
			}
			columns[index] = original
		}
		var metadata any
		metadataRestored := false
		if restoration.names == nil || len(result.Metadata) > 0 {
			metadata, metadataRestored = restoration.value(result.Metadata, sourceValueText)
		}
		restoreRows := false
		if restoration.names != nil {
			restoreRows = isExplainOrProfile(restoration.names.Original)
		} else if len(result.Rows) > 0 {
			restoreRows = isExplainOrProfileText(restoration.source.Text)
		}
		var rows any
		rowsRestored := false
		if restoreRows {
			rows, rowsRestored = restoration.value(result.Rows, sourceValueText)
		}
		if restoration.names != nil || columns != nil || metadataRestored || rowsRestored {
			restored := *result
			if columns != nil {
				restored.Columns = columns
			}
			if metadataRestored {
				restored.Metadata = metadata.(map[string]any)
			}
			if rowsRestored {
				restored.Rows = rows.([][]any)
			}
			if restoration.names != nil {
				if metadataRestored {
					// Re-render restored plans so padding and truncation use the
					// displayed names, rather than replacing rendered text.
					if plan, ok := restored.Metadata["plan"].(*ExecutionPlan); ok && plan != nil {
						if _, rendered := restored.Metadata["planString"]; rendered {
							restored.Metadata["planString"] = formatPlan(plan)
						}
					}
				}
				if !restoreRows && len(restored.Columns) > 0 {
					restoration.names.restoreResultColumns(&restored, parseItems)
				}
			}
			result = &restored
		}
	}
	if err != nil {
		if restoration.names == nil {
			err, _ = restoration.source.restoreSourceError(err)
		} else if len(restoration.names.internal) > 0 {
			if message := err.Error(); restoration.names.restoreSourceText(message, quotedNamesOnly) != message {
				err = &quotedVariableError{cause: err, message: restoration.names.restoreSourceText(message, quotedNamesOnly)}
			}
		}
	}
	return result, err
}

func (restoration resultRestoration) value(value any, purpose sourceRestorationPurpose) (any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		var copied map[string]any
		if restoration.names != nil {
			copied = make(map[string]any, len(typed))
		}
		for key, item := range typed {
			restored, changed := restoration.value(item, sourceValueText)
			if !changed && restoration.names == nil {
				continue
			}
			if copied == nil && changed {
				copied = make(map[string]any, len(typed))
				for k, v := range typed {
					copied[k] = v
				}
			}
			if copied != nil {
				if restoration.names != nil {
					key = restoration.names.restoreSourceText(key, quotedNamesOnly)
				}
				copied[key] = restored
			}
		}
		if copied != nil {
			return copied, true
		}
	case []any:
		var copied []any
		if restoration.names != nil {
			copied = make([]any, len(typed))
		}
		for index, item := range typed {
			restored, changed := restoration.value(item, sourceValueText)
			if !changed && restoration.names == nil {
				continue
			}
			if copied == nil && changed {
				copied = append([]any(nil), typed...)
			}
			if copied != nil {
				copied[index] = restored
			}
		}
		if copied != nil {
			return copied, true
		}
	case [][]any:
		var copied [][]any
		if restoration.names != nil {
			copied = make([][]any, len(typed))
		}
		for index, row := range typed {
			restored, changed := restoration.value(row, sourceValueText)
			if !changed && restoration.names == nil {
				continue
			}
			if copied == nil && changed {
				copied = append([][]any(nil), typed...)
			}
			if copied != nil {
				copied[index] = restored.([]any)
			}
		}
		if copied != nil {
			return copied, true
		}

	case localization.Message:
		if restoration.names != nil {
			return value, false
		}
		if typed.ID == localization.MessageCypherANTLRUnexpectedInput {
			line, lineOK := typed.Data["Line"].(int)
			column, columnOK := typed.Data["Column"].(int)
			if originalLine, originalColumn, found := restoration.source.OriginalPosition(line, column); lineOK && columnOK && found {
				if originalLine != line || originalColumn != column {
					return localization.CypherANTLRUnexpectedInput(originalLine, originalColumn), true
				}
			}
		}
		fallback, _ := restoration.value(typed.Fallback, sourceDiagnosticText)
		data, mapped := restoration.value(typed.Data, sourceValueText)
		if !mapped && fallback == typed.Fallback {
			return value, false
		}
		typed.Fallback = fallback.(string)
		if mapped {
			typed.Data = data.(map[string]any)
		}
		return typed, true
	case localization.MessageSequence:
		if restoration.names != nil {
			return value, false
		}
		var messages []localization.Message
		for index, message := range typed.Messages {
			if restored, changed := restoration.value(message, sourceValueText); changed {
				if messages == nil {
					messages = append([]localization.Message(nil), typed.Messages...)
				}
				messages[index] = restored.(localization.Message)
			}
		}
		if messages == nil {
			return value, false
		}
		typed.Messages = messages
		return typed, true
	case error:
		if restoration.names != nil {
			return value, false
		}
		return restoration.source.restoreSourceError(typed)
	case string:
		if restoration.names != nil {
			return restoration.names.restoreSourceText(typed, quotedNamesOnly), true
		}
		if purpose == sourceValueText {
			if restored := restoration.source.originalText(typed); restored != typed {
				return restored, true
			}
		}
		message := typed
		if strings.Contains(message, restoration.source.Text) {
			message = strings.ReplaceAll(message, restoration.source.Text, restoration.source.Original)
		}
		var out strings.Builder
		last := 0
		for index := 0; index < len(message); index++ {
			switch quote := message[index]; {
			case quote == '\'' || quote == '"' || quote == '`':
				close := strings.IndexByte(message[index+1:], quote)
				if close <= 0 {
					continue
				}
				inner := message[index+1 : index+1+close]
				if restored := restoration.source.originalText(inner); restored != inner {
					out.WriteString(message[last : index+1])
					out.WriteString(restored)
					last = index + 1 + close
				}
				index += close + 1
			case strings.HasPrefix(message[index:], "line "):
				line, column, end, ok := parseLineColumn(message, index+len("line "))
				if !ok {
					continue
				}
				originalLine, originalColumn, found := restoration.source.OriginalPosition(line, column)
				if !found {
					continue
				}
				out.WriteString(message[last : index+len("line ")])
				out.WriteString(strconv.Itoa(originalLine) + ":" + strconv.Itoa(originalColumn))
				last = end
				index = end - 1
			}
		}
		if last != 0 {
			out.WriteString(message[last:])
			message = out.String()
		}
		return message, message != typed
	case *ExecutionPlan:
		if restoration.names == nil {
			return value, false
		}
		if typed == nil {
			return value, false
		}
		plan := *typed
		plan.Query = restoration.names.restoreSourceText(typed.Query, quotedStatementFragment)
		root, _ := restoration.value(typed.Root, sourceValueText)
		plan.Root, _ = root.(*PlanOperator)
		return &plan, true
	case *PlanOperator:
		if restoration.names == nil {
			return value, false
		}
		if typed == nil {
			return value, false
		}
		operator := *typed
		operator.Description = restoration.names.restoreSourceText(typed.Description, quotedNamesOnly)
		if typed.Arguments != nil {
			arguments, _ := restoration.value(typed.Arguments, sourceValueText)
			operator.Arguments = arguments.(map[string]interface{})
		}
		operator.Identifiers = make([]string, len(typed.Identifiers))
		for i, identifier := range typed.Identifiers {
			if internal, ok := restoration.names.internal[identifier]; ok {
				identifier = internal.name
			}
			operator.Identifiers[i] = identifier
		}
		operator.Children = make([]*PlanOperator, len(typed.Children))
		for i, child := range typed.Children {
			restoredChild, _ := restoration.value(child, sourceValueText)
			operator.Children[i], _ = restoredChild.(*PlanOperator)
		}
		return &operator, true
	}
	return value, false
}
