package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// Author: #1663 implementation lane. Shell grammar owns these source extents.
// The close callback consumes the lexical record while the original product
// is built, and the retained record when execution projects the command.
func ShellCommandSpans(text string, closeAt func(int) (int, bool)) (command, rest cyphertext.Span, ok bool) {
	input := cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{End: len(text)})
	rest = input
	if input.Start == input.End || text[input.Start] != ':' {
		return command, rest, false
	}
	lineEnd := len(text)
	if at := strings.IndexByte(text[input.Start:], '\n'); at >= 0 {
		lineEnd = input.Start + at
	}
	first := text[input.Start:lineEnd]
	fields := strings.Fields(first)
	if len(fields) == 0 {
		return command, rest, false
	}
	name := LowerASCII(fields[0])
	end := lineEnd
	if name == ":use" && len(fields) > 2 {
		database := input.Start + len(fields[0])
		for database < lineEnd && cyphertext.IsWhitespaceAt(text, database) {
			database++
		}
		end = database + len(fields[1])
		return cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{Start: input.Start, End: end}), cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{Start: end, End: len(text)}), true
	}
	argument := input.Start + len(fields[0])
	for argument < len(text) && cyphertext.IsWhitespaceAt(text, argument) {
		argument++
	}
	if (name == ":param" || name == ":params") && argument < len(text) && cyphertext.IsOpenBrace(text[argument]) {
		close, complete := closeAt(argument)
		if !complete {
			return command, rest, false
		}
		end = close + 1
		after := end
		for after < len(text) && (text[after] == ';' || cyphertext.IsWhitespaceAt(text, after)) {
			after++
		}
		return cyphertext.Span{Start: input.Start, End: end}, cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{Start: after, End: len(text)}), true
	}
	after := end
	if after < len(text) {
		after++
	}
	return cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{Start: input.Start, End: end}), cyphertext.TrimWhitespaceSpan(text, cyphertext.Span{Start: after, End: len(text)}), true
}

type ShellParameterInput struct {
	Key, Value cyphertext.Span
	MapForm    bool
}

// ReadShellParameterInput is the one shorthand producer. It binds its existing
// value extent directly; no outer map is emitted or admitted as new input.
func ReadShellParameterInput(input string) (ShellParameterInput, error) {
	whole := cyphertext.TrimWhitespaceSpan(input, cyphertext.Span{End: len(input)})
	result := ShellParameterInput{Value: whole}
	if whole.Start == whole.End {
		return result, braceLocalizedError(localization.CypherCommandRoutingParameterArgumentRequired(), nil)
	}
	text := input[whole.Start:whole.End]
	if cyphertext.IsOpenBrace(text[0]) {
		result.MapForm = true
		return result, nil
	}
	if at := strings.Index(text, "=>"); at >= 0 {
		result.Key = cyphertext.TrimWhitespaceSpan(input, cyphertext.Span{Start: whole.Start, End: whole.Start + at})
		result.Value = cyphertext.TrimWhitespaceSpan(input, cyphertext.Span{Start: whole.Start + at + 2, End: whole.End})
	} else {
		fields := strings.Fields(text)
		if len(fields) < 2 {
			return result, braceLocalizedError(localization.CypherCommandRoutingParameterUsage(), nil)
		}
		result.Key = cyphertext.Span{Start: whole.Start, End: whole.Start + len(fields[0])}
		result.Value = cyphertext.TrimWhitespaceSpan(input, cyphertext.Span{Start: result.Key.End, End: whole.End})
		if result.Value.Start < result.Value.End && input[result.Value.Start] == ':' {
			result.Value.Start++
			result.Value = cyphertext.TrimWhitespaceSpan(input, result.Value)
		}
	}
	key := input[result.Key.Start:result.Key.End]
	name, valid := cyphertext.ScanIdentifier(key, 0)
	if !valid || name.End != len(key) || cyphertext.SymbolicNameValue(key) == "" {
		return result, braceLocalizedError(localization.CypherCommandRoutingParameterUsage(), nil)
	}
	if result.Value.Start == result.Value.End {
		return result, braceLocalizedError(localization.CypherCommandRoutingParameterValueRequired(), nil)
	}
	return result, nil
}

func shellOriginalBraceFrames(source *cyphertext.Source, lexical *cyphertext.BraceFacts) ([]originalBraceFrame, bool) {
	if !strings.HasPrefix(strings.TrimSpace(source.Text), ":") {
		return nil, false
	}
	var frames []originalBraceFrame
	from := 0
	for from < len(source.Text) {
		command, rest, ok := ShellCommandSpans(source.Text[from:], func(open int) (int, bool) {
			record, found := lexical.At(from + open)
			return record.Close - from, found && record.Close >= record.Open
		})
		if !ok {
			break
		}
		text := source.Text[from+command.Start : from+command.End]
		fields := strings.Fields(text)
		if len(fields) > 0 && (LowerASCII(fields[0]) == ":param" || LowerASCII(fields[0]) == ":params") {
			argument := command.Start + len(fields[0])
			input, _ := ReadShellParameterInput(source.Text[from+argument : from+command.End])
			if input.Value.End > input.Value.Start {
				frames = append(frames, originalBraceFrame{span: cyphertext.Span{Start: from + argument + input.Value.Start, End: from + argument + input.Value.End}, expression: true})
			}
		}
		next := from + rest.Start
		if next <= from {
			break
		}
		from = next
		if from == len(source.Text) || source.Text[from] != ':' {
			break
		}
	}
	if from < len(source.Text) {
		frames = append(frames, originalBraceFrame{span: cyphertext.Span{Start: from, End: len(source.Text)}})
	}
	return frames, true
}
