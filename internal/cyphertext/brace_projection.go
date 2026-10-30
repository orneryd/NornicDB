package cyphertext

import (
	"fmt"
	"sort"
)

// DelimiterSpan projects the endpoint pair already retained by the lexical
// owner. It never pairs rewritten tokens. Producer-declared query-body frames
// use the same endpoint projection. A generated or removed endpoint without a
// declaration is an explicit error. Author: #1663 implementation lane.
func (source *Source) DelimiterSpan(opening int) (Span, bool, error) {
	if opening < 0 || opening >= len(source.Text) || DelimiterCloser(source.Text[opening]) == 0 {
		return Span{}, false, nil
	}
	if source.Text[opening] == '{' {
		if record, found, err := source.QueryBodyAt(opening); found {
			if err != nil {
				return Span{}, true, err
			}
			return Span{Start: record.Open, End: record.Close + 1}, true, nil
		}
	}
	origin := source.Origin(Span{opening, opening + 1})
	if origin.Generated {
		return Span{}, false, fmt.Errorf("cyphertext: generated delimiter has no endpoint record")
	}
	facts := source.BraceFacts()
	open, found := facts.delimiterByOriginal[origin.Original]
	if !found {
		return Span{}, false, nil
	}
	close, complete := facts.Close(open)
	if !complete {
		return Span{}, false, nil
	}
	spans, err := source.ProjectBrace(BraceRecord{source: facts.source, Open: open, Close: close})
	if err != nil {
		return Span{}, true, err
	}
	for _, span := range spans {
		if span.Start == opening {
			return span, true, nil
		}
	}
	return Span{}, false, nil
}

// ConstructedBrace records endpoints declared by a rewrite producer. It owns
// no grammar: the producer supplies its typed construction to the statement's
// brace product. Origin continues to identify the new tokens as generated.
func (source *Source) ConstructedBrace(open, close int) BraceRecord {
	return BraceRecord{source: source, Open: open, Close: close}
}

// WithQueryBodies retains the query-body frames explicitly created by a
// producer. Original brace roles are unaffected; generated tokens keep their
// generated origins. No generated text is scanned to discover these frames.
func (source *Source) WithQueryBodies(spans []Span) *Source {
	if len(spans) == 0 {
		return source
	}
	view := source.Window(0, len(source.Text))
	for _, span := range spans {
		record := view.ConstructedBrace(span.Start, span.End-1)
		record.Body = true
		view.queryBodies = append(view.queryBodies, record)
	}
	return view
}

// QueryBodyAt reads a producer-declared frame in this exact occurrence.
// Original query-body records remain in BraceFacts; both use ProjectBrace.
func (source *Source) QueryBodyAt(open int) (BraceRecord, bool, error) {
	for stage := source; stage != nil; stage = stage.parent {
		for _, record := range stage.queryBodies {
			openings := source.ProjectFrom(stage, Span{record.Open, record.Open + 1})
			present := false
			for _, span := range openings {
				if span.Start == open && span.End == open+1 {
					present = true
					break
				}
			}
			if !present {
				continue
			}
			spans, err := source.ProjectBrace(record)
			if err != nil {
				return BraceRecord{}, true, err
			}
			for _, span := range spans {
				if span.Start == open {
					projected := source.ConstructedBrace(span.Start, span.End-1)
					projected.Body = true
					return projected, true, nil
				}
			}
		}
	}
	return BraceRecord{}, false, nil
}

// ProjectBrace projects the endpoint identity already paired by a retained
// record. It never scans output or substitutes a generated closing token.
// Repeated whole copies produce occurrences in output order. Missing either
// endpoint, or an unequal endpoint population, is an explicit error.
func (source *Source) ProjectBrace(record BraceRecord) ([]Span, error) {
	if record.source == nil || record.Close < record.Open {
		return nil, fmt.Errorf("cyphertext: brace record has no complete endpoint identity")
	}
	copies, err := source.CopyRelationsFrom(record.source)
	if err != nil {
		return nil, err
	}
	var openings, closings []int
	for _, copy := range copies {
		if copy.Original.Start <= record.Open && record.Open < copy.Original.End {
			openings = append(openings, copy.Accepted.Start+record.Open-copy.Original.Start)
		}
		if copy.Original.Start <= record.Close && record.Close < copy.Original.End {
			closings = append(closings, copy.Accepted.Start+record.Close-copy.Original.Start)
		}
	}
	if len(openings) == 0 || len(openings) != len(closings) {
		return nil, fmt.Errorf("cyphertext: brace endpoint was removed or generated")
	}
	sort.Ints(openings)
	sort.Ints(closings)
	result := make([]Span, len(openings))
	for index, open := range openings {
		if closings[index] <= open {
			return nil, fmt.Errorf("cyphertext: copied brace endpoints are not an occurrence")
		}
		result[index] = Span{Start: open, End: closings[index] + 1}
	}
	return result, nil
}
