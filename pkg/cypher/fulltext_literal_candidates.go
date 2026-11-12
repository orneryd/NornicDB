package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
)

// fulltextLiteralReader lowers positive restrictions directly from the existing
// immutable dialect tree. Matching, exclusions and scores keep their old owner;
// no second query tree or forwarding matcher is allocated.
type fulltextLiteralReader struct{}

func (fulltextLiteralReader) Describe(ctx context.Context, node ftNode) (search.FulltextLiteralNode, error) {
	if err := ctx.Err(); err != nil {
		return search.FulltextLiteralNode{}, err
	}
	switch node := node.(type) {
	case *ftLeafNode:
		if text, ok := node.pattern.(patText); ok {
			return search.FulltextLiteralNode{Op: search.FulltextLiteral, Text: text.text}, nil
		}
	case *ftBoostNode, *occurMarker:
		return search.FulltextLiteralNode{Op: search.FulltextAllOf, Children: 1}, nil
	case *ftBooleanNode:
		mandatory, optional := 0, 0
		for i, clause := range node.clauses {
			if i&31 == 0 {
				if err := ctx.Err(); err != nil {
					return search.FulltextLiteralNode{}, err
				}
			}
			switch node.occurrence(clause) {
			case occurMust:
				mandatory++
			case occurShould:
				optional++
			}
		}
		if mandatory > 0 {
			return search.FulltextLiteralNode{Op: search.FulltextAllOf, Children: len(node.clauses)}, nil
		}
		if optional > 0 {
			return search.FulltextLiteralNode{Op: search.FulltextAnyOf, Children: len(node.clauses)}, nil
		}
	}
	return search.FulltextLiteralNode{Op: search.FulltextAllCandidates}, nil
}

func (fulltextLiteralReader) Child(ctx context.Context, node ftNode, operation search.FulltextLiteralOperation, index int) (ftNode, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	switch node := node.(type) {
	case *ftBoostNode:
		return node.inner, true, nil
	case *occurMarker:
		return node.inner, true, nil
	case *ftBooleanNode:
		selected := occurShould
		if operation == search.FulltextAllOf {
			selected = occurMust
		}
		clause := node.clauses[index]
		return clause.node, node.occurrence(clause) == selected, nil
	}
	return nil, false, nil
}
