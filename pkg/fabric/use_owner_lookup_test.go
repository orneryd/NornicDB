package fabric

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

type useOwnerLookupColumns map[string][]string

func (p useOwnerLookupColumns) CachedResultColumns(_ context.Context, query string, _ []string) resultmetadata.Names {
	if columns, ok := p[query]; ok {
		return resultmetadata.Known(columns)
	}
	return resultmetadata.Unknown()
}

func (p useOwnerLookupColumns) ResultProjection(ctx context.Context, query string, incoming []string, stage SourceStage) resultmetadata.Projection {
	return resultmetadata.Projection{Names: p.CachedResultColumns(ctx, query, incoming)}
}

func TestUseOwnerLookupExtent(t *testing.T) {
	for _, selector := range []string{"", "USE cmp.a ", "USE GRAPH cmp.a ", "USE GRAPH match ", "USE match ", "USE graph.byName('match') "} {
		t.Run(selector, func(t *testing.T) {
			spec, ok := parseApplyCorrelatedLookupSubquery(t.Context(), "WITH k "+selector+"MATCH (n) WHERE n.id = k RETURN count(*) AS c")
			require.True(t, ok)
			require.Equal(t, "k", spec.importCol)
			require.Equal(t, "(n)", spec.matchPart)
			require.Equal(t, "n.id = k", spec.wherePart)
			require.Equal(t, "count(*) AS c", spec.returnProjection)
			if selector == "" {
				require.Empty(t, spec.useClause)
			} else {
				require.Equal(t, selector[:len(selector)-1], spec.useClause)
			}
		})
	}
}

// Generate raw selector spellings for every correlated lookup consumer.
func generatedLookupSelectors() []string {
	selectors := []string{""}
	for _, gap := range []rune{'\t', '\n', '\v', '\f', '\r', ' ', '\u0085', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000'} {
		for _, modifier := range []string{"", "GRAPH" + string(gap)} {
			for _, reference := range []string{"cmp.a", "match", "`cmp`.`a`", "graph.byName('cmp.a')"} {
				selectors = append(selectors, "UsE"+string(gap)+modifier+reference+string(gap))
			}
		}
	}
	return selectors
}

func observeUseOwnerLookup(selector, shape string, rewrite func(string) string) error {
	projection, batchProjection := "n.v AS v", "n.v AS v"
	var value interface{} = "hit"
	if shape == "count" {
		projection, batchProjection, value = "count(*) AS c", "count(*) AS c", int64(1)
	} else if shape == "collect" {
		projection, batchProjection, value = "collect(n.v) AS v", "collect(n.v) AS v", []interface{}{"hit"}
	}
	column := "v"
	if shape == "count" {
		column = "c"
	}
	prefix := ""
	if selector != "" {
		prefix = strings.TrimSpace(selector) + " "
	}
	batchQuery := prefix + "MATCH (n) WHERE n.id IN $__fabric_apply_keys RETURN n.id AS __fabric_apply_key, " + batchProjection
	mock := &mockCypherExecutor{
		results: map[string]*ResultStream{batchQuery: {Columns: []string{"__fabric_apply_key", column}, Rows: [][]interface{}{{"key", value}}}},
		calls:   map[string]int{},
	}
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	input := &ResultStream{Columns: []string{"k"}, Rows: [][]interface{}{{"key"}}}
	inner := &FragmentExec{
		Input:     &FragmentInit{Columns: input.Columns, ImportColumns: input.Columns},
		Query:     "WITH k " + selector + "MATCH (n) WHERE n.id = k RETURN " + projection,
		GraphName: "db1", Columns: []string{column},
	}
	if rewrite != nil {
		inner.Query = rewrite(inner.Query)
	}
	// Standalone Fabric needs the embedding owner's established columns.
	// This fixture supplies exact facts, without another query reader.
	exec.SetResultColumnProvider(useOwnerLookupColumns{inner.Query: inner.Columns})
	var result *ResultStream
	var handled bool
	var err error
	ctx := context.Background()
	switch shape {
	case "stream":
		result, handled, err = exec.tryExecuteApplyBatchedLookup(ctx, nil, input.Columns, NewResultRowIterator(input), inner, nil, "", nil)
	case "rows":
		result, handled, err = exec.tryExecuteApplyBatchedLookupRows(ctx, nil, input, inner, nil, "")
	case "count":
		result, handled, err = exec.tryExecuteApplyBatchedCountLookup(ctx, nil, input, inner, nil, "")
	case "collect":
		result, handled, err = exec.tryExecuteApplyBatchedLookup(ctx, nil, input.Columns, nil, inner, nil, "", input)
	}
	if err != nil {
		return err
	}
	if !handled || result == nil {
		return fmt.Errorf("lookup consumer declined generated shape")
	}
	if !reflect.DeepEqual([]string{"k", column}, result.Columns) || !reflect.DeepEqual([][]interface{}{{"key", value}}, result.Rows) || mock.calls[batchQuery] != 1 {
		return fmt.Errorf("lookup result or selected source disagrees with owner")
	}
	return nil
}

func TestUseOwnerLookupExecution(t *testing.T) {
	for i, selector := range generatedLookupSelectors() {
		for _, shape := range []string{"stream", "rows", "count", "collect"} {
			t.Run(fmt.Sprintf("%s/selector%d", shape, i), func(t *testing.T) { require.NoError(t, observeUseOwnerLookup(selector, shape, nil)) })
		}
	}
}

func TestUseOwnerLookupNegativeControls(t *testing.T) {
	for _, shape := range []string{"stream", "rows", "count", "collect"} {
		selector := "USE\u00a0GRAPH\u00a0cmp.a "
		t.Run(shape, func(t *testing.T) {
			require.NoError(t, observeUseOwnerLookup(selector, shape, nil))
			// An independent text reader at this consumer's input selects a different
			// graph. Keep the expected batch, rows and owner product unchanged.
			rewrite := func(query string) string { return strings.Replace(query, selector, "USE cmp.decoy ", 1) }
			require.Error(t, observeUseOwnerLookup(selector, shape, rewrite), "consumer mutation survived")
		})
	}
}
