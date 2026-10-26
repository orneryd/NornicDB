// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type fulltextHit struct {
	id    string
	score float64
	row   *fulltextDocumentRow
}

type fulltextDocumentRow struct {
	doc  *ftDoc
	node *storage.Node
	edge *storage.Edge
}

// fulltextHits is the one node/relationship Lucene scoring path. The search
// service owns retained documents and substring postings; the existing parser
// and evaluator remain the authority for the dialect's matching and scores.
func (e *StorageExecutor) withFulltextDocuments(ctx context.Context, indexName string, labels, properties []string, relationships, ordered bool, prepare func() (bool, error), read func(search.FulltextDocuments) error) error {
	svc := e.search()
	requestLocal := svc == nil && e.searchResolver != nil
	if svc == nil {
		svc = search.NewService(e.storage, search.DefaultVectorDimensions, nil)
		e.keepUnmanagedSearchService(svc)
	}
	if ordered {
		scope := search.FulltextDocumentScope{Relationships: relationships, Ordered: true}
		return svc.ReadFulltextDocuments(ctx, e.storage, scope, nil, read)
	}
	// Decay and REVEAL can change the visible corpus without a graph write.
	// Its owner remains the same, but its document view is request-scoped.
	fresh := requestLocal
	base := storage.UnwrapEngine(e.storage)
	if decay, ok := base.(interface{ IsDecayEnabled() bool }); ok && decay.IsDecayEnabled() {
		fresh = true
	}
	if suppression, ok := base.(interface{ HasManualSuppressions() bool }); ok && suppression.HasManualSuppressions() {
		fresh = true
	}
	// Schema identities use the same lossless string owner as catalog/schema
	// persistence, including admitted unpaired UTF16 units. A normalized JSON
	// replacement character cannot certify that two definitions are equal.
	definition, _ := jsontext.Marshal([][]string{labels, properties})
	name := "nodes:" + indexName
	if relationships {
		name = "relationships:" + indexName
	}
	retain := func(name string) bool {
		_, index, _ := strings.Cut(name, ":")
		if index == "" || builtInFulltextIndexes[index] {
			return true
		}
		if schema := e.storage.GetSchema(); schema != nil {
			_, exists := schema.GetFulltextIndex(index)
			return exists
		}
		return false
	}
	build := func(node *storage.Node, edge *storage.Edge) *search.FulltextDocument {
		var doc *ftDoc
		if relationships {
			if !matchesRelationshipTypes(edge, labels) {
				return nil
			}
			doc = buildEdgeFulltextDoc(edge, properties)
		} else {
			if !matchesLabels(node, labels) {
				return nil
			}
			doc = buildNodeFulltextDoc(node, properties)
		}
		if doc == nil {
			return nil
		}
		return &search.FulltextDocument{Text: doc.contentLower, Tokens: doc.contentTokenN, Value: &fulltextDocumentRow{doc: doc, node: node, edge: edge}}
	}
	scope := search.FulltextDocumentScope{Name: name, Definition: string(definition), Relationships: relationships, Fresh: fresh, Ordered: ordered, BeforeBuild: prepare, Retain: retain}
	return svc.ReadFulltextDocuments(ctx, e.storage, scope, build, read)
}

// fulltextUnrankedRows evaluates the original raw-row predicates during
// acquisition. Only accepted output rows are constructed, in release order.
func (e *StorageExecutor) fulltextUnrankedRows(ctx context.Context, indexName string, labels, properties []string, relationships, wildcard bool, property string, opts fulltextQueryOptions, result *ExecuteResult) error {
	seen := 0
	return e.withFulltextDocuments(ctx, indexName, labels, properties, relationships, true, nil, func(source search.FulltextDocuments) error {
		if relationships {
			// VisitEdges checks ctx before each relationship, as VisitNodes does.
			return source.VisitEdges(func(edge *storage.Edge) error {
				if !matchesRelationshipTypes(edge, labels) {
					return nil
				}
				var matched bool
				if wildcard {
					matched = len(properties) == 0 || edgeHasAnyNonEmptyProperty(edge, properties)
				} else {
					matched = edgeHasNonEmptyProperty(edge, property)
				}
				if !matched {
					return nil
				}
				if seen < opts.skip {
					seen++
					return nil
				}
				result.Rows = append(result.Rows, []any{e.procedureRelationship(edge), float64(1)})
				if opts.limit >= 0 && len(result.Rows) >= opts.limit {
					return storage.ErrIterationStopped
				}
				return nil
			})
		}
		return source.VisitNodes(func(node *storage.Node) error {
			if !matchesLabels(node, labels) {
				return nil
			}
			var matched bool
			if wildcard {
				matched = extractTextContent(node, properties) != ""
			} else {
				matched = nodeHasNonEmptyProperty(node, property)
			}
			if matched {
				result.Rows = append(result.Rows, []any{node, float64(1)})
			}
			return nil
		})
	})
}

func (e *StorageExecutor) fulltextHits(ctx context.Context, indexName, query string, labels, properties []string, relationships bool) ([]fulltextHit, error) {
	var hits []fulltextHit
	var parsed *FulltextQuery
	var err error
	// Nodes parse before acquiring rows; ranked relationships acquire AllEdges
	// first. This ordering is observable when both query and storage fail.
	if !relationships {
		parsed, err = ParseFulltextQuery(query)
		if err != nil {
			return nil, err
		}
		if parsed.IsEmpty() {
			return nil, nil
		}
	}
	err = e.withFulltextDocuments(ctx, indexName, labels, properties, relationships, false, func() (bool, error) {
		if relationships {
			parsed, err = ParseFulltextQuery(query)
			if err != nil {
				return false, err
			}
		}
		return !parsed.IsEmpty(), nil
	}, func(corpus search.FulltextDocuments) error {
		ctx := &ftEvalCtx{DefaultFields: properties, avgDocLen: corpus.AverageLength(), totalDocs: corpus.Count(), docFreq: make(map[string]int)}
		for _, term := range parsed.PrimaryTerms() {
			ctx.docFreq[term] = corpus.BodyDocumentFrequency(term)
		}
		corpus.Each(func(_ string, document *search.FulltextDocument) {
			row := document.Value.(*fulltextDocumentRow)
			matched, score := parsed.Match(ctx, row.doc)
			if !matched {
				return
			}
			if score <= 0 {
				score = 1
			}
			id := ""
			if relationships {
				id = string(row.edge.ID)
			} else {
				id = string(row.node.ID)
			}
			hits = append(hits, fulltextHit{id: id, score: score, row: row})
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score == hits[j].score {
			return hits[i].id < hits[j].id
		}
		return hits[i].score > hits[j].score
	})
	return hits, nil
}
