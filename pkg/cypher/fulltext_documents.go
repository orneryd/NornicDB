// Author: Codex implementation lane, 2026-10-26 (NOR-148).
// Author of change relevance: #1683 review-correction lane (DeepSeek), 2026-11-02 (NOR-148).
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
	id       string
	score    float64
	node     *storage.Node
	edge     *storage.Edge
	document *search.FulltextDocument
}

// withFulltextDocuments acquires one node/relationship corpus. The search
// service owns retained documents and their body document frequencies; the
// existing parser and evaluator remain the authority for the dialect's matching
// and scores. read receives the service's accepted read context, and this
// dialect forwards it to every document, entity and population read.
func (e *StorageExecutor) withFulltextDocuments(ctx context.Context, indexName string, labels, properties []string, relationships, ordered bool, read func(context.Context, search.FulltextDocuments) error) error {
	svc := e.search()
	requestLocal := svc == nil && e.searchResolver != nil
	if svc == nil {
		svc = e.newUnmanagedSearchService(search.DefaultVectorDimensions)
	}
	if requestLocal {
		// A database resolver that disables search owns no retained service.
		// Dispose this statement's service after its synchronous read returns.
		defer svc.Close()
	}
	if ordered {
		scope := search.FulltextDocumentScope{QueryNodes: e.queryNodeReader(ctx, e.storage) != nil, Relationships: relationships, Ordered: true}
		return svc.ReadFulltextDocuments(ctx, e.storage, scope, nil, read)
	}
	// Decay and REVEAL can change the visible corpus without a graph write.
	// Its owner remains the same, but its document view is request-scoped.
	fresh := requestLocal
	if storageHasDecayFiltering(e.storage) {
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
		if fulltextIndexNeedsNoDeclaration(index) {
			return true
		}
		_, exists := readFulltextIndexDefinition(ctx, e.storage.GetSchema(), index)
		return exists
	}
	candidate := func(node *storage.Node, edge *storage.Edge) bool { return fulltextIndexMember(node, edge, labels) }
	build := func(node *storage.Node, edge *storage.Edge) *search.FulltextDocument {
		if !candidate(node, edge) {
			return nil
		}
		doc := buildFulltextDoc(node, edge, properties)
		if doc == nil {
			return nil
		}
		return &search.FulltextDocument{Text: doc.contentLower, Tokens: doc.contentTokenN, Value: doc, MatchFields: doc.properties, RetainedBytes: doc.retainedSize}
	}
	scope := search.FulltextDocumentScope{QueryNodes: e.queryNodeReader(ctx, e.storage) != nil, NodeLabels: labels, Name: name, Definition: string(definition), Relationships: relationships, Fresh: fresh, Retain: retain, Candidate: candidate, EntityRows: true}
	return svc.ReadFulltextDocuments(ctx, e.storage, scope, build, read)
}

// fulltextIndexMember is the index's label or relationship-type membership, the
// one test that decides whether an entity can be one of its documents or rows.
// At most one of node and edge is non-nil.
func fulltextIndexMember(node *storage.Node, edge *storage.Edge, labels []string) bool {
	if edge != nil {
		return matchesRelationshipTypes(edge, labels)
	}
	return node != nil && matchesLabels(node, labels)
}

// fulltextUnrankedRows evaluates the original raw-row predicates during
// acquisition. Only accepted output rows are constructed, in release order.
func (e *StorageExecutor) fulltextUnrankedRows(ctx context.Context, indexName string, labels, properties []string, relationships, wildcard bool, property string, opts fulltextQueryOptions, result *ExecuteResult) error {
	if opts.limit == 0 {
		return nil
	}
	page := fulltextRowPage{result: result, opts: opts}
	return e.withFulltextDocuments(ctx, indexName, labels, properties, relationships, true, func(readCtx context.Context, source search.FulltextDocuments) error {
		if relationships {
			// VisitEdges checks readCtx before each relationship, as VisitNodes does.
			return source.VisitEdges(readCtx, func(edge *storage.Edge) error {
				if !fulltextIndexMember(nil, edge, labels) {
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
				if page.append([]any{e.procedureRelationship(edge), float64(1)}) {
					return storage.ErrIterationStopped
				}
				return nil
			})
		}
		return source.VisitNodes(readCtx, func(node *storage.Node) error {
			if !fulltextIndexMember(node, nil, labels) {
				return nil
			}
			var matched bool
			if wildcard {
				matched = extractTextContent(node, properties) != ""
			} else {
				matched = nodeHasNonEmptyProperty(node, property)
			}
			if matched {
				if page.append([]any{node, float64(1)}) {
					return storage.ErrIterationStopped
				}
			}
			return nil
		})
	})
}

func (e *StorageExecutor) fulltextHits(ctx context.Context, indexName, query string, labels, properties []string, relationships bool, opts fulltextQueryOptions) ([]fulltextHit, error) {
	parsed, err := ParseFulltextQuery(query)
	if err != nil {
		return nil, err
	}
	if parsed.IsEmpty() {
		return nil, nil
	}
	var hits []fulltextHit
	err = e.withFulltextDocuments(ctx, indexName, labels, properties, relationships, false, func(readCtx context.Context, corpus search.FulltextDocuments) error {
		hits = nil
		// The service supplies exact complete-corpus body frequencies; candidate
		// selection never substitutes final-match counts for those score inputs.
		frequencies, err := corpus.BodyDocumentFrequencies(readCtx, parsed.PrimaryTerms())
		if err != nil {
			return err
		}
		evalCtx := &ftEvalCtx{DefaultFields: properties, avgDocLen: corpus.AverageLength(), totalDocs: corpus.Count(), docFreq: frequencies}
		if err := search.ReadFulltextCandidates(readCtx, corpus, parsed.root, fulltextLiteralReader{}, func(id string, document *search.FulltextDocument) error {
			matched, score := parsed.Match(evalCtx, document.Value.(*ftDoc))
			if !matched {
				return nil
			}
			if score <= 0 {
				score = 1
			}
			hits = append(hits, fulltextHit{id: id, score: score, document: document})
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(hits, func(i, j int) bool {
			if hits[i].score == hits[j].score {
				return hits[i].id < hits[j].id
			}
			return hits[i].score > hits[j].score
		})
		hits = hits[min(opts.skip, len(hits)):]
		if opts.limit >= 0 && opts.limit < len(hits) {
			hits = hits[:opts.limit]
		}
		for i := range hits {
			node, edge, err := corpus.Entity(readCtx, hits[i].id, hits[i].document)
			if err != nil {
				return err
			}
			hits[i].node, hits[i].edge = node, edge
			hits[i].document = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return hits, nil
}
