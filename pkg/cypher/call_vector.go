package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: result headers come from procedure_result_columns.go.

import (
	"context"
	"errors"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"log/slog"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Neo4j Vector Index Procedures
// ========================================

// callDbIndexVectorQueryNodesWithVector implements the search half of
// db.index.vector.queryNodes. The statement has already bound and resolved the
// procedure's arguments, once, in callVectorQueryArguments.
//
// Parameters:
//   - indexName: Name of the vector index (from CREATE VECTOR INDEX)
//   - k: Number of results to return
//   - queryVector: The query embedding to search with
//
// Returns:
//   - node: The matched node with all properties
//   - score: Cosine similarity score (0.0 to 1.0)
func (e *StorageExecutor) callDbIndexVectorQueryNodesWithVector(ctx context.Context, indexName string, k int, queryVector []float32) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: columnNames(nodeScoreColumns),
		Rows:    [][]interface{}{},
	}

	// Get vector index configuration (if it exists) and decide service dimensions.
	// Prefer: 1) schema index dimensions, 2) query vector length, 3) search package default.
	var targetLabel, targetProperty string
	var similarityFunc string = "cosine"

	schema := e.storage.GetSchema()
	wantDims := 0
	indexFound := false
	if schema != nil {
		if vectorIdx, exists := schema.GetVectorIndex(indexName); exists {
			indexFound = true
			targetLabel = vectorIdx.Label
			targetProperty = vectorIdx.Property
			similarityFunc = vectorIdx.SimilarityFunc
			if vectorIdx.Dimensions > 0 {
				wantDims = vectorIdx.Dimensions
			}
		}
	}
	if !indexFound {
		svc := e.search()
		managed := svc != nil && svc.EmbeddingCount() > 0
		if !managed {
			stop := errors.New("managed vector found")
			err := e.storage.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{WithEmbeddings: true, Projection: []string{}}, func(node *storage.Node) error {
				for _, vector := range node.NamedEmbeddings {
					if len(vector) > 0 {
						return stop
					}
				}
				for _, vector := range node.ChunkEmbeddings {
					if len(vector) > 0 {
						return stop
					}
				}
				return nil
			})
			if err != nil && !errors.Is(err, stop) {
				return nil, err
			}
			managed = errors.Is(err, stop)
		}
		if !managed {
			return nil, noSuchVectorIndexError(indexName)
		}
	}
	if wantDims <= 0 && len(queryVector) > 0 {
		wantDims = len(queryVector)
	}
	if wantDims <= 0 {
		wantDims = search.DefaultVectorDimensions
	}

	// Per-DB master switch: when vector search is disabled for this database,
	// queryNodes must NOT silently fail and must NOT build an in-memory
	// index. Honour the wired-in service's flag *before* any replacement —
	// if we honoured it after, a dimension-driven replacement would
	// instantiate a fresh Service whose default flags are both ON, silently
	// bypassing the operator's disable.
	svc := e.search()
	if svc != nil && !svc.VectorEnabled() {
		e.logEvent(slog.LevelWarn, localization.CypherVectorSearchDisabledEvent(indexName))
		return result, nil
	}

	useService, err := prepareWiredNodeVectorService(ctx, svc, wantDims)
	if err != nil {
		return nil, err
	}
	if !useService {
		nodeScores, ok := e.fetchCosineNodeScoresExact(ctx, targetLabel, targetProperty, similarityFunc, k, queryVector, true, wantDims)
		if !ok {
			return nil, localizedError(localization.CypherSpecializedCallsVectorQueryFailed(), nil)
		}
		for _, hit := range nodeScores {
			result.Rows = append(result.Rows, []interface{}{hit.node, hit.score})
		}
		return result, nil
	}

	// Belt-and-braces: if anyone else flipped the flag on the wired service
	// since we entered this function, still bail.
	if !svc.VectorEnabled() {
		e.logEvent(slog.LevelWarn, localization.CypherVectorSearchDisabledEvent(indexName))
		return result, nil
	}

	hits, err := svc.VectorQueryNodes(ctx, queryVector, search.VectorQuerySpec{
		IndexName:  indexName,
		Label:      targetLabel,
		Property:   targetProperty,
		Similarity: similarityFunc,
		Limit:      k,
	})
	if err != nil {
		return nil, err
	}

	seenOrphans := make(map[string]bool)
	for _, hit := range hits {
		node, err := e.storage.GetNode(ctx, storage.NodeID(hit.ID))
		if err != nil || node == nil {
			if err != nil && errors.Is(err, storage.ErrNotFound) && svc != nil {
				if !seenOrphans[hit.ID] {
					e.logEvent(slog.LevelWarn, localization.CypherOrphanedEmbeddingDetectedEvent(hit.ID))
					// Unless the node was stored again after this read.
					if removeErr := svc.RefreshNode(ctx, storage.NodeID(hit.ID)); removeErr != nil {
						e.logEvent(slog.LevelError, localization.CypherOrphanedEmbeddingRemovalFailedEvent(hit.ID, removeErr))
					}
					seenOrphans[hit.ID] = true
				}
			}
			continue
		}
		result.Rows = append(result.Rows, []interface{}{
			node,
			hit.Score,
		})
	}

	return result, nil
}

// callDbIndexVectorEmbed implements db.index.vector.embed: the embedding of
// the call's evaluated STRING argument.
// Syntax: CALL db.index.vector.embed('query text') YIELD embedding
func (e *StorageExecutor) callDbIndexVectorEmbed(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	ctx, finish, err := e.queryEmbedder.BeginUse(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	embedder := e.queryEmbedderForStatement(ctx)
	if embedder == nil {
		return nil, localizedError(localization.CypherSpecializedCallsEmbedderNotConfigured(), nil)
	}
	text, err := requiredProcedureString("db.index.vector.embed", args, 0, "text")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return nil, localizedError(localization.CypherSpecializedCallsVectorEmbedTextRequired(), nil)
	}

	embedding, err := embedQueryChunked(ctx, embedder, text)
	if err != nil {
		return nil, localizedError(localization.CypherSpecializedCallsVectorEmbedFailed(err), err)
	}

	return &ExecuteResult{
		Columns: columnNames(dbIndexVectorEmbedColumns),
		Rows:    [][]interface{}{{embedding}},
	}, nil
}

// noSuchVectorIndexError is the failure of a vector query naming no vector
// index, as Neo4j's ProcedureCallFailed.
func noSuchVectorIndexError(indexName string) error {
	return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed",
		fmt.Sprintf("There is no such vector schema index: %s", indexName))
}

// vectorQueryInputTypes is the third argument's contract in the diagnostics a
// vector query reports for a value of another type.
const vectorQueryInputTypes = "[]float32, []float64, []interface{}, or string"

// vectorQueryInputDiagnostics names the localized messages one vector query
// procedure reports while it resolves its third argument. The node and
// relationship procedures keep their own message identities; the resolution
// itself is shared.
type vectorQueryInputDiagnostics struct {
	stringQueryEmbedderRequired func() localization.Message
	embedQueryFailed            func(query string, cause error) localization.Message
	queryInputRequired          func() localization.Message
}

var (
	nodeVectorQueryInputDiagnostics = vectorQueryInputDiagnostics{
		stringQueryEmbedderRequired: localization.CypherSpecializedCallsStringQueryEmbedderRequired,
		embedQueryFailed:            localization.CypherSpecializedCallsEmbedQueryTextFailed,
		queryInputRequired:          localization.CypherSpecializedCallsQueryInputRequired,
	}
	relationshipVectorQueryInputDiagnostics = vectorQueryInputDiagnostics{
		stringQueryEmbedderRequired: localization.CypherProceduresStringQueryEmbedderRequired,
		embedQueryFailed:            localization.CypherProceduresEmbedQueryFailed,
		queryInputRequired:          localization.CypherProceduresQueryInputRequired,
	}
)

// vectorQueryCoordinates is the one owner of a vector query's third-argument
// resolution. The statement has already evaluated and bound the argument, so
// this is the actual value: search text is embedded server-side, a numeric
// list is the query vector, and any other value is the argument error its type
// earns.
func (e *StorageExecutor) vectorQueryCoordinates(ctx context.Context, procedure string, diagnostics vectorQueryInputDiagnostics, query interface{}) ([]float32, error) {
	if text, isText := query.(string); isText {
		if text == "" {
			return nil, localizedError(diagnostics.queryInputRequired(), nil)
		}
		if e.queryEmbedderForStatement(ctx) == nil {
			return nil, localizedError(diagnostics.stringQueryEmbedderRequired(), nil)
		}
		embedded, err := e.embedVectorQueryText(ctx, text)
		if err != nil {
			return nil, localizedError(diagnostics.embedQueryFailed(text, err), err)
		}
		return embedded, nil
	}
	list, isList := valueconvert.ReadList(query)
	if !isList {
		return nil, localizedError(localization.CypherProceduresArgumentType(procedure, "query", vectorQueryInputTypes, cypherTypeName(query)), nil)
	}
	coordinates, issue := list.Numbers32()
	if issue != nil {
		if issue.OutOfRange {
			return nil, localizedError(localization.CypherCoreFunctionArgumentInvalid("vector query", "representable numeric coordinates", fmt.Sprint(issue.Value)), nil)
		}
		return nil, localizedError(localization.CypherProceduresArgumentType(procedure, "query", "numeric coordinates", cypherTypeName(issue.Value)), nil)
	}
	return coordinates, nil
}

// callVectorQueryArguments is the one owner of the two vector query
// procedures' arguments: it checks the index name and neighbour count, resolves
// the query input, then hands the resolved vector to the procedure's own
// search.
func (e *StorageExecutor) callVectorQueryArguments(ctx context.Context, args []interface{}, relationships bool) (*ExecuteResult, error) {
	if len(args) != 3 {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentCount", "vector queries require three arguments")
	}
	procedure := "db.index.vector.queryNodes"
	diagnostics := nodeVectorQueryInputDiagnostics
	if relationships {
		procedure = "db.index.vector.queryRelationships"
		diagnostics = relationshipVectorQueryInputDiagnostics
	}
	// A null name or count fails the call (ProcedureCallFailed), as in
	// Neo4j; a value of another type is a TypeError.
	indexName, err := requiredProcedureString(procedure, args, 0, "indexName")
	if err != nil {
		return nil, err
	}
	count, err := requiredProcedureInteger(procedure, args, 1, "numberOfNearestNeighbours")
	if err != nil {
		return nil, err
	}
	queryVector, err := e.vectorQueryCoordinates(ctx, procedure, diagnostics, args[2])
	if err != nil {
		return nil, err
	}
	if relationships {
		return e.callDbIndexVectorQueryRelationshipsWithVector(ctx, indexName, int(count), queryVector)
	}
	return e.callDbIndexVectorQueryNodesWithVector(ctx, indexName, int(count), queryVector)
}
