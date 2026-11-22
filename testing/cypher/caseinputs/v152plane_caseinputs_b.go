package caseinputs

// Shared NOR-403 statements keep executor and HTTP reproduction paths identical.
const NOR403FulltextIndex = "pdsource_text"

const NOR403RawStatement = `CALL db.index.fulltext.queryNodes('` + NOR403FulltextIndex + `', $query) YIELD node, score
RETURN node, score ORDER BY score DESC, node.id LIMIT $limit`

const NOR403EligibilityStatement = `CALL db.index.fulltext.queryNodes('` + NOR403FulltextIndex + `', $query) YIELD node, score
WHERE ((NOT EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(:PDCollection) }
        OR EXISTS { MATCH (:PDDocument {id: node.document_id})-[:MEMBER_OF]->(open_in:PDCollection) WHERE NOT coalesce(open_in.archived, false) }))
  AND (NOT node:PDSummary OR (coalesce(node.current, false) AND
        ((node.owner_kind = 'collection' AND EXISTS {
            MATCH (summary_collection:PDCollection {id: node.owner_id})
            WHERE NOT coalesce(summary_collection.archived, false) })
         OR (node.owner_kind = 'document' AND EXISTS {
            MATCH (summary_document:PDDocument {id: node.owner_id})
            MATCH (summary_version:PDVersion {id: summary_document.current_version_id, document: summary_document.id})
            WHERE summary_version.current_source_id IN node.source_ids
              AND (NOT EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(:PDCollection) }
                   OR EXISTS { MATCH (summary_document)-[:MEMBER_OF]->(open_in:PDCollection) WHERE NOT coalesce(open_in.archived, false) }) }))))
RETURN node, score ORDER BY score DESC, node.id LIMIT $limit`
