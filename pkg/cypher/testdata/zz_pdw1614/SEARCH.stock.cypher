
CALL () {
  UNWIND CASE WHEN $qid IS NULL THEN $anchors ELSE [] END AS anchor_id
  OPTIONAL MATCH (given:PDGraphNode {id: anchor_id})
  OPTIONAL MATCH (given_source:PDSearchSource {id: anchor_id})
  OPTIONAL MATCH (given_version:PDVersion {id: given.current_version_id, document_id: given.id})
OPTIONAL MATCH (given_current:PDSearchSource {id: given_version.current_source_id})

  WITH CASE WHEN given:PDDocument THEN given_current
            ELSE coalesce(given, given_source) END AS anchor_hit
  WHERE anchor_hit IS NOT NULL AND (NOT anchor_hit:PDSummary OR (coalesce(anchor_hit.current, false) AND anchor_hit.search_hidden = false))
  RETURN anchor_hit AS hit, null AS score, true AS anchored, null AS ranking,
         null AS matches
  UNION ALL
  CALL db.rretrieve(apoc.map.fromPairs([option IN [['query', $query],
        ['types', ['PDSearchSource', 'PDSearchable']], ['where', CASE WHEN $qid IS NULL
           THEN '(node:PDSearchSource OR $history OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT coalesce(node.search_hidden, false) AND (NOT node:PDSummary OR node.search_hidden = false) AND node.record_set_id IS NULL'
           END],
        ['n', $page_size], ['qid', $qid], ['failClosed', true], ['minRRFScore', 0],
        ['minSimilarity', $min_similarity], ['rerankMinScore', $rerank_min_score]]
      WHERE option[1] IS NOT NULL]))
  YIELD page
  UNWIND CASE WHEN size(page.results) = 0 THEN [null] ELSE page.results END AS result
  RETURN result.node AS hit, result.score AS score,
         false AS anchored,
         {search_method: page.search_method, fallback_triggered: page.fallback_triggered,
          fallback_reason: page.fallback_reason,
          query_chunks_searched: coalesce(page.query_chunks_searched, 0),
          query_chunks_total: coalesce(page.query_chunks_total, 0),
          qid: page.qid, has_more: page.has_more, total: page.total} AS ranking,
         result.matches AS matches
}
WITH
     hit, max(score) AS score,
     max(CASE WHEN anchored THEN 1 ELSE 0 END) = 1 AS anchored,
     head([outcome IN collect(ranking) WHERE outcome IS NOT NULL]) AS ranking,
     head([found IN collect(matches) WHERE found IS NOT NULL]) AS matches
WITH
     collect({node: hit, source: hit:PDSearchSource,
              document_id: hit.document_id, score: score, anchored: anchored,
              ranking: ranking, matches: matches}) AS candidates
WITH
     [c IN candidates WHERE c.node IS NOT NULL] AS candidates,
     head([c IN candidates WHERE c.ranking IS NOT NULL]).ranking AS ranking
UNWIND CASE WHEN size(candidates) = 0 THEN [null] ELSE candidates END AS candidate
WITH
     candidate, ranking
OPTIONAL MATCH (owner:PDDocument {id: candidate.document_id})
WHERE candidate.source
OPTIONAL MATCH (owner_version:PDVersion {id: owner.current_version_id, document_id: owner.id})
OPTIONAL MATCH (owner_current:PDSearchSource {id: owner_version.current_source_id})

WITH
     candidate, ranking,
     [{node: candidate.node, matched: true, matches: candidate.matches}] +
     CASE WHEN owner_current IS NULL OR owner_current.id = candidate.node.id THEN []
          ELSE [{node: owner_current, matched: false, matches: null}] END
       AS entries
UNWIND entries AS entry
WITH
     entry.node AS node, ranking,
     max(CASE WHEN entry.matched THEN candidate.score END) AS score,
     max(CASE WHEN entry.matched AND candidate.anchored THEN 1 ELSE 0 END) = 1 AS anchored,
     max(CASE WHEN entry.matched THEN 1 ELSE 0 END) = 1 AS matched,
     head([found IN collect(entry.matches) WHERE found IS NOT NULL]) AS matches
OPTIONAL MATCH (d:PDDocument {id: node.document_id})
WHERE node:PDSearchSource
OPTIONAL MATCH (current:PDVersion {id: d.current_version_id, document_id: d.id})

OPTIONAL MATCH (:PDVersion {id: node.version_id})-[:HAS_ORIGINAL]->(original:PDOriginal)

CALL (node, matches) {
  UNWIND coalesce(matches, []) AS m
  WITH node, m, matches,
       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL
            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_starts[i] < m.end AND node.view_ends[i] > m.start] | CASE WHEN node.view_library[at] THEN node.view_source_starts[at] ELSE node.view_source_starts[at] + CASE WHEN m.start > node.view_starts[at] THEN m.start - node.view_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_starts[i] < m.end AND node.view_ends[i] > m.start] | CASE WHEN node.view_library[at] THEN node.view_source_ends[at] ELSE node.view_source_starts[at] + CASE WHEN m.end < node.view_ends[at] THEN m.end ELSE node.view_ends[at] END - node.view_starts[at] END])}
            ELSE {start: m.start, end: m.end} END AS placed
  WITH node, m, placed, matches,
       CASE WHEN node:PDSearchSource AND (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own'))
            THEN [i IN range(0, size(coalesce(node.section_roles, [])) - 1)
                  WHERE node.section_starts[i] < placed.end AND placed.start < node.section_ends[i]
                  | {start: CASE WHEN placed.start > node.section_starts[i]
                                 THEN placed.start ELSE node.section_starts[i] END,
                     end: CASE WHEN placed.end < node.section_ends[i]
                               THEN placed.end ELSE node.section_ends[i] END,
                     role: node.section_roles[i], key: node.section_content_keys[i]}]
            ELSE [{start: placed.start, end: placed.end, role: 'own', key: ''}] END AS spans
  UNWIND spans AS span
  WITH node, m, placed, span, matches
  WHERE span.role <> 'duplicate'
    AND (NOT (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own')) OR m.kind = 'term'
         OR NOT any(t IN matches WHERE t.kind = 'term')
         OR any(t IN matches WHERE t.kind = 'term'
                AND span.start < t.end AND t.start < span.end))
    AND (span.role <> 'quoted' OR NOT EXISTS {
    MATCH (key:PDEmailText {id: span.key})<-[:HAS_EMAIL_OWN]-(written:PDSearchSource)
    WHERE written.id <> node.id AND (written:PDSearchSource OR false OR coalesce(written.current, true)) AND (NOT written:PDSummary OR coalesce(written.current, false)) AND NOT coalesce(written.search_hidden, false) AND (NOT written:PDSummary OR written.search_hidden = false)
      AND span.key <> ''
  })
  WITH m, placed, span,
       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL
            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_source_starts[i] < span.end AND node.view_source_ends[i] > span.start] | CASE WHEN node.view_library[at] THEN node.view_starts[at] ELSE node.view_starts[at] + CASE WHEN span.start > node.view_source_starts[at] THEN span.start - node.view_source_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_source_starts[i] < span.end AND node.view_source_ends[i] > span.start] | CASE WHEN node.view_library[at] THEN node.view_ends[at] ELSE node.view_starts[at] + CASE WHEN span.end < node.view_source_ends[at] THEN span.end ELSE node.view_source_ends[at] END - node.view_source_starts[at] END])} END AS seen
  RETURN collect({kind: m.kind, property: m.property, start: span.start, end: span.end,
                  view_start: CASE WHEN seen IS NULL THEN null
                                   WHEN span.start = placed.start THEN m.start
                                   ELSE last(apoc.coll.sort([m.start, seen.start])) END,
                  view_end: CASE WHEN seen IS NULL THEN null
                                 WHEN span.end = placed.end THEN m.end
                                 ELSE head(apoc.coll.sort([m.end, seen.end])) END,
                  score: m.score, term: m.term, role: span.role}) AS visible_matches
}

WITH d, node, score, anchored, matched, ranking,
     current, original, visible_matches AS matches,
     (node IS NULL OR anchored OR NOT matched OR NOT node:PDSearchSource
      OR NOT (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own')) OR size(visible_matches) > 0) AS visible
OPTIONAL MATCH (fam_member:PDDocument {family_key: d.family_key})
WITH node, score, anchored, matched, visible, matches, ranking, current, original, d, collect(DISTINCT fam_member.id) AS fam_member_ids
WITH node, score, anchored, matched, visible, matches, ranking, current, original, d,
     CASE WHEN d IS NULL THEN []
          ELSE [d.id]
               + apoc.coll.sort([fam_id IN fam_member_ids WHERE fam_id <> d.id]) END
       AS family_ids,
     CASE WHEN d IS NULL THEN null
          ELSE coalesce(d.family_key, d.id) END AS family_key
WITH
     node, score, anchored, matched, visible, ranking, current, original,
     coalesce(family_key, node.id) AS family,
     CASE WHEN node:PDSearchSource
          THEN apoc.coll.sort([(d)-[:MEMBER_OF]->(__pd_collection:PDCollection) | __pd_collection.id])
          ELSE node.collection_ids END AS collection_ids,
     [m IN coalesce(matches, []) |
      {kind: m.kind,
       property: CASE WHEN m.property = 'view_text' THEN 'text' ELSE m.property END,
       start: m.start, end: m.end, view_start: m.view_start, view_end: m.view_end,
       score: m.score,
       role: CASE WHEN node:PDSummary THEN 'generated' ELSE m.role END,
       term: m.term,
       quote: substring(node[m.property], coalesce(m.view_start, m.start),
                        coalesce(m.view_end, m.end) - coalesce(m.view_start, m.start)),
       pages: [i IN range(0, size(coalesce(node.page_starts, [])) - 1) WHERE node.page_starts[i] < m.end AND m.start < node.page_ends[i] | node.page_numbers[i]], sections: [title IN [i IN range(0, size(coalesce(node.section_starts, [])) - 1) WHERE node.section_starts[i] < m.end AND m.start < node.section_ends[i] | node.section_titles[i]] WHERE title <> ''], cells: [i IN range(0, size(coalesce(node.cell_starts, [])) - 1) WHERE node.cell_starts[i] < m.end AND m.start < node.cell_ends[i] | node.cell_refs[i]]}] AS passages
OPTIONAL MATCH (node)-[by:PD_RELATION|ABOUT]-(linked)
WHERE NOT node:PDSearchSource AND coalesce(by.eff_usable, false)
WITH
     node, score, anchored, matched, visible, ranking, current, original, family,
     collection_ids, passages,
     [x IN collect(DISTINCT {id: linked.id, kind: coalesce(by.kind, type(by)),
                             labels: labels(linked), citation_unsupported: by.citation_unsupported, 
         citation_reason: by.citation_reason}) WHERE x.id IS NOT NULL] AS linked

CALL (node) {
  WITH node WHERE node:PDSummary
  OPTIONAL MATCH (owner_document:PDDocument {id: node.owner_id})
  WHERE node:PDSummary AND node.owner_kind = 'document'
  OPTIONAL MATCH (owner_collection:PDCollection {id: node.owner_id})
  WHERE node:PDSummary AND node.owner_kind = 'collection'
  CALL (node, owner_collection) {
    CALL (node, owner_collection) {
      WITH node WHERE node:PDSummary AND node.owner_kind = 'document'
      UNWIND node.document_ids AS document_id
      MATCH (covered:PDDocument {id: document_id}) RETURN covered
      UNION ALL
      WITH node, owner_collection WHERE owner_collection IS NOT NULL
      MATCH (:PDCollection {id: owner_collection.id})<-[:MEMBER_OF]-(covered:PDDocument)
      WHERE node.summary_kind = 'collection' OR covered.id IN node.document_ids
      RETURN covered
    }
OPTIONAL MATCH (version:PDVersion {id: covered.current_version_id, document_id: covered.id})

    WITH covered, version ORDER BY covered.id
    RETURN collect({document_id: covered.id, version_id: version.id,
                    source_id: version.current_source_id}) AS sources
  }
  CALL (node) {
    UNWIND range(0, size(coalesce(node.citation_source_ids, [])) - 1) AS at
    MATCH (source:PDSearchSource {id: node.citation_source_ids[at]})
    RETURN collect({statement: node.citation_statement[at], source_id: source.id,
                    document_id: source.document_id, start: node.cite_start[at],
                    end: node.cite_end[at],
                    quote: substring(source.text, node.cite_start[at],
                                     node.cite_end[at] - node.cite_start[at])}) AS citations
  }
  RETURN {sources: sources, citations: citations, child_ids: node.child_ids,
          collection_ids: CASE WHEN owner_collection IS NOT NULL THEN [owner_collection.id]
            ELSE apoc.coll.sort([(owner_document)-[:MEMBER_OF]->(__pd_collection:PDCollection) | __pd_collection.id]) END,
          workflow_id: coalesce(owner_document.summary_workflow_id,
                                owner_collection.summary_workflow_id),
          status: coalesce(owner_document.summary_status, owner_collection.summary_status),
          error_code: coalesce(owner_document.summary_error_code,
                               owner_collection.summary_error_code),
          error_message: coalesce(owner_document.summary_error_message,
                                  owner_collection.summary_error_message),
          provider_request_id: coalesce(owner_document.summary_provider_request_id,
                                        owner_collection.summary_provider_request_id),
          next_action: coalesce(owner_document.summary_next_action,
                                owner_collection.summary_next_action),
          out_of_date: coalesce(node.input_fingerprint <>
                               coalesce(owner_document.summary_input,
                                        owner_collection.summary_input), true)} AS summary_data
  UNION ALL
  WITH node WHERE node IS NULL OR NOT node:PDSummary
  RETURN {} AS summary_data
}

WITH
     family, CASE WHEN visible THEN score END AS score,
     anchored, ranking,
     CASE WHEN visible THEN {type: CASE WHEN node:PDSearchSource THEN 'source'
                 WHEN node:PDSummary THEN 'summary' ELSE 'record' END,
      kind: CASE WHEN node:PDSearchSource THEN null ELSE coalesce(node.kind, 'record') END,
      id: node.id, score: score, anchored: anchored, matched: matched,
      generated: node:PDSummary,
      label: CASE WHEN node:PDSummary THEN 'Generated summary' END,
      summary_kind: node.summary_kind,
      out_of_date: CASE WHEN node:PDSummary THEN summary_data.out_of_date END,
      sources: CASE WHEN node:PDSummary THEN summary_data.sources END,
      citations: CASE WHEN node:PDSummary THEN summary_data.citations END,
      statement_citation_reasons: node.statement_citation_reasons,
      child_ids: node.child_ids,
      summary_workflow_id: summary_data.workflow_id, summary_status: summary_data.status,
      summary_error_code: summary_data.error_code,
      summary_error_message: summary_data.error_message,
      summary_provider_request_id: summary_data.provider_request_id,
      summary_next_action: summary_data.next_action,
      document_id: node.document_id,
      version_id: node.version_id,
      current: CASE WHEN node:PDSearchSource
                    THEN current IS NOT NULL AND current.id = node.version_id
                         AND current.current_source_id = node.id
                    ELSE coalesce(node.current, true) END,
      current_version_id: current.id, current_source_id: current.current_source_id,
      original_hash: original.content_hash,
      record_sets: CASE WHEN node:PDSearchSource AND (coalesce(node.profiled_run_layout, '') = coalesce(node.run_layout, '') OR (NOT (true IN coalesce(node.run_bulk, []))
        AND NOT EXISTS { MATCH (node)-[:PD_HAS_RECORD_SET|PD_MEMBER_OF_SET]->(:PDRecordSet) })) THEN [(node)-[run:PD_HAS_RECORD_SET]->(set:PDRecordSet) |
        {set_id: set.id, kind: set.kind, run: run.run, title: set.title, records: set.records,
         fields: set.fields,
         start: node.run_starts[run.run], end: node.run_ends[run.run],
         same_as: head([i IN range(0, size(coalesce(node.set_copy_runs, [])) - 1)
                        WHERE node.set_copy_runs[i] = run.run
                        | {set_id: node.set_copy_of[i], document_id: node.set_copy_documents[i]}]),
         matched: [ordinal IN apoc.coll.toSet([p IN passages
                       WHERE p.kind = 'term' AND p.property = 'text' AND p.start >= node.run_starts[run.run]
                             AND p.start < node.run_ends[run.run]
                       | CASE WHEN size([first IN node.run_record_starts[
                                            reduce(before = 0, i IN range(0, run.run - 1)
                                                   | before + node.run_records[i])
                                            ..reduce(upto = 0, i IN range(0, run.run)
                                                     | upto + node.run_records[i])]
                                         WHERE first <= p.start])
                                 > size([last IN node.run_record_ends[
                                            reduce(before = 0, i IN range(0, run.run - 1)
                                                   | before + node.run_records[i])
                                            ..reduce(upto = 0, i IN range(0, run.run)
                                                     | upto + node.run_records[i])]
                                         WHERE last <= p.start])
                              THEN size([first IN node.run_record_starts[
                                            reduce(before = 0, i IN range(0, run.run - 1)
                                                   | before + node.run_records[i])
                                            ..reduce(upto = 0, i IN range(0, run.run)
                                                     | upto + node.run_records[i])]
                                         WHERE first <= p.start]) - 1
                              ELSE -1 END])
                   WHERE ordinal >= 0]}]
      + [(node)-[member:PD_MEMBER_OF_SET]->(group:PDRecordSet) |
         {set_id: group.id, kind: 'group', member_document_id: node.document_id,
          member_source_id: node.id, title: group.title,
          records: group.records, fields: group.fields, members: group.members,
          document_id: group.document_id}] END,
      rows_requested_at: CASE WHEN node:PDSearchSource
                              THEN head(COLLECT { MATCH (requested:PDDocument {id: node.document_id})
                                                  WHERE coalesce(requested.rows_requested_version_id = node.version_id, false)
                                                  RETURN requested.rows_requested_at AS at }) END,
      collection_ids: CASE WHEN node:PDSummary THEN summary_data.collection_ids
                          ELSE collection_ids END,
      passages: passages, origin: node.origin, status: node.status,
      revision: node.revision, citation_unsupported: node.citation_unsupported, 
         citation_reason: node.citation_reason,
      text: CASE WHEN node:PDSearchSource THEN null ELSE node.search_text END,
      linked: linked[0..25], linked_total: size(linked),
      linked_capped: size(linked) > 25} END AS hit
ORDER BY anchored DESC, score DESC, hit.id
WITH
     family, ranking,
     collect(hit) AS variants, max(CASE WHEN anchored THEN 1 ELSE 0 END) = 1 AS anchored,
     max(score) AS score
ORDER BY anchored DESC, score DESC, family
WITH
     ranking,
     [f IN collect({family: family, anchored: anchored, score: score, variants: variants})
      WHERE f.family IS NOT NULL AND size(f.variants) > 0] AS families
CALL () {
  UNWIND CASE WHEN $qid IS NULL THEN [true] ELSE [] END AS first_page
  CALL db.retrieve(apoc.map.fromPairs([option IN [['query', $query], ['types', ['PDSearchSource']],
        ['where', '(node:PDSearchSource OR $history OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT coalesce(node.search_hidden, false) AND (NOT node:PDSummary OR node.search_hidden = false) AND node.record_set_id IS NOT NULL'], ['failClosed', true], ['minRRFScore', 0],
        ['minSimilarity', $min_similarity], ['limit', 250]]
      WHERE option[1] IS NOT NULL]))
  YIELD node AS member, score AS member_score, matches AS member_matches
  MATCH (set:PDRecordSet {id: member.record_set_id})
CALL (member, member_matches) {
  UNWIND coalesce(member_matches, []) AS m
  WITH member, m, member_matches,
       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL
            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_starts[i] < m.end AND member.view_ends[i] > m.start] | CASE WHEN member.view_library[at] THEN member.view_source_starts[at] ELSE member.view_source_starts[at] + CASE WHEN m.start > member.view_starts[at] THEN m.start - member.view_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_starts[i] < m.end AND member.view_ends[i] > m.start] | CASE WHEN member.view_library[at] THEN member.view_source_ends[at] ELSE member.view_source_starts[at] + CASE WHEN m.end < member.view_ends[at] THEN m.end ELSE member.view_ends[at] END - member.view_starts[at] END])}
            ELSE {start: m.start, end: m.end} END AS placed
  WITH member, m, placed, member_matches,
       CASE WHEN member:PDSearchSource AND (size(coalesce(member.email_own_keys, [])) > 0 OR any(role IN coalesce(member.section_roles, []) WHERE role <> 'own'))
            THEN [i IN range(0, size(coalesce(member.section_roles, [])) - 1)
                  WHERE member.section_starts[i] < placed.end AND placed.start < member.section_ends[i]
                  | {start: CASE WHEN placed.start > member.section_starts[i]
                                 THEN placed.start ELSE member.section_starts[i] END,
                     end: CASE WHEN placed.end < member.section_ends[i]
                               THEN placed.end ELSE member.section_ends[i] END,
                     role: member.section_roles[i], key: member.section_content_keys[i]}]
            ELSE [{start: placed.start, end: placed.end, role: 'own', key: ''}] END AS spans
  UNWIND spans AS span
  WITH member, m, placed, span, member_matches
  WHERE span.role <> 'duplicate'
    AND (NOT (size(coalesce(member.email_own_keys, [])) > 0 OR any(role IN coalesce(member.section_roles, []) WHERE role <> 'own')) OR m.kind = 'term'
         OR NOT any(t IN member_matches WHERE t.kind = 'term')
         OR any(t IN member_matches WHERE t.kind = 'term'
                AND span.start < t.end AND t.start < span.end))
    AND (span.role <> 'quoted' OR NOT EXISTS {
    MATCH (key:PDEmailText {id: span.key})<-[:HAS_EMAIL_OWN]-(written:PDSearchSource)
    WHERE written.id <> member.id AND (written:PDSearchSource OR false OR coalesce(written.current, true)) AND (NOT written:PDSummary OR coalesce(written.current, false)) AND NOT coalesce(written.search_hidden, false) AND (NOT written:PDSummary OR written.search_hidden = false)
      AND span.key <> ''
  })
  WITH m, placed, span,
       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL
            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_source_starts[i] < span.end AND member.view_source_ends[i] > span.start] | CASE WHEN member.view_library[at] THEN member.view_starts[at] ELSE member.view_starts[at] + CASE WHEN span.start > member.view_source_starts[at] THEN span.start - member.view_source_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_source_starts[i] < span.end AND member.view_source_ends[i] > span.start] | CASE WHEN member.view_library[at] THEN member.view_ends[at] ELSE member.view_starts[at] + CASE WHEN span.end < member.view_source_ends[at] THEN span.end ELSE member.view_source_ends[at] END - member.view_source_starts[at] END])} END AS seen
  RETURN collect({kind: m.kind, property: m.property, start: span.start, end: span.end,
                  view_start: CASE WHEN seen IS NULL THEN null
                                   WHEN span.start = placed.start THEN m.start
                                   ELSE last(apoc.coll.sort([m.start, seen.start])) END,
                  view_end: CASE WHEN seen IS NULL THEN null
                                 WHEN span.end = placed.end THEN m.end
                                 ELSE head(apoc.coll.sort([m.end, seen.end])) END,
                  score: m.score, term: m.term, role: span.role}) AS visible_matches
}

  WITH set, member, member_score, visible_matches AS member_visible
  ORDER BY member_score DESC, member.id
  WITH set, max(member_score) AS best,
       collect({document_id: member.document_id, source_id: member.id,
                score: member_score,
                passages: [m IN coalesce(member_visible, []) |
      {kind: m.kind,
       property: CASE WHEN m.property = 'view_text' THEN 'text' ELSE m.property END,
       start: m.start, end: m.end, view_start: m.view_start, view_end: m.view_end,
       score: m.score,
       role: CASE WHEN member:PDSummary THEN 'generated' ELSE m.role END,
       term: m.term,
       quote: substring(member[m.property], coalesce(m.view_start, m.start),
                        coalesce(m.view_end, m.end) - coalesce(m.view_start, m.start)),
       pages: [i IN range(0, size(coalesce(member.page_starts, [])) - 1) WHERE member.page_starts[i] < m.end AND m.start < member.page_ends[i] | member.page_numbers[i]], sections: [title IN [i IN range(0, size(coalesce(member.section_starts, [])) - 1) WHERE member.section_starts[i] < m.end AND m.start < member.section_ends[i] | member.section_titles[i]] WHERE title <> ''], cells: [i IN range(0, size(coalesce(member.cell_starts, [])) - 1) WHERE member.cell_starts[i] < m.end AND m.start < member.cell_ends[i] | member.cell_refs[i]]}][0..5]})
         AS members
  ORDER BY best DESC, set.id
  RETURN collect({set_id: set.id, document_id: set.document_id, title: set.title,
                  records: set.records, fields: set.fields, members_total: size(members),
                  members: members[0..25], score: best}) AS groups
}
WITH ranking, families, groups,
     [f IN families | [v IN f.variants | v.document_id]] AS ranked_documents
WITH ranking, groups,
     [f IN families
      | {family: f.family, anchored: f.anchored, score: f.score,
         variants: [v IN f.variants
                    | apoc.map.merge(v, {group: head([g IN groups
                                                     WHERE g.document_id = v.document_id])})]}]
     + [g IN groups WHERE NOT g.document_id IN apoc.coll.flatten(ranked_documents)
        | {family: coalesce(g.document_id, g.set_id), anchored: false, score: null,
           variants: [apoc.map.merge(g, {type: 'group', id: g.set_id})]}] AS families
UNWIND CASE WHEN size(families) = 0 THEN [null] ELSE families END AS f
RETURN f.family AS family, f.anchored AS anchored, f.score AS score,
       ranking.search_method AS search_method, ranking.fallback_triggered AS fallback_triggered,
       ranking.fallback_reason AS fallback_reason,
       ranking.query_chunks_searched AS query_chunks_searched,
       ranking.query_chunks_total AS query_chunks_total,
       ranking.qid AS qid, ranking.has_more AS has_more, ranking.total AS total,
       false AS exhaustive,
       f.variants AS variants
