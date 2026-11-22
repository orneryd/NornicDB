UNWIND $ids AS hd_id
WITH DISTINCT hd_id
MATCH (document:PDDocument {id: hd_id})
  CALL (document) {
  MATCH (document)-[l:PD_RELATION]-(shared:PDGraphNode)
  WHERE NOT shared:PDDefinition AND coalesce(shared.document_id, '') <> document.id
    AND NOT shared:PDSummary AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false)
  WITH collect({l: l, shared: shared}) AS found
  WITH found, [each IN found | each.l.source_id] AS source_ids
  CALL (source_ids) {
    UNWIND apoc.coll.toSet([source_id IN source_ids WHERE source_id IS NOT NULL]) AS source_id
    MATCH (cited_source:PDSearchSource {id: source_id})
    WHERE any(role IN coalesce(cited_source.section_roles, []) WHERE role <> 'own')
    RETURN apoc.map.fromPairs(collect([source_id,
           [i IN range(0, size(cited_source.section_roles) - 1)
            WHERE cited_source.section_roles[i] <> 'own'
            | [cited_source.section_starts[i], cited_source.section_ends[i]]]])) AS quoted
  }
  RETURN [each IN found WHERE NOT any(span IN coalesce(quoted[coalesce(each.l.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.l.cite_start, [])) - 1)
        WHERE span[0] < each.l.cite_end[c] AND each.l.cite_start[c] < span[1]))
          | {shared: each.shared, via: {link: each.l.id, item: null, kind: each.l.kind, source_id: each.l.source_id, cite_start: each.l.cite_start, cite_end: each.l.cite_end, citation_unsupported: each.l.citation_unsupported, citation_reason: each.l.citation_reason}}] AS held, source_ids,
         [each IN found | ['link', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]] AS read
  UNION ALL
  MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode)
  WHERE own.document_id = document.id AND NOT shared:PDDefinition
    AND NOT own:PDSummary AND NOT shared:PDSummary
    AND coalesce(shared.document_id, '') <> document.id
    AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false) AND NOT coalesce(l2.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l2.eff_usable, false)
  WITH collect({l: l, own: own, l2: l2, shared: shared}) AS found
  WITH found, [each IN found | each.l.source_id] + [each IN found | each.own.source_id]
              + [each IN found | each.l2.source_id] AS source_ids
  CALL (source_ids) {
    UNWIND apoc.coll.toSet([source_id IN source_ids WHERE source_id IS NOT NULL]) AS source_id
    MATCH (cited_source:PDSearchSource {id: source_id})
    WHERE any(role IN coalesce(cited_source.section_roles, []) WHERE role <> 'own')
    RETURN apoc.map.fromPairs(collect([source_id,
           [i IN range(0, size(cited_source.section_roles) - 1)
            WHERE cited_source.section_roles[i] <> 'own'
            | [cited_source.section_starts[i], cited_source.section_ends[i]]]])) AS quoted
  }
  RETURN [each IN found WHERE NOT any(span IN coalesce(quoted[coalesce(each.l.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.l.cite_start, [])) - 1)
        WHERE span[0] < each.l.cite_end[c] AND each.l.cite_start[c] < span[1])) AND NOT any(span IN coalesce(quoted[coalesce(each.own.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.own.cite_start, [])) - 1)
        WHERE span[0] < each.own.cite_end[c] AND each.own.cite_start[c] < span[1]))
                              AND NOT any(span IN coalesce(quoted[coalesce(each.l2.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.l2.cite_start, [])) - 1)
        WHERE span[0] < each.l2.cite_end[c] AND each.l2.cite_start[c] < span[1]))
          | {shared: each.shared, via: {link: each.l2.id, item: each.own.id, kind: each.l2.kind, source_id: each.own.source_id, cite_start: each.own.cite_start, cite_end: each.own.cite_end, citation_unsupported: CASE WHEN each.own.citation_unsupported = true THEN each.own.citation_unsupported ELSE each.l2.citation_unsupported END, citation_reason: CASE WHEN each.own.citation_unsupported = true THEN each.own.citation_reason ELSE each.l2.citation_reason END}}] AS held, source_ids,
         [each IN found | ['item', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.own.id, each.own.source_id, each.own.cite_start, each.own.cite_end, each.own.citation_unsupported, each.own.citation_reason, each.l2.id, each.l2.kind, each.l2.eff_usable, each.l2.source_id, each.l2.cite_start, each.l2.cite_end, each.l2.citation_unsupported, each.l2.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]]
           AS read
  UNION ALL
  MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[b:PD_BINDS]->(shared:PDGraphNode:PDIdentity)
  WHERE own.document_id = document.id AND b.status = 'confirmed'
    AND NOT own:PDSummary AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false)
  WITH collect({l: l, own: own, b: b, shared: shared}) AS found
  WITH found, [each IN found | each.l.source_id] + [each IN found | each.own.source_id]
    AS source_ids
  CALL (source_ids) {
    UNWIND apoc.coll.toSet([source_id IN source_ids WHERE source_id IS NOT NULL]) AS source_id
    MATCH (cited_source:PDSearchSource {id: source_id})
    WHERE any(role IN coalesce(cited_source.section_roles, []) WHERE role <> 'own')
    RETURN apoc.map.fromPairs(collect([source_id,
           [i IN range(0, size(cited_source.section_roles) - 1)
            WHERE cited_source.section_roles[i] <> 'own'
            | [cited_source.section_starts[i], cited_source.section_ends[i]]]])) AS quoted
  }
  RETURN [each IN found WHERE NOT any(span IN coalesce(quoted[coalesce(each.l.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.l.cite_start, [])) - 1)
        WHERE span[0] < each.l.cite_end[c] AND each.l.cite_start[c] < span[1])) AND NOT any(span IN coalesce(quoted[coalesce(each.own.source_id, '')], [])
      WHERE any(c IN range(0, size(coalesce(each.own.cite_start, [])) - 1)
        WHERE span[0] < each.own.cite_end[c] AND each.own.cite_start[c] < span[1]))
          | {shared: each.shared, via: {link: null, item: each.own.id, kind: 'binds', source_id: each.own.source_id, cite_start: each.own.cite_start, cite_end: each.own.cite_end, citation_unsupported: CASE WHEN each.own.citation_unsupported = true THEN each.own.citation_unsupported ELSE each.b.citation_unsupported END, citation_reason: CASE WHEN each.own.citation_unsupported = true THEN each.own.citation_reason ELSE each.b.citation_reason END}}] AS held, source_ids,
         [each IN found | ['bound', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.own.id, each.own.source_id, each.own.cite_start, each.own.cite_end, each.own.citation_unsupported, each.own.citation_reason, each.b.status,
                          each.b.citation_unsupported, each.b.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]]
           AS read
}
  WITH document, collect(held) AS hd_parts, collect(source_ids) AS hd_cited,
       collect(read) AS hd_reads
  CALL (hd_cited) {
    UNWIND hd_cited AS hd_cited_part
    UNWIND hd_cited_part AS hd_source_id
    WITH DISTINCT hd_source_id WHERE hd_source_id IS NOT NULL
    MATCH (hd_source:PDSearchSource {id: hd_source_id})
    RETURN apoc.coll.sort(collect(hd_source.id)) AS hd_cites,
           collect([hd_source.id, hd_source.section_roles, hd_source.section_starts,
                    hd_source.section_ends]) AS hd_sections
  }
RETURN document.id AS id, hd_reads AS read, hd_sections AS sections
