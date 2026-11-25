WITH $links AS effl_link_ids, $node_ids AS effl_node_ids, $documents AS effl_documents,
     $correction_ids AS effl_correction_ids, $selected AS effl_selected,
     $gone_ids AS effl_gone_ids
CALL (effl_link_ids, effl_node_ids, effl_documents, effl_correction_ids, effl_selected,
      effl_gone_ids) {
  WITH *, datetime.realtime().epochMillis AS diag_start
  CALL (effl_correction_ids, effl_selected) {
    CALL (effl_selected) {
      UNWIND effl_selected AS effl_document_id
      MATCH (:PDVersion {document_id: effl_document_id})<-[:SCOPED_TO]-(effl_scoped:PDCorrection)
      RETURN collect(DISTINCT effl_scoped.id) AS effl_scoped_ids
    }
    UNWIND effl_correction_ids + effl_scoped_ids AS effl_correction_id
    MATCH (effl_named:PDCorrection {id: effl_correction_id})
    OPTIONAL MATCH (effl_named)-[:CORRECTS]->()<-[:CORRECTS]-(effl_sharer:PDCorrection)
    WITH collect(effl_named.id) + collect(effl_sharer.id) AS effl_found
    RETURN apoc.coll.toSet(effl_found) AS effl_corrections
  }
  WITH *, datetime.realtime().epochMillis AS diag_affected
  CALL (effl_link_ids, effl_node_ids, effl_documents, effl_corrections) {
    CALL (effl_link_ids) {
      CALL (effl_link_ids) {
UNWIND effl_link_ids AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDGraphNode {id: rr_node})
MATCH (rr_start)-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]->(rr_end)
WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed
WHERE effl_seed IS NOT NULL
RETURN effl_l, effl_seed
UNION
UNWIND effl_link_ids AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDOcrReport {id: rr_node})
MATCH (rr_start)-[effl_l:ABOUT]->(rr_end)
WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed
WHERE effl_seed IS NOT NULL
RETURN effl_l, effl_seed
UNION
UNWIND effl_link_ids AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDDefinition {id: rr_node})
MATCH (rr_start)-[effl_l:PD_RELATION]->(rr_end)
WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed
WHERE effl_seed IS NOT NULL
RETURN effl_l, effl_seed
      }
      RETURN collect(DISTINCT effl_l) AS effl_named_links
    }
    CALL (effl_node_ids, effl_documents) {
      CALL (effl_documents) {
        UNWIND effl_documents AS effl_document_id
        MATCH (effl_owned:PDGraphNode {document_id: effl_document_id})
        RETURN collect(effl_owned.id) AS effl_owned_ids
      }
      UNWIND apoc.coll.toSet(effl_node_ids + effl_documents + effl_owned_ids) AS effl_node_id
      MATCH (:PDGraphNode {id: effl_node_id})-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]-()
      RETURN collect(DISTINCT effl_l) AS effl_incident_links
    }
    CALL (effl_corrections) {
      UNWIND effl_corrections AS effl_correction_id
      MATCH (effl_k:PDCorrection {id: effl_correction_id})-[:CORRECTS]->(effl_t)
      OPTIONAL MATCH (effl_t)-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]-(effl_other)
      WHERE effl_other.id IN effl_k.targets
         OR (effl_k.operation = 'replace_interpretation' AND effl_l.document_id = effl_t.id)
      WITH effl_k, effl_t, collect(DISTINCT effl_l) AS effl_between
      OPTIONAL MATCH (:PDGraphNode {document_id: CASE WHEN effl_k.operation = 'replace_interpretation' THEN effl_t.id END})
                     -[effl_d:PD_RELATION]-()
      WHERE effl_d.document_id = effl_t.id
      WITH effl_between, collect(DISTINCT effl_d) AS effl_direct
      UNWIND effl_between + effl_direct AS effl_l
      RETURN collect(DISTINCT effl_l) AS effl_concerned_links
    }
    RETURN effl_named_links + effl_incident_links + effl_concerned_links AS effl_found_links
  }
  WITH *, datetime.realtime().epochMillis AS diag_candidates
  CALL (effl_found_links) {
    UNWIND effl_found_links AS effl_found
    WITH DISTINCT effl_found AS effl_r
    CALL (effl_r) {
  WITH effl_r, [startNode(effl_r).id + '|' + endNode(effl_r).id + '|' + coalesce(effl_r.kind, ''), startNode(effl_r).id + '|' + endNode(effl_r).id + '|*', CASE WHEN coalesce(effl_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(effl_r.document_id, '') ELSE '' END] AS eff_keys
  CALL (effl_r) {
    WITH effl_r, startNode(effl_r) AS eff_s, endNode(effl_r) AS eff_e
    OPTIONAL MATCH (eff_owner:PDGraphNode {id: coalesce(effl_r.document_id, '')})
    UNWIND [eff_s, eff_e] + CASE WHEN eff_owner IS NULL THEN [] ELSE [eff_owner] END AS eff_t
    OPTIONAL MATCH (eff_t)<-[:CORRECTS]-(eff_c:PDCorrection)
    WHERE eff_c.status = 'active'
    WITH DISTINCT eff_c
    ORDER BY eff_c.id
    CALL (eff_c) {
      OPTIONAL MATCH (eff_c)-[:CORRECTS]->(eff_named)
      RETURN apoc.coll.sort(collect(DISTINCT eff_named.id)) AS eff_named
    }
    CALL (eff_c) {
      UNWIND CASE WHEN eff_c IS NULL THEN [] ELSE apoc.coll.toSet(coalesce(eff_c.scope, [])) END
        AS eff_version
      WITH eff_version WHERE eff_version IS NOT NULL
      MATCH (eff_d:PDDocument {current_version_id: eff_version})
      WITH eff_d ORDER BY eff_d.id
      RETURN collect(eff_d.id) AS eff_docs
    }
    RETURN collect(CASE WHEN eff_c IS NULL THEN null
                        ELSE eff_c {.*, named: eff_named, docs: eff_docs,
                                    all: eff_c.scope = ['all']} END) AS eff_gathered
  }
  WITH eff_keys, effl_r.correction_id AS eff_own_id, eff_gathered
  WITH eff_own_id,
       [eff_one IN eff_gathered WHERE
          CASE WHEN eff_own_id IS NULL
               THEN any(eff_h IN (CASE
    WHEN eff_one.operation = 'exclude_link'
      THEN [eff_m IN eff_one.targets[1..] WHERE eff_m IN eff_one.named | {key: eff_m + '|' + eff_one.targets[0] + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]
         + [eff_m IN eff_one.targets[1..] WHERE eff_one.targets[0] IN eff_one.named | {key: eff_one.targets[0] + '|' + eff_m + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation = 'replace_interpretation'
      THEN [eff_m IN eff_one.targets[0..1] WHERE eff_m IN eff_one.named | {key: 'doc|' + eff_m, all: eff_one.all, docs: eff_one.docs}]
    ELSE [] END) WHERE eff_h.key IN eff_keys)
               ELSE eff_one.id = eff_own_id
                 OR any(eff_own IN eff_gathered WHERE eff_own.id = eff_own_id
                        AND eff_one.id <> eff_own.id
                        AND any(eff_shared IN eff_one.named WHERE eff_shared IN eff_own.targets)
                        AND ((eff_own.operation = 'include_link' AND (eff_one.operation = 'exclude_link' AND (eff_one.link_kind IS NULL OR eff_one.link_kind = eff_own.link_kind)) AND (eff_one.targets = eff_own.targets OR eff_one.targets = reverse(eff_own.targets))) OR (eff_one.operation = 'include_link' AND (eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = eff_one.link_kind)) AND (eff_own.targets = eff_one.targets OR eff_own.targets = reverse(eff_one.targets))))) END] AS eff_corrections
  WITH eff_corrections,
       reduce(eff_all = [], eff_one IN eff_corrections | eff_all + (CASE
    WHEN eff_one.operation = 'exclude_link'
      THEN [eff_m IN eff_one.targets[1..] WHERE eff_m IN eff_one.named | {key: eff_m + '|' + eff_one.targets[0] + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]
         + [eff_m IN eff_one.targets[1..] WHERE eff_one.targets[0] IN eff_one.named | {key: eff_one.targets[0] + '|' + eff_m + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation = 'replace_interpretation'
      THEN [eff_m IN eff_one.targets[0..1] WHERE eff_m IN eff_one.named | {key: 'doc|' + eff_m, all: eff_one.all, docs: eff_one.docs}]
    ELSE [] END)) AS eff_hides,
       [eff_rival IN eff_corrections WHERE eff_own_id IS NOT NULL AND eff_rival.id <> eff_own_id
        | {key: eff_own_id, id: eff_rival.id, all: eff_rival.all, docs: eff_rival.docs}]
         AS eff_rivals
  CALL (eff_hides) {
    UNWIND eff_hides AS eff_h
    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs, id: eff_h.id}) AS eff_s
    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_hidden
  }
  CALL (eff_rivals) {
    UNWIND eff_rivals AS eff_h
    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs, id: eff_h.id}) AS eff_s
    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_contested
  }
  RETURN {hidden: eff_hidden, contested: eff_contested,
          hiding: apoc.map.fromPairs([eff_h IN eff_hides | [split(eff_h.key, '|')[0], true]]),
          scopes: apoc.map.fromPairs([eff_s IN eff_corrections
                                      | [eff_s.id, [{all: eff_s.all, docs: eff_s.docs}]]])}
           AS eff_view,
         [eff_one IN eff_corrections
          | [eff_one.id, eff_one.revision, eff_one.operation, eff_one.link_kind,
             eff_one.targets, eff_one.named, eff_one.all, eff_one.docs]] AS eff_incident
}
WITH effl_r, eff_view,
     apoc.convert.toJson([type(effl_r), effl_r.id, startNode(effl_r).id, endNode(effl_r).id, effl_r.kind,
                          effl_r.origin, effl_r.stage, effl_r.current, effl_r.correction_id,
                          effl_r.document_id, startNode(effl_r).current,
                          startNode(effl_r).document_id, endNode(effl_r).current,
                          endNode(effl_r).document_id, eff_incident]) AS eff_inputs
    WITH effl_r, eff_view, eff_inputs
    WHERE coalesce(effl_r.eff_applied, '') <> eff_inputs
    WITH effl_r, eff_view, eff_inputs, (((coalesce(effl_r.origin, "") <> "automatic" OR effl_r.stage = "interpretation")
  AND coalesce(effl_r.current, true)) AND coalesce(startNode(effl_r).current, true) AND coalesce(endNode(effl_r).current, true)
  AND (effl_r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(effl_r).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(effl_r.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(effl_r).id + '|' + endNode(effl_r).id + '|' + coalesce(effl_r.kind, ''), startNode(effl_r).id + '|' + endNode(effl_r).id + '|*', CASE WHEN coalesce(effl_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(effl_r.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (effl_r.correction_id IS NULL OR (effl_r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(effl_r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])))))) AS effl_base, [eff_rival IN coalesce(eff_view.contested[coalesce(effl_r.correction_id, '')], [])
         WHERE (effl_r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(effl_r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])))) AND (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])) | eff_rival.id] AS effl_rivals
    WITH effl_r, eff_view, eff_inputs, effl_base, effl_rivals, size(effl_rivals) > 0 AS effl_disputed
    WITH effl_r, eff_view, eff_inputs, effl_base, effl_rivals, effl_disputed, CASE WHEN NOT coalesce(effl_r.current, true) THEN 'retired'
            WHEN NOT ((coalesce(effl_r.origin, "") <> "automatic" OR effl_r.stage = "interpretation")
  AND coalesce(effl_r.current, true)) THEN 'earlier'
            WHEN NOT (coalesce(startNode(effl_r).current, true) AND coalesce(endNode(effl_r).current, true)) THEN 'retired_end'
            WHEN NOT effl_base AND effl_r.correction_id IS NOT NULL
              THEN 'unapplied:' + effl_r.correction_id
            WHEN NOT effl_base THEN 'hidden'
            WHEN effl_disputed THEN 'contested:' + effl_r.correction_id END AS effl_reason,
         startNode(effl_r) AS effl_s, endNode(effl_r) AS effl_e
    RETURN collect({type: type(effl_r), from: effl_s.id, start: CASE WHEN effl_s:PDGraphNode THEN 'PDGraphNode' WHEN effl_s:PDOcrReport THEN 'PDOcrReport' WHEN effl_s:PDDefinition THEN 'PDDefinition' END,
                     link: coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id), key: apoc.convert.toJson([startNode(effl_r).id, coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id)]),
                     base: effl_base, disputed: effl_disputed, reason: effl_reason,
                     inputs: eff_inputs, was: effl_r.eff_applied, correction: effl_r.correction_id,
                     rivals: effl_rivals,
                     family: type(effl_r) = 'PD_RELATION'
                             AND coalesce(effl_r.kind, 'same_document') = 'same_document',
                     ends: [effl_s.id, effl_e.id], to: effl_e.id,
                     end: CASE WHEN effl_e:PDDefinition THEN 'definition' ELSE 'node' END,
                     origin: coalesce(effl_r.origin, ''),
                     documents: [effl_x IN [effl_r.document_id, effl_s.document_id,
                                            effl_e.document_id,
                                            CASE WHEN effl_s:PDDocument THEN effl_s.id END,
                                            CASE WHEN effl_e:PDDocument THEN effl_e.id END]
                                 WHERE effl_x IS NOT NULL]}) AS effl_rows
  }
  WITH *, datetime.realtime().epochMillis AS diag_verdicts
  CALL (effl_corrections) {
    UNWIND effl_corrections AS effl_correction_id
    MATCH (effl_w:PDCorrection {id: effl_correction_id})
    WHERE effl_w.status <> 'active'
    RETURN collect(effl_w.id) AS effl_withdrawn
  }
  CALL (effl_gone_ids) {
    UNWIND effl_gone_ids AS effl_gone_id
    MATCH (effl_g:PDGraphNode {id: effl_gone_id})
    RETURN collect(coalesce(effl_g.document_id, CASE WHEN effl_g:PDDocument THEN effl_g.id END))
      AS effl_gone_documents
  }
  RETURN {rows: effl_rows, withdrawn: effl_withdrawn, gone_documents: effl_gone_documents,
          gone: effl_gone_ids,
          documents: apoc.coll.toSet(reduce(effl_all = effl_gone_documents, effl_x IN effl_rows
                                            | effl_all + effl_x.documents)),
          family: apoc.coll.toSet(reduce(effl_all = effl_gone_ids,
                                         effl_x IN [effl_y IN effl_rows WHERE effl_y.family]
                                         | effl_all + effl_x.ends)) , diagnostic: {start: diag_start, affected: diag_affected, candidates: diag_candidates, verdicts: diag_verdicts, found: size(effl_found_links), correction_count: size(effl_corrections), seeds: [size(effl_link_ids), size(effl_node_ids), size(effl_documents), size(effl_correction_ids), size(effl_selected), size(effl_gone_ids)]}} AS effl_out
}
RETURN effl_out.rows AS rows, effl_out.withdrawn AS withdrawn,
       effl_out.documents AS documents, effl_out.family AS family,
       effl_out.gone_documents AS gone_documents, effl_out.gone AS gone, apoc.map.merge(effl_out.diagnostic, {finished: datetime.realtime().epochMillis}) AS evaluate_diag
