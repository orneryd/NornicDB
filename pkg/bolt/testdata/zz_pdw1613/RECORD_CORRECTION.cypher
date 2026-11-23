
OPTIONAL MATCH (existing:PDCorrection {id: $correction.id})
WITH existing
WHERE existing IS NULL AND $correction.id IS NOT NULL
  AND all(key IN keys($correction) WHERE key IN ['id', 'operation', 'targets', 'link_kind', 'scope', 'author', 'basis', 'reason'])
  AND $correction.operation IN ['exclude_link', 'include_link', 'replace_interpretation']
  AND $correction.author IS NOT NULL AND $correction.basis IS NOT NULL
  AND ($correction.scope = ['all'] OR (size($correction.scope) > 0 AND all(v IN $correction.scope WHERE EXISTS { MATCH (:PDVersion {id: v}) })))
  AND (($correction.operation IN ['exclude_link', 'include_link'] AND size($correction.targets) = 2
      AND ($correction.operation <> 'include_link' OR $correction.link_kind IS NOT NULL))
    OR ($correction.operation IN ['replace_interpretation'] AND size($correction.targets) = 1))
  AND size([i IN range(0, size($correction.targets) - 1) WHERE $correction.targets[i] IN $correction.targets[0..i]]) = 0
  AND NOT coalesce($correction.link_kind, '') IN ['resolves_to', 'merged_into', 'same_as']
  AND all(t IN $correction.targets WHERE (EXISTS { MATCH (:PDGraphNode {id: t}) } OR EXISTS { MATCH (:PDDefinition {id: t}) }))
CREATE (c:PDCorrection)
SET c = $correction
SET c.status = 'active', c.revision = 1, c.created_at = toString(datetime())
SET c.updated_at = c.created_at
WITH c
CALL (c) {
  UNWIND c.targets AS target_id
  OPTIONAL MATCH (target_node:PDGraphNode {id: target_id})
OPTIONAL MATCH (target_definition:PDDefinition {id: target_id})
WITH *, coalesce(target_node, target_definition) AS target
WHERE target IS NOT NULL
  CREATE (c)-[:CORRECTS]->(target)
  RETURN count(target) AS targeted
}
CALL (c) {
  OPTIONAL MATCH (c)-[scoped_was:SCOPED_TO]->(:PDVersion)
  DELETE scoped_was
  RETURN count(*) AS unscoped
}
CALL (c) {
  UNWIND CASE WHEN c.scope = ['all'] THEN [] ELSE coalesce(c.scope, []) END AS scoped_id
  MATCH (scoped_version:PDVersion {id: scoped_id})
  MERGE (c)-[:SCOPED_TO]->(scoped_version)
  RETURN count(*) AS scoped
}

CALL (c) {
  WITH c, CASE WHEN c.operation = 'include_link' THEN [[c.targets[0], c.targets[1], c.link_kind, '0']]
                 ELSE [] END AS edges
  UNWIND edges AS edge
  OPTIONAL MATCH (start_node:PDGraphNode {id: edge[0]})
OPTIONAL MATCH (start_definition:PDDefinition {id: edge[0]})
WITH *, coalesce(start_node, start_definition) AS start
WHERE start IS NOT NULL
  OPTIONAL MATCH (end_node:PDGraphNode {id: edge[1]})
OPTIONAL MATCH (end_definition:PDDefinition {id: edge[1]})
WITH *, coalesce(end_node, end_definition) AS end
WHERE end IS NOT NULL
  CREATE (start)-[link:PD_RELATION]->(end)
  SET link.id = c.id + '#' + edge[3], link.kind = edge[2], link.statement = c.reason,
      link.origin = 'user', link.correction_id = c.id, link.revision = c.revision,
      link.updated_at = c.updated_at
  RETURN count(link) AS projected
}
WITH *, c.targets AS derived_locks
CALL (derived_locks) {
  UNWIND derived_locks AS derived_lock_id
  WITH DISTINCT derived_lock_id ORDER BY derived_lock_id
  MATCH (derived_locked:PDGraphNode {id: derived_lock_id})
  SET derived_locked += {}
  RETURN count(derived_locked) AS derived_lock_count
}
WITH c, projected, [] AS effl_link_ids, [] AS effl_node_ids, [] AS effl_documents,
     [c.id] AS effl_correction_ids, [] AS effl_selected, [] AS effl_gone_ids
CALL (effl_link_ids, effl_node_ids, effl_documents, effl_correction_ids, effl_selected,
      effl_gone_ids) {
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
                     inputs: eff_inputs, correction: effl_r.correction_id,
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
                                         | effl_all + effl_x.ends))} AS effl_out
}
CALL (effl_out) {
UNWIND [effl_x IN coalesce(effl_out.rows, []) WHERE effl_x.start = 'PDGraphNode' AND (effl_x.type <> 'PD_RELATION' OR effl_x.origin <> 'automatic')] AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDGraphNode {id: rr_node})
MATCH (rr_start)-[effl_r:PD_RELATION|ATTACHMENT_OF|ABOUT]->(rr_end)
WITH effl_r, rr_wanted[coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id)] AS effl_x
WHERE effl_x IS NOT NULL
    WITH effl_x, effl_r, true AS effl_same
  FOREACH (effl_go IN CASE WHEN effl_same THEN [1] ELSE [] END |
    SET effl_r.eff_base = effl_x.base, effl_r.eff_disputed = effl_x.disputed,
        effl_r.eff_usable = effl_x.base AND NOT effl_x.disputed,
        effl_r.eff_reason = effl_x.reason, effl_r.eff_applied = effl_x.inputs)
  RETURN collect(CASE WHEN effl_same THEN effl_x.key END) AS effl_PDGraphNode_kept,
         collect(CASE WHEN effl_same THEN null ELSE effl_x.key END) AS effl_PDGraphNode_stale
}
CALL (effl_out) {
UNWIND [effl_x IN coalesce(effl_out.rows, []) WHERE effl_x.start = 'PDOcrReport' AND (effl_x.type <> 'PD_RELATION' OR effl_x.origin <> 'automatic')] AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDOcrReport {id: rr_node})
MATCH (rr_start)-[effl_r:ABOUT]->(rr_end)
WITH effl_r, rr_wanted[coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id)] AS effl_x
WHERE effl_x IS NOT NULL
    WITH effl_x, effl_r, true AS effl_same
  FOREACH (effl_go IN CASE WHEN effl_same THEN [1] ELSE [] END |
    SET effl_r.eff_base = effl_x.base, effl_r.eff_disputed = effl_x.disputed,
        effl_r.eff_usable = effl_x.base AND NOT effl_x.disputed,
        effl_r.eff_reason = effl_x.reason, effl_r.eff_applied = effl_x.inputs)
  RETURN collect(CASE WHEN effl_same THEN effl_x.key END) AS effl_PDOcrReport_kept,
         collect(CASE WHEN effl_same THEN null ELSE effl_x.key END) AS effl_PDOcrReport_stale
}
CALL (effl_out) {
UNWIND [effl_x IN coalesce(effl_out.rows, []) WHERE effl_x.start = 'PDDefinition' AND (effl_x.type <> 'PD_RELATION' OR effl_x.origin <> 'automatic')] AS rr_item
WITH rr_item.from AS rr_node,
     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted
MATCH (rr_start:PDDefinition {id: rr_node})
MATCH (rr_start)-[effl_r:PD_RELATION]->(rr_end)
WITH effl_r, rr_wanted[coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id)] AS effl_x
WHERE effl_x IS NOT NULL
    WITH effl_x, effl_r, true AS effl_same
  FOREACH (effl_go IN CASE WHEN effl_same THEN [1] ELSE [] END |
    SET effl_r.eff_base = effl_x.base, effl_r.eff_disputed = effl_x.disputed,
        effl_r.eff_usable = effl_x.base AND NOT effl_x.disputed,
        effl_r.eff_reason = effl_x.reason, effl_r.eff_applied = effl_x.inputs)
  RETURN collect(CASE WHEN effl_same THEN effl_x.key END) AS effl_PDDefinition_kept,
         collect(CASE WHEN effl_same THEN null ELSE effl_x.key END) AS effl_PDDefinition_stale
}
CALL (effl_out) {
UNWIND [effl_x IN coalesce(effl_out.rows, []) WHERE effl_x.type = 'PD_RELATION'          AND effl_x.origin = 'automatic' AND effl_x.end = 'node'] AS rr_item
MATCH (rr_start:PDGraphNode {id: rr_item.from})
MATCH (rr_end:PDGraphNode {id: rr_item.to})
MERGE (rr_start)-[effl_r:PD_RELATION {id: rr_item.link}]->(rr_end)
WITH effl_r, rr_item AS effl_x
    WITH effl_x, effl_r, true AS effl_same
  FOREACH (effl_go IN CASE WHEN effl_same THEN [1] ELSE [] END |
    SET effl_r.eff_base = effl_x.base, effl_r.eff_disputed = effl_x.disputed,
        effl_r.eff_usable = effl_x.base AND NOT effl_x.disputed,
        effl_r.eff_reason = effl_x.reason, effl_r.eff_applied = effl_x.inputs)
  RETURN collect(CASE WHEN effl_same THEN effl_x.key END) AS effl_automatic_node_kept,
         collect(CASE WHEN effl_same THEN null ELSE effl_x.key END) AS effl_automatic_node_stale
}
CALL (effl_out) {
UNWIND [effl_x IN coalesce(effl_out.rows, []) WHERE effl_x.type = 'PD_RELATION'          AND effl_x.origin = 'automatic' AND effl_x.end = 'definition'] AS rr_item
MATCH (rr_start:PDGraphNode {id: rr_item.from})
MATCH (rr_end:PDDefinition {id: rr_item.to})
MERGE (rr_start)-[effl_r:PD_RELATION {id: rr_item.link}]->(rr_end)
WITH effl_r, rr_item AS effl_x
    WITH effl_x, effl_r, true AS effl_same
  FOREACH (effl_go IN CASE WHEN effl_same THEN [1] ELSE [] END |
    SET effl_r.eff_base = effl_x.base, effl_r.eff_disputed = effl_x.disputed,
        effl_r.eff_usable = effl_x.base AND NOT effl_x.disputed,
        effl_r.eff_reason = effl_x.reason, effl_r.eff_applied = effl_x.inputs)
  RETURN collect(CASE WHEN effl_same THEN effl_x.key END) AS effl_automatic_definition_kept,
         collect(CASE WHEN effl_same THEN null ELSE effl_x.key END) AS effl_automatic_definition_stale
}
WITH *, effl_PDGraphNode_kept + effl_PDOcrReport_kept + effl_PDDefinition_kept + effl_automatic_node_kept + effl_automatic_definition_kept AS effl_kept, effl_PDGraphNode_stale + effl_PDOcrReport_stale + effl_PDDefinition_stale + effl_automatic_node_stale + effl_automatic_definition_stale AS effl_stale
CALL (effl_out, effl_kept) {
  UNWIND [effl_x IN coalesce(effl_out.rows, [])
          WHERE effl_x.correction IS NOT NULL AND effl_x.key IN effl_kept] AS effl_x
  UNWIND effl_x.rivals AS effl_rival
  WITH effl_x, CASE WHEN effl_x.correction < effl_rival THEN [effl_x.correction, effl_rival]
                    ELSE [effl_rival, effl_x.correction] END AS effl_pair
  MERGE (effl_k:PDEffectiveConflict {id: 'rival|' + effl_pair[0] + '|' + effl_pair[1]})
  SET effl_k.kind = 'rival', effl_k.correction_id = effl_pair[0],
      effl_k.rival_id = effl_pair[1], effl_k.link_ids = [effl_x.link]
  RETURN count(*) AS effl_conflicted
}
CALL (effl_out, effl_kept) {
  UNWIND [effl_x IN coalesce(effl_out.rows, [])
          WHERE effl_x.correction IS NOT NULL AND effl_x.key IN effl_kept] AS effl_x
  CALL (effl_x) {
    MATCH (effl_k:PDEffectiveConflict {correction_id: effl_x.correction})
    RETURN effl_k
    UNION
    MATCH (effl_k:PDEffectiveConflict {rival_id: effl_x.correction})
    RETURN effl_k
  }
  WITH effl_x, effl_k
  WHERE effl_k.kind = 'rival'
    AND NOT (CASE WHEN effl_k.correction_id = effl_x.correction THEN effl_k.rival_id
                  ELSE effl_k.correction_id END) IN effl_x.rivals
  RETURN collect(DISTINCT effl_k.id) AS effl_unrivalled
}
CALL (effl_out) {
  UNWIND coalesce(effl_out.withdrawn, []) AS effl_w
  CALL (effl_w) {
    MATCH (effl_k:PDEffectiveConflict {correction_id: effl_w})
    RETURN effl_k
    UNION
    MATCH (effl_k:PDEffectiveConflict {rival_id: effl_w})
    RETURN effl_k
  }
  RETURN collect(DISTINCT effl_k.id) AS effl_unwithdrawn
}
CALL (effl_unrivalled, effl_unwithdrawn) {
  UNWIND apoc.coll.toSet(effl_unrivalled + effl_unwithdrawn) AS effl_id
  MATCH (effl_k:PDEffectiveConflict {id: effl_id})
  DETACH DELETE effl_k
  RETURN count(*) AS effl_unconflicted
}
WITH c, projected, effl_out.documents AS effl_documents, effl_out.family AS effl_family
WITH c, projected, effl_documents, c.targets + effl_family AS fam_seed_ids
CALL (fam_seed_ids) {
CALL (fam_seed_ids) {
  CALL (fam_seed_ids) {
    UNWIND fam_seed_ids AS fam_seed_id
    MATCH (fam_seed:PDGraphNode {id: fam_seed_id})
    OPTIONAL MATCH (fam_seed)-[fam_near:PD_RELATION]-(fam_neighbour:PDGraphNode)
    WHERE coalesce(fam_near.kind, 'same_document') = 'same_document'
    WITH collect(fam_seed) + collect(fam_neighbour) AS fam_nodes
    UNWIND fam_nodes AS fam_node
    WITH DISTINCT fam_node
    OPTIONAL MATCH (fam_node:PDDocument)-[:HAS_VERSION]->(:PDVersion)-[:HAS_ORIGINAL]
                   ->(:PDOriginal)<-[:HAS_ORIGINAL]-(:PDVersion)<-[:HAS_VERSION]-(fam_twin:PDDocument)
    WITH collect(fam_node) + collect(fam_twin) AS fam_found
    UNWIND fam_found AS fam_d
    RETURN collect(DISTINCT fam_d.id) AS fam_near_ids,
           collect(DISTINCT fam_d.family_key) AS fam_keys
  }
  CALL (fam_keys) {
    UNWIND fam_keys AS fam_k
    MATCH (fam_m:PDGraphNode {family_key: fam_k})
    RETURN collect(DISTINCT fam_m.id) AS fam_keyed
  }
  RETURN apoc.coll.sort(apoc.coll.toSet(fam_near_ids + fam_keyed)) AS fam_members
}
CALL (fam_members) {
  UNWIND fam_members AS fam_lock_id
  MATCH (fam_lock:PDGraphNode {id: fam_lock_id})
  SET fam_lock += {}
  RETURN count(*) AS fam_locked
}
CALL (fam_members) {
  UNWIND fam_members AS fam_id
  MATCH (fam_m:PDGraphNode {id: fam_id})
  CALL (fam_m) {
    OPTIONAL MATCH (fam_m)-[fam_l:PD_RELATION]-(fam_o:PDGraphNode)
    WHERE coalesce(fam_l.kind, 'same_document') = 'same_document' AND coalesce(fam_l.eff_usable, false)
    WITH fam_l, fam_o ORDER BY fam_l.id
    RETURN collect(fam_l.id) AS fam_links, collect(fam_o.id) AS fam_linked
  }
  CALL (fam_m) {
    OPTIONAL MATCH (fam_m:PDDocument)-[:HAS_VERSION]->(:PDVersion)-[:HAS_ORIGINAL]->(fam_o:PDOriginal)
    OPTIONAL MATCH (fam_o)<-[:HAS_ORIGINAL]-(:PDVersion)<-[:HAS_VERSION]-(fam_twin:PDDocument)
    WITH fam_o, apoc.coll.sort(collect(DISTINCT fam_twin.id)) AS fam_twins
    ORDER BY fam_o.id
    RETURN collect(CASE WHEN fam_o IS NULL THEN null ELSE [fam_o.id, fam_twins] END)
      AS fam_originals
  }
  RETURN collect({id: fam_m.id, document: fam_m:PDDocument, key: fam_m.family_key,
                  applied: fam_m.family_applied,
                  near: [fam_n IN apoc.coll.toSet(fam_linked
                                                  + reduce(fam_all = [], fam_x IN fam_originals
                                                           | fam_all + fam_x[1]))
                         WHERE fam_n <> fam_m.id AND fam_n IN fam_members],
                  text: [fam_m.id, fam_links, fam_originals]}) AS fam_rows
}
WITH fam_rows,
     apoc.map.fromPairs([fam_r IN fam_rows | [fam_r.id, fam_r.near]]) AS fam_adjacent
WITH fam_rows,
     reduce(fam_l = apoc.map.fromPairs([fam_r IN fam_rows | [fam_r.id, fam_r.id]]),
            fam_round IN range(1, size(fam_rows) - 1)
            | apoc.map.fromPairs([fam_r IN fam_rows
                | [fam_r.id, reduce(fam_least = fam_l[fam_r.id],
                                    fam_n IN fam_adjacent[fam_r.id]
                                    | CASE WHEN fam_l[fam_n] < fam_least
                                           THEN fam_l[fam_n] ELSE fam_least END)]]))
       AS fam_labels
WITH fam_rows,
     [fam_r IN fam_rows
      | {id: fam_r.id, was: fam_r.key, had: fam_r.applied,
         key: head([fam_o IN fam_rows
                    WHERE fam_o.document AND fam_labels[fam_o.id] = fam_labels[fam_r.id]
                    | fam_o.id]),
         applied: apoc.convert.toJson([fam_o IN fam_rows
                                       WHERE fam_labels[fam_o.id] = fam_labels[fam_r.id]
                                       | fam_o.text])}] AS fam_derived
WITH {members: [fam_r IN fam_rows | fam_r.id],
       read: apoc.convert.toJson([fam_r IN fam_rows | fam_r.text]),
       writes: [fam_w IN fam_derived
                WHERE coalesce(fam_w.was, '') <> coalesce(fam_w.key, '')
                   OR coalesce(fam_w.had, '') <> fam_w.applied
                | {id: fam_w.id, key: fam_w.key, applied: fam_w.applied}]} AS fam_out
  WITH fam_out, true AS fam_same
CALL (fam_out, fam_same) {
  UNWIND CASE WHEN fam_same THEN fam_out.writes ELSE [] END AS fam_w
  MATCH (fam_d:PDGraphNode {id: fam_w.id})
  SET fam_d.family_key = fam_w.key, fam_d.family_applied = fam_w.applied
  RETURN count(*) AS fam_written
}
  RETURN fam_written AS fam_count
}
WITH c, projected, effl_documents, fam_count AS fam_derived
WITH c, projected, effl_documents AS hd_ids
CALL (hd_ids) {
  UNWIND hd_ids AS hd_id
  WITH DISTINCT hd_id
  MATCH (document:PDDocument {id: hd_id})
  SET document += {}
WITH document
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
  CALL (hd_parts) {
    UNWIND hd_parts AS hd_part
    UNWIND hd_part AS hd_held
    WITH hd_held.shared AS shared, hd_held.via AS hd_via
    OPTIONAL MATCH (held_survivor:PDGraphNode:PDIdentity {id: shared.survivor})
WITH hd_via, coalesce(held_survivor, shared) AS shared
WITH shared.id AS hd_thing, shared:PDIdentity AS hd_identity, hd_via
    ORDER BY hd_thing, coalesce(hd_via.link, ''), coalesce(hd_via.item, '')
    WITH hd_thing, head(collect({identity: hd_identity, via: hd_via})) AS hd_least
    RETURN collect({thing: hd_thing, identity: hd_least.identity, via: hd_least.via,
                    key: apoc.convert.toJson([hd_thing, hd_least.identity, hd_least.via.link, hd_least.via.item, hd_least.via.kind, hd_least.via.source_id, hd_least.via.cite_start, hd_least.via.cite_end, hd_least.via.citation_unsupported, hd_least.via.citation_reason])}) AS hd_next
  }
  CALL (document) {
    CALL (document) {
      OPTIONAL MATCH (document)-[has:PD_HAS]->(held:PDHeld)
      WITH held, has, {link: has.link, item: has.item, kind: has.kind, source_id: has.source_id, cite_start: has.cite_start, cite_end: has.cite_end, citation_unsupported: has.citation_unsupported, citation_reason: has.citation_reason} AS hd_has_via
      RETURN collect(CASE WHEN held IS NOT NULL
                          THEN {thing: held.thing_id, identity: coalesce(has.identity, false),
                                key: apoc.convert.toJson([held.thing_id, coalesce(has.identity, false), hd_has_via.link, hd_has_via.item, hd_has_via.kind, hd_has_via.source_id, hd_has_via.cite_start, hd_has_via.cite_end, hd_has_via.citation_unsupported, hd_has_via.citation_reason])} END) AS hd_old
    }
    CALL (document) {
      OPTIONAL MATCH (document)-[:PD_CITES]->(hd_old_source:PDSearchSource)
      RETURN apoc.coll.sort(collect(hd_old_source.id)) AS hd_old_cites
    }
    RETURN hd_old, hd_old_cites,
           apoc.convert.toJson([apoc.coll.sort([hd_x IN hd_old | hd_x.key]), hd_old_cites])
             AS hd_stored
  }
  WITH document, hd_next, hd_cites, hd_old, hd_old_cites, hd_stored, hd_reads,
       hd_sections,
       apoc.convert.toJson([apoc.coll.sort([hd_x IN hd_next | hd_x.key]), hd_cites]) AS hd_now
  RETURN collect({id: document.id, next: hd_next, cites: hd_cites, old: hd_old,
                  old_cites: hd_old_cites, stored: hd_stored, changed: hd_now <> hd_stored,
                  read: hd_reads, sections: hd_sections})
         AS hd_rows
}
CALL (hd_rows) {
  UNWIND hd_rows AS hd_row
  MATCH (hd_d:PDDocument {id: hd_row.id})
  WITH hd_row, true AS hd_same
  RETURN collect(CASE WHEN hd_same THEN hd_row END) AS hd_fresh,
         collect(CASE WHEN hd_same THEN null ELSE hd_row.id END) AS hd_moved
}
CALL (hd_fresh) {
  UNWIND [hd_r IN hd_fresh WHERE hd_r.changed] AS hd_r
  WITH hd_r, [hd_x IN hd_r.next | hd_x.key] AS hd_next_keys,
       [hd_x IN hd_r.old | hd_x.key] AS hd_old_keys,
       [hd_x IN hd_r.next | hd_x.thing] AS hd_next_things,
       [hd_x IN hd_r.old | hd_x.thing] AS hd_old_things
  RETURN collect({
    id: hd_r.id,
    removed: [hd_x IN hd_r.old WHERE NOT hd_x.key IN hd_next_keys | hd_x.thing],
    added: [hd_x IN hd_r.next WHERE NOT hd_x.key IN hd_old_keys],
    cites_gone: [hd_c IN hd_r.old_cites WHERE NOT hd_c IN hd_r.cites],
    cites_new: [hd_c IN hd_r.cites WHERE NOT hd_c IN hd_r.old_cites],
    counts: [hd_t IN hd_next_things WHERE NOT hd_t IN hd_old_things | [hd_t, 1]]
            + [hd_t IN hd_old_things WHERE NOT hd_t IN hd_next_things | [hd_t, -1]],
    pairs: reduce(hd_a = [], hd_i IN [hd_x IN hd_r.next WHERE hd_x.identity | hd_x.thing]
                  | hd_a + [hd_t IN hd_next_things | [hd_i, hd_t, 1]])
           + reduce(hd_a = [], hd_i IN [hd_x IN hd_r.old WHERE hd_x.identity | hd_x.thing]
                    | hd_a + [hd_t IN hd_old_things | [hd_i, hd_t, -1]])
  }) AS hd_diffs
}
CALL (hd_diffs) {
UNWIND reduce(hd_all = [], hd_x IN hd_diffs | hd_all + [hd_g IN hd_x.removed | {document: hd_x.id, gone: hd_g}]) AS rr_item
WITH rr_item.document AS rr_node,
     apoc.map.fromPairs(collect([rr_item.gone, rr_item])) AS rr_wanted
MATCH (rr_start:PDDocument {id: rr_node})
MATCH (rr_start)-[hd_has:PD_HAS]->(rr_end)
WITH hd_has, rr_wanted[endNode(hd_has).thing_id] AS hd_gone
WHERE hd_gone IS NOT NULL
  DELETE hd_has
  RETURN count(*) AS hd_has_deleted
}
CALL (hd_diffs) {
UNWIND reduce(hd_all = [], hd_x IN hd_diffs | hd_all + [hd_g IN hd_x.cites_gone | {document: hd_x.id, gone: hd_g}]) AS rr_item
WITH rr_item.document AS rr_node,
     apoc.map.fromPairs(collect([rr_item.gone, rr_item])) AS rr_wanted
MATCH (rr_start:PDDocument {id: rr_node})
MATCH (rr_start)-[hd_cites:PD_CITES]->(rr_end)
WITH hd_cites, rr_wanted[endNode(hd_cites).id] AS hd_gone
WHERE hd_gone IS NOT NULL
  DELETE hd_cites
  RETURN count(*) AS hd_cites_deleted
}
CALL (hd_diffs) {
  UNWIND hd_diffs AS hd_x
  UNWIND hd_x.cites_new AS hd_s
  MATCH (hd_cd:PDDocument {id: hd_x.id})
  MATCH (hd_cs:PDSearchSource {id: hd_s})
  CREATE (hd_cd)-[:PD_CITES]->(hd_cs)
  RETURN count(*) AS hd_cites_created
}
CALL (hd_diffs) {
  UNWIND hd_diffs AS hd_x
  UNWIND hd_x.counts AS hd_c
  WITH hd_c[0] AS hd_t, sum(hd_c[1]) AS hd_delta
  WHERE hd_delta <> 0
  WITH hd_t, hd_delta ORDER BY hd_t
  OPTIONAL MATCH (hd_held:PDHeld {thing_id: hd_t})
  SET hd_held.holders = coalesce(hd_held.holders, 0) + hd_delta
  WITH collect(hd_t) AS hd_counted,
       collect(CASE WHEN hd_held IS NULL THEN [hd_t, hd_delta] END) AS hd_unheld
  CALL (hd_unheld) {
    UNWIND hd_unheld AS hd_new
    MERGE (hd_h:PDHeld {thing_id: hd_new[0]})
    SET hd_h.holders = coalesce(hd_h.holders, 0) + hd_new[1]
    RETURN count(*) AS hd_first_held
  }
  RETURN hd_counted
}
CALL (hd_diffs) {
  UNWIND hd_diffs AS hd_x
  UNWIND hd_x.added AS hd_n
  MATCH (hd_nd:PDDocument {id: hd_x.id})
  MATCH (hd_nh:PDHeld {thing_id: hd_n.thing})
  CREATE (hd_nd)-[hd_new:PD_HAS]->(hd_nh)
  SET hd_new += hd_n.via, hd_new.identity = hd_n.identity
  RETURN count(*) AS hd_has_created
}
CALL (hd_diffs) {
  UNWIND hd_diffs AS hd_x
  UNWIND hd_x.pairs AS hd_p
  WITH hd_p[0] AS hd_i, hd_p[1] AS hd_t, sum(hd_p[2]) AS hd_delta
  WHERE hd_delta <> 0
  MATCH (hd_hi:PDHeld {thing_id: hd_i})
  MATCH (hd_ht:PDHeld {thing_id: hd_t})
  MERGE (hd_hi)-[hd_co:PD_COHELD]->(hd_ht)
  SET hd_co.n = coalesce(hd_co.n, 0) + hd_delta
  WITH collect(hd_co) AS hd_cos
  CALL (hd_cos) {
    UNWIND hd_cos AS hd_zero
    WITH hd_zero WHERE hd_zero.n = 0
    DELETE hd_zero
    RETURN count(*) AS hd_unpaired
  }
  RETURN size(hd_cos) AS hd_paired
}
CALL (hd_counted) {
  UNWIND hd_counted AS hd_t
  MATCH (hd_gone:PDHeld {thing_id: hd_t})
  WHERE hd_gone.holders = 0
  DETACH DELETE hd_gone
  RETURN count(*) AS hd_held_deleted
}
WITH *, [hd_r IN hd_fresh | hd_r.id] AS hd_written, [hd_x IN hd_diffs | hd_x.id] AS hd_changed,
     hd_has_deleted + hd_cites_deleted + hd_cites_created + size(hd_counted) + hd_has_created
       + hd_paired + hd_held_deleted AS hd_writes
CALL (hd_changed) {
  UNWIND hd_changed AS hd_changed_id
  MATCH (:PDDocument {id: hd_changed_id})-[:MEMBER_OF]->(hd_collection:PDCollection)
  RETURN collect(DISTINCT hd_collection.id) AS hd_collections
}
CALL (hd_written) {
  UNWIND hd_written AS debt_key
  WITH DISTINCT debt_key WHERE debt_key IS NOT NULL
  MERGE (debt_o:PDOwed {id: 'holdings:' + debt_key})
  ON CREATE SET debt_o.kind = 'holdings', debt_o.key = debt_key, debt_o.since = toString(datetime())
  SET debt_o.token = toString(datetime()) + '/' + toString(rand()), debt_o.after = null,
      debt_o.source = null
  RETURN count(debt_o) AS debt_owed_holdings
}
CALL (hd_collections) {
  UNWIND hd_collections AS debt_key
  WITH DISTINCT debt_key WHERE debt_key IS NOT NULL
  MERGE (debt_o:PDOwed {id: 'topics:' + debt_key})
  ON CREATE SET debt_o.kind = 'topics', debt_o.key = debt_key, debt_o.since = toString(datetime())
  SET debt_o.token = toString(datetime()) + '/' + toString(rand()), debt_o.after = null,
      debt_o.source = null
  RETURN count(debt_o) AS debt_owed_topics
}
WITH c, projected, size(hd_written) AS hd_derived

RETURN c.id AS id, c.revision AS revision, c.targets AS targets, projected
