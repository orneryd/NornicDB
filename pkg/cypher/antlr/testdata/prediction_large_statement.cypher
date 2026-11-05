CALL () {
  OPTIONAL MATCH (eff_c:KbCorrection {status: 'active'})
  OPTIONAL MATCH (eff_c)-[:CORRECTS]->(eff_t)
  WITH eff_c, collect(eff_t.id) AS eff_named
  OPTIONAL MATCH (eff_d:KbDocument) WHERE eff_d.current_version_id IN coalesce(eff_c.scope, [])
  WITH eff_c, eff_named, collect(eff_d.id) AS eff_docs
  WITH collect(CASE WHEN eff_c IS NULL THEN null
                    ELSE eff_c {.*, named: eff_named, docs: eff_docs,
                                all: eff_c.scope = ['all']} END) AS eff_corrections
  WITH eff_corrections,
       reduce(eff_all = [], eff_one IN eff_corrections | eff_all + (CASE
    WHEN eff_one.operation = 'exclude_link'
      THEN [eff_p IN [[eff_one.targets[0], eff_one.targets[1]],
                      [eff_one.targets[1], eff_one.targets[0]]] WHERE eff_p[0] IN eff_one.named
            | {key: eff_p[0] + '|' + eff_p[1] + '|' + coalesce(eff_one.link_kind, '*'),
               all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation IN ['resolve_mention', 'unresolve_mention']
         OR (eff_one.operation = 'include_link' AND eff_one.link_kind = 'resolves_to')
      THEN [eff_m IN eff_one.targets[0..1] WHERE eff_m IN eff_one.named
            | {key: eff_m + '|*|resolves_to|A', all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation = 'different_identity'
      THEN [eff_p IN [[eff_one.targets[0], eff_one.targets[1]],
                      [eff_one.targets[1], eff_one.targets[0]]] WHERE eff_p[0] IN eff_one.named
            | {key: eff_p[0] + '|' + eff_p[1] + '|resolves_to|A',
               all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation = 'split'
      THEN [eff_m IN eff_one.targets[1..] WHERE eff_m IN eff_one.named
            | {key: eff_m + '|' + eff_one.targets[0] + '|resolves_to|A',
               all: eff_one.all, docs: eff_one.docs}]
    WHEN eff_one.operation = 'replace_interpretation' AND size(eff_one.targets) = 1
         AND eff_one.targets[0] IN eff_one.named
      THEN [{key: 'doc|' + eff_one.targets[0], all: eff_one.all, docs: eff_one.docs}]
    ELSE [] END)) AS eff_hides,
       reduce(eff_all = [], eff_own IN eff_corrections | eff_all + [eff_rival IN eff_corrections
         WHERE eff_rival.id <> eff_own.id
           AND any(eff_shared IN eff_rival.named WHERE eff_shared IN eff_own.targets)
           AND ((
     (eff_own.operation = 'include_link' AND (eff_rival.operation = 'exclude_link' AND (eff_rival.link_kind IS NULL OR eff_rival.link_kind = eff_own.link_kind)) AND (eff_rival.targets = eff_own.targets OR eff_rival.targets = reverse(eff_own.targets)))
  OR ((eff_own.operation = 'same_identity' OR (eff_own.operation = 'include_link' AND eff_own.link_kind = 'same_as')) AND (
        (eff_rival.operation = 'different_identity' AND (eff_rival.targets = eff_own.targets OR eff_rival.targets = reverse(eff_own.targets)))
     OR ((eff_rival.operation = 'exclude_link' AND (eff_rival.link_kind IS NULL OR eff_rival.link_kind = 'same_as')) AND (eff_rival.targets = eff_own.targets OR eff_rival.targets = reverse(eff_own.targets)))
     OR (eff_rival.operation = 'split' AND (
           (eff_own.targets[0] = eff_rival.targets[0] AND eff_own.targets[1] IN eff_rival.targets[1..])
        OR (eff_own.targets[1] = eff_rival.targets[0] AND eff_own.targets[0] IN eff_rival.targets[1..])))))
  OR ((eff_own.operation = 'resolve_mention' OR (eff_own.operation = 'include_link' AND eff_own.link_kind = 'resolves_to')) AND (
        ((eff_rival.operation = 'resolve_mention' OR (eff_rival.operation = 'include_link' AND eff_rival.link_kind = 'resolves_to')) AND eff_rival.targets[0] = eff_own.targets[0] AND eff_rival.targets[1] <> eff_own.targets[1])
     OR (eff_rival.operation = 'unresolve_mention' AND eff_rival.targets[0] = eff_own.targets[0])
     OR (eff_rival.operation = 'different_identity' AND (eff_rival.targets = eff_own.targets OR eff_rival.targets = reverse(eff_own.targets)))
     OR ((eff_rival.operation = 'exclude_link' AND (eff_rival.link_kind IS NULL OR eff_rival.link_kind = 'resolves_to')) AND (eff_rival.targets = eff_own.targets OR eff_rival.targets = reverse(eff_own.targets)))
     OR (eff_rival.operation = 'split' AND eff_rival.targets[0] = eff_own.targets[1]
         AND eff_own.targets[0] IN eff_rival.targets[1..])))
  OR ((eff_own.operation = 'merge' OR (eff_own.operation = 'include_link' AND eff_own.link_kind = 'merged_into')) AND (
        (eff_rival.operation = 'split' AND eff_rival.targets[0] IN eff_own.targets
         AND any(part IN eff_rival.targets[1..] WHERE part IN eff_own.targets))
     OR (eff_rival.operation = 'different_identity' AND all(t IN eff_rival.targets WHERE t IN eff_own.targets))
     OR ((eff_rival.operation = 'merge' OR (eff_rival.operation = 'include_link' AND eff_rival.link_kind = 'merged_into')) AND (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[0] ELSE eff_rival.targets[1] END) <> (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[0] ELSE eff_own.targets[1] END)
         AND (any(t IN (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[1..] ELSE eff_own.targets[0..1] END) WHERE t IN (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[1..] ELSE eff_rival.targets[0..1] END))
              OR ((CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[0] ELSE eff_rival.targets[1] END) IN (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[1..] ELSE eff_own.targets[0..1] END) AND (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[0] ELSE eff_own.targets[1] END) IN (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[1..] ELSE eff_rival.targets[0..1] END))))
     OR ((eff_rival.operation = 'exclude_link' AND (eff_rival.link_kind IS NULL OR eff_rival.link_kind = 'merged_into')) AND (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[0] ELSE eff_own.targets[1] END) IN eff_rival.targets
         AND any(t IN (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[1..] ELSE eff_own.targets[0..1] END) WHERE t IN eff_rival.targets))))) OR (
     (eff_rival.operation = 'include_link' AND (eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = eff_rival.link_kind)) AND (eff_own.targets = eff_rival.targets OR eff_own.targets = reverse(eff_rival.targets)))
  OR ((eff_rival.operation = 'same_identity' OR (eff_rival.operation = 'include_link' AND eff_rival.link_kind = 'same_as')) AND (
        (eff_own.operation = 'different_identity' AND (eff_own.targets = eff_rival.targets OR eff_own.targets = reverse(eff_rival.targets)))
     OR ((eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = 'same_as')) AND (eff_own.targets = eff_rival.targets OR eff_own.targets = reverse(eff_rival.targets)))
     OR (eff_own.operation = 'split' AND (
           (eff_rival.targets[0] = eff_own.targets[0] AND eff_rival.targets[1] IN eff_own.targets[1..])
        OR (eff_rival.targets[1] = eff_own.targets[0] AND eff_rival.targets[0] IN eff_own.targets[1..])))))
  OR ((eff_rival.operation = 'resolve_mention' OR (eff_rival.operation = 'include_link' AND eff_rival.link_kind = 'resolves_to')) AND (
        ((eff_own.operation = 'resolve_mention' OR (eff_own.operation = 'include_link' AND eff_own.link_kind = 'resolves_to')) AND eff_own.targets[0] = eff_rival.targets[0] AND eff_own.targets[1] <> eff_rival.targets[1])
     OR (eff_own.operation = 'unresolve_mention' AND eff_own.targets[0] = eff_rival.targets[0])
     OR (eff_own.operation = 'different_identity' AND (eff_own.targets = eff_rival.targets OR eff_own.targets = reverse(eff_rival.targets)))
     OR ((eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = 'resolves_to')) AND (eff_own.targets = eff_rival.targets OR eff_own.targets = reverse(eff_rival.targets)))
     OR (eff_own.operation = 'split' AND eff_own.targets[0] = eff_rival.targets[1]
         AND eff_rival.targets[0] IN eff_own.targets[1..])))
  OR ((eff_rival.operation = 'merge' OR (eff_rival.operation = 'include_link' AND eff_rival.link_kind = 'merged_into')) AND (
        (eff_own.operation = 'split' AND eff_own.targets[0] IN eff_rival.targets
         AND any(part IN eff_own.targets[1..] WHERE part IN eff_rival.targets))
     OR (eff_own.operation = 'different_identity' AND all(t IN eff_own.targets WHERE t IN eff_rival.targets))
     OR ((eff_own.operation = 'merge' OR (eff_own.operation = 'include_link' AND eff_own.link_kind = 'merged_into')) AND (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[0] ELSE eff_own.targets[1] END) <> (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[0] ELSE eff_rival.targets[1] END)
         AND (any(t IN (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[1..] ELSE eff_rival.targets[0..1] END) WHERE t IN (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[1..] ELSE eff_own.targets[0..1] END))
              OR ((CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[0] ELSE eff_own.targets[1] END) IN (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[1..] ELSE eff_rival.targets[0..1] END) AND (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[0] ELSE eff_rival.targets[1] END) IN (CASE WHEN eff_own.operation = 'merge' THEN eff_own.targets[1..] ELSE eff_own.targets[0..1] END))))
     OR ((eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = 'merged_into')) AND (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[0] ELSE eff_rival.targets[1] END) IN eff_own.targets
         AND any(t IN (CASE WHEN eff_rival.operation = 'merge' THEN eff_rival.targets[1..] ELSE eff_rival.targets[0..1] END) WHERE t IN eff_own.targets))))))
         | {key: eff_own.id, all: eff_rival.all, docs: eff_rival.docs}]) AS eff_rivals
  CALL (eff_hides) {
    UNWIND eff_hides AS eff_h
    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs}) AS eff_s
    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_hidden
  }
  CALL (eff_rivals) {
    UNWIND eff_rivals AS eff_h
    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs}) AS eff_s
    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_contested
  }
  RETURN eff_hidden, eff_contested,
         apoc.map.fromPairs([eff_h IN eff_hides | [split(eff_h.key, '|')[0], true]])
           AS eff_hiding,
         apoc.map.fromPairs([eff_s IN eff_corrections
                             | [eff_s.id, [{all: eff_s.all, docs: eff_s.docs}]]]) AS eff_scopes
}
WITH {hidden: eff_hidden, hiding: eff_hiding, scopes: eff_scopes,
   contested: eff_contested, apart_links: {}} AS eff_view
CALL (eff_view) {
  OPTIONAL MATCH (eff_apart:KbCorrection {status: 'active'})
  WHERE eff_apart.operation = 'different_identity'
  OPTIONAL MATCH (eff_x:KbGraphNode {id: eff_apart.targets[0]})
  OPTIONAL MATCH (eff_y:KbGraphNode {id: eff_apart.targets[1]})
  OPTIONAL MATCH eff_left = (eff_x)(()-[eff_l:KB_RELATION WHERE eff_l.kind = 'merged_into' AND (((coalesce(eff_l.origin, "") <> "automatic" OR eff_l.stage = "interpretation")
  AND coalesce(eff_l.current, true)) AND coalesce(startNode(eff_l).current, true) AND coalesce(endNode(eff_l).current, true)
  AND (eff_l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_l).id + '|' + endNode(eff_l).id + '|' + coalesce(eff_l.kind, ''), startNode(eff_l).id + '|' + endNode(eff_l).id + '|*', CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|' + endNode(eff_l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_l.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_l.correction_id IS NULL OR (eff_l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join:KbGraphNode)
  OPTIONAL MATCH eff_right = (eff_y)(()-[eff_r:KB_RELATION WHERE eff_r.kind = 'merged_into' AND (((coalesce(eff_r.origin, "") <> "automatic" OR eff_r.stage = "interpretation")
  AND coalesce(eff_r.current, true)) AND coalesce(startNode(eff_r).current, true) AND coalesce(endNode(eff_r).current, true)
  AND (eff_r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_r).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_r.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_r).id + '|' + endNode(eff_r).id + '|' + coalesce(eff_r.kind, ''), startNode(eff_r).id + '|' + endNode(eff_r).id + '|*', CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|' + endNode(eff_r).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_r.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_r.correction_id IS NULL OR (eff_r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join)
  WITH eff_apart, eff_left, eff_right
  WHERE eff_right IS NOT NULL
  WITH eff_apart, eff_left, eff_right,
       nodes(eff_left) + nodes(eff_right) AS eff_nodes,
       relationships(eff_left) + relationships(eff_right) AS eff_links
  WHERE (eff_apart.scope = ['all'] OR EXISTS {
      MATCH (eff_document:KbDocument)
      WHERE eff_document.current_version_id IN eff_apart.scope
        AND eff_document.id IN [eff_n IN eff_nodes | eff_n.id] + [eff_n IN eff_nodes | eff_n.document_id] + [eff_m IN eff_links | eff_m.document_id] })
  UNWIND relationships(eff_left) + relationships(eff_right) AS eff_link
  WITH DISTINCT eff_apart.id AS eff_apart_id, eff_link.id AS eff_link_id,
       eff_link.correction_id AS eff_merge_id
  RETURN collect([eff_apart_id, eff_link_id, eff_merge_id]) AS eff_apart_pairs
}
WITH eff_apart_pairs,
     {hidden: eff_view.hidden, hiding: eff_view.hiding, scopes: eff_view.scopes,
       contested: eff_view.contested,
       apart_links: apoc.map.fromPairs([eff_p IN eff_apart_pairs | [eff_p[1], true]])}
       AS eff_view


CALL () {
MATCH (eff_separation:KbCorrection {status: 'active', operation: 'different_identity'})
UNWIND [eff_separation.targets, reverse(eff_separation.targets)] AS eff_pair
WITH eff_pair[0] AS eff_first, eff_pair[1] AS eff_second,
     collect(eff_separation) AS eff_rules
WITH eff_first, apoc.map.fromPairs(collect([eff_second, eff_rules])) AS eff_targets
RETURN apoc.map.fromPairs(collect([eff_first, eff_targets])) AS eff_separations
}
CALL (eff_view) {
  CALL (eff_view) {
  MATCH (party:KbGraphNode:KbEntity)
  WHERE party.name_key IS NOT NULL AND coalesce(party.current, true)
    AND ($collection_ids IS NULL OR party:KbIdentity OR EXISTS {
         MATCH (:KbDocument {id: party.document_id})-[:MEMBER_OF]->(project:KbCollection)
         WHERE project.id IN $collection_ids })
    AND (party:KbIdentity OR EXISTS {
      MATCH (party)-[membership:KB_RELATION]-()
      WHERE ((((coalesce(membership.origin, "") <> "automatic" OR membership.stage = "interpretation")
  AND coalesce(membership.current, true)) AND coalesce(startNode(membership).current, true) AND coalesce(endNode(membership).current, true)
  AND (membership.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(membership).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(membership.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(membership).id + '|' + endNode(membership).id + '|' + coalesce(membership.kind, ''), startNode(membership).id + '|' + endNode(membership).id + '|*', CASE WHEN membership.kind = 'resolves_to' AND coalesce(membership.origin, '') = 'automatic' THEN startNode(membership).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN membership.kind = 'resolves_to' AND coalesce(membership.origin, '') = 'automatic' THEN startNode(membership).id + '|' + endNode(membership).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(membership.origin, '') = 'automatic' THEN 'doc|' + coalesce(membership.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [membership.document_id, startNode(membership).id, endNode(membership).id, startNode(membership).document_id, endNode(membership).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (membership.correction_id IS NULL OR (membership.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(membership.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [membership.document_id, startNode(membership).id, endNode(membership).id, startNode(membership).document_id, endNode(membership).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(membership.id, '')] IS NOT NULL
      OR (membership.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(membership.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [membership.document_id, startNode(membership).id, endNode(membership).id, startNode(membership).document_id, endNode(membership).document_id] WHERE eff_id IS NOT NULL])))))) })
  RETURN collect(party) AS parties
  }
  CALL (parties, eff_view) {
    WITH [party IN parties WHERE NOT party:KbIdentity | party.id] AS mention_ids, eff_view
    CALL (mention_ids, eff_view) {
CALL (mention_ids) {
  UNWIND mention_ids AS eff_mention_id
  MATCH (eff_mention:KbGraphNode {id: eff_mention_id})
  RETURN eff_mention.document_id AS eff_document_id, collect(eff_mention) AS eff_mentions
}
CALL (eff_mentions, eff_document_id, eff_view) {
  OPTIONAL MATCH (eff_apart:KbCorrection {status: 'active'})
  WHERE eff_apart.operation = 'different_identity'
  OPTIONAL MATCH (eff_x:KbGraphNode {id: eff_apart.targets[0]})
  OPTIONAL MATCH (eff_y:KbGraphNode {id: eff_apart.targets[1]})
  OPTIONAL MATCH eff_left = (eff_x)(()-[eff_l:KB_RELATION WHERE eff_l.kind = 'merged_into' AND (((coalesce(eff_l.origin, "") <> "automatic" OR eff_l.stage = "interpretation")
  AND coalesce(eff_l.current, true)) AND coalesce(startNode(eff_l).current, true) AND coalesce(endNode(eff_l).current, true)
  AND (eff_l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_l).id + '|' + endNode(eff_l).id + '|' + coalesce(eff_l.kind, ''), startNode(eff_l).id + '|' + endNode(eff_l).id + '|*', CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|' + endNode(eff_l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_l.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] + [eff_document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_l.correction_id IS NULL OR (eff_l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] + [eff_document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join:KbGraphNode)
  OPTIONAL MATCH eff_right = (eff_y)(()-[eff_r:KB_RELATION WHERE eff_r.kind = 'merged_into' AND (((coalesce(eff_r.origin, "") <> "automatic" OR eff_r.stage = "interpretation")
  AND coalesce(eff_r.current, true)) AND coalesce(startNode(eff_r).current, true) AND coalesce(endNode(eff_r).current, true)
  AND (eff_r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_r).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_r.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_r).id + '|' + endNode(eff_r).id + '|' + coalesce(eff_r.kind, ''), startNode(eff_r).id + '|' + endNode(eff_r).id + '|*', CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|' + endNode(eff_r).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_r.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] + [eff_document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_r.correction_id IS NULL OR (eff_r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] + [eff_document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join)
  WITH eff_mentions, eff_document_id, eff_apart, eff_left, eff_right
  WHERE eff_right IS NOT NULL
  WITH eff_mentions, eff_document_id, eff_apart, eff_left, eff_right,
       nodes(eff_left) + nodes(eff_right) AS eff_nodes,
       relationships(eff_left) + relationships(eff_right) AS eff_links
  WHERE (eff_apart.scope = ['all'] OR EXISTS {
      MATCH (eff_document:KbDocument)
      WHERE eff_document.current_version_id IN eff_apart.scope
        AND eff_document.id IN [eff_n IN eff_nodes | eff_n.id] + [eff_n IN eff_nodes | eff_n.document_id] + [eff_m IN eff_links | eff_m.document_id] + [eff_document_id] })
  UNWIND relationships(eff_left) + relationships(eff_right) AS eff_link
  WITH DISTINCT eff_apart.id AS eff_apart_id, eff_link.id AS eff_link_id,
       eff_link.correction_id AS eff_merge_id
  RETURN collect([eff_apart_id, eff_link_id, eff_merge_id]) AS eff_apart_pairs
}
WITH eff_mentions, eff_document_id, eff_apart_pairs,
     {hidden: eff_view.hidden, hiding: eff_view.hiding, scopes: eff_view.scopes,
       contested: eff_view.contested,
       apart_links: apoc.map.fromPairs([eff_p IN eff_apart_pairs | [eff_p[1], true]])}
       AS eff_view
UNWIND eff_mentions AS m
OPTIONAL MATCH (m)-[r:KB_RELATION]->(resolved:KbGraphNode)
WHERE r.kind = 'resolves_to' AND ((((coalesce(r.origin, "") <> "automatic" OR r.stage = "interpretation")
  AND coalesce(r.current, true)) AND coalesce(startNode(r).current, true) AND coalesce(endNode(r).current, true)
  AND (r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(r).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(r.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(r).id + '|' + endNode(r).id + '|' + coalesce(r.kind, ''), startNode(r).id + '|' + endNode(r).id + '|*', CASE WHEN r.kind = 'resolves_to' AND coalesce(r.origin, '') = 'automatic' THEN startNode(r).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN r.kind = 'resolves_to' AND coalesce(r.origin, '') = 'automatic' THEN startNode(r).id + '|' + endNode(r).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(r.origin, '') = 'automatic' THEN 'doc|' + coalesce(r.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [r.document_id, startNode(r).id, endNode(r).id, startNode(r).document_id, endNode(r).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
  AND (r.correction_id IS NULL OR (r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [r.document_id, startNode(r).id, endNode(r).id, startNode(r).document_id, endNode(r).document_id] + [m.document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(r.id, '')] IS NOT NULL
      OR (r.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(r.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [r.document_id, startNode(r).id, endNode(r).id, startNode(r).document_id, endNode(r).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
OPTIONAL MATCH (resolved)(()-[merged:KB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:KbGraphNode)
OPTIONAL MATCH (reached)-[next:KB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
WITH m, eff_view, r, resolved, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:KB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [m.document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [m.document_id] WHERE eff_id IS NOT NULL])))))
WITH m, eff_view, r, resolved, reached, terminal, count(step) > 0 AS step_disputed
WITH m, eff_view, r, resolved, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed
WITH m, eff_view,
     collect(CASE WHEN resolved IS NULL THEN null
                  ELSE [CASE WHEN size(terminals) = 1 THEN terminals[0] END, resolved.id,
                        coalesce(r.origin, ''), r.correction_id,
                        chain_disputed OR size(terminals) <> 1]
             END) AS found
WITH m, eff_view, found,
     reduce(ids = [], x IN found |
            CASE WHEN x[0] IS NULL OR x[0] IN ids THEN ids ELSE ids + x[0] END) AS identities,
     size([x IN found WHERE x[4]]) > 0 AS chain_disputed
OPTIONAL MATCH (m)<-[:CORRECTS]-(active:KbCorrection {status: 'active'})
WITH m, eff_view, found, identities, chain_disputed, collect(active.id) AS corrections
OPTIONAL MATCH (c:KbCorrection {status: 'active'})-[:CORRECTS]->(m)
OPTIONAL MATCH (c)-[:CORRECTS]->()<-[:CORRECTS]-(o:KbCorrection {status: 'active'})
WHERE o.id <> c.id AND ((
     (c.operation = 'include_link' AND (o.operation = 'exclude_link' AND (o.link_kind IS NULL OR o.link_kind = c.link_kind)) AND (o.targets = c.targets OR o.targets = reverse(c.targets)))
  OR ((c.operation = 'same_identity' OR (c.operation = 'include_link' AND c.link_kind = 'same_as')) AND (
        (o.operation = 'different_identity' AND (o.targets = c.targets OR o.targets = reverse(c.targets)))
     OR ((o.operation = 'exclude_link' AND (o.link_kind IS NULL OR o.link_kind = 'same_as')) AND (o.targets = c.targets OR o.targets = reverse(c.targets)))
     OR (o.operation = 'split' AND (
           (c.targets[0] = o.targets[0] AND c.targets[1] IN o.targets[1..])
        OR (c.targets[1] = o.targets[0] AND c.targets[0] IN o.targets[1..])))))
  OR ((c.operation = 'resolve_mention' OR (c.operation = 'include_link' AND c.link_kind = 'resolves_to')) AND (
        ((o.operation = 'resolve_mention' OR (o.operation = 'include_link' AND o.link_kind = 'resolves_to')) AND o.targets[0] = c.targets[0] AND o.targets[1] <> c.targets[1])
     OR (o.operation = 'unresolve_mention' AND o.targets[0] = c.targets[0])
     OR (o.operation = 'different_identity' AND (o.targets = c.targets OR o.targets = reverse(c.targets)))
     OR ((o.operation = 'exclude_link' AND (o.link_kind IS NULL OR o.link_kind = 'resolves_to')) AND (o.targets = c.targets OR o.targets = reverse(c.targets)))
     OR (o.operation = 'split' AND o.targets[0] = c.targets[1]
         AND c.targets[0] IN o.targets[1..])))
  OR ((c.operation = 'merge' OR (c.operation = 'include_link' AND c.link_kind = 'merged_into')) AND (
        (o.operation = 'split' AND o.targets[0] IN c.targets
         AND any(part IN o.targets[1..] WHERE part IN c.targets))
     OR (o.operation = 'different_identity' AND all(t IN o.targets WHERE t IN c.targets))
     OR ((o.operation = 'merge' OR (o.operation = 'include_link' AND o.link_kind = 'merged_into')) AND (CASE WHEN o.operation = 'merge' THEN o.targets[0] ELSE o.targets[1] END) <> (CASE WHEN c.operation = 'merge' THEN c.targets[0] ELSE c.targets[1] END)
         AND (any(t IN (CASE WHEN c.operation = 'merge' THEN c.targets[1..] ELSE c.targets[0..1] END) WHERE t IN (CASE WHEN o.operation = 'merge' THEN o.targets[1..] ELSE o.targets[0..1] END))
              OR ((CASE WHEN o.operation = 'merge' THEN o.targets[0] ELSE o.targets[1] END) IN (CASE WHEN c.operation = 'merge' THEN c.targets[1..] ELSE c.targets[0..1] END) AND (CASE WHEN c.operation = 'merge' THEN c.targets[0] ELSE c.targets[1] END) IN (CASE WHEN o.operation = 'merge' THEN o.targets[1..] ELSE o.targets[0..1] END))))
     OR ((o.operation = 'exclude_link' AND (o.link_kind IS NULL OR o.link_kind = 'merged_into')) AND (CASE WHEN c.operation = 'merge' THEN c.targets[0] ELSE c.targets[1] END) IN o.targets
         AND any(t IN (CASE WHEN c.operation = 'merge' THEN c.targets[1..] ELSE c.targets[0..1] END) WHERE t IN o.targets))))) OR (
     (o.operation = 'include_link' AND (c.operation = 'exclude_link' AND (c.link_kind IS NULL OR c.link_kind = o.link_kind)) AND (c.targets = o.targets OR c.targets = reverse(o.targets)))
  OR ((o.operation = 'same_identity' OR (o.operation = 'include_link' AND o.link_kind = 'same_as')) AND (
        (c.operation = 'different_identity' AND (c.targets = o.targets OR c.targets = reverse(o.targets)))
     OR ((c.operation = 'exclude_link' AND (c.link_kind IS NULL OR c.link_kind = 'same_as')) AND (c.targets = o.targets OR c.targets = reverse(o.targets)))
     OR (c.operation = 'split' AND (
           (o.targets[0] = c.targets[0] AND o.targets[1] IN c.targets[1..])
        OR (o.targets[1] = c.targets[0] AND o.targets[0] IN c.targets[1..])))))
  OR ((o.operation = 'resolve_mention' OR (o.operation = 'include_link' AND o.link_kind = 'resolves_to')) AND (
        ((c.operation = 'resolve_mention' OR (c.operation = 'include_link' AND c.link_kind = 'resolves_to')) AND c.targets[0] = o.targets[0] AND c.targets[1] <> o.targets[1])
     OR (c.operation = 'unresolve_mention' AND c.targets[0] = o.targets[0])
     OR (c.operation = 'different_identity' AND (c.targets = o.targets OR c.targets = reverse(o.targets)))
     OR ((c.operation = 'exclude_link' AND (c.link_kind IS NULL OR c.link_kind = 'resolves_to')) AND (c.targets = o.targets OR c.targets = reverse(o.targets)))
     OR (c.operation = 'split' AND c.targets[0] = o.targets[1]
         AND o.targets[0] IN c.targets[1..])))
  OR ((o.operation = 'merge' OR (o.operation = 'include_link' AND o.link_kind = 'merged_into')) AND (
        (c.operation = 'split' AND c.targets[0] IN o.targets
         AND any(part IN c.targets[1..] WHERE part IN o.targets))
     OR (c.operation = 'different_identity' AND all(t IN c.targets WHERE t IN o.targets))
     OR ((c.operation = 'merge' OR (c.operation = 'include_link' AND c.link_kind = 'merged_into')) AND (CASE WHEN c.operation = 'merge' THEN c.targets[0] ELSE c.targets[1] END) <> (CASE WHEN o.operation = 'merge' THEN o.targets[0] ELSE o.targets[1] END)
         AND (any(t IN (CASE WHEN o.operation = 'merge' THEN o.targets[1..] ELSE o.targets[0..1] END) WHERE t IN (CASE WHEN c.operation = 'merge' THEN c.targets[1..] ELSE c.targets[0..1] END))
              OR ((CASE WHEN c.operation = 'merge' THEN c.targets[0] ELSE c.targets[1] END) IN (CASE WHEN o.operation = 'merge' THEN o.targets[1..] ELSE o.targets[0..1] END) AND (CASE WHEN o.operation = 'merge' THEN o.targets[0] ELSE o.targets[1] END) IN (CASE WHEN c.operation = 'merge' THEN c.targets[1..] ELSE c.targets[0..1] END))))
     OR ((c.operation = 'exclude_link' AND (c.link_kind IS NULL OR c.link_kind = 'merged_into')) AND (CASE WHEN o.operation = 'merge' THEN o.targets[0] ELSE o.targets[1] END) IN c.targets
         AND any(t IN (CASE WHEN o.operation = 'merge' THEN o.targets[1..] ELSE o.targets[0..1] END) WHERE t IN c.targets))))))
  AND (c.scope = ['all'] OR EXISTS {
      MATCH (eff_document:KbDocument)
      WHERE eff_document.current_version_id IN c.scope
        AND eff_document.id IN [m.document_id] }) AND (o.scope = ['all'] OR EXISTS {
      MATCH (eff_document:KbDocument)
      WHERE eff_document.current_version_id IN o.scope
        AND eff_document.id IN [m.document_id] })
WITH m, eff_view, found, identities, chain_disputed, corrections,
     count(o) > 0 AS mention_disputed
WITH m, eff_view, found, identities, corrections,
     CASE WHEN mention_disputed OR chain_disputed OR size(identities) > 1 THEN 'disputed'
          WHEN size(identities) = 0 THEN 'unresolved' ELSE 'resolved' END AS state
RETURN m.id AS mention_id, state

}
    RETURN apoc.map.fromPairs(collect([mention_id, state])) AS states
  }
  UNWIND parties AS party
  WITH party, eff_view WHERE party:KbIdentity OR states[party.id] = 'unresolved'
  OPTIONAL MATCH (party)(()-[merged:KB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:KbGraphNode)
OPTIONAL MATCH (reached)-[next:KB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
WITH eff_view, party, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:KB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL])))))
WITH eff_view, party, reached, terminal, count(step) > 0 AS step_disputed
WITH eff_view, party, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed
OPTIONAL MATCH (survivor:KbGraphNode)
WHERE NOT chain_disputed AND size(terminals) = 1 AND survivor.id = terminals[0]
WITH eff_view, coalesce(survivor, party) AS party
WITH DISTINCT party, party:KbIdentity AS identity
  WITH party, identity,
       CASE WHEN identity THEN party.kind ELSE $classes[coalesce(party.definition_id, '')] END
         AS class
  UNWIND [party.name_key] + [at IN range(0, size(coalesce(party.alias_keys, [])) - 1) WHERE coalesce(party.alias_types[at], '') <> $rejected | party.alias_keys[at]] AS name
  WITH party, identity, class, apoc.text.join([word IN split(string.regexReplace(toLower(name), '[^\\p{L}\\p{N}]+', ' '), ' ') WHERE word <> '' | CASE word WHEN 'limited' THEN 'ltd' WHEN 'incorporated' THEN 'inc' WHEN 'corporation' THEN 'corp' ELSE word END], ' ') AS key
  WHERE key <> ''
  WITH key, collect(DISTINCT {party: party, identity: identity, class: class}) AS named
  WHERE size(named) > 1
  RETURN collect(named) AS groups
}
CALL (eff_view, groups) {
  UNWIND groups AS named
  UNWIND named AS each
  WITH DISTINCT eff_view, each
  CALL (each, eff_view) {
    WITH each WHERE NOT each.identity
    RETURN each.party.document_id AS document_id
    UNION
    WITH each, eff_view WHERE each.identity
    WITH each.party AS party, eff_view
    CALL (party, eff_view) {
  OPTIONAL MATCH (party)(()<-[merged:KB_RELATION WHERE merged.kind = 'merged_into'
                                AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]-())*(alias:KbGraphNode)
  WITH party, alias, eff_view
  OPTIONAL MATCH (alias)(()-[merged:KB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:KbGraphNode)
OPTIONAL MATCH (reached)-[next:KB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
WITH party, eff_view, alias, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:KB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL])))))
WITH party, eff_view, alias, reached, terminal, count(step) > 0 AS step_disputed
WITH party, eff_view, alias, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed

  WITH party, alias, eff_view, chain_disputed, terminals
  WHERE alias = party OR (NOT chain_disputed AND terminals = [party.id])
  WITH alias, eff_view
  MATCH (alias)-[l:KB_RELATION]-(near:KbGraphNode)
  WHERE coalesce(l.kind, '') <> 'merged_into' AND coalesce(l.kind, '') <> 'same_document' AND ((((coalesce(l.origin, "") <> "automatic" OR l.stage = "interpretation")
  AND coalesce(l.current, true)) AND coalesce(startNode(l).current, true) AND coalesce(endNode(l).current, true)
  AND (l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(l).id + '|' + endNode(l).id + '|' + coalesce(l.kind, ''), startNode(l).id + '|' + endNode(l).id + '|*', CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|' + endNode(l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(l.origin, '') = 'automatic' THEN 'doc|' + coalesce(l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (l.correction_id IS NULL OR (l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(l.id, '')] IS NOT NULL
      OR (l.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(l.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  OPTIONAL MATCH (own_document:KbDocument {id: near.document_id})
  WITH alias, l, near, eff_view,
       CASE WHEN near:KbDocument THEN near ELSE own_document END AS holder
  WHERE holder IS NOT NULL AND coalesce(alias.document_id, '') <> holder.id
    AND NOT alias:KbSummary AND NOT near:KbSummary
    AND (near:KbDocument OR EXISTS {
         MATCH (holder)-[own_link:KB_RELATION]-(near)
         WHERE coalesce(own_link.kind, '') <> 'same_document' AND ((((coalesce(own_link.origin, "") <> "automatic" OR own_link.stage = "interpretation")
  AND coalesce(own_link.current, true)) AND coalesce(startNode(own_link).current, true) AND coalesce(endNode(own_link).current, true)
  AND (own_link.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(own_link).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(own_link.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(own_link).id + '|' + endNode(own_link).id + '|' + coalesce(own_link.kind, ''), startNode(own_link).id + '|' + endNode(own_link).id + '|*', CASE WHEN own_link.kind = 'resolves_to' AND coalesce(own_link.origin, '') = 'automatic' THEN startNode(own_link).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN own_link.kind = 'resolves_to' AND coalesce(own_link.origin, '') = 'automatic' THEN startNode(own_link).id + '|' + endNode(own_link).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(own_link.origin, '') = 'automatic' THEN 'doc|' + coalesce(own_link.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (own_link.correction_id IS NULL OR (own_link.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(own_link.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(own_link.id, '')] IS NOT NULL
      OR (own_link.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(own_link.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: own_link.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(own_link.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < own_link.cite_end[c]
              AND own_link.cite_start[c] < email_source.section_ends[i]))
    } })
    AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: l.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(l.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < l.cite_end[c]
              AND l.cite_start[c] < email_source.section_ends[i]))
    } AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: near.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(near.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < near.cite_end[c]
              AND near.cite_start[c] < email_source.section_ends[i]))
    }
  RETURN holder, CASE WHEN near:KbDocument
                      THEN {link: l.id, item: null, kind: l.kind, source_id: l.source_id,
                             cite_start: l.cite_start, cite_end: l.cite_end}
                      ELSE {link: l.id, item: near.id, kind: l.kind,
                             source_id: near.source_id, cite_start: near.cite_start,
                             cite_end: near.cite_end} END AS via
}
    RETURN holder.id AS document_id
  }
  WITH each.party.id AS party_id, collect(DISTINCT document_id) AS documents
  RETURN apoc.map.fromPairs(collect([party_id, documents])) AS documents_of
}
CALL (eff_view, documents_of) {
  CALL (documents_of) {
    UNWIND keys(documents_of) AS party_id
    UNWIND documents_of[party_id] AS document_id
    MATCH (document:KbDocument {id: document_id})
    RETURN collect(DISTINCT document) AS documents
  }
  CALL (documents, eff_view) {
  CALL (documents, eff_view) {
    CALL (documents, eff_view) {
  CALL (documents, eff_view) {
    UNWIND documents AS document
    CALL (document, eff_view) {
  MATCH (document)-[l:KB_RELATION]-(shared:KbGraphNode)
  WHERE NOT shared:KbDefinition AND coalesce(shared.document_id, '') <> document.id
    AND NOT shared:KbSummary
    AND coalesce(l.kind, '') <> 'same_document' AND ((((coalesce(l.origin, "") <> "automatic" OR l.stage = "interpretation")
  AND coalesce(l.current, true)) AND coalesce(startNode(l).current, true) AND coalesce(endNode(l).current, true)
  AND (l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(l).id + '|' + endNode(l).id + '|' + coalesce(l.kind, ''), startNode(l).id + '|' + endNode(l).id + '|*', CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|' + endNode(l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(l.origin, '') = 'automatic' THEN 'doc|' + coalesce(l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (l.correction_id IS NULL OR (l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(l.id, '')] IS NOT NULL
      OR (l.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(l.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: l.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(l.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < l.cite_end[c]
              AND l.cite_start[c] < email_source.section_ends[i]))
    }
  RETURN shared, {link: l.id, item: null, kind: l.kind, source_id: l.source_id,
                   cite_start: l.cite_start, cite_end: l.cite_end} AS via
  UNION
  MATCH (document)-[l:KB_RELATION]-(own:KbGraphNode)-[l2:KB_RELATION]-(shared:KbGraphNode)
  WHERE own.document_id = document.id AND NOT shared:KbDefinition
    AND NOT own:KbSummary AND NOT shared:KbSummary
    AND coalesce(shared.document_id, '') <> document.id
    AND coalesce(l.kind, '') <> 'same_document' AND ((((coalesce(l.origin, "") <> "automatic" OR l.stage = "interpretation")
  AND coalesce(l.current, true)) AND coalesce(startNode(l).current, true) AND coalesce(endNode(l).current, true)
  AND (l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(l).id + '|' + endNode(l).id + '|' + coalesce(l.kind, ''), startNode(l).id + '|' + endNode(l).id + '|*', CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|' + endNode(l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(l.origin, '') = 'automatic' THEN 'doc|' + coalesce(l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (l.correction_id IS NULL OR (l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(l.id, '')] IS NOT NULL
      OR (l.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(l.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND coalesce(l2.kind, '') <> 'same_document' AND ((((coalesce(l2.origin, "") <> "automatic" OR l2.stage = "interpretation")
  AND coalesce(l2.current, true)) AND coalesce(startNode(l2).current, true) AND coalesce(endNode(l2).current, true)
  AND (l2.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(l2).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(l2.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(l2).id + '|' + endNode(l2).id + '|' + coalesce(l2.kind, ''), startNode(l2).id + '|' + endNode(l2).id + '|*', CASE WHEN l2.kind = 'resolves_to' AND coalesce(l2.origin, '') = 'automatic' THEN startNode(l2).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN l2.kind = 'resolves_to' AND coalesce(l2.origin, '') = 'automatic' THEN startNode(l2).id + '|' + endNode(l2).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(l2.origin, '') = 'automatic' THEN 'doc|' + coalesce(l2.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [l2.document_id, startNode(l2).id, endNode(l2).id, startNode(l2).document_id, endNode(l2).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (l2.correction_id IS NULL OR (l2.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(l2.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [l2.document_id, startNode(l2).id, endNode(l2).id, startNode(l2).document_id, endNode(l2).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(l2.id, '')] IS NOT NULL
      OR (l2.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(l2.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [l2.document_id, startNode(l2).id, endNode(l2).id, startNode(l2).document_id, endNode(l2).document_id] WHERE eff_id IS NOT NULL]))))))
    AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: l.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(l.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < l.cite_end[c]
              AND l.cite_start[c] < email_source.section_ends[i]))
    } AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: own.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(own.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < own.cite_end[c]
              AND own.cite_start[c] < email_source.section_ends[i]))
    } AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: l2.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(l2.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < l2.cite_end[c]
              AND l2.cite_start[c] < email_source.section_ends[i]))
    }
  RETURN shared, {link: l2.id, item: own.id, kind: l2.kind, source_id: own.source_id,
                   cite_start: own.cite_start, cite_end: own.cite_end} AS via
}
    RETURN collect({document: document, shared: shared, via: via}) AS incidence
  }
  CALL (incidence, eff_view) {
    UNWIND incidence AS each
    WITH each.shared AS shared, head(collect(eff_view)) AS eff_view
    WITH shared.id AS raw_id, shared, eff_view
    OPTIONAL MATCH (shared)(()-[merged:KB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:KbGraphNode)
OPTIONAL MATCH (reached)-[next:KB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
WITH raw_id, eff_view, shared, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:KB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL])))))
WITH raw_id, eff_view, shared, reached, terminal, count(step) > 0 AS step_disputed
WITH raw_id, eff_view, shared, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed
OPTIONAL MATCH (survivor:KbGraphNode)
WHERE NOT chain_disputed AND size(terminals) = 1 AND survivor.id = terminals[0]
WITH raw_id, eff_view, coalesce(survivor, shared) AS shared

    RETURN apoc.map.fromPairs(collect([raw_id, shared])) AS survivor_of
  }
  UNWIND incidence AS each
  WITH each.document AS document, survivor_of[each.shared.id] AS thing,
       head(collect(each.via)) AS via
  RETURN document, thing, via
}
    RETURN collect({document: document, thing: thing, via: via}) AS incidence
  }
  CALL (incidence, eff_view) {
    UNWIND incidence AS each
    WITH DISTINCT eff_view, each.thing AS thing
    CALL (thing, eff_view) {
  
  CALL (thing, eff_view) {
  OPTIONAL MATCH (thing)(()<-[merged:KB_RELATION WHERE merged.kind = 'merged_into'
                                AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]-())*(alias:KbGraphNode)
  WITH thing, alias, eff_view
  OPTIONAL MATCH (alias)(()-[merged:KB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:KbGraphNode)
OPTIONAL MATCH (reached)-[next:KB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] WHERE eff_id IS NOT NULL]))))))
WITH thing, eff_view, alias, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:KB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] WHERE eff_id IS NOT NULL])))))
WITH thing, eff_view, alias, reached, terminal, count(step) > 0 AS step_disputed
WITH thing, eff_view, alias, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed

  WITH thing, alias, eff_view, chain_disputed, terminals
  WHERE alias = thing OR (NOT chain_disputed AND terminals = [thing.id])
  WITH alias, eff_view
  MATCH (alias)-[l:KB_RELATION]-(near:KbGraphNode)
  WHERE coalesce(l.kind, '') <> 'merged_into' AND coalesce(l.kind, '') <> 'same_document' AND ((((coalesce(l.origin, "") <> "automatic" OR l.stage = "interpretation")
  AND coalesce(l.current, true)) AND coalesce(startNode(l).current, true) AND coalesce(endNode(l).current, true)
  AND (l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(l).id + '|' + endNode(l).id + '|' + coalesce(l.kind, ''), startNode(l).id + '|' + endNode(l).id + '|*', CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN l.kind = 'resolves_to' AND coalesce(l.origin, '') = 'automatic' THEN startNode(l).id + '|' + endNode(l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(l.origin, '') = 'automatic' THEN 'doc|' + coalesce(l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (l.correction_id IS NULL OR (l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(l.id, '')] IS NOT NULL
      OR (l.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(l.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [l.document_id, startNode(l).id, endNode(l).id, startNode(l).document_id, endNode(l).document_id] WHERE eff_id IS NOT NULL]))))))
  OPTIONAL MATCH (own_document:KbDocument {id: near.document_id})
  WITH alias, l, near, eff_view,
       CASE WHEN near:KbDocument THEN near ELSE own_document END AS holder
  WHERE holder IS NOT NULL AND coalesce(alias.document_id, '') <> holder.id
    AND NOT alias:KbSummary AND NOT near:KbSummary
    AND (near:KbDocument OR EXISTS {
         MATCH (holder)-[own_link:KB_RELATION]-(near)
         WHERE coalesce(own_link.kind, '') <> 'same_document' AND ((((coalesce(own_link.origin, "") <> "automatic" OR own_link.stage = "interpretation")
  AND coalesce(own_link.current, true)) AND coalesce(startNode(own_link).current, true) AND coalesce(endNode(own_link).current, true)
  AND (own_link.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(own_link).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(own_link.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(own_link).id + '|' + endNode(own_link).id + '|' + coalesce(own_link.kind, ''), startNode(own_link).id + '|' + endNode(own_link).id + '|*', CASE WHEN own_link.kind = 'resolves_to' AND coalesce(own_link.origin, '') = 'automatic' THEN startNode(own_link).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN own_link.kind = 'resolves_to' AND coalesce(own_link.origin, '') = 'automatic' THEN startNode(own_link).id + '|' + endNode(own_link).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(own_link.origin, '') = 'automatic' THEN 'doc|' + coalesce(own_link.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL]))))))
  AND (own_link.correction_id IS NULL OR (own_link.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(own_link.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT (eff_view.apart_links[coalesce(own_link.id, '')] IS NOT NULL
      OR (own_link.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(own_link.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [own_link.document_id, startNode(own_link).id, endNode(own_link).id, startNode(own_link).document_id, endNode(own_link).document_id] WHERE eff_id IS NOT NULL])))))) AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: own_link.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(own_link.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < own_link.cite_end[c]
              AND own_link.cite_start[c] < email_source.section_ends[i]))
    } })
    AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: l.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(l.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < l.cite_end[c]
              AND l.cite_start[c] < email_source.section_ends[i]))
    } AND NOT EXISTS {
      MATCH (email_source:KbSearchSource {id: near.source_id})
      WHERE any(i IN range(0, size(coalesce(email_source.section_roles, [])) - 1)
        WHERE email_source.section_roles[i] <> 'own'
          AND any(c IN range(0, size(coalesce(near.cite_start, [])) - 1)
            WHERE email_source.section_starts[i] < near.cite_end[c]
              AND near.cite_start[c] < email_source.section_ends[i]))
    }
  RETURN holder, CASE WHEN near:KbDocument
                      THEN {link: l.id, item: null, kind: l.kind, source_id: l.source_id,
                             cite_start: l.cite_start, cite_end: l.cite_end}
                      ELSE {link: l.id, item: near.id, kind: l.kind,
                             source_id: near.source_id, cite_start: near.cite_start,
                             cite_end: near.cite_end} END AS via
}
  WITH collect({holder: holder, via: via}) AS held,
       count(DISTINCT holder) AS having,
       apoc.map.fromPairs(collect([holder.id, true])) AS documents

  RETURN having
}
    RETURN apoc.map.fromPairs(collect([thing.id, having])) AS having_of
  }
  RETURN [each IN incidence WHERE having_of[each.thing.id] > 0 |
          {document: each.document, thing: each.thing, via: each.via,
            having: having_of[each.thing.id]}] AS held
}
  UNWIND held AS each
  WITH each.document.id AS document_id,
       collect({thing: each.thing, having: each.having, via: each.via}) AS things
  RETURN apoc.map.fromPairs(collect([document_id, things])) AS things_of
}
CALL (documents_of, things_of) {
  UNWIND keys(documents_of) AS party_id
  UNWIND documents_of[party_id] AS document_id
  UNWIND coalesce(things_of[document_id], []) AS held
  WITH party_id, held.thing AS thing, held.having AS having, head(collect(held.via)) AS via
  WITH party_id, collect({thing: thing, having: having, via: via}) AS things
  RETURN apoc.map.fromPairs(collect([party_id, things])) AS party_things
}
CALL (groups, things_of) {
  CALL (groups, things_of) {
    UNWIND range(0, size(groups) - 1) AS group_id
    WITH group_id, groups[group_id] AS named, things_of
    CALL (named, things_of) {
      UNWIND named AS one
      WITH one, things_of WHERE NOT one.identity
      UNWIND coalesce(things_of[one.party.document_id], []) AS held
      WITH held.thing.id AS thing_id, collect(one) AS holding
      RETURN apoc.map.fromPairs(collect([thing_id, holding])) AS holding_of
    }
    WITH group_id, named, holding_of ORDER BY group_id
    RETURN collect({named: named, holding_of: holding_of}) AS indexed_groups
  }
  CALL (indexed_groups) {
    UNWIND range(0, size(indexed_groups) - 1) AS group_id
    UNWIND indexed_groups[group_id].named AS each
    WITH each, collect(group_id) AS groups
    RETURN collect({party: each.party, identity: each.identity,
                    class: each.class, groups: groups}) AS parties
  }
  RETURN indexed_groups, parties
}
CALL (eff_separations, indexed_groups, parties, things_of, party_things) {
UNWIND parties AS a
CALL (a, eff_separations, indexed_groups, things_of, party_things) {
CALL (a, indexed_groups, things_of) {
  UNWIND a.groups AS group_id
  WITH a, indexed_groups[group_id] AS name_group, things_of
  CALL (a, name_group, things_of) {
    WITH a, name_group WHERE a.identity
    UNWIND name_group.named AS b
    RETURN b
    UNION ALL
    WITH a, name_group WHERE NOT a.identity
    UNWIND [each IN name_group.named WHERE each.identity] AS b
    RETURN b
    UNION ALL
    WITH a, name_group, things_of WHERE NOT a.identity
    UNWIND coalesce(things_of[a.party.document_id], []) AS held
    WITH a, name_group, held.thing.id AS thing_id WHERE thing_id <> a.party.id
    UNWIND coalesce(name_group.holding_of[thing_id], []) AS b
    WITH a, b, thing_id
    WHERE a.party.document_id <> b.party.document_id AND thing_id <> b.party.id
    RETURN b
  }
  WITH a, b WHERE a.party.id < b.party.id
  RETURN DISTINCT b
}
WITH eff_separations, party_things, a, b
WHERE (a.class IS NULL OR b.class IS NULL OR NOT a.class IN $class_names
       OR NOT b.class IN $class_names OR a.class = b.class)
  AND NOT any(eff_separation IN coalesce(eff_separations[a.party.id][b.party.id], [])
  WHERE (eff_separation.targets = [a.party.id, b.party.id] OR eff_separation.targets = [b.party.id, a.party.id])
    AND (eff_separation.scope = ['all'] OR EXISTS {
      MATCH (eff_document:KbDocument)
      WHERE eff_document.current_version_id IN eff_separation.scope
        AND eff_document.id IN [a.party.id, b.party.id, a.party.document_id, b.party.document_id] }))
WITH a.party AS a, b.party AS b, party_things
WITH a, b, [held IN coalesce(party_things[a.id], []) WHERE NOT held.thing.id IN [a.id, b.id]] AS a_things, [held IN coalesce(party_things[b.id], []) WHERE NOT held.thing.id IN [a.id, b.id]] AS b_things
CALL (a_things, b_things) {
  UNWIND a_things AS held
  WITH held WHERE held.having > 0
    AND any(other IN b_things WHERE other.thing.id = held.thing.id)
  WITH held.thing AS thing, held.having AS having, head(collect(held.via)) AS via
  WITH thing, having, via ORDER BY having, thing.id
  WITH collect({weight: 1.0 / having}) AS shared
  RETURN reduce(total = 0.0, item IN shared | total + item.weight) AS score
}
WITH a, b, score,
     [k IN coalesce(a.identifier_keys, []) WHERE k IN coalesce(b.identifier_keys, [])]
       AS same_identifiers,
     CASE WHEN size(coalesce(a.identifier_keys, [])) > 0
               AND size(coalesce(b.identifier_keys, [])) > 0
               AND none(k IN a.identifier_keys WHERE k IN b.identifier_keys)
          THEN ['identifiers differ'] ELSE [] END AS conflicts
WITH {a: a, b: b, score: score, identifiers: same_identifiers,
       conflicts: conflicts} AS pair ORDER BY size(pair.identifiers) DESC, pair.score DESC, pair.a.id, pair.b.id
RETURN collect(pair) AS found
}
RETURN collect(found[0..$limit]) AS partitions, sum(size(found)) AS reached
}
CALL (partitions) {
  UNWIND partitions AS partition
  UNWIND partition AS pair
  WITH pair ORDER BY size(pair.identifiers) DESC, pair.score DESC, pair.a.id, pair.b.id
  RETURN collect(pair)[0..$limit] AS selected
}
CALL (selected, party_things) {
  UNWIND selected AS pair
  WITH pair, party_things, pair.a AS a, pair.b AS b
  WITH pair, [held IN coalesce(party_things[a.id], []) WHERE NOT held.thing.id IN [a.id, b.id]] AS a_things, [held IN coalesce(party_things[b.id], []) WHERE NOT held.thing.id IN [a.id, b.id]] AS b_things
  CALL (a_things, b_things) {
  UNWIND a_things AS held
  WITH held WHERE held.having > 0
    AND any(other IN b_things WHERE other.thing.id = held.thing.id)
  WITH held.thing AS thing, held.having AS having, head(collect(held.via)) AS via
  WITH thing, having, via ORDER BY having, thing.id
  WITH collect({id: thing.id, name: coalesce(thing.name, thing.title, thing.label, thing.id),
                labels: labels(thing), weight: 1.0 / having, documents: having,
                via: via}) AS shared
  RETURN shared, reduce(total = 0.0, item IN shared | total + item.weight) AS score
}
  WITH pair, shared ORDER BY size(pair.identifiers) DESC, pair.score DESC, pair.a.id, pair.b.id
  RETURN collect({a: pair.a, b: pair.b, score: pair.score, shared: shared,
                   identifiers: pair.identifiers, conflicts: pair.conflicts}) AS pairs
}
RETURN pairs, reached, reached > $limit AS capped
