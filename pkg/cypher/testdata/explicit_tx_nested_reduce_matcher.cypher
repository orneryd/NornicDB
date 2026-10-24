
CALL () {
  OPTIONAL MATCH (eff_c:LibCorrection {status: 'active'})
  OPTIONAL MATCH (eff_c)-[:CORRECTS]->(eff_t)
  WITH eff_c, collect(eff_t.id) AS eff_named
  OPTIONAL MATCH (eff_d:LibDocument) WHERE eff_d.current_version_id IN coalesce(eff_c.scope, [])
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
  OPTIONAL MATCH (eff_apart:LibCorrection {status: 'active'})
  WHERE eff_apart.operation = 'different_identity'
  OPTIONAL MATCH (eff_x:LibGraphNode {id: eff_apart.targets[0]})
  OPTIONAL MATCH (eff_y:LibGraphNode {id: eff_apart.targets[1]})
  OPTIONAL MATCH eff_left = (eff_x)(()-[eff_l:LIB_RELATION WHERE eff_l.kind = 'merged_into' AND (((coalesce(eff_l.origin, "") <> "automatic" OR eff_l.stage = "interpretation")
  AND coalesce(eff_l.current, true)) AND coalesce(startNode(eff_l).current, true) AND coalesce(endNode(eff_l).current, true)
  AND (eff_l.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_l).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_l.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_l).id + '|' + endNode(eff_l).id + '|' + coalesce(eff_l.kind, ''), startNode(eff_l).id + '|' + endNode(eff_l).id + '|*', CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_l.kind = 'resolves_to' AND coalesce(eff_l.origin, '') = 'automatic' THEN startNode(eff_l).id + '|' + endNode(eff_l).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_l.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_l.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_l.correction_id IS NULL OR (eff_l.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_l.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_l.document_id, startNode(eff_l).id, endNode(eff_l).id, startNode(eff_l).document_id, endNode(eff_l).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join:LibGraphNode)
  OPTIONAL MATCH eff_right = (eff_y)(()-[eff_r:LIB_RELATION WHERE eff_r.kind = 'merged_into' AND (((coalesce(eff_r.origin, "") <> "automatic" OR eff_r.stage = "interpretation")
  AND coalesce(eff_r.current, true)) AND coalesce(startNode(eff_r).current, true) AND coalesce(endNode(eff_r).current, true)
  AND (eff_r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(eff_r).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(eff_r.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(eff_r).id + '|' + endNode(eff_r).id + '|' + coalesce(eff_r.kind, ''), startNode(eff_r).id + '|' + endNode(eff_r).id + '|*', CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN eff_r.kind = 'resolves_to' AND coalesce(eff_r.origin, '') = 'automatic' THEN startNode(eff_r).id + '|' + endNode(eff_r).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(eff_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(eff_r.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_r.correction_id IS NULL OR (eff_r.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(eff_r.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [eff_r.document_id, startNode(eff_r).id, endNode(eff_r).id, startNode(eff_r).document_id, endNode(eff_r).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))]->())*(eff_join)
  WITH eff_apart, eff_left, eff_right
  WHERE eff_right IS NOT NULL
  WITH eff_apart, eff_left, eff_right,
       nodes(eff_left) + nodes(eff_right) AS eff_nodes,
       relationships(eff_left) + relationships(eff_right) AS eff_links
  WHERE (eff_apart.scope = ['all'] OR EXISTS {
      MATCH (eff_document:LibDocument)
      WHERE eff_document.current_version_id IN eff_apart.scope
        AND eff_document.id IN [eff_n IN eff_nodes | eff_n.id] + [eff_n IN eff_nodes | eff_n.document_id] + [eff_m IN eff_links | eff_m.document_id] + [$document_id] })
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

WITH eff_view,
     apoc.map.fromPairs([m IN $mentions WHERE m.candidate_id IS NOT NULL
                        | [m.candidate_id, true]]) AS candidate_ids,
     apoc.map.fromPairs(reduce(every = [], m IN $mentions
                               | every + [name IN m.names | [name, true]])) AS names,
     reduce(every = [], m IN $mentions | every + m.identifiers) AS identifiers
CALL (identifiers) {
  UNWIND identifiers AS identifier
  WITH split(identifier, '\u001F')[0] + '\u001F' + split(identifier, '\u001F')[1] AS value, collect(identifier) AS keys
  RETURN apoc.map.fromPairs(collect([value, keys])) AS identifier_keys
}
MATCH (i:LibGraphNode:LibIdentity)
WHERE coalesce(i.current, true)
WITH eff_view, candidate_ids, names, identifier_keys, i, CASE WHEN NOT coalesce(i.origin, '') IN ['', 'automatic'] THEN {name_key: trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(coalesce(i.name, ''), NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ' ')), alias_keys: [alias IN coalesce(i.alias_names, []) | trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(alias, NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ' '))], identifier_keys: [at IN [index IN range(0, size(coalesce(i.identifier_values, [])) - 1) WHERE string.regexReplace(i.identifier_values[index], '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', '') <> '' AND CASE WHEN string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_schemes[index], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[^\\p{L}\\p{M}\\p{N}]+', ' ') =~ '.*[\\p{L}\\p{N}].*' THEN replace(trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_schemes[index], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[^\\p{L}\\p{M}\\p{N}]+', ' ')), ' ', '_') ELSE '' END <> '' AND coalesce(i.identifier_categories[index], 'identifier') = 'identifier'] | CASE WHEN string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_schemes[at], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[^\\p{L}\\p{M}\\p{N}]+', ' ') =~ '.*[\\p{L}\\p{N}].*' THEN replace(trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_schemes[at], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[^\\p{L}\\p{M}\\p{N}]+', ' ')), ' ', '_') ELSE '' END + '\u001F' + CASE WHEN string.regexReplace(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_values[at], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ''), '[-./]+', '') =~ '[0-9]+' THEN string.regexReplace(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_values[at], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ''), '[-./]+', '') ELSE string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(i.identifier_values[at], NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', '') END + '\u001F' + trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(coalesce(i.identifier_issuers[at], ''), NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ' ')) + '\u001F' + trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(coalesce(i.identifier_jurisdictions[at], ''), NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ' ')) + '\u001F' + trim(string.regexReplace(apoc.text.join([fold_char IN split(normalize(replace(replace(toLower(normalize(coalesce(i.identifier_periods[at], ''), NFKC)), 'ß', 'ss'), 'ς', 'σ'), NFKC), '') | coalesce($digits[fold_char], fold_char)], ''), '[\\s\\x{0b}\\x{1c}-\\x{1f}\\x{85}\\p{Z}]+', ' '))]} ELSE i END AS k
WHERE candidate_ids[i.id] IS NOT NULL OR names[k.name_key] IS NOT NULL
   OR any(name IN coalesce(k.alias_keys, []) WHERE names[name] IS NOT NULL)
   OR any(theirs IN coalesce(k.identifier_keys, [])
          WHERE any(mine IN coalesce(identifier_keys[split(theirs, '\u001F')[0] + '\u001F' + split(theirs, '\u001F')[1]], [])
                    WHERE (split(mine, '\u001F')[0..2] = split(theirs, '\u001F')[0..2] AND all(id_part IN [2, 3, 4] WHERE split(mine, '\u001F')[id_part] = '' OR split(theirs, '\u001F')[id_part] = '' OR split(mine, '\u001F')[id_part] = split(theirs, '\u001F')[id_part]))))
OPTIONAL MATCH (i)(()-[merged:LIB_RELATION
    WHERE merged.kind = 'merged_into' AND (((coalesce(merged.origin, "") <> "automatic" OR merged.stage = "interpretation")
  AND coalesce(merged.current, true)) AND coalesce(startNode(merged).current, true) AND coalesce(endNode(merged).current, true)
  AND (merged.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(merged).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(merged.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(merged).id + '|' + endNode(merged).id + '|' + coalesce(merged.kind, ''), startNode(merged).id + '|' + endNode(merged).id + '|*', CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN merged.kind = 'resolves_to' AND coalesce(merged.origin, '') = 'automatic' THEN startNode(merged).id + '|' + endNode(merged).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(merged.origin, '') = 'automatic' THEN 'doc|' + coalesce(merged.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (merged.correction_id IS NULL OR (merged.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(merged.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [merged.document_id, startNode(merged).id, endNode(merged).id, startNode(merged).document_id, endNode(merged).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))]->())*
    (reached:LibGraphNode)
OPTIONAL MATCH (reached)-[next:LIB_RELATION]->()
WHERE next.kind = 'merged_into' AND (((coalesce(next.origin, "") <> "automatic" OR next.stage = "interpretation")
  AND coalesce(next.current, true)) AND coalesce(startNode(next).current, true) AND coalesce(endNode(next).current, true)
  AND (next.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(next).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(next.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(next).id + '|' + endNode(next).id + '|' + coalesce(next.kind, ''), startNode(next).id + '|' + endNode(next).id + '|*', CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN next.kind = 'resolves_to' AND coalesce(next.origin, '') = 'automatic' THEN startNode(next).id + '|' + endNode(next).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(next.origin, '') = 'automatic' THEN 'doc|' + coalesce(next.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (next.correction_id IS NULL OR (next.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(next.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [next.document_id, startNode(next).id, endNode(next).id, startNode(next).document_id, endNode(next).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
WITH eff_view, k, i, reached, count(next) = 0 AS terminal
OPTIONAL MATCH (reached)-[step:LIB_RELATION]->()
WHERE step.kind = 'merged_into' AND (((coalesce(step.origin, "") <> "automatic" OR step.stage = "interpretation")
  AND coalesce(step.current, true)) AND coalesce(startNode(step).current, true) AND coalesce(endNode(step).current, true)
  AND (step.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(step).id] IS NOT NULL
   OR eff_view.hidden['doc|' + coalesce(step.document_id, '')] IS NOT NULL)
  AND any(eff_key IN [startNode(step).id + '|' + endNode(step).id + '|' + coalesce(step.kind, ''), startNode(step).id + '|' + endNode(step).id + '|*', CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|*|resolves_to|A' ELSE '' END, CASE WHEN step.kind = 'resolves_to' AND coalesce(step.origin, '') = 'automatic' THEN startNode(step).id + '|' + endNode(step).id + '|resolves_to|A' ELSE '' END, CASE WHEN coalesce(step.origin, '') = 'automatic' THEN 'doc|' + coalesce(step.document_id, '') ELSE '' END] WHERE
      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])
          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (step.correction_id IS NULL OR (step.correction_id IS NOT NULL
  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(step.correction_id, '')], [])
          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [$document_id] WHERE eff_id IS NOT NULL]))))))
  AND (eff_view.apart_links[coalesce(step.id, '')] IS NOT NULL
      OR (step.correction_id IS NOT NULL
          AND any(eff_rival IN coalesce(eff_view.contested[coalesce(step.correction_id, '')], [])
                  WHERE (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [step.document_id, startNode(step).id, endNode(step).id, startNode(step).document_id, endNode(step).document_id] + [$document_id] WHERE eff_id IS NOT NULL])))))
WITH eff_view, k, i, reached, terminal, count(step) > 0 AS step_disputed
WITH eff_view, k, i, collect(DISTINCT CASE WHEN terminal THEN reached.id END) AS terminals,
     max(CASE WHEN step_disputed THEN 1 ELSE 0 END) = 1 AS chain_disputed
OPTIONAL MATCH chain =
  (i)-[:LIB_RELATION*1.. {kind: 'merged_into'}]-(:LibGraphNode)
WITH i, k, terminals, chain_disputed,
     reduce(links = [], path IN collect(chain) | links + relationships(path)) AS links
WITH collect({id: i.id, revision: coalesce(i.revision, 0),
    class: CASE WHEN i.kind IN $classes THEN i.kind END, survivor: CASE WHEN size(terminals) = 1 AND NOT chain_disputed THEN terminals[0] ELSE i.id END,
    identifiers: coalesce(k.identifier_keys, []),
    typed: [x IN range(0, size(coalesce(k.alias_keys, [])) - 1) WHERE i.alias_types[x] IN $naming AND coalesce(i.alias_scopes[x], '') IN ['', 'all'] + $collections
            | k.alias_keys[x]],
    named: [k.name_key] + [x IN range(0, size(coalesce(k.alias_keys, [])) - 1)
            WHERE i.alias_types[x] IN $naming + ['other'] AND coalesce(i.alias_scopes[x], '') IN ['', 'all'] + $collections
            | k.alias_keys[x]],
    links: [link IN links | {id: link.id, revision: coalesce(link.revision, 0)}],
    members: [member IN reduce(ends = [], link IN links
                               | ends + [startNode(link), endNode(link)])
              | {id: member.id, revision: coalesce(member.revision, 0)}]}) AS known
WITH known, reduce(acc = {created: [], results: [], deciding: []}, m IN $mentions |
  head([pool IN [known + acc.created] |
  head([hits IN [[hit IN [i IN pool | {identity: i, basis: CASE
        WHEN any(mine IN m.identifiers
                 WHERE any(theirs IN i.identifiers WHERE (split(mine, '\u001F')[0..2] = split(theirs, '\u001F')[0..2] AND all(id_part IN [2, 3, 4] WHERE split(mine, '\u001F')[id_part] = '' OR split(theirs, '\u001F')[id_part] = '' OR split(mine, '\u001F')[id_part] = split(theirs, '\u001F')[id_part]))))
          THEN 'identifier'
        WHEN any(name IN m.names WHERE name IN i.typed) THEN 'alias'
        WHEN m.candidate_id = i.id AND any(name IN m.names WHERE name IN i.named)
          THEN 'candidate' END}]
       WHERE hit.basis IS NOT NULL]] |
  head([bearers IN [[b IN pool WHERE any(name IN m.names WHERE name IN b.named)]] |
  head([kept IN [[hit IN hits WHERE (m.class IS NULL OR hit.identity.class IS NULL OR m.class = hit.identity.class) AND (hit.basis = 'identifier'
       OR all(b IN bearers WHERE b.survivor = hit.identity.survivor))]] |
  head([found IN [[s IN reduce(seen = [], hit IN kept |
         CASE WHEN hit.identity.survivor IN seen THEN seen
              ELSE seen + hit.identity.survivor END)
       | [s, head([basis IN $bases WHERE any(hit IN kept
                   WHERE hit.identity.survivor = s AND hit.basis = basis)])]]] |
  head([borne IN [CASE WHEN size(found) = 0 AND m.strong IS NOT NULL
       THEN [b IN pool WHERE any(theirs IN b.identifiers
                                 WHERE (split(m.strong, '\u001F')[0..2] = split(theirs, '\u001F')[0..2] AND all(id_part IN [2, 3, 4] WHERE split(m.strong, '\u001F')[id_part] = '' OR split(theirs, '\u001F')[id_part] = '' OR split(m.strong, '\u001F')[id_part] = split(theirs, '\u001F')[id_part])))]
       ELSE [] END] |
  head([creates IN [size(found) = 0 AND m.strong IS NOT NULL AND size(borne) = 0] |
  {created: acc.created + CASE WHEN creates
         THEN [{id: m.created.id, revision: coalesce(m.created.revision, 0),
      class: CASE WHEN m.created.kind IN $classes THEN m.created.kind END, survivor: m.created.id,
      identifiers: coalesce(m.created.identifier_keys, []),
      typed: [x IN range(0, size(coalesce(m.created.alias_keys, [])) - 1) WHERE m.created.alias_types[x] IN $naming AND coalesce(m.created.alias_scopes[x], '') IN ['', 'all'] + $collections
              | m.created.alias_keys[x]],
      named: [m.created.name_key] + [x IN range(0, size(coalesce(m.created.alias_keys, [])) - 1)
              WHERE m.created.alias_types[x] IN $naming + ['other'] AND coalesce(m.created.alias_scopes[x], '') IN ['', 'all'] + $collections
              | m.created.alias_keys[x]]}] ELSE [] END,
    results: acc.results + [{id: m.id, creates: creates,
         found: CASE WHEN creates THEN [[m.created.id, 'identifier']] ELSE found END}],
    deciding: acc.deciding + [hit IN hits | hit.identity.id]
         + CASE WHEN any(hit IN hits WHERE (m.class IS NULL OR hit.identity.class IS NULL OR m.class = hit.identity.class) AND hit.basis <> 'identifier')
                THEN [b IN bearers | b.id] ELSE [] END
         + [b IN borne | b.id]}])])])])])])])) AS matched
WITH matched, [item IN known WHERE item.id IN matched.deciding] AS deciders
RETURN matched.results AS results,
       [at IN range(0, size($mentions) - 1) WHERE matched.results[at].creates
        | $mentions[at].created] AS created,
       [item IN deciders | {id: item.id, revision: item.revision}] AS decisive,
       reduce(links = [], item IN deciders | links + item.links) AS links,
       reduce(members = [], item IN deciders | members + item.members) AS identities
