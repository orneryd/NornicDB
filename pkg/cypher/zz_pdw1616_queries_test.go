package cypher

// Byte-for-byte query census entries from tail DIAG2 r3.
var pdw1616Stock = pdw1616Queries{
	"evaluate": {wire: "b46ba6e9fb303d97", text: "WITH $links AS effl_link_ids, $node_ids AS effl_node_ids, $documents AS effl_documents,\n" +
		"     $correction_ids AS effl_correction_ids, $selected AS effl_selected,\n" +
		"     $gone_ids AS effl_gone_ids\n" +
		"CALL (effl_link_ids, effl_node_ids, effl_documents, effl_correction_ids, effl_selected,\n" +
		"      effl_gone_ids) {\n" +
		"  CALL (effl_correction_ids, effl_selected) {\n" +
		"    CALL (effl_selected) {\n" +
		"      UNWIND effl_selected AS effl_document_id\n" +
		"      MATCH (:PDVersion {document_id: effl_document_id})<-[:SCOPED_TO]-(effl_scoped:PDCorrection)\n" +
		"      RETURN collect(DISTINCT effl_scoped.id) AS effl_scoped_ids\n" +
		"    }\n" +
		"    UNWIND effl_correction_ids + effl_scoped_ids AS effl_correction_id\n" +
		"    MATCH (effl_named:PDCorrection {id: effl_correction_id})\n" +
		"    OPTIONAL MATCH (effl_named)-[:CORRECTS]->()<-[:CORRECTS]-(effl_sharer:PDCorrection)\n" +
		"    WITH collect(effl_named.id) + collect(effl_sharer.id) AS effl_found\n" +
		"    RETURN apoc.coll.toSet(effl_found) AS effl_corrections\n" +
		"  }\n" +
		"  CALL (effl_link_ids, effl_node_ids, effl_documents, effl_corrections) {\n" +
		"    CALL (effl_link_ids) {\n" +
		"      CALL (effl_link_ids) {\n" +
		"UNWIND effl_link_ids AS rr_item\n" +
		"WITH rr_item.from AS rr_node,\n" +
		"     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted\n" +
		"MATCH (rr_start:PDGraphNode {id: rr_node})\n" +
		"MATCH (rr_start)-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]->(rr_end)\n" +
		"WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed\n" +
		"WHERE effl_seed IS NOT NULL\n" +
		"RETURN effl_l, effl_seed\n" +
		"UNION\n" +
		"UNWIND effl_link_ids AS rr_item\n" +
		"WITH rr_item.from AS rr_node,\n" +
		"     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted\n" +
		"MATCH (rr_start:PDOcrReport {id: rr_node})\n" +
		"MATCH (rr_start)-[effl_l:ABOUT]->(rr_end)\n" +
		"WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed\n" +
		"WHERE effl_seed IS NOT NULL\n" +
		"RETURN effl_l, effl_seed\n" +
		"UNION\n" +
		"UNWIND effl_link_ids AS rr_item\n" +
		"WITH rr_item.from AS rr_node,\n" +
		"     apoc.map.fromPairs(collect([rr_item.link, rr_item])) AS rr_wanted\n" +
		"MATCH (rr_start:PDDefinition {id: rr_node})\n" +
		"MATCH (rr_start)-[effl_l:PD_RELATION]->(rr_end)\n" +
		"WITH effl_l, rr_wanted[coalesce(effl_l.id, type(effl_l) + '>' + endNode(effl_l).id)] AS effl_seed\n" +
		"WHERE effl_seed IS NOT NULL\n" +
		"RETURN effl_l, effl_seed\n" +
		"      }\n" +
		"      RETURN collect(DISTINCT effl_l) AS effl_named_links\n" +
		"    }\n" +
		"    CALL (effl_node_ids, effl_documents) {\n" +
		"      CALL (effl_documents) {\n" +
		"        UNWIND effl_documents AS effl_document_id\n" +
		"        MATCH (effl_owned:PDGraphNode {document_id: effl_document_id})\n" +
		"        RETURN collect(effl_owned.id) AS effl_owned_ids\n" +
		"      }\n" +
		"      UNWIND apoc.coll.toSet(effl_node_ids + effl_documents + effl_owned_ids) AS effl_node_id\n" +
		"      MATCH (:PDGraphNode {id: effl_node_id})-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]-()\n" +
		"      RETURN collect(DISTINCT effl_l) AS effl_incident_links\n" +
		"    }\n" +
		"    CALL (effl_corrections) {\n" +
		"      UNWIND effl_corrections AS effl_correction_id\n" +
		"      MATCH (effl_k:PDCorrection {id: effl_correction_id})-[:CORRECTS]->(effl_t)\n" +
		"      OPTIONAL MATCH (effl_t)-[effl_l:PD_RELATION|ATTACHMENT_OF|ABOUT]-(effl_other)\n" +
		"      WHERE effl_other.id IN effl_k.targets\n" +
		"         OR (effl_k.operation = 'replace_interpretation' AND effl_l.document_id = effl_t.id)\n" +
		"      WITH effl_k, effl_t, collect(DISTINCT effl_l) AS effl_between\n" +
		"      OPTIONAL MATCH (:PDGraphNode {document_id: CASE WHEN effl_k.operation = 'replace_interpretation' THEN effl_t.id END})\n" +
		"                     -[effl_d:PD_RELATION]-()\n" +
		"      WHERE effl_d.document_id = effl_t.id\n" +
		"      WITH effl_between, collect(DISTINCT effl_d) AS effl_direct\n" +
		"      UNWIND effl_between + effl_direct AS effl_l\n" +
		"      RETURN collect(DISTINCT effl_l) AS effl_concerned_links\n" +
		"    }\n" +
		"    RETURN effl_named_links + effl_incident_links + effl_concerned_links AS effl_found_links\n" +
		"  }\n" +
		"  CALL (effl_found_links) {\n" +
		"    UNWIND effl_found_links AS effl_found\n" +
		"    WITH DISTINCT effl_found AS effl_r\n" +
		"    WITH collect(effl_r) AS effl_links\n" +
		"  CALL (effl_links) {\n" +
		"    CALL (effl_links) {\n" +
		"      UNWIND effl_links AS eff_l\n" +
		"      UNWIND [startNode(eff_l), endNode(eff_l)] AS eff_t\n" +
		"      WITH DISTINCT eff_t\n" +
		"      RETURN 'n|' + elementId(eff_t) AS eff_at, eff_t\n" +
		"      UNION\n" +
		"      UNWIND effl_links AS eff_l\n" +
		"      WITH DISTINCT coalesce(eff_l.document_id, '') AS eff_document_id\n" +
		"      MATCH (eff_t:PDGraphNode {id: eff_document_id})\n" +
		"      RETURN 'd|' + eff_document_id AS eff_at, eff_t\n" +
		"    }\n" +
		"    MATCH (eff_t)<-[:CORRECTS]-(eff_c:PDCorrection)\n" +
		"    WHERE eff_c.status = 'active'\n" +
		"    WITH collect([eff_at, eff_c.id]) AS eff_pairs, collect(DISTINCT eff_c) AS eff_cs\n" +
		"    CALL (eff_pairs) {\n" +
		"      UNWIND eff_pairs AS eff_p\n" +
		"      WITH eff_p[0] AS eff_k, collect(eff_p[1]) AS eff_ids\n" +
		"      RETURN apoc.map.fromPairs(collect([eff_k, eff_ids])) AS eff_targets\n" +
		"    }\n" +
		"    CALL (eff_cs) {\n" +
		"      UNWIND eff_cs AS eff_c\n" +
		"    CALL (eff_c) {\n" +
		"      OPTIONAL MATCH (eff_c)-[:CORRECTS]->(eff_named)\n" +
		"      RETURN apoc.coll.sort(collect(DISTINCT eff_named.id)) AS eff_named\n" +
		"    }\n" +
		"    CALL (eff_c) {\n" +
		"      UNWIND CASE WHEN eff_c IS NULL THEN [] ELSE apoc.coll.toSet(coalesce(eff_c.scope, [])) END\n" +
		"        AS eff_version\n" +
		"      WITH eff_version WHERE eff_version IS NOT NULL\n" +
		"      MATCH (eff_d:PDDocument {current_version_id: eff_version})\n" +
		"      WITH eff_d ORDER BY eff_d.id\n" +
		"      RETURN collect(eff_d.id) AS eff_docs\n" +
		"    }\n" +
		"      RETURN apoc.map.fromPairs(collect([eff_c.id, eff_c {.*, named: eff_named, docs: eff_docs,\n" +
		"                                    all: eff_c.scope = ['all']}])) AS eff_entries\n" +
		"    }\n" +
		"    RETURN {targets: eff_targets, entries: eff_entries} AS eff_incidence\n" +
		"  }\n" +
		"    UNWIND effl_links AS effl_r\n" +
		"    CALL (effl_r, eff_incidence) {\n" +
		"  WITH effl_r, [startNode(effl_r).id + '|' + endNode(effl_r).id + '|' + coalesce(effl_r.kind, ''), startNode(effl_r).id + '|' + endNode(effl_r).id + '|*', CASE WHEN coalesce(effl_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(effl_r.document_id, '') ELSE '' END] AS eff_keys,\n" +
		"       [eff_id IN apoc.coll.sort(apoc.coll.toSet(\n" +
		"          coalesce(eff_incidence.targets['n|' + elementId(startNode(effl_r))], [])\n" +
		"          + coalesce(eff_incidence.targets['n|' + elementId(endNode(effl_r))], [])\n" +
		"          + coalesce(eff_incidence.targets['d|' + coalesce(effl_r.document_id, '')], [])))\n" +
		"        | eff_incidence.entries[eff_id]] AS eff_gathered\n" +
		"  WITH eff_keys, effl_r.correction_id AS eff_own_id, eff_gathered\n" +
		"  WITH eff_own_id,\n" +
		"       [eff_one IN eff_gathered WHERE\n" +
		"          CASE WHEN eff_own_id IS NULL\n" +
		"               THEN any(eff_h IN (CASE\n" +
		"    WHEN eff_one.operation = 'exclude_link'\n" +
		"      THEN [eff_m IN eff_one.targets[1..] WHERE eff_m IN eff_one.named | {key: eff_m + '|' + eff_one.targets[0] + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]\n" +
		"         + [eff_m IN eff_one.targets[1..] WHERE eff_one.targets[0] IN eff_one.named | {key: eff_one.targets[0] + '|' + eff_m + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]\n" +
		"    WHEN eff_one.operation = 'replace_interpretation'\n" +
		"      THEN [eff_m IN eff_one.targets[0..1] WHERE eff_m IN eff_one.named | {key: 'doc|' + eff_m, all: eff_one.all, docs: eff_one.docs}]\n" +
		"    ELSE [] END) WHERE eff_h.key IN eff_keys)\n" +
		"               ELSE eff_one.id = eff_own_id\n" +
		"                 OR any(eff_own IN eff_gathered WHERE eff_own.id = eff_own_id\n" +
		"                        AND eff_one.id <> eff_own.id\n" +
		"                        AND any(eff_shared IN eff_one.named WHERE eff_shared IN eff_own.targets)\n" +
		"                        AND ((eff_own.operation = 'include_link' AND (eff_one.operation = 'exclude_link' AND (eff_one.link_kind IS NULL OR eff_one.link_kind = eff_own.link_kind)) AND (eff_one.targets = eff_own.targets OR eff_one.targets = reverse(eff_own.targets))) OR (eff_one.operation = 'include_link' AND (eff_own.operation = 'exclude_link' AND (eff_own.link_kind IS NULL OR eff_own.link_kind = eff_one.link_kind)) AND (eff_own.targets = eff_one.targets OR eff_own.targets = reverse(eff_one.targets))))) END] AS eff_corrections\n" +
		"  WITH eff_corrections,\n" +
		"       reduce(eff_all = [], eff_one IN eff_corrections | eff_all + (CASE\n" +
		"    WHEN eff_one.operation = 'exclude_link'\n" +
		"      THEN [eff_m IN eff_one.targets[1..] WHERE eff_m IN eff_one.named | {key: eff_m + '|' + eff_one.targets[0] + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]\n" +
		"         + [eff_m IN eff_one.targets[1..] WHERE eff_one.targets[0] IN eff_one.named | {key: eff_one.targets[0] + '|' + eff_m + '|' + coalesce(eff_one.link_kind, '*'), all: eff_one.all, docs: eff_one.docs}]\n" +
		"    WHEN eff_one.operation = 'replace_interpretation'\n" +
		"      THEN [eff_m IN eff_one.targets[0..1] WHERE eff_m IN eff_one.named | {key: 'doc|' + eff_m, all: eff_one.all, docs: eff_one.docs}]\n" +
		"    ELSE [] END)) AS eff_hides,\n" +
		"       [eff_rival IN eff_corrections WHERE eff_own_id IS NOT NULL AND eff_rival.id <> eff_own_id\n" +
		"        | {key: eff_own_id, id: eff_rival.id, all: eff_rival.all, docs: eff_rival.docs}]\n" +
		"         AS eff_rivals\n" +
		"  CALL (eff_hides) {\n" +
		"    UNWIND eff_hides AS eff_h\n" +
		"    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs, id: eff_h.id}) AS eff_s\n" +
		"    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_hidden\n" +
		"  }\n" +
		"  CALL (eff_rivals) {\n" +
		"    UNWIND eff_rivals AS eff_h\n" +
		"    WITH eff_h.key AS eff_k, collect({all: eff_h.all, docs: eff_h.docs, id: eff_h.id}) AS eff_s\n" +
		"    RETURN apoc.map.fromPairs(collect([eff_k, eff_s])) AS eff_contested\n" +
		"  }\n" +
		"  RETURN {hidden: eff_hidden, contested: eff_contested,\n" +
		"          hiding: apoc.map.fromPairs([eff_h IN eff_hides | [split(eff_h.key, '|')[0], true]]),\n" +
		"          scopes: apoc.map.fromPairs([eff_s IN eff_corrections\n" +
		"                                      | [eff_s.id, [{all: eff_s.all, docs: eff_s.docs}]]])}\n" +
		"           AS eff_view,\n" +
		"         [eff_one IN eff_corrections\n" +
		"          | [eff_one.id, eff_one.revision, eff_one.operation, eff_one.link_kind,\n" +
		"             eff_one.targets, eff_one.named, eff_one.all, eff_one.docs]] AS eff_incident\n" +
		"}\n" +
		"WITH effl_r, eff_view,\n" +
		"     apoc.convert.toJson([type(effl_r), effl_r.id, startNode(effl_r).id, endNode(effl_r).id, effl_r.kind,\n" +
		"                          effl_r.origin, effl_r.stage, effl_r.current, effl_r.correction_id,\n" +
		"                          effl_r.document_id, startNode(effl_r).current,\n" +
		"                          startNode(effl_r).document_id, endNode(effl_r).current,\n" +
		"                          endNode(effl_r).document_id, eff_incident]) AS eff_inputs\n" +
		"    WITH effl_r, eff_view, eff_inputs\n" +
		"    WHERE coalesce(effl_r.eff_applied, '') <> apoc.convert.toJson([eff_inputs, effl_r.eff_base, effl_r.eff_disputed, effl_r.eff_usable, effl_r.eff_reason])\n" +
		"    WITH effl_r, eff_view, eff_inputs, (((coalesce(effl_r.origin, \"\") <> \"automatic\" OR effl_r.stage = \"interpretation\")\n" +
		"  AND coalesce(effl_r.current, true)) AND coalesce(startNode(effl_r).current, true) AND coalesce(endNode(effl_r).current, true)\n" +
		"  AND (effl_r.correction_id IS NOT NULL OR NOT ((eff_view.hiding[startNode(effl_r).id] IS NOT NULL\n" +
		"   OR eff_view.hidden['doc|' + coalesce(effl_r.document_id, '')] IS NOT NULL)\n" +
		"  AND any(eff_key IN [startNode(effl_r).id + '|' + endNode(effl_r).id + '|' + coalesce(effl_r.kind, ''), startNode(effl_r).id + '|' + endNode(effl_r).id + '|*', CASE WHEN coalesce(effl_r.origin, '') = 'automatic' THEN 'doc|' + coalesce(effl_r.document_id, '') ELSE '' END] WHERE\n" +
		"      any(eff_hide IN coalesce(eff_view.hidden[eff_key], [])\n" +
		"          WHERE (eff_hide.all OR any(eff_doc IN eff_hide.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL]))))))\n" +
		"  AND (effl_r.correction_id IS NULL OR (effl_r.correction_id IS NOT NULL\n" +
		"  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(effl_r.correction_id, '')], [])\n" +
		"          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])))))) AS effl_base, [eff_rival IN coalesce(eff_view.contested[coalesce(effl_r.correction_id, '')], [])\n" +
		"         WHERE (effl_r.correction_id IS NOT NULL\n" +
		"  AND any(eff_source IN coalesce(eff_view.scopes[coalesce(effl_r.correction_id, '')], [])\n" +
		"          WHERE (eff_source.all OR any(eff_doc IN eff_source.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])))) AND (eff_rival.all OR any(eff_doc IN eff_rival.docs WHERE eff_doc IN [eff_id IN [effl_r.document_id, startNode(effl_r).id, endNode(effl_r).id, startNode(effl_r).document_id, endNode(effl_r).document_id] WHERE eff_id IS NOT NULL])) | eff_rival.id] AS effl_rivals\n" +
		"    WITH effl_r, eff_view, eff_inputs, effl_base, effl_rivals, size(effl_rivals) > 0 AS effl_disputed\n" +
		"    WITH effl_r, eff_view, eff_inputs, effl_base, effl_rivals, effl_disputed, CASE WHEN NOT coalesce(effl_r.current, true) THEN 'retired'\n" +
		"            WHEN NOT ((coalesce(effl_r.origin, \"\") <> \"automatic\" OR effl_r.stage = \"interpretation\")\n" +
		"  AND coalesce(effl_r.current, true)) THEN 'earlier'\n" +
		"            WHEN NOT (coalesce(startNode(effl_r).current, true) AND coalesce(endNode(effl_r).current, true)) THEN 'retired_end'\n" +
		"            WHEN NOT effl_base AND effl_r.correction_id IS NOT NULL\n" +
		"              THEN 'unapplied:' + effl_r.correction_id\n" +
		"            WHEN NOT effl_base THEN 'hidden'\n" +
		"            WHEN effl_disputed THEN 'contested:' + effl_r.correction_id END AS effl_reason,\n" +
		"         startNode(effl_r) AS effl_s, endNode(effl_r) AS effl_e\n" +
		"    RETURN collect({type: type(effl_r), from: effl_s.id, start: CASE WHEN effl_s:PDGraphNode THEN 'PDGraphNode' WHEN effl_s:PDOcrReport THEN 'PDOcrReport' WHEN effl_s:PDDefinition THEN 'PDDefinition' END,\n" +
		"                     link: coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id), key: apoc.convert.toJson([startNode(effl_r).id, coalesce(effl_r.id, type(effl_r) + '>' + endNode(effl_r).id)]),\n" +
		"                     base: effl_base, disputed: effl_disputed, reason: effl_reason,\n" +
		"                     inputs: eff_inputs, was: effl_r.eff_applied, correction: effl_r.correction_id,\n" +
		"                     rivals: effl_rivals,\n" +
		"                     family: type(effl_r) = 'PD_RELATION'\n" +
		"                             AND coalesce(effl_r.kind, 'same_document') = 'same_document',\n" +
		"                     ends: [effl_s.id, effl_e.id], to: effl_e.id,\n" +
		"                     end: CASE WHEN effl_e:PDDefinition THEN 'definition' ELSE 'node' END,\n" +
		"                     origin: coalesce(effl_r.origin, ''),\n" +
		"                     documents: [effl_x IN [effl_r.document_id, effl_s.document_id,\n" +
		"                                            effl_e.document_id,\n" +
		"                                            CASE WHEN effl_s:PDDocument THEN effl_s.id END,\n" +
		"                                            CASE WHEN effl_e:PDDocument THEN effl_e.id END]\n" +
		"                                 WHERE effl_x IS NOT NULL]}) AS effl_rows\n" +
		"  }\n" +
		"  CALL (effl_corrections) {\n" +
		"    UNWIND effl_corrections AS effl_correction_id\n" +
		"    MATCH (effl_w:PDCorrection {id: effl_correction_id})\n" +
		"    WHERE effl_w.status <> 'active'\n" +
		"    RETURN collect(effl_w.id) AS effl_withdrawn\n" +
		"  }\n" +
		"  CALL (effl_gone_ids) {\n" +
		"    UNWIND effl_gone_ids AS effl_gone_id\n" +
		"    MATCH (effl_g:PDGraphNode {id: effl_gone_id})\n" +
		"    RETURN collect(coalesce(effl_g.document_id, CASE WHEN effl_g:PDDocument THEN effl_g.id END))\n" +
		"      AS effl_gone_documents\n" +
		"  }\n" +
		"  RETURN {rows: effl_rows, withdrawn: effl_withdrawn, gone_documents: effl_gone_documents,\n" +
		"          gone: effl_gone_ids,\n" +
		"          documents: apoc.coll.toSet(reduce(effl_all = effl_gone_documents, effl_x IN effl_rows\n" +
		"                                            | effl_all + effl_x.documents)),\n" +
		"          family: apoc.coll.toSet(reduce(effl_all = effl_gone_ids,\n" +
		"                                         effl_x IN [effl_y IN effl_rows WHERE effl_y.family]\n" +
		"                                         | effl_all + effl_x.ends))} AS effl_out\n" +
		"}\n" +
		"RETURN effl_out.rows AS rows, effl_out.withdrawn AS withdrawn,\n" +
		"       effl_out.documents AS documents, effl_out.family AS family,\n" +
		"       effl_out.gone_documents AS gone_documents, effl_out.gone AS gone\n"},
	"inputs": {wire: "c4951557f9bbe760", text: "UNWIND $ids AS hd_id\n" +
		"WITH DISTINCT hd_id\n" +
		"MATCH (document:PDDocument {id: hd_id})\n" +
		"  CALL (document) {\n" +
		"  MATCH (document)-[l:PD_RELATION]-(shared:PDGraphNode)\n" +
		"  WHERE NOT shared:PDDefinition AND coalesce(shared.document_id, '') <> document.id\n" +
		"    AND NOT shared:PDSummary AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false)\n" +
		"  WITH collect({l: l, shared: shared}) AS found\n" +
		"  WITH found, [each IN found | each.l.source_id] AS source_ids\n" +
		"  RETURN source_ids,\n" +
		"         [each IN found | ['link', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]] AS read\n" +
		"  UNION ALL\n" +
		"  MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[l2:PD_RELATION]-(shared:PDGraphNode)\n" +
		"  WHERE own.document_id = document.id AND NOT shared:PDDefinition\n" +
		"    AND NOT own:PDSummary AND NOT shared:PDSummary\n" +
		"    AND coalesce(shared.document_id, '') <> document.id\n" +
		"    AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false) AND NOT coalesce(l2.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l2.eff_usable, false)\n" +
		"  WITH collect({l: l, own: own, l2: l2, shared: shared}) AS found\n" +
		"  WITH found, [each IN found | each.l.source_id] + [each IN found | each.own.source_id]\n" +
		"              + [each IN found | each.l2.source_id] AS source_ids\n" +
		"  RETURN source_ids,\n" +
		"         [each IN found | ['item', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.own.id, each.own.source_id, each.own.cite_start, each.own.cite_end, each.own.citation_unsupported, each.own.citation_reason, each.l2.id, each.l2.kind, each.l2.eff_usable, each.l2.source_id, each.l2.cite_start, each.l2.cite_end, each.l2.citation_unsupported, each.l2.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]]\n" +
		"           AS read\n" +
		"  UNION ALL\n" +
		"  MATCH (document)-[l:PD_RELATION]-(own:PDGraphNode)-[b:PD_BINDS]->(shared:PDGraphNode:PDIdentity)\n" +
		"  WHERE own.document_id = document.id AND b.status = 'confirmed'\n" +
		"    AND NOT own:PDSummary AND NOT coalesce(l.kind, '') IN ['same_document', 'resolves_to', 'merged_into', 'same_as'] AND coalesce(l.eff_usable, false)\n" +
		"  WITH collect({l: l, own: own, b: b, shared: shared}) AS found\n" +
		"  WITH found, [each IN found | each.l.source_id] + [each IN found | each.own.source_id]\n" +
		"    AS source_ids\n" +
		"  RETURN source_ids,\n" +
		"         [each IN found | ['bound', each.l.id, each.l.kind, each.l.eff_usable, each.l.source_id, each.l.cite_start, each.l.cite_end, each.l.citation_unsupported, each.l.citation_reason, each.own.id, each.own.source_id, each.own.cite_start, each.own.cite_end, each.own.citation_unsupported, each.own.citation_reason, each.b.status,\n" +
		"                          each.b.citation_unsupported, each.b.citation_reason, each.shared.id, each.shared.survivor, 'PDIdentity' IN labels(each.shared)]]\n" +
		"           AS read\n" +
		"}\n" +
		"  WITH document, collect(source_ids) AS hd_cited,\n" +
		"       collect(read) AS hd_reads\n" +
		"  CALL (hd_cited) {\n" +
		"    UNWIND hd_cited AS hd_cited_part\n" +
		"    UNWIND hd_cited_part AS hd_source_id\n" +
		"    WITH DISTINCT hd_source_id WHERE hd_source_id IS NOT NULL\n" +
		"    MATCH (hd_source:PDSearchSource {id: hd_source_id})\n" +
		"    RETURN collect([hd_source.id, hd_source.section_roles, hd_source.section_starts,\n" +
		"                    hd_source.section_ends]) AS hd_sections\n" +
		"  }\n" +
		"RETURN document.id AS id, hd_reads AS read, hd_sections AS sections\n"},
	"search": {wire: "f510803046eb6ffb", text: "\n" +
		"CALL () {\n" +
		"  UNWIND CASE WHEN $qid IS NULL THEN $anchors ELSE [] END AS anchor_id\n" +
		"  OPTIONAL MATCH (given:PDGraphNode {id: anchor_id})\n" +
		"  OPTIONAL MATCH (given_source:PDSearchSource {id: anchor_id})\n" +
		"  OPTIONAL MATCH (given_version:PDVersion {id: given.current_version_id, document_id: given.id})\n" +
		"OPTIONAL MATCH (given_current:PDSearchSource {id: given_version.current_source_id})\n" +
		"\n" +
		"  WITH CASE WHEN given:PDDocument THEN given_current\n" +
		"            ELSE coalesce(given, given_source) END AS anchor_hit\n" +
		"  WHERE anchor_hit IS NOT NULL AND (NOT anchor_hit:PDSummary OR (coalesce(anchor_hit.current, false) AND anchor_hit.search_hidden = false))\n" +
		"  RETURN anchor_hit AS hit, null AS score, true AS anchored, null AS ranking,\n" +
		"         null AS matches\n" +
		"  UNION ALL\n" +
		"  CALL db.rretrieve(apoc.map.fromPairs([option IN [['query', $query],\n" +
		"        ['types', ['PDSearchSource', 'PDSearchable']], ['where', CASE WHEN $qid IS NULL\n" +
		"           THEN '(node:PDSearchSource OR $history OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT coalesce(node.search_hidden, false) AND (NOT node:PDSummary OR node.search_hidden = false) AND node.record_set_id IS NULL'\n" +
		"           END],\n" +
		"        ['n', $page_size], ['qid', $qid], ['failClosed', true], ['minRRFScore', 0],\n" +
		"        ['minSimilarity', $min_similarity], ['rerankMinScore', $rerank_min_score]]\n" +
		"      WHERE option[1] IS NOT NULL]))\n" +
		"  YIELD page\n" +
		"  UNWIND CASE WHEN size(page.results) = 0 THEN [null] ELSE page.results END AS result\n" +
		"  RETURN result.node AS hit, result.score AS score,\n" +
		"         false AS anchored,\n" +
		"         {search_method: page.search_method, fallback_triggered: page.fallback_triggered,\n" +
		"          fallback_reason: page.fallback_reason,\n" +
		"          query_chunks_searched: coalesce(page.query_chunks_searched, 0),\n" +
		"          query_chunks_total: coalesce(page.query_chunks_total, 0),\n" +
		"          qid: page.qid, has_more: page.has_more, total: page.total} AS ranking,\n" +
		"         result.matches AS matches\n" +
		"}\n" +
		"WITH\n" +
		"     hit, max(score) AS score,\n" +
		"     max(CASE WHEN anchored THEN 1 ELSE 0 END) = 1 AS anchored,\n" +
		"     head([outcome IN collect(ranking) WHERE outcome IS NOT NULL]) AS ranking,\n" +
		"     head([found IN collect(matches) WHERE found IS NOT NULL]) AS matches\n" +
		"WITH\n" +
		"     collect({node: hit, source: hit:PDSearchSource,\n" +
		"              document_id: hit.document_id, score: score, anchored: anchored,\n" +
		"              ranking: ranking, matches: matches}) AS candidates\n" +
		"WITH\n" +
		"     [c IN candidates WHERE c.node IS NOT NULL] AS candidates,\n" +
		"     head([c IN candidates WHERE c.ranking IS NOT NULL]).ranking AS ranking\n" +
		"UNWIND CASE WHEN size(candidates) = 0 THEN [null] ELSE candidates END AS candidate\n" +
		"WITH\n" +
		"     candidate, ranking\n" +
		"OPTIONAL MATCH (owner:PDDocument {id: candidate.document_id})\n" +
		"WHERE candidate.source\n" +
		"OPTIONAL MATCH (owner_version:PDVersion {id: owner.current_version_id, document_id: owner.id})\n" +
		"OPTIONAL MATCH (owner_current:PDSearchSource {id: owner_version.current_source_id})\n" +
		"\n" +
		"WITH\n" +
		"     candidate, ranking,\n" +
		"     [{node: candidate.node, matched: true, matches: candidate.matches}] +\n" +
		"     CASE WHEN owner_current IS NULL OR owner_current.id = candidate.node.id THEN []\n" +
		"          ELSE [{node: owner_current, matched: false, matches: null}] END\n" +
		"       AS entries\n" +
		"UNWIND entries AS entry\n" +
		"WITH\n" +
		"     entry.node AS node, ranking,\n" +
		"     max(CASE WHEN entry.matched THEN candidate.score END) AS score,\n" +
		"     max(CASE WHEN entry.matched AND candidate.anchored THEN 1 ELSE 0 END) = 1 AS anchored,\n" +
		"     max(CASE WHEN entry.matched THEN 1 ELSE 0 END) = 1 AS matched,\n" +
		"     head([found IN collect(entry.matches) WHERE found IS NOT NULL]) AS matches\n" +
		"OPTIONAL MATCH (d:PDDocument {id: node.document_id})\n" +
		"WHERE node:PDSearchSource\n" +
		"OPTIONAL MATCH (current:PDVersion {id: d.current_version_id, document_id: d.id})\n" +
		"\n" +
		"OPTIONAL MATCH (:PDVersion {id: node.version_id})-[:HAS_ORIGINAL]->(original:PDOriginal)\n" +
		"\n" +
		"CALL (node, matches) {\n" +
		"  UNWIND coalesce(matches, []) AS m\n" +
		"  WITH node, m, matches,\n" +
		"       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL\n" +
		"            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_starts[i] < m.end AND node.view_ends[i] > m.start] | CASE WHEN node.view_library[at] THEN node.view_source_starts[at] ELSE node.view_source_starts[at] + CASE WHEN m.start > node.view_starts[at] THEN m.start - node.view_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_starts[i] < m.end AND node.view_ends[i] > m.start] | CASE WHEN node.view_library[at] THEN node.view_source_ends[at] ELSE node.view_source_starts[at] + CASE WHEN m.end < node.view_ends[at] THEN m.end ELSE node.view_ends[at] END - node.view_starts[at] END])}\n" +
		"            ELSE {start: m.start, end: m.end} END AS placed\n" +
		"  WITH node, m, placed, matches,\n" +
		"       CASE WHEN node:PDSearchSource AND (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own'))\n" +
		"            THEN [i IN range(0, size(coalesce(node.section_roles, [])) - 1)\n" +
		"                  WHERE node.section_starts[i] < placed.end AND placed.start < node.section_ends[i]\n" +
		"                  | {start: CASE WHEN placed.start > node.section_starts[i]\n" +
		"                                 THEN placed.start ELSE node.section_starts[i] END,\n" +
		"                     end: CASE WHEN placed.end < node.section_ends[i]\n" +
		"                               THEN placed.end ELSE node.section_ends[i] END,\n" +
		"                     role: node.section_roles[i], key: node.section_content_keys[i]}]\n" +
		"            ELSE [{start: placed.start, end: placed.end, role: 'own', key: ''}] END AS spans\n" +
		"  UNWIND spans AS span\n" +
		"  WITH node, m, placed, span, matches\n" +
		"  WHERE span.role <> 'duplicate'\n" +
		"    AND (NOT (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own')) OR m.kind = 'term'\n" +
		"         OR NOT any(t IN matches WHERE t.kind = 'term')\n" +
		"         OR any(t IN matches WHERE t.kind = 'term'\n" +
		"                AND span.start < t.end AND t.start < span.end))\n" +
		"    AND (span.role <> 'quoted' OR NOT EXISTS {\n" +
		"    MATCH (key:PDEmailText {id: span.key})<-[:HAS_EMAIL_OWN]-(written:PDSearchSource)\n" +
		"    WHERE written.id <> node.id AND (written:PDSearchSource OR false OR coalesce(written.current, true)) AND (NOT written:PDSummary OR coalesce(written.current, false)) AND NOT coalesce(written.search_hidden, false) AND (NOT written:PDSummary OR written.search_hidden = false)\n" +
		"      AND span.key <> ''\n" +
		"  })\n" +
		"  WITH m, placed, span,\n" +
		"       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL\n" +
		"            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_source_starts[i] < span.end AND node.view_source_ends[i] > span.start] | CASE WHEN node.view_library[at] THEN node.view_starts[at] ELSE node.view_starts[at] + CASE WHEN span.start > node.view_source_starts[at] THEN span.start - node.view_source_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(node.view_starts, [])) - 1) WHERE node.view_source_starts[i] < span.end AND node.view_source_ends[i] > span.start] | CASE WHEN node.view_library[at] THEN node.view_ends[at] ELSE node.view_starts[at] + CASE WHEN span.end < node.view_source_ends[at] THEN span.end ELSE node.view_source_ends[at] END - node.view_source_starts[at] END])} END AS seen\n" +
		"  RETURN collect({kind: m.kind, property: m.property, start: span.start, end: span.end,\n" +
		"                  view_start: CASE WHEN seen IS NULL THEN null\n" +
		"                                   WHEN span.start = placed.start THEN m.start\n" +
		"                                   ELSE last(apoc.coll.sort([m.start, seen.start])) END,\n" +
		"                  view_end: CASE WHEN seen IS NULL THEN null\n" +
		"                                 WHEN span.end = placed.end THEN m.end\n" +
		"                                 ELSE head(apoc.coll.sort([m.end, seen.end])) END,\n" +
		"                  score: m.score, term: m.term, role: span.role}) AS visible_matches\n" +
		"}\n" +
		"\n" +
		"WITH d, node, score, anchored, matched, ranking,\n" +
		"     current, original, visible_matches AS matches,\n" +
		"     (node IS NULL OR anchored OR NOT matched OR NOT node:PDSearchSource\n" +
		"      OR NOT (size(coalesce(node.email_own_keys, [])) > 0 OR any(role IN coalesce(node.section_roles, []) WHERE role <> 'own')) OR size(visible_matches) > 0) AS visible,\n" +
		"     CASE WHEN size(coalesce(visible_matches, [])) > 0 THEN {pages: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(node.page_starts, []), coalesce(node.page_ends, [])), coalesce(node.page_numbers, [])) | {start: x[0][0], end: x[0][1], value: x[1]}], sections: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(node.section_starts, []), coalesce(node.section_ends, [])), coalesce(node.section_titles, [])) | {start: x[0][0], end: x[0][1], value: x[1]}], cells: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(node.cell_starts, []), coalesce(node.cell_ends, [])), coalesce(node.cell_refs, [])) | {start: x[0][0], end: x[0][1], value: x[1]}]} END AS locators\n" +
		"WITH\n" +
		"     node, score, anchored, matched, visible, ranking, current, original,\n" +
		"     coalesce(d.family_key, d.id, node.id) AS family,\n" +
		"     CASE WHEN node:PDSearchSource\n" +
		"          THEN apoc.coll.sort([(d)-[:MEMBER_OF]->(__pd_collection:PDCollection) | __pd_collection.id])\n" +
		"          ELSE node.collection_ids END AS collection_ids,\n" +
		"     [m IN coalesce(matches, []) |\n" +
		"      {kind: m.kind,\n" +
		"       property: CASE WHEN m.property = 'view_text' THEN 'text' ELSE m.property END,\n" +
		"       start: m.start, end: m.end, view_start: m.view_start, view_end: m.view_end,\n" +
		"       score: m.score,\n" +
		"       role: CASE WHEN node:PDSummary THEN 'generated' ELSE m.role END,\n" +
		"       term: m.term,\n" +
		"       quote: substring(node[m.property], coalesce(m.view_start, m.start),\n" +
		"                        coalesce(m.view_end, m.end) - coalesce(m.view_start, m.start)),\n" +
		"       pages: [located IN locators.pages WHERE located.start < m.end AND m.start < located.end | located.value], sections: [title IN [located IN locators.sections WHERE located.start < m.end AND m.start < located.end | located.value] WHERE title <> ''], cells: [located IN locators.cells WHERE located.start < m.end AND m.start < located.end | located.value]}] AS passages\n" +
		"OPTIONAL MATCH (node)-[by:PD_RELATION|ABOUT]-(linked)\n" +
		"WHERE NOT node:PDSearchSource AND coalesce(by.eff_usable, false)\n" +
		"WITH\n" +
		"     node, score, anchored, matched, visible, ranking, current, original, family,\n" +
		"     collection_ids, passages,\n" +
		"     [x IN collect(DISTINCT {id: linked.id, kind: coalesce(by.kind, type(by)),\n" +
		"                             labels: labels(linked), citation_unsupported: by.citation_unsupported, \n" +
		"         citation_reason: by.citation_reason}) WHERE x.id IS NOT NULL] AS linked\n" +
		"\n" +
		"CALL (node) {\n" +
		"  WITH node WHERE node:PDSummary\n" +
		"  OPTIONAL MATCH (owner_document:PDDocument {id: node.owner_id})\n" +
		"  WHERE node:PDSummary AND node.owner_kind = 'document'\n" +
		"  OPTIONAL MATCH (owner_collection:PDCollection {id: node.owner_id})\n" +
		"  WHERE node:PDSummary AND node.owner_kind = 'collection'\n" +
		"  CALL (node, owner_collection) {\n" +
		"    CALL (node, owner_collection) {\n" +
		"      WITH node WHERE node:PDSummary AND node.owner_kind = 'document'\n" +
		"      UNWIND node.document_ids AS document_id\n" +
		"      MATCH (covered:PDDocument {id: document_id}) RETURN covered\n" +
		"      UNION ALL\n" +
		"      WITH node, owner_collection WHERE owner_collection IS NOT NULL\n" +
		"      MATCH (:PDCollection {id: owner_collection.id})<-[:MEMBER_OF]-(covered:PDDocument)\n" +
		"      WHERE node.summary_kind = 'collection' OR covered.id IN node.document_ids\n" +
		"      RETURN covered\n" +
		"    }\n" +
		"OPTIONAL MATCH (version:PDVersion {id: covered.current_version_id, document_id: covered.id})\n" +
		"\n" +
		"    WITH covered, version ORDER BY covered.id\n" +
		"    RETURN collect({document_id: covered.id, version_id: version.id,\n" +
		"                    source_id: version.current_source_id}) AS sources\n" +
		"  }\n" +
		"  CALL (node) {\n" +
		"    UNWIND range(0, size(coalesce(node.citation_source_ids, [])) - 1) AS at\n" +
		"    MATCH (source:PDSearchSource {id: node.citation_source_ids[at]})\n" +
		"    RETURN collect({statement: node.citation_statement[at], source_id: source.id,\n" +
		"                    document_id: source.document_id, start: node.cite_start[at],\n" +
		"                    end: node.cite_end[at],\n" +
		"                    quote: substring(source.text, node.cite_start[at],\n" +
		"                                     node.cite_end[at] - node.cite_start[at])}) AS citations\n" +
		"  }\n" +
		"  RETURN {sources: sources, citations: citations, child_ids: node.child_ids,\n" +
		"          collection_ids: CASE WHEN owner_collection IS NOT NULL THEN [owner_collection.id]\n" +
		"            ELSE apoc.coll.sort([(owner_document)-[:MEMBER_OF]->(__pd_collection:PDCollection) | __pd_collection.id]) END,\n" +
		"          workflow_id: coalesce(owner_document.summary_workflow_id,\n" +
		"                                owner_collection.summary_workflow_id),\n" +
		"          status: coalesce(owner_document.summary_status, owner_collection.summary_status),\n" +
		"          error_code: coalesce(owner_document.summary_error_code,\n" +
		"                               owner_collection.summary_error_code),\n" +
		"          error_message: coalesce(owner_document.summary_error_message,\n" +
		"                                  owner_collection.summary_error_message),\n" +
		"          provider_request_id: coalesce(owner_document.summary_provider_request_id,\n" +
		"                                        owner_collection.summary_provider_request_id),\n" +
		"          next_action: coalesce(owner_document.summary_next_action,\n" +
		"                                owner_collection.summary_next_action),\n" +
		"          out_of_date: coalesce(node.input_fingerprint <>\n" +
		"                               coalesce(owner_document.summary_input,\n" +
		"                                        owner_collection.summary_input), true)} AS summary_data\n" +
		"  UNION ALL\n" +
		"  WITH node WHERE node IS NULL OR NOT node:PDSummary\n" +
		"  RETURN {} AS summary_data\n" +
		"}\n" +
		"\n" +
		"WITH\n" +
		"     family, CASE WHEN visible THEN score END AS score,\n" +
		"     anchored, ranking,\n" +
		"     CASE WHEN visible THEN {type: CASE WHEN node:PDSearchSource THEN 'source'\n" +
		"                 WHEN node:PDSummary THEN 'summary' ELSE 'record' END,\n" +
		"      kind: CASE WHEN node:PDSearchSource THEN null ELSE coalesce(node.kind, 'record') END,\n" +
		"      id: node.id, score: score, anchored: anchored, matched: matched,\n" +
		"      generated: node:PDSummary,\n" +
		"      label: CASE WHEN node:PDSummary THEN 'Generated summary' END,\n" +
		"      summary_kind: node.summary_kind,\n" +
		"      out_of_date: CASE WHEN node:PDSummary THEN summary_data.out_of_date END,\n" +
		"      sources: CASE WHEN node:PDSummary THEN summary_data.sources END,\n" +
		"      citations: CASE WHEN node:PDSummary THEN summary_data.citations END,\n" +
		"      statement_citation_reasons: node.statement_citation_reasons,\n" +
		"      child_ids: node.child_ids,\n" +
		"      summary_workflow_id: summary_data.workflow_id, summary_status: summary_data.status,\n" +
		"      summary_error_code: summary_data.error_code,\n" +
		"      summary_error_message: summary_data.error_message,\n" +
		"      summary_provider_request_id: summary_data.provider_request_id,\n" +
		"      summary_next_action: summary_data.next_action,\n" +
		"      document_id: node.document_id,\n" +
		"      version_id: node.version_id,\n" +
		"      current: CASE WHEN node:PDSearchSource\n" +
		"                    THEN current IS NOT NULL AND current.id = node.version_id\n" +
		"                         AND current.current_source_id = node.id\n" +
		"                    ELSE coalesce(node.current, true) END,\n" +
		"      current_version_id: current.id, current_source_id: current.current_source_id,\n" +
		"      original_hash: original.content_hash,\n" +
		"      record_sets: CASE WHEN node:PDSearchSource AND (coalesce(node.profiled_run_layout, '') = coalesce(node.run_layout, '') OR (NOT (true IN coalesce(node.run_bulk, []))\n" +
		"        AND NOT EXISTS { MATCH (node)-[:PD_HAS_RECORD_SET|PD_MEMBER_OF_SET]->(:PDRecordSet) })) THEN [r IN [(node)-[run:PD_HAS_RECORD_SET]->(set:PDRecordSet) |\n" +
		"          {set_id: set.id, kind: set.kind, run: run.run, title: set.title, records: set.records,\n" +
		"           fields: set.fields, start: node.run_starts[run.run], end: node.run_ends[run.run],\n" +
		"           record_starts: CASE WHEN any(p IN passages WHERE p.kind = 'term')\n" +
		"                               THEN node.run_record_starts[\n" +
		"                                      reduce(before = 0, i IN range(0, run.run - 1)\n" +
		"                                             | before + node.run_records[i])\n" +
		"                                      ..reduce(upto = 0, i IN range(0, run.run)\n" +
		"                                               | upto + node.run_records[i])] END,\n" +
		"           record_ends: CASE WHEN any(p IN passages WHERE p.kind = 'term')\n" +
		"                             THEN node.run_record_ends[\n" +
		"                                    reduce(before = 0, i IN range(0, run.run - 1)\n" +
		"                                           | before + node.run_records[i])\n" +
		"                                    ..reduce(upto = 0, i IN range(0, run.run)\n" +
		"                                             | upto + node.run_records[i])] END}]\n" +
		"       | {set_id: r.set_id, kind: r.kind, run: r.run, title: r.title, records: r.records,\n" +
		"          fields: r.fields, start: r.start, end: r.end,\n" +
		"          same_as: head([i IN range(0, size(coalesce(node.set_copy_runs, [])) - 1)\n" +
		"                         WHERE node.set_copy_runs[i] = r.run\n" +
		"                         | {set_id: node.set_copy_of[i], document_id: node.set_copy_documents[i]}]),\n" +
		"          matched: [ordinal IN apoc.coll.toSet([p IN passages\n" +
		"                        WHERE p.kind = 'term' AND p.property = 'text' AND p.start >= r.start\n" +
		"                              AND p.start < r.end\n" +
		"                        | head([started IN [size([first IN r.record_starts WHERE first <= p.start])]\n" +
		"                                | CASE WHEN started > size([last IN r.record_ends\n" +
		"                                                            WHERE last <= p.start])\n" +
		"                                       THEN started - 1 ELSE -1 END])])\n" +
		"                    WHERE ordinal >= 0]}]\n" +
		"      + [(node)-[member:PD_MEMBER_OF_SET]->(group:PDRecordSet) |\n" +
		"         {set_id: group.id, kind: 'group', member_document_id: node.document_id,\n" +
		"          member_source_id: node.id, title: group.title,\n" +
		"          records: group.records, fields: group.fields, members: group.members,\n" +
		"          document_id: group.document_id}] END,\n" +
		"      rows_requested_at: CASE WHEN node:PDSearchSource\n" +
		"                              THEN head(COLLECT { MATCH (requested:PDDocument {id: node.document_id})\n" +
		"                                                  WHERE coalesce(requested.rows_requested_version_id = node.version_id, false)\n" +
		"                                                  RETURN requested.rows_requested_at AS at }) END,\n" +
		"      collection_ids: CASE WHEN node:PDSummary THEN summary_data.collection_ids\n" +
		"                          ELSE collection_ids END,\n" +
		"      passages: passages, origin: node.origin, status: node.status,\n" +
		"      revision: node.revision, citation_unsupported: node.citation_unsupported, \n" +
		"         citation_reason: node.citation_reason,\n" +
		"      text: CASE WHEN node:PDSearchSource THEN null ELSE node.search_text END,\n" +
		"      linked: linked[0..25], linked_total: size(linked),\n" +
		"      linked_capped: size(linked) > 25} END AS hit\n" +
		"ORDER BY anchored DESC, score DESC, hit.id\n" +
		"WITH\n" +
		"     family, ranking,\n" +
		"     collect(hit) AS variants, max(CASE WHEN anchored THEN 1 ELSE 0 END) = 1 AS anchored,\n" +
		"     max(score) AS score\n" +
		"ORDER BY anchored DESC, score DESC, family\n" +
		"WITH\n" +
		"     ranking,\n" +
		"     [f IN collect({family: family, anchored: anchored, score: score, variants: variants})\n" +
		"      WHERE f.family IS NOT NULL AND size(f.variants) > 0] AS families\n" +
		"CALL () {\n" +
		"  UNWIND CASE WHEN $qid IS NULL THEN [true] ELSE [] END AS first_page\n" +
		"  CALL db.retrieve(apoc.map.fromPairs([option IN [['query', $query], ['types', ['PDSearchSource']],\n" +
		"        ['where', '(node:PDSearchSource OR $history OR coalesce(node.current, true)) AND (NOT node:PDSummary OR coalesce(node.current, false)) AND NOT coalesce(node.search_hidden, false) AND (NOT node:PDSummary OR node.search_hidden = false) AND node.record_set_id IS NOT NULL'], ['failClosed', true], ['minRRFScore', 0],\n" +
		"        ['minSimilarity', $min_similarity], ['limit', 250]]\n" +
		"      WHERE option[1] IS NOT NULL]))\n" +
		"  YIELD node AS member, score AS member_score, matches AS member_matches\n" +
		"  MATCH (set:PDRecordSet {id: member.record_set_id})\n" +
		"  WITH set, member, member_score, member_matches\n" +
		"  ORDER BY member_score DESC, member.id\n" +
		"  WITH set, max(member_score) AS best,\n" +
		"       collect({member: member, score: member_score, matches: member_matches}) AS found\n" +
		"  ORDER BY best DESC, set.id\n" +
		"  WITH collect({set_id: set.id, document_id: set.document_id, title: set.title,\n" +
		"                records: set.records, fields: set.fields, best: best, found: found}) AS sets\n" +
		"  UNWIND range(0, size(sets) - 1) AS set_at\n" +
		"  UNWIND range(0, size(sets[set_at].found[0..25]) - 1) AS member_at\n" +
		"  WITH sets[set_at] AS listed, set_at, member_at,\n" +
		"       sets[set_at].found[member_at].member AS member,\n" +
		"       sets[set_at].found[member_at].score AS member_score,\n" +
		"       sets[set_at].found[member_at].matches AS member_matches\n" +
		"CALL (member, member_matches) {\n" +
		"  UNWIND coalesce(member_matches, []) AS m\n" +
		"  WITH member, m, member_matches,\n" +
		"       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL\n" +
		"            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_starts[i] < m.end AND member.view_ends[i] > m.start] | CASE WHEN member.view_library[at] THEN member.view_source_starts[at] ELSE member.view_source_starts[at] + CASE WHEN m.start > member.view_starts[at] THEN m.start - member.view_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_starts[i] < m.end AND member.view_ends[i] > m.start] | CASE WHEN member.view_library[at] THEN member.view_source_ends[at] ELSE member.view_source_starts[at] + CASE WHEN m.end < member.view_ends[at] THEN m.end ELSE member.view_ends[at] END - member.view_starts[at] END])}\n" +
		"            ELSE {start: m.start, end: m.end} END AS placed\n" +
		"  WITH member, m, placed, member_matches,\n" +
		"       CASE WHEN member:PDSearchSource AND (size(coalesce(member.email_own_keys, [])) > 0 OR any(role IN coalesce(member.section_roles, []) WHERE role <> 'own'))\n" +
		"            THEN [i IN range(0, size(coalesce(member.section_roles, [])) - 1)\n" +
		"                  WHERE member.section_starts[i] < placed.end AND placed.start < member.section_ends[i]\n" +
		"                  | {start: CASE WHEN placed.start > member.section_starts[i]\n" +
		"                                 THEN placed.start ELSE member.section_starts[i] END,\n" +
		"                     end: CASE WHEN placed.end < member.section_ends[i]\n" +
		"                               THEN placed.end ELSE member.section_ends[i] END,\n" +
		"                     role: member.section_roles[i], key: member.section_content_keys[i]}]\n" +
		"            ELSE [{start: placed.start, end: placed.end, role: 'own', key: ''}] END AS spans\n" +
		"  UNWIND spans AS span\n" +
		"  WITH member, m, placed, span, member_matches\n" +
		"  WHERE span.role <> 'duplicate'\n" +
		"    AND (NOT (size(coalesce(member.email_own_keys, [])) > 0 OR any(role IN coalesce(member.section_roles, []) WHERE role <> 'own')) OR m.kind = 'term'\n" +
		"         OR NOT any(t IN member_matches WHERE t.kind = 'term')\n" +
		"         OR any(t IN member_matches WHERE t.kind = 'term'\n" +
		"                AND span.start < t.end AND t.start < span.end))\n" +
		"    AND (span.role <> 'quoted' OR NOT EXISTS {\n" +
		"    MATCH (key:PDEmailText {id: span.key})<-[:HAS_EMAIL_OWN]-(written:PDSearchSource)\n" +
		"    WHERE written.id <> member.id AND (written:PDSearchSource OR false OR coalesce(written.current, true)) AND (NOT written:PDSummary OR coalesce(written.current, false)) AND NOT coalesce(written.search_hidden, false) AND (NOT written:PDSummary OR written.search_hidden = false)\n" +
		"      AND span.key <> ''\n" +
		"  })\n" +
		"  WITH m, placed, span,\n" +
		"       CASE WHEN m.property = 'view_text' AND m.start IS NOT NULL\n" +
		"            THEN {start: apoc.coll.min([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_source_starts[i] < span.end AND member.view_source_ends[i] > span.start] | CASE WHEN member.view_library[at] THEN member.view_starts[at] ELSE member.view_starts[at] + CASE WHEN span.start > member.view_source_starts[at] THEN span.start - member.view_source_starts[at] ELSE 0 END END]), end: apoc.coll.max([at IN [i IN range(0, size(coalesce(member.view_starts, [])) - 1) WHERE member.view_source_starts[i] < span.end AND member.view_source_ends[i] > span.start] | CASE WHEN member.view_library[at] THEN member.view_ends[at] ELSE member.view_starts[at] + CASE WHEN span.end < member.view_source_ends[at] THEN span.end ELSE member.view_source_ends[at] END - member.view_source_starts[at] END])} END AS seen\n" +
		"  RETURN collect({kind: m.kind, property: m.property, start: span.start, end: span.end,\n" +
		"                  view_start: CASE WHEN seen IS NULL THEN null\n" +
		"                                   WHEN span.start = placed.start THEN m.start\n" +
		"                                   ELSE last(apoc.coll.sort([m.start, seen.start])) END,\n" +
		"                  view_end: CASE WHEN seen IS NULL THEN null\n" +
		"                                 WHEN span.end = placed.end THEN m.end\n" +
		"                                 ELSE head(apoc.coll.sort([m.end, seen.end])) END,\n" +
		"                  score: m.score, term: m.term, role: span.role}) AS visible_matches\n" +
		"}\n" +
		"\n" +
		"  WITH listed, set_at, member_at, member, member_score, visible_matches[0..5] AS member_visible\n" +
		"  WITH listed, set_at, member_at, member, member_score, member_visible,\n" +
		"       CASE WHEN size(coalesce(member_visible, [])) > 0 THEN {pages: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(member.page_starts, []), coalesce(member.page_ends, [])), coalesce(member.page_numbers, [])) | {start: x[0][0], end: x[0][1], value: x[1]}], sections: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(member.section_starts, []), coalesce(member.section_ends, [])), coalesce(member.section_titles, [])) | {start: x[0][0], end: x[0][1], value: x[1]}], cells: [x IN apoc.coll.zip(apoc.coll.zip(coalesce(member.cell_starts, []), coalesce(member.cell_ends, [])), coalesce(member.cell_refs, [])) | {start: x[0][0], end: x[0][1], value: x[1]}]} END AS member_locators\n" +
		"  ORDER BY set_at, member_at\n" +
		"  WITH listed, set_at,\n" +
		"       collect({document_id: member.document_id, source_id: member.id,\n" +
		"                score: member_score,\n" +
		"                passages: [m IN coalesce(member_visible, []) |\n" +
		"      {kind: m.kind,\n" +
		"       property: CASE WHEN m.property = 'view_text' THEN 'text' ELSE m.property END,\n" +
		"       start: m.start, end: m.end, view_start: m.view_start, view_end: m.view_end,\n" +
		"       score: m.score,\n" +
		"       role: CASE WHEN member:PDSummary THEN 'generated' ELSE m.role END,\n" +
		"       term: m.term,\n" +
		"       quote: substring(member[m.property], coalesce(m.view_start, m.start),\n" +
		"                        coalesce(m.view_end, m.end) - coalesce(m.view_start, m.start)),\n" +
		"       pages: [located IN member_locators.pages WHERE located.start < m.end AND m.start < located.end | located.value], sections: [title IN [located IN member_locators.sections WHERE located.start < m.end AND m.start < located.end | located.value] WHERE title <> ''], cells: [located IN member_locators.cells WHERE located.start < m.end AND m.start < located.end | located.value]}]})\n" +
		"         AS members\n" +
		"  ORDER BY set_at\n" +
		"  RETURN collect({set_id: listed.set_id, document_id: listed.document_id,\n" +
		"                  title: listed.title, records: listed.records, fields: listed.fields,\n" +
		"                  members_total: size(listed.found), members: members,\n" +
		"                  score: listed.best}) AS groups\n" +
		"}\n" +
		"WITH ranking, families, groups,\n" +
		"     [f IN families | [v IN f.variants | v.document_id]] AS ranked_documents\n" +
		"WITH ranking, groups,\n" +
		"     [f IN families\n" +
		"      | {family: f.family, anchored: f.anchored, score: f.score,\n" +
		"         variants: [v IN f.variants\n" +
		"                    | apoc.map.merge(v, {group: head([g IN groups\n" +
		"                                                     WHERE g.document_id = v.document_id])})]}]\n" +
		"     + [g IN groups WHERE NOT g.document_id IN apoc.coll.flatten(ranked_documents)\n" +
		"        | {family: coalesce(g.document_id, g.set_id), anchored: false, score: null,\n" +
		"           variants: [apoc.map.merge(g, {type: 'group', id: g.set_id})]}] AS families\n" +
		"UNWIND CASE WHEN size(families) = 0 THEN [null] ELSE families END AS f\n" +
		"RETURN f.family AS family, f.anchored AS anchored, f.score AS score,\n" +
		"       ranking.search_method AS search_method, ranking.fallback_triggered AS fallback_triggered,\n" +
		"       ranking.fallback_reason AS fallback_reason,\n" +
		"       ranking.query_chunks_searched AS query_chunks_searched,\n" +
		"       ranking.query_chunks_total AS query_chunks_total,\n" +
		"       ranking.qid AS qid, ranking.has_more AS has_more, ranking.total AS total,\n" +
		"       false AS exhaustive,\n" +
		"       f.variants AS variants\n"},
	"mark": {wire: "672a1f0ed7858930", text: "\n" +
		"UNWIND $items AS item\n" +
		"MATCH (n:PDDocument {id: item.id})\n" +
		"SET n += {}\n" +
		"WITH n, item\n" +
		"WITH n, item, n.summary_input AS previous_input\n" +
		"WHERE coalesce(n.summary_input,'') = coalesce(item.previous,'') SET n.summary_input = item.fingerprint, \n" +
		"    n.summary_owed = CASE WHEN coalesce(n.summary_output, '') = item.fingerprint THEN null\n" +
		"                          ELSE true END,\n" +
		"    n.summary_status = CASE\n" +
		"      WHEN n.summary_output = item.fingerprint AND EXISTS {\n" +
		"        MATCH (root:PDSummary {owner_kind:'document', owner_id:n.id,\n" +
		"              summary_kind:'document', current:true, input_fingerprint:item.fingerprint})\n" +
		"      } THEN 'ready'\n" +
		"      WHEN previous_input IS NOT NULL AND previous_input <> item.fingerprint THEN 'waiting'\n" +
		"      ELSE n.summary_status END\n"},
}
