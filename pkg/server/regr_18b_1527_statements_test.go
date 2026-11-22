package server

// Statement texts copied verbatim from the application's query_goldens.json
// (decisions.SUBJECTS, decisions.TARGETS) at its commit ccc619eb.

const regrSubjectsStatement = `
UNWIND $targets AS id
MATCH (n:PDGraphNode {id: id})
WITH n
WHERE n.origin = 'automatic' AND n.document_id = $document_id AND NOT n:PDIdentity
  AND (n:PDEntity OR n:PDEvent) AND ($current OR n.source_id = $source_id)

CALL (n) {
  OPTIONAL MATCH (n)-[p:PD_RELATION {kind: 'involves'}]->(party:PDGraphNode)
  CALL (party) {
  OPTIONAL MATCH (survivor:PDGraphNode {id: party.survivor})
  WHERE party:PDIdentity AND survivor:PDIdentity
  CALL (survivor) {
  OPTIONAL MATCH (survivor_member:PDIdentity {survivor: survivor.id})
  OPTIONAL MATCH (survivor_anchor:PDIdentityAnchor {party: survivor_member.id})
  RETURN apoc.coll.sort(collect(DISTINCT survivor_anchor.key)) AS keys
}
  RETURN CASE WHEN survivor IS NULL THEN null
              ELSE {identity: survivor.id, name: survivor.names[0],
                    identifiers: [key IN keys | {scheme: split(key, '|')[0], value: split(key, '|')[1], issuer: null, jurisdiction: null, period: null, category: 'identifier'}]} END AS party_given
}
WITH n, p, party, party_given ORDER BY coalesce(p.role, ''), party.id
  RETURN collect(CASE WHEN party IS NOT NULL THEN {
    given: p.origin = 'automatic' OR (((coalesce(p.origin, "") <> "automatic" OR p.stage = "interpretation")
  AND coalesce(p.current, true)) AND coalesce(party.current, true)),
    participant: CASE WHEN party:PDIdentity
     THEN {role: p.role, id: party.id, name: party_given.name, identifiers: party_given.identifiers, continued: true}
     ELSE {role: p.role, id: party.id, name: party.name, identifiers: [at IN range(0, size(coalesce(party.identifier_values, [])) - 1) | {scheme: CASE WHEN party.identifier_schemes[at] = '' THEN null ELSE party.identifier_schemes[at] END, value: CASE WHEN party.identifier_values[at] = '' THEN null ELSE party.identifier_values[at] END, issuer: CASE WHEN party.identifier_issuers[at] = '' THEN null ELSE party.identifier_issuers[at] END, jurisdiction: CASE WHEN party.identifier_jurisdictions[at] = '' THEN null ELSE party.identifier_jurisdictions[at] END, period: CASE WHEN party.identifier_periods[at] = '' THEN null ELSE party.identifier_periods[at] END, category: coalesce(party.identifier_categories[at], 'identifier')}], continued: coalesce(party.reading_id, '') <> coalesce(n.reading_id, '')} END,
    linked: [['role', p.id, CASE WHEN p IS NULL THEN null ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson([true, p.kind, p.role, p.origin, coalesce(p.current, true)]))) END],
             ['party', party.id, CASE WHEN party IS NULL THEN null WHEN party:PDIdentity THEN toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson([false, party_given.identity, party_given.name, party_given.identifiers, coalesce(party.current, true)]))) ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson([false, party.name, party.identifier_schemes, party.identifier_values, party.identifier_issuers, party.identifier_jurisdictions, party.identifier_periods, party.identifier_categories, coalesce(party.current, true)]))) END]]} END) AS involved
}
OPTIONAL MATCH (speaker:PDGraphNode {id: n.speaker_id})
CALL (speaker) {
  OPTIONAL MATCH (survivor:PDGraphNode {id: speaker.survivor})
  WHERE speaker:PDIdentity AND survivor:PDIdentity
  CALL (survivor) {
  OPTIONAL MATCH (survivor_member:PDIdentity {survivor: survivor.id})
  OPTIONAL MATCH (survivor_anchor:PDIdentityAnchor {party: survivor_member.id})
  RETURN apoc.coll.sort(collect(DISTINCT survivor_anchor.key)) AS keys
}
  RETURN CASE WHEN survivor IS NULL THEN null
              ELSE {identity: survivor.id, name: survivor.names[0],
                    identifiers: [key IN keys | {scheme: split(key, '|')[0], value: split(key, '|')[1], issuer: null, jurisdiction: null, period: null, category: 'identifier'}]} END AS speaker_given
}
OPTIONAL MATCH (source:PDSearchSource {id: n.source_id})
WITH n, involved, speaker, speaker_given, coalesce(source.text, '') AS text
WITH n, involved, speaker, speaker_given, CASE WHEN n IS NULL THEN null
                                WHEN n:PDEvent THEN {id: n.id, type: 'event', reading_id: coalesce(n.reading_id, ''),
  confidence: n.confidence,
  explanation: n.explanation,
  citations: [at IN range(0, size(coalesce(n.cite_start, [])) - 1) | head([before IN [split(substring(text, 0, n.cite_start[at]), '\n')] |
head([after IN [head(split(substring(text, n.cite_end[at]), '\n'))] |

  {line: size(before), column: size(last(before)) + 1,
   quote: substring(text, n.cite_start[at], n.cite_end[at] - n.cite_start[at]),
   context: substring(text, n.cite_start[at] - (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END),
                      n.cite_end[at] - n.cite_start[at] + (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END)
                      + (CASE WHEN size(after) < 200 THEN size(after) ELSE 200 END))}])])],
  speaker: CASE WHEN speaker IS NULL THEN null ELSE CASE WHEN speaker:PDIdentity
     THEN {id: speaker.id, name: speaker_given.name, identifiers: speaker_given.identifiers, continued: true}
     ELSE {id: speaker.id, name: speaker.name, identifiers: [at IN range(0, size(coalesce(speaker.identifier_values, [])) - 1) | {scheme: CASE WHEN speaker.identifier_schemes[at] = '' THEN null ELSE speaker.identifier_schemes[at] END, value: CASE WHEN speaker.identifier_values[at] = '' THEN null ELSE speaker.identifier_values[at] END, issuer: CASE WHEN speaker.identifier_issuers[at] = '' THEN null ELSE speaker.identifier_issuers[at] END, jurisdiction: CASE WHEN speaker.identifier_jurisdictions[at] = '' THEN null ELSE speaker.identifier_jurisdictions[at] END, period: CASE WHEN speaker.identifier_periods[at] = '' THEN null ELSE speaker.identifier_periods[at] END, category: coalesce(speaker.identifier_categories[at], 'identifier')}], continued: coalesce(speaker.reading_id, '') <> coalesce(n.reading_id, '')} END END,
  polarity: n.polarity,
  status: n.status,
  dispute: n.dispute,
  dates: [at IN range(0, size(coalesce(n.date_types, [])) - 1) | {type: n.date_types[at], value: n.date_values[at], precision: n.date_precisions[at]}],
  name: n.name,
  kind: n.definition_id,
  kind_term: n.kind_term,
  participants: [x IN involved WHERE x.given | x.participant]}
                                ELSE {id: n.id, type: 'entity', reading_id: coalesce(n.reading_id, ''),
  confidence: n.confidence,
  explanation: n.explanation,
  citations: [at IN range(0, size(coalesce(n.cite_start, [])) - 1) | head([before IN [split(substring(text, 0, n.cite_start[at]), '\n')] |
head([after IN [head(split(substring(text, n.cite_end[at]), '\n'))] |

  {line: size(before), column: size(last(before)) + 1,
   quote: substring(text, n.cite_start[at], n.cite_end[at] - n.cite_start[at]),
   context: substring(text, n.cite_start[at] - (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END),
                      n.cite_end[at] - n.cite_start[at] + (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END)
                      + (CASE WHEN size(after) < 200 THEN size(after) ELSE 200 END))}])])],
  name: n.name,
  kind: n.definition_id,
  kind_term: n.kind_term,
  identifiers: [at IN range(0, size(coalesce(n.identifier_values, [])) - 1) | {scheme: CASE WHEN n.identifier_schemes[at] = '' THEN null ELSE n.identifier_schemes[at] END, value: CASE WHEN n.identifier_values[at] = '' THEN null ELSE n.identifier_values[at] END, issuer: CASE WHEN n.identifier_issuers[at] = '' THEN null ELSE n.identifier_issuers[at] END, jurisdiction: CASE WHEN n.identifier_jurisdictions[at] = '' THEN null ELSE n.identifier_jurisdictions[at] END, period: CASE WHEN n.identifier_periods[at] = '' THEN null ELSE n.identifier_periods[at] END, category: coalesce(n.identifier_categories[at], 'identifier')}],
  aliases: [at IN range(0, size(coalesce(n.alias_names, [])) - 1) | {name: n.alias_names[at], type: n.alias_types[at]}]} END AS subject
WITH n, subject, CASE WHEN subject IS NULL THEN null ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson(subject {.*, reading_id: null,
      speaker: subject.speaker.id,
      participants: [x IN subject.participants | [coalesce(x.role, ''), x.id]]}))) END AS version,
     reduce(found = CASE WHEN speaker IS NULL THEN []
                         ELSE [['party', speaker.id, CASE WHEN speaker IS NULL THEN null WHEN speaker:PDIdentity THEN toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson([false, speaker_given.identity, speaker_given.name, speaker_given.identifiers, coalesce(speaker.current, true)]))) ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson([false, speaker.name, speaker.identifier_schemes, speaker.identifier_values, speaker.identifier_issuers, speaker.identifier_jurisdictions, speaker.identifier_periods, speaker.identifier_categories, coalesce(speaker.current, true)]))) END]] END,
            x IN involved | found + x.linked) AS linked

WITH collect(subject) AS subjects, collect({id: subject.id, version: version}) AS versions,
     reduce(found = [], entry IN reduce(all = [], each IN collect(linked) | all + each)
            | CASE WHEN entry IN found THEN found ELSE found + [entry] END) AS linked
WITH subjects, versions, linked, [s IN subjects | s.id] AS carriable
RETURN subjects, {subject: versions,
  party: [e IN linked WHERE e[0] = 'party' | {id: e[1], version: e[2]}], role: [e IN linked WHERE e[0] = 'role' | {id: e[1], version: e[2]}]} AS given,
  reduce(chosen = {ids: [], carried: []}, item IN $items
    | CASE WHEN item.continues IN carriable AND NOT item.continues IN chosen.carried
        THEN {ids: chosen.ids + [item.continues], carried: chosen.carried + [item.continues]}
        ELSE {ids: chosen.ids + [item.own], carried: chosen.carried} END) AS carry
`

const regrTargetsStatement = `
UNWIND $targets AS id
MATCH (n:PDGraphNode {id: id})
RETURN n.id AS id, CASE WHEN n IS NULL THEN null ELSE toString(uuid.v5('d0522127-356e-598a-b9ee-637f373807a7', apoc.convert.toJson({identity: n:PDIdentity, entity: n:PDEntity,
       event: n:PDEvent, origin: n.origin, document_id: n.document_id,
       source_id: n.source_id, name: n.name, kind: n.kind, kind_term: n.kind_term,
       explanation: n.explanation, identifier_schemes: coalesce(n.identifier_schemes, []), identifier_values: coalesce(n.identifier_values, []), identifier_issuers: coalesce(n.identifier_issuers, []), identifier_jurisdictions: coalesce(n.identifier_jurisdictions, []), identifier_periods: coalesce(n.identifier_periods, []), identifier_categories: coalesce(n.identifier_categories, [])}))) END AS version
`
