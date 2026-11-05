
UNWIND apoc.coll.sort(apoc.coll.toSet(reduce(ids = [], c IN $active | ids + c.targets))) AS id
MATCH (n:RowAggregateNode {id: id})
WHERE n.origin = 'automatic' AND n.group_id = $group_id AND NOT n:RowAggregateExcluded
  AND (n:RowAggregateItem OR n:RowAggregateEvent) AND ($current OR n.source_key = $source_key)

CALL (n) {
  OPTIONAL MATCH (n)-[p:ROW_AGG_LINK {kind: 'involves'}]->(party:RowAggregateNode)
  WITH n, p, party ORDER BY coalesce(p.role, ''), party.id
  RETURN collect(CASE WHEN party IS NOT NULL THEN {
    given: p.origin = 'automatic'
           OR (coalesce(p.current, true) <> false AND coalesce(party.current, true) <> false),
    participant: {role: p.role, id: party.id, name: party.name, identifiers: [at IN range(0, size(coalesce(party.identifier_values, [])) - 1) | {scheme: CASE WHEN party.identifier_schemes[at] = '' THEN null ELSE party.identifier_schemes[at] END, value: CASE WHEN party.identifier_values[at] = '' THEN null ELSE party.identifier_values[at] END, issuer: CASE WHEN party.identifier_issuers[at] = '' THEN null ELSE party.identifier_issuers[at] END, jurisdiction: CASE WHEN party.identifier_jurisdictions[at] = '' THEN null ELSE party.identifier_jurisdictions[at] END, period: CASE WHEN party.identifier_periods[at] = '' THEN null ELSE party.identifier_periods[at] END, category: coalesce(party.identifier_categories[at], 'identifier')}], continued: coalesce(party.batch_id, '') <> coalesce(n.batch_id, '')},
    linked: [['role', p.id, CASE WHEN p IS NULL THEN null ELSE toString(uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', apoc.convert.toJson([true, p.kind, p.role, p.origin, coalesce(p.current, true) <> false]))) END],
             ['party', party.id, CASE WHEN party IS NULL THEN null ELSE toString(uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', apoc.convert.toJson([false, party.name, party.identifier_schemes, party.identifier_values, party.identifier_issuers, party.identifier_jurisdictions, party.identifier_periods, party.identifier_categories, coalesce(party.current, true) <> false]))) END]]} END) AS involved
}
OPTIONAL MATCH (speaker:RowAggregateNode {id: n.peer_key})
OPTIONAL MATCH (source:RowAggregateSource {id: n.source_key})
WITH n, involved, speaker, coalesce(source.text, '') AS text
WITH n, involved, speaker, CASE WHEN n IS NULL THEN null
                                WHEN n:RowAggregateEvent THEN {id: n.id, type: 'event', batch_id: coalesce(n.batch_id, ''),
  confidence: n.confidence,
  explanation: n.explanation,
  citations: [at IN range(0, size(coalesce(n.cite_start, [])) - 1) | head([before IN [split(substring(text, 0, n.cite_start[at]), '\n')] |
head([after IN [head(split(substring(text, n.cite_end[at]), '\n'))] |

  {line: size(before), column: size(last(before)) + 1,
   quote: substring(text, n.cite_start[at], n.cite_end[at] - n.cite_start[at]),
   context: substring(text, n.cite_start[at] - (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END),
                      n.cite_end[at] - n.cite_start[at] + (CASE WHEN size(last(before)) < 200 THEN size(last(before)) ELSE 200 END)
                      + (CASE WHEN size(after) < 200 THEN size(after) ELSE 200 END))}])])],
  speaker: CASE WHEN speaker IS NULL THEN null ELSE {id: speaker.id, name: speaker.name, identifiers: [at IN range(0, size(coalesce(speaker.identifier_values, [])) - 1) | {scheme: CASE WHEN speaker.identifier_schemes[at] = '' THEN null ELSE speaker.identifier_schemes[at] END, value: CASE WHEN speaker.identifier_values[at] = '' THEN null ELSE speaker.identifier_values[at] END, issuer: CASE WHEN speaker.identifier_issuers[at] = '' THEN null ELSE speaker.identifier_issuers[at] END, jurisdiction: CASE WHEN speaker.identifier_jurisdictions[at] = '' THEN null ELSE speaker.identifier_jurisdictions[at] END, period: CASE WHEN speaker.identifier_periods[at] = '' THEN null ELSE speaker.identifier_periods[at] END, category: coalesce(speaker.identifier_categories[at], 'identifier')}], continued: coalesce(speaker.batch_id, '') <> coalesce(n.batch_id, '')} END,
  polarity: n.polarity,
  status: n.status,
  dispute: n.dispute,
  dates: [at IN range(0, size(coalesce(n.date_types, [])) - 1) | {type: n.date_types[at], value: n.date_values[at], precision: n.date_precisions[at]}],
  name: n.name,
  kind: n.definition_id,
  kind_term: n.kind_term,
  participants: [x IN involved WHERE x.given | x.participant]}
                                ELSE {id: n.id, type: 'entity', batch_id: coalesce(n.batch_id, ''),
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
  candidate: n.candidate_id,
  identifiers: [at IN range(0, size(coalesce(n.identifier_values, [])) - 1) | {scheme: CASE WHEN n.identifier_schemes[at] = '' THEN null ELSE n.identifier_schemes[at] END, value: CASE WHEN n.identifier_values[at] = '' THEN null ELSE n.identifier_values[at] END, issuer: CASE WHEN n.identifier_issuers[at] = '' THEN null ELSE n.identifier_issuers[at] END, jurisdiction: CASE WHEN n.identifier_jurisdictions[at] = '' THEN null ELSE n.identifier_jurisdictions[at] END, period: CASE WHEN n.identifier_periods[at] = '' THEN null ELSE n.identifier_periods[at] END, category: coalesce(n.identifier_categories[at], 'identifier')}],
  aliases: [at IN range(0, size(coalesce(n.alias_names, [])) - 1) | {name: n.alias_names[at], type: n.alias_types[at]}]} END AS subject
WITH n, subject, CASE WHEN subject IS NULL THEN null ELSE toString(uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', apoc.convert.toJson(subject {.*, batch_id: null,
      speaker: subject.speaker.id,
      participants: [x IN subject.participants | [coalesce(x.role, ''), x.id]]}))) END AS version,
     reduce(found = CASE WHEN speaker IS NULL THEN []
                         ELSE [['party', speaker.id, CASE WHEN speaker IS NULL THEN null ELSE toString(uuid.v5('6ba7b810-9dad-11d1-80b4-00c04fd430c8', apoc.convert.toJson([false, speaker.name, speaker.identifier_schemes, speaker.identifier_values, speaker.identifier_issuers, speaker.identifier_jurisdictions, speaker.identifier_periods, speaker.identifier_categories, coalesce(speaker.current, true) <> false]))) END]] END,
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
