UNWIND apoc.coll.sort(apoc.coll.toSet(reduce(ids = [], c IN $active | ids + c.targets))) AS id
MATCH (n:RowAggregateNode {id: id})
WHERE n.origin = 'automatic' AND n.group_id = $group_id AND NOT n:RowAggregateExcluded
  AND (n:RowAggregateItem OR n:RowAggregateEvent) AND ($current OR n.source_key = $source_key)
CALL (n) {
  OPTIONAL MATCH (n)-[p:ROW_AGG_LINK {kind: 'involves'}]->(party:RowAggregateNode)
  WITH n, p, party ORDER BY coalesce(p.role, ''), party.id
  RETURN collect(CASE WHEN party IS NOT NULL THEN {id:party.id} END) AS involved
}
OPTIONAL MATCH (peer:RowAggregateNode {id:n.peer_key})
OPTIONAL MATCH (source:RowAggregateSource {id:n.source_key})
WITH n, involved, peer, coalesce(source.text, '') AS text
WITH collect({id:n.id, involved:involved}) AS subjects
WITH subjects, [s IN subjects | s.id] AS carriable
RETURN subjects, {subject:[s IN subjects | {id:s.id}], party:[], role:[]} AS given,
  reduce(chosen = {ids:[], carried:[]}, item IN $items |
    CASE WHEN item.continues IN carriable AND NOT item.continues IN chosen.carried
      THEN {ids:chosen.ids + [item.continues], carried:chosen.carried + [item.continues]}
      ELSE {ids:chosen.ids + [item.own], carried:chosen.carried} END) AS carry