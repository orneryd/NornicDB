WITH $keys AS fam_seed_ids
CALL (fam_seed_ids) {
 CALL (fam_seed_ids) {
  UNWIND fam_seed_ids AS fam_seed_id
  MATCH (fam_seed:PDGraphNode {id:fam_seed_id})
  OPTIONAL MATCH (fam_seed)-[fam_near:PD_RELATION]-(fam_neighbour:PDGraphNode)
  WHERE coalesce(fam_near.kind,'') = 'same_document'
  WITH collect(fam_seed)+collect(fam_neighbour) AS fam_nodes
  UNWIND fam_nodes AS fam_node
  WITH DISTINCT fam_node WHERE fam_node IS NOT NULL
  RETURN collect(fam_node.id) AS ids
 }
 RETURN ids
}
RETURN ids
