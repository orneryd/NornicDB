package queryprofile

// The profile ledger names a physical read by what the reader read, not by the
// operator that asked for it: AllNodesScan reads every node, NodeByLabelScan
// the nodes of one label, NodeByIDSeek one node by id, AllRelationshipsScan
// every relationship. These are the kinds an execution and the packages it
// calls both report, so they are canonical here and every reporter spells them
// through these constants, one read never appearing under two spellings. A kind
// this file does not name belongs to the one owner that reads it.
const (
	// KindTemporalEligibilityRequest names one returned current-version decision.
	KindTemporalEligibilityRequest = "TemporalEligibilityRequest"
	// KindNodeBatchByIDSeek is one bulk node-image acquisition.
	KindNodeBatchByIDSeek = "NodeBatchByIdSeek"
	// KindSchemaConstraintCatalog is one acquisition of schema constraints.
	KindSchemaConstraintCatalog = "SchemaConstraintCatalog"
	// KindAllNodesScan names the read of the whole node population.
	KindAllNodesScan = "AllNodesScan"
	// KindAllRelationshipsScan names the read of the whole relationship
	// population.
	KindAllRelationshipsScan = "AllRelationshipsScan"
	// KindNodeByLabelScan names the read of the nodes of one label.
	KindNodeByLabelScan = "NodeByLabelScan"
	// KindNodeByIDSeek names the point read of one node by id. The value keeps
	// the plan operator's spelling, NodeByIdSeek.
	KindNodeByIDSeek = "NodeByIdSeek"
	// KindRelationshipByTypeScan names the delivered relationship population of one type.
	KindRelationshipByTypeScan = "RelationshipByTypeScan"
	// KindRelationshipByIDSeek names a point read of one relationship by id.
	KindRelationshipByIDSeek = "RelationshipByIdSeek"
)
