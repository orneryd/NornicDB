package cypher

// ProcedureCypherInput declares text interpreted by a handler. Field selects a
// canonical retrieval request property; otherwise Argument is statement text.
// ParametersArgument is one-based (zero means the caller's existing bindings).
// StoredText describes a custom procedure's registered body.
type ProcedureCypherInput struct {
	Argument int
	Field string
	ParametersArgument int
	StoredText string
}

// ProcedureResultCache describes execution, not an independent cache decision.
// The query determinism owner combines these declarations with the caller's
// actual arguments and classifies every executable text with the same rules.
// Unknown handlers (including plugins) remain non-cacheable by default.
type ProcedureResultCache struct {
	Deterministic bool
	RetrievalRequest bool
	Continuation bool
	CypherInputs []ProcedureCypherInput
}

type functionCacheVolatility uint8

const (
	functionStable functionCacheVolatility = iota
	functionVolatile
	functionVolatileWithoutArguments
	functionTemporalConstructor
)

// These are registry declarations for the supported text interpreters. Read
// mode alone is not a determinism declaration: live/opaque handlers are omitted.
var builtinProcedureResultCache = map[string]ProcedureResultCache{
	"db.labels": {Deterministic:true},
	"db.relationshipTypes": {Deterministic:true},
	"db.propertyKeys": {Deterministic:true},
	"db.indexes": {Deterministic:true},
	"db.index.stats": {Deterministic:true},
	"db.constraints": {Deterministic:true},
	"db.schema.nodeTypeProperties": {Deterministic:true},
	"db.schema.relTypeProperties": {Deterministic:true},
	"db.schema.visualization": {Deterministic:true},
	"nornicdb.version": {Deterministic:true},
	"db.rerank": {Deterministic:true},
	"db.index.vector.embed": {Deterministic:true},
	"db.index.vector.queryNodes": {Deterministic:true},
	"db.index.vector.queryRelationships": {Deterministic:true},
	"db.index.fulltext.queryNodes": {Deterministic:true},
	"db.index.fulltext.queryRelationships": {Deterministic:true},
	"db.retrieve": {RetrievalRequest:true, Continuation:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,Field:"where"}}},
	"db.rretrieve": {Deterministic:true, RetrievalRequest:true, Continuation:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,Field:"where"}}},
	"db.retrieveRelationships": {Deterministic:true, RetrievalRequest:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,Field:"where"}}},
	"db.retrieveGraph": {Deterministic:true, RetrievalRequest:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,Field:"where"}}},
	"db.rretrieveGraph": {Deterministic:true, RetrievalRequest:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,Field:"where"}}},
	"apoc.cypher.run": {Deterministic:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,ParametersArgument:2}}},
	"apoc.cypher.doitall": {Deterministic:true, CypherInputs:[]ProcedureCypherInput{{Argument:0,ParametersArgument:2}}},
	"apoc.cypher.doIt": {CypherInputs:[]ProcedureCypherInput{{Argument:0,ParametersArgument:2}}},
	"apoc.cypher.runMany": {CypherInputs:[]ProcedureCypherInput{{Argument:0,ParametersArgument:2}}},
	"apoc.periodic.iterate": {CypherInputs:[]ProcedureCypherInput{{Argument:0},{Argument:1}}},
	"apoc.periodic.rock_n_roll": {CypherInputs:[]ProcedureCypherInput{{Argument:0},{Argument:1}}},
	"apoc.periodic.commit": {CypherInputs:[]ProcedureCypherInput{{Argument:0,ParametersArgument:2}}},
	"apoc.export.json.query": {CypherInputs:[]ProcedureCypherInput{{Argument:0}}},
	"apoc.export.csv.query": {CypherInputs:[]ProcedureCypherInput{{Argument:0}}},
	"apoc.custom.installProcedure": {CypherInputs:[]ProcedureCypherInput{{Argument:1}}},
	"apoc.custom.declareProcedure": {CypherInputs:[]ProcedureCypherInput{{Argument:1}}},
}
