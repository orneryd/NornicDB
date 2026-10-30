package cypher

import (
	"fmt"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Clause composition follows the grammar's categories, not physical effects.
// A procedure or subquery can write while still being a readingStatement.
// The historical reference population is retained as evidence. Current release
// admits mixed read/write chains; only its procedure CALL barrier is carried
// here. Rebase verification must refresh the per-kind public expectations.
type boundClauseAdmissionClass struct {
	name                          string
	updating, boundary            bool
	requiresWith5, requiresWith25 bool
}

var boundClauseAdmissionClasses = map[pipelineClauseKind]boundClauseAdmissionClass{
	pipelineClauseMatch:         {name: "MATCH"},
	pipelineClauseOptionalMatch: {name: "MATCH"},
	pipelineClauseUnwind:        {name: "UNWIND"},
	pipelineClauseCall:          {name: "CALL", requiresWith5: true, requiresWith25: true},
	pipelineClauseCallSubquery:  {name: "CALL"},
	pipelineClauseCreate:        {name: "CREATE", updating: true},
	pipelineClauseMerge:         {name: "MERGE", updating: true},
	pipelineClauseDelete:        {name: "DELETE", updating: true},
	pipelineClauseSet:           {name: "SET", updating: true},
	pipelineClauseRemove:        {name: "REMOVE", updating: true},
	pipelineClauseForeach:       {name: "FOREACH", updating: true},
	pipelineClauseWith:          {name: "WITH", boundary: true},
	pipelineClauseReturn:        {name: "RETURN"},
	// Neo4j 5 has no LET/FILTER grammar. Preserve the existing NornicDB
	// extension; its syntax failure is not evidence for a new rejection.
	pipelineClauseLet:    {name: "LET"},
	pipelineClauseFilter: {name: "FILTER"},
}

var boundClauseAdmissionVariants = map[int]boundClauseAdmissionClass{
	// FOR shares the iteration operator with UNWIND, but is a preserved
	// language extension rather than a Neo4j-5 reading clause.
	cypherantlr.CypherParserRULE_forSt: {name: "FOR"},
}

func clauseAdmissionClass(clause *boundClause) (boundClauseAdmissionClass, bool) {
	if variant, found := boundClauseAdmissionVariants[clause.source.syntax.Rule()]; found {
		return variant, true
	}
	class, found := boundClauseAdmissionClasses[clause.source.kind]
	return class, found
}

// admitClauseTransition is the one composition decision in the construction.
// The current release grammar's read-after-write behavior is retained. Its existing
// procedure-CALL barrier is retained too; the Neo4j-5 probe alone does not
// authorize changing accepted Cypher-25 behavior.
func (compilation *boundCompilation) admitClauseTransition(clause *boundClause) error {
	class, found := clauseAdmissionClass(clause)
	if !found {
		return unresolvedStatementError()
	}
	requiresWith := class.requiresWith5
	if compilation.owner.cypher25 {
		requiresWith = class.requiresWith25
	}
	if requiresWith && clause.branch.pendingUpdate != "" {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidClauseComposition", fmt.Sprintf("WITH is required between %s and %s", clause.branch.pendingUpdate, class.name))
	}
	return nil
}

// advanceClauseAdmission has the sole writer of the branch's pending-update
// state. Child bodies and UNION branches have independent construction frames.
func (compilation *boundCompilation) advanceClauseAdmission(clause *boundClause) {
	class, _ := clauseAdmissionClass(clause)
	if class.boundary {
		clause.branch.pendingUpdate = ""
	} else if class.updating {
		clause.branch.pendingUpdate = class.name
	}
}
