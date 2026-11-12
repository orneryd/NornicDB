package cypher

import "time"

// observedExecutionPlan renders execution observations, never a second parse of
// the statement. Child clause programs belong to the operator that invoked
// them; actual reader records belong to their executing clause. Distinct source
// positions remain distinct even when their text is equal.
func observedExecutionPlan(query string, result *ExecuteResult, elapsed time.Duration, collector *profileRuntimeCollector) *ExecutionPlan {
	observation := collector.observation()
	root := &PlanOperator{OperatorType: "ProduceResults", Description: "Return the executed statement's rows", DBHitsObserved: true}
	plan := &ExecutionPlan{Query: query, Mode: ModeProfile, Root: root, TotalTime: elapsed, Unobserved: observation.Unobserved, TotalDBHitsObserved: true}
	if result != nil && result.Rows != nil {
		root.ActualRows = int64(len(result.Rows))
		root.RowsObserved = true
		plan.TotalRows = root.ActualRows
	} else if observation.FinalRowsObserved {
		root.ActualRows = observation.FinalRows
		root.RowsObserved = true
		plan.TotalRows = root.ActualRows
	}
	operators := make(map[int64]*PlanOperator, len(observation.Clauses))
	var top *PlanOperator
	lastByParent := make(map[int64]*PlanOperator)
	for _, clause := range observation.Clauses {
		kind := profileClauseOperator(clause.Kind)
		if clause.Operator != "" {
			kind = clause.Operator
		}
		if clause.NodeFilterObserved {
			kind = "Filter"
		}
		op := &PlanOperator{
			OperatorType: kind, Description: clause.Source,
			ActualRows: clause.Rows, RowsObserved: clause.RowsObserved,
			DBHits: clause.DBHits, DBHitsObserved: clause.DBHitsObserved,
			Arguments: map[string]interface{}{"executions": clause.Executions},
		}
		operators[clause.ID] = op
		if kind == "UnionBranch" {
			op.Arguments["branch"] = clause.Ordinal
		}
		if owner := operators[clause.Parent]; owner != nil && (owner.OperatorType == "Union" || owner.OperatorType == "UnionAll") {
			owner.Children = append(owner.Children, op)
			plan.TotalDBHits += clause.DBHits
			continue
		}
		if previous := lastByParent[clause.Parent]; previous != nil {
			op.Children = append(op.Children, previous)
		}
		lastByParent[clause.Parent] = op
		if clause.Parent == 0 {
			top = op
		}
		plan.TotalDBHits += clause.DBHits
	}
	for parent, last := range lastByParent {
		if parent != 0 {
			if owner := operators[parent]; owner != nil {
				owner.Children = append(owner.Children, last)
			}
		}
	}
	if top != nil {
		root.Children = append(root.Children, top)
	}
	for _, access := range collector.accessObservation() {
		if access.Unknown {
			plan.TotalDBHitsObserved = false
			plan.Unobserved = append(plan.Unobserved, "database reads: "+access.Detail)
			owner := operators[access.Parent]
			if owner == nil {
				owner = root
			}
			owner.DBHitsObserved = false
			if owner.Arguments == nil {
				owner.Arguments = make(map[string]interface{})
			}
			owner.Arguments["DBHitsObserved"] = false
			continue
		}
		op := &PlanOperator{OperatorType: access.Kind, Description: access.Detail,
			ActualRows: access.Rows, RowsObserved: true, DBHits: access.Hits, DBHitsObserved: true}
		owner := operators[access.Parent]
		if owner == nil {
			owner = root
		}
		owner.Children = append(owner.Children, op)
		plan.TotalDBHits += access.Hits
	}
	if len(plan.Unobserved) > 0 {
		if root.Arguments == nil {
			root.Arguments = make(map[string]interface{})
		}
		root.Arguments["unobserved"] = plan.Unobserved
	}
	if root.Arguments == nil {
		root.Arguments = make(map[string]interface{})
	}
	root.Arguments["TotalDBHitsObserved"] = plan.TotalDBHitsObserved
	return plan
}

func profileClauseOperator(kind pipelineClauseKind) string {
	switch kind {
	case pipelineClauseMatch:
		return "Match"
	case pipelineClauseOptionalMatch:
		return "OptionalMatch"
	case pipelineClauseCreate:
		return "Create"
	case pipelineClauseMerge:
		return "Merge"
	case pipelineClauseDelete:
		return "Delete"
	case pipelineClauseSet:
		return "Set"
	case pipelineClauseRemove:
		return "Remove"
	case pipelineClauseWith, pipelineClauseLet:
		return "Projection"
	case pipelineClauseReturn:
		return "Projection"
	case pipelineClauseUnwind:
		return "Unwind"
	case pipelineClauseForeach:
		return "Foreach"
	case pipelineClauseCallSubquery:
		return "Apply"
	case pipelineClauseCall:
		return "ProcedureCall"
	case pipelineClauseFilter:
		return "Filter"
	case pipelineClauseLoadCSV:
		return "LoadCSV"
	case pipelineClauseCommand:
		return "Command"
	default:
		return "Clause"
	}
}
