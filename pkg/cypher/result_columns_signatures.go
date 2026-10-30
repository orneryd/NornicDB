package cypher

import (
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// ParticipantProcedure supplies the selected participant's declaration, using
// the same ProcedureSpec as the local registry. A nil Spec is unresolved, never
// an admitted void procedure. OpaqueOutputs preserves the
// NornicDB extension for a user handler without declared result fields.
type ParticipantProcedure struct {
	Spec          *ProcedureSpec
	OpaqueOutputs bool
}

// ParticipantSchema reads declaration metadata, without admitting another
// query. The canonical constructor applies argument and YIELD rules once, using
// this declaration instead of a same-named local procedure. A nil provider uses
// the local registry; a locally unknown procedure still requires a provider.
// Participant answers are never cached by query text.
type ParticipantSchema func(resultmetadata.SchemaObligation) (ParticipantProcedure, error)

// freezeProcedureDeclaration snapshots the metadata consumed by construction
// and retained-ID type checking. Caller-owned slices and schema callbacks cannot
// change a previously admitted program. Display metadata remains in the registry.
func freezeProcedureDeclaration(spec ProcedureSpec) ProcedureSpec {
	spec.Params = slices.Clone(spec.Params)
	// Keep the raw declaration: signature-only user procedures retain the
	// existing open-output behavior. The shared declaration reader handles
	// display-signature fallback without changing explicit Returns presence.
	spec.Returns = slices.Clone(spec.Returns)
	if spec.ResultSchemas != nil {
		headers := slices.Clone(spec.ResultSchemas())
		for i := range headers {
			headers[i] = slices.Clone(headers[i])
		}
		spec.ResultSchemas = func() [][]string {
			copy := make([][]string, len(headers))
			for i := range headers {
				copy[i] = slices.Clone(headers[i])
			}
			return copy
		}
	}
	return spec
}

func (compilation *boundCompilation) procedureDeclaration(ctx context.Context, clause *boundClause, name string) (registeredProcedure, bool, error) {
	if clause.branch.graph.selected && compilation.owner.schema != nil {
		graph := clause.branch.graph
		selection := resultmetadata.GraphSelection{}
		if graph.selection != nil {
			selection = *graph.selection
			selection.Arguments = slices.Clone(selection.Arguments)
		}
		answer, err := compilation.owner.schema(resultmetadata.SchemaObligation{Procedure: name, Start: clause.source.start, End: clause.source.end, UseStart: graph.start, UseEnd: graph.end, Incoming: boundBindingScope(projectedMapNames(clause.before), clause.branch.ids), Routing: graph.routing, Selection: selection})
		if err != nil {
			return registeredProcedure{}, false, err
		}
		if err := ctx.Err(); err != nil {
			return registeredProcedure{}, false, err
		}
		if answer.Spec == nil {
			return registeredProcedure{}, false, nil
		}
		spec := *answer.Spec
		if spec.Name == "" {
			spec.Name = name
		}
		if err := validateProcedureSpec(spec); err != nil {
			return registeredProcedure{}, false, err
		}
		clause.step.ParticipantResolved = true
		return registeredProcedure{Spec: freezeProcedureDeclaration(spec), User: answer.OpaqueOutputs}, true, nil
	}
	procedure, found := globalProcedureRegistry.Get(name)
	if found {
		procedure.Spec = freezeProcedureDeclaration(procedure.Spec)
	}
	return procedure, found, nil
}
