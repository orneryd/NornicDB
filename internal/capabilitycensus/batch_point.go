package capabilitycensus

import "go/types"

// Batch candidates are selected from actual entity key/slice/map types. A
// function's spelling says nothing about its read mode or point counterpart;
// those require an individual body-read disposition and public adapter proof.
// Author: Lane #1644 (Codex).
func batchReadShape(signature *types.Signature) string {
	for i := 0; i < signature.Params().Len(); i++ {
		slice, ok := signature.Params().At(i).Type().Underlying().(*types.Slice)
		if !ok {
			continue
		}
		key, ok := types.Unalias(slice.Elem()).(*types.Named)
		if !ok || key.Obj().Pkg() == nil || key.Obj().Pkg().Path() != storagePackage {
			continue
		}
		// Use this parameter's actual package instance. A package path does not
		// substitute for type identity across compiled source/build variants.
		pkg := key.Obj().Pkg()
		for _, pair := range []struct{ key, entity, contract string }{{"NodeID", "Node", "typed-node-batch"}, {"EdgeID", "Edge", "typed-edge-batch"}} {
			declaredKey, entity := pkg.Scope().Lookup(pair.key), pkg.Scope().Lookup(pair.entity)
			if declaredKey == nil || entity == nil || !types.Identical(key, declaredKey.Type()) {
				continue
			}
			value := types.NewPointer(entity.Type())
			for j := 0; j < signature.Results().Len(); j++ {
				result, ok := signature.Results().At(j).Type().Underlying().(*types.Map)
				if ok && types.Identical(result.Key(), key) && types.Identical(result.Elem(), value) {
					return pair.contract
				}
			}
		}
	}
	return ""
}

// batchIDOperationShape selects ID-slice/error-only operations from their
// compiled types. This is a candidate shape, not a deletion disposition:
// each body must establish its actual effect and source-resolution owner.
func batchIDOperationShape(signature *types.Signature) string {
	if signature.Results().Len() != 1 || !types.Identical(signature.Results().At(0).Type(), types.Universe.Lookup("error").Type()) {
		return ""
	}
	for i := 0; i < signature.Params().Len(); i++ {
		slice, ok := types.Unalias(signature.Params().At(i).Type()).Underlying().(*types.Slice)
		if !ok {
			continue
		}
		key, ok := types.Unalias(slice.Elem()).(*types.Named)
		if !ok || key.Obj().Pkg() == nil || key.Obj().Pkg().Path() != storagePackage {
			continue
		}
		pkg := key.Obj().Pkg()
		for _, candidate := range []struct{ declaration, shape string }{{"NodeID", "typed-node-id-batch-error"}, {"EdgeID", "typed-edge-id-batch-error"}} {
			if declared := pkg.Scope().Lookup(candidate.declaration); declared != nil && types.Identical(key, declared.Type()) {
				return candidate.shape
			}
		}
	}
	return ""
}
