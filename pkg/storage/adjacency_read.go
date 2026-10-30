package storage

type adjacencyRead uint8

const (
	adjacencyOutgoing adjacencyRead = iota
	adjacencyIncoming
	adjacencyBetween
	adjacencyBoth
)

// readAdjacency owns request routing, direction order and result projection.
// Only an actual namespace view qualifies IDs. Optional providers are bound on
// its immediate decorator; native physical readers are never found by unwrap.
func readAdjacency(engine Engine, kind adjacencyRead, start, end NodeID, version *MVCCVersion, visibility readVisibility) (out, in []*Edge, err error) {
	var namespace *NamespacedEngine
	if n, ok := engine.(*NamespacedEngine); ok {
		namespace = n
		engine = n.inner
		start = n.PrefixNodeID(start)
		if kind == adjacencyBetween {
			end = n.PrefixNodeID(end)
		}
	}
	var wantOut, wantIn bool
	switch kind {
	case adjacencyOutgoing, adjacencyBetween:
		wantOut = true
	case adjacencyIncoming:
		wantIn = true
	case adjacencyBoth:
		wantOut, wantIn = true, true
	default:
		return nil, nil, ErrInvalidData
	}
	// The dispatcher is the sole owner of ordering and partial-result policy.
	consume := func(read func(bool) ([]*Edge, error)) error {
		if wantOut {
			out, err = read(true)
			if err != nil {
				return err
			}
		}
		if wantIn {
			in, err = read(false)
		}
		return err
	}
	if version != nil {
		provider, ok := engine.(MVCCIndexedVisibilityEngine)
		if !ok {
			return nil, nil, ErrNotImplemented
		}
		if kind == adjacencyBoth {
			return nil, nil, ErrInvalidData
		}
		err = consume(func(outgoing bool) ([]*Edge, error) {
			if kind == adjacencyBetween {
				return provider.GetEdgesBetweenVisibleAt(start, end, *version)
			}
			if outgoing {
				return provider.GetOutgoingEdgesVisibleAt(start, *version)
			}
			return provider.GetIncomingEdgesVisibleAt(start, *version)
		})
	} else {
		native, isNative := engine.(*BadgerEngine)
		if isNative && kind != adjacencyBetween {
			reader, captureErr := native.captureAdjacencyReader(start, wantOut, wantIn, visibility)
			if captureErr != nil {
				err = captureErr
			} else {
				if reader.view != nil {
					defer reader.view.Close()
				}
				err = consume(reader.deliver)
				// End physical ownership before any namespace copying/projection.
				if reader.view != nil {
					reader.view.Close()
				}
			}
		} else {
			var combined AdjacentEdgesEngine
			if kind == adjacencyBoth {
				combined, _ = engine.(AdjacentEdgesEngine)
			}
			if combined != nil {
				out, in, err = combined.GetAdjacentEdges(start)
			} else {
				err = consume(func(outgoing bool) ([]*Edge, error) {
					if kind == adjacencyBetween {
						return engine.GetEdgesBetween(start, end)
					}
					if outgoing {
						return engine.GetOutgoingEdges(start)
					}
					return engine.GetIncomingEdges(start)
				})
			}
		}
	}

	if namespace != nil {
		if err != nil {
			return nil, nil, err
		}
		pinned := version != nil
		// Physical reader leases have ended before copying namespace projections.
		out = namespace.toUserEdgesInNamespace(out, false, pinned)
		in = namespace.toUserEdgesInNamespace(in, false, pinned)
	}
	return out, in, err
}
