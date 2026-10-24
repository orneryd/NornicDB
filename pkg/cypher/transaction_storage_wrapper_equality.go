package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

var _ storage.TransactionEqualityLookup = (*transactionStorageWrapper)(nil)

// LookupNodesByEquality keeps candidates in the transaction's database and
// converts only the matching results back to the caller's namespace view.
func (w *transactionStorageWrapper) LookupNodesByEquality(label string, properties map[string]interface{}) ([]*storage.Node, bool, error) {
	if w.namespace != "" {
		if err := w.tx.SetNamespace(w.namespace); err != nil {
			return nil, false, err
		}
	}
	nodes, complete, err := w.tx.LookupNodesByEquality(label, properties)
	if err != nil || !complete || w.namespace == "" {
		return nodes, complete, err
	}
	return w.toUserNamespacedNodes(nodes), true, nil
}
