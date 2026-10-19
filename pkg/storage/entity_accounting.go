package storage

import (
	"errors"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// StoredAccountingPairs is a commit's physical prior/final full-entity images.
// IDs are namespace-prefixed; nil means absent. Sidecars are included in nodes.
type StoredAccountingPairs struct {
	Prior TransactionChanges
	Final TransactionChanges
}

// AccountingOwner prepares a publication before a base-engine commit. The
// resulting callback receives exact commit-version images after publication.
// Managed commits instead publish the batch they already used for admission.
// A failed image read is reported to this same owner, never guessed as absence.
type AccountingOwner func() func(StoredAccountingPairs, error)

// RegisterAccountingOwner binds one database accounting owner to the physical
// engine. Namespace wrappers forward registration to their underlying engine.
func RegisterAccountingOwner(engine Engine, namespace string, owner AccountingOwner) {
	for engine != nil {
		if registrar, ok := engine.(interface{ RegisterAccountingOwner(string, AccountingOwner) }); ok {
			registrar.RegisterAccountingOwner(namespace, owner)
			return
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return
		}
		engine = wrapper.GetInnerEngine()
	}
}

func (b *BadgerEngine) RegisterAccountingOwner(namespace string, owner AccountingOwner) {
	b.accountingOwners.Store(namespace, owner)
}

// UseManagedAccounting transfers publication responsibility to the admitted
// batch owner. In particular replay must not charge a buffered ACK a second time.
func (tx *BadgerTransaction) UseManagedAccounting(publish func()) {
	tx.mu.Lock()
	tx.managedAccounting = publish
	tx.mu.Unlock()
}

type bufferedAccountingPublication struct {
	pairs   StoredAccountingPairs
	publish func(StoredAccountingPairs, error)
	err     error
}

func (tx *BadgerTransaction) prepareBufferedAccountingLocked() (*bufferedAccountingPublication, error) {
	if tx.managedAccounting != nil {
		return nil, nil
	}
	owner, ok := tx.engine.accountingOwners.Load(tx.namespace)
	if !ok {
		return nil, nil
	}
	publication := &bufferedAccountingPublication{publish: owner.(AccountingOwner)(), pairs: StoredAccountingPairs{Final: tx.pendingChangesLocked(true)}}
	for id, node := range publication.pairs.Final.Nodes {
		if node == nil {
			continue
		}
		image, err := tx.storedNodeImageLocked(node)
		if err != nil {
			return nil, err
		}
		publication.pairs.Final.Nodes[id] = image
	}
	for id, edge := range publication.pairs.Final.Edges {
		if edge == nil {
			continue
		}
		image, err := tx.storedEdgeImageLocked(edge)
		if err != nil {
			return nil, err
		}
		publication.pairs.Final.Edges[id] = image
	}
	return publication, nil
}

// The ACK gate makes the prior overlay and its appended replacement one
// indivisible accounting transition. Codec normalization only references keys
// already tokenized by the writes that produced those buffered images.
func (p *bufferedAccountingPublication) capturePriorLocked(tx *BadgerTransaction) {
	p.pairs.Prior = TransactionChanges{Nodes: make(map[NodeID]*Node), Edges: make(map[EdgeID]*Edge)}
	for id := range p.pairs.Final.Nodes {
		node, err := tx.engine.GetStoredNode(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err == nil {
			node, err = tx.storedNodeImageLocked(node)
		}
		if err != nil {
			p.err = err
			return
		}
		p.pairs.Prior.Nodes[id] = node
	}
	for id := range p.pairs.Final.Edges {
		edge, err := tx.engine.GetStoredEdge(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err == nil {
			edge, err = tx.storedEdgeImageLocked(edge)
		}
		if err != nil {
			p.err = err
			return
		}
		p.pairs.Prior.Edges[id] = edge
	}
}

func (cw *commitWriter) attachAccounting(publication *commitPublication) {
	groups := make(map[string][]any)
	for _, key := range publication.keys {
		var namespace string
		switch id := key.(type) {
		case NodeID:
			namespace = namespaceForNodeID(id)
		case EdgeID:
			namespace = namespaceForEdgeID(id)
		default:
			continue
		}
		if _, ok := cw.engine.accountingOwners.Load(namespace); ok {
			groups[namespace] = append(groups[namespace], key)
		}
	}
	if len(groups) == 0 {
		return
	}
	type delivery struct {
		keys    []any
		publish func(StoredAccountingPairs, error)
	}
	deliveries := make([]delivery, 0, len(groups))
	for namespace, keys := range groups {
		owner, _ := cw.engine.accountingOwners.Load(namespace)
		deliveries = append(deliveries, delivery{keys: keys, publish: owner.(AccountingOwner)()})
	}
	publish := publication.run
	publication.run = func() {
		publish()
		finalTs := publication.ts
		if cw.large != nil {
			finalTs = cw.large.lastTs
		}
		for _, delivery := range deliveries {
			prior, err := cw.engine.accountingImagesAt(publication.ts-1, delivery.keys)
			var final TransactionChanges
			if err == nil {
				final, err = cw.engine.accountingImagesAt(finalTs, delivery.keys)
			}
			if err != nil {
				cw.engine.log.Error("entity accounting image read failed", slog.Any("error", err))
			}
			delivery.publish(StoredAccountingPairs{Prior: prior, Final: final}, err)
		}
	}
}

// The caller's registered read keeps both boundary versions available through
// publication. The prior image is immediately before this commit's timestamp
// (the intent timestamp for a large commit), not its older read snapshot: a
// no-op policy check may have overlapped another writer and must not charge
// that writer's transition. Neither boundary includes any newer writer.
func (b *BadgerEngine) accountingImagesAt(ts uint64, keys []any) (TransactionChanges, error) {
	txn := b.db.newTxnAt(ts, false)
	defer txn.Discard()
	return b.accountingImagesInTxn(txn, keys)
}

func (b *BadgerEngine) accountingImagesInTxn(txn *badger.Txn, keys []any) (TransactionChanges, error) {
	images := TransactionChanges{Nodes: make(map[NodeID]*Node), Edges: make(map[EdgeID]*Edge)}
	for _, key := range keys {
		switch id := key.(type) {
		case NodeID:
			if _, found := images.Nodes[id]; found {
				continue
			}
			images.Nodes[id] = nil
			item, err := txn.Get(nodeKey(id))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return images, err
			}
			err = item.Value(func(data []byte) error {
				node, err := b.decodeNodeWithEmbeddings(txn, data, id)
				images.Nodes[id] = node
				return err
			})
			if err != nil {
				return images, err
			}
		case EdgeID:
			if _, found := images.Edges[id]; found {
				continue
			}
			images.Edges[id] = nil
			item, err := txn.Get(edgeKey(id))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return images, err
			}
			err = item.Value(func(data []byte) error {
				edge, err := b.decodeEdgeBodyByID(data, id)
				images.Edges[id] = edge
				return err
			})
			if err != nil {
				return images, err
			}
		}
	}
	return images, nil
}

// captureAccountingReset covers primitives that replace/drop a keyspace rather
// than commit individual records. Its caller excludes other namespace writers.
func (b *BadgerEngine) captureAccountingReset(prefix string, replacement bool) (func(), error) {
	type delivery struct {
		namespace string
		prior     TransactionChanges
		publish   func(StoredAccountingPairs, error)
	}
	var deliveries []delivery
	b.accountingOwners.Range(func(key, value any) bool {
		namespace := key.(string)
		if strings.HasPrefix(prefix, namespace+":") || strings.HasPrefix(namespace+":", prefix) {
			deliveries = append(deliveries, delivery{namespace: namespace, publish: value.(AccountingOwner)()})
		}
		return true
	})
	if len(deliveries) == 0 {
		return func() {}, nil
	}
	for i := range deliveries {
		err := b.db.View(func(txn *badger.Txn) error {
			var err error
			deliveries[i].prior, err = b.accountingSnapshotInTxn(txn, prefix, deliveries[i].namespace)
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	return func() {
		for _, delivery := range deliveries {
			var final TransactionChanges
			var err error
			if replacement {
				// Restore holds ordinary readers off until its dictionaries and
				// counts are ready; use its exclusive offline view here.
				txn := b.db.newTxnAt(b.db.oracle.published.Load(), false)
				final, err = b.accountingSnapshotInTxn(txn, prefix, delivery.namespace)
				txn.Discard()
			}
			delivery.publish(StoredAccountingPairs{Prior: delivery.prior, Final: final}, err)
		}
	}, nil
}

func (b *BadgerEngine) accountingSnapshotInTxn(txn *badger.Txn, prefix, namespace string) (TransactionChanges, error) {
	var keys []any
	for _, kind := range []byte{prefixNode, prefixEdge} {
		keyPrefix := append([]byte{kind}, []byte(prefix)...)
		it := txn.NewIterator(badgerPrefixIteratorOptions(keyPrefix))
		for it.Rewind(); it.ValidForPrefix(keyPrefix); it.Next() {
			id := string(it.Item().Key()[1:])
			if !strings.HasPrefix(id, namespace+":") {
				continue
			}
			if kind == prefixNode {
				keys = append(keys, NodeID(id))
			} else {
				keys = append(keys, EdgeID(id))
			}
		}
		it.Close()
	}
	return b.accountingImagesInTxn(txn, keys)
}
