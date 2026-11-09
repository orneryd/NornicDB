package storage

import (
	"bytes"
)

// Compatibility records retain at most one bounded per-record table. Epoch
// authority retains one active dictionary plan instead of per-record tables.
const walBindingCacheKeys = 4096
const walBindingCacheBytes = 128 << 10

// Epoch hits require the same immutable authority object and destination ID
// domain. Compatibility hits require exact table bytes and dictionary revision.
// Legacy records cannot consult either; neither supplies omitted authority.
type walPropertyImportPlan struct {
	epoch    *walPropertyEpochState
	table    []byte
	revision uint64
	domain   uint64
	bindings map[walPropertyToken]walBoundProperty
	drain    propKeyTxnDrain
	resolve  bool
	remap    bool
}

// Caller holds walPropertySource and an engine lifetime admission.
func (b *BadgerEngine) cachedWALPropertyPlan(table []byte, epoch *walPropertyEpochState) (walPropertyImportPlan, bool) {
	d := b.propKeyDict
	d.mu.RLock()
	defer d.mu.RUnlock()
	plan := b.walPropertySource.bindings
	if epoch != nil && len(table) > 0 && plan.epoch == epoch && plan.domain == d.walBindingDomain {
		// A new name or a persisted drain cannot change an existing meaning.
		// Refresh only the epoch's staged drain, never rebuild its binding map.
		if len(plan.drain.pending) != 0 && plan.revision != d.walBindingRevision {
			remaining := 0
			for _, entry := range plan.drain.pending {
				if !d.persisted[entry.namespace][entry.name] {
					remaining++
				}
			}
			if remaining == 0 {
				plan.drain = propKeyTxnDrain{}
			} else if remaining != len(plan.drain.pending) {
				pending := make([]propKeyPersistEntry, 0, remaining)
				for _, entry := range plan.drain.pending {
					if !d.persisted[entry.namespace][entry.name] {
						pending = append(pending, entry)
					}
				}
				plan.drain.pending = pending
			}
			plan.revision = d.walBindingRevision
			b.walPropertySource.bindings = plan
		}
		return plan, true
	}
	return plan, epoch == nil && plan.epoch == nil && len(table) > 0 && plan.revision == d.walBindingRevision && bytes.Equal(plan.table, table)
}

func (b *BadgerEngine) rememberWALPropertyPlan(table []byte, bindings map[walPropertyToken]walBoundProperty, drain propKeyTxnDrain, resolve, remap bool, revision, domain uint64, epoch *walPropertyEpochState) {
	if epoch == nil && (len(table) > walBindingCacheBytes || len(bindings) > walBindingCacheKeys) {
		b.walPropertySource.bindings = walPropertyImportPlan{}
		return
	}
	if epoch != nil {
		table = nil
	} else {
		table = bytes.Clone(table)
	}
	b.walPropertySource.bindings = walPropertyImportPlan{epoch: epoch, table: table, revision: revision, domain: domain, bindings: bindings, drain: drain, resolve: resolve, remap: remap}
}

func (d *propertyKeyDictionary) walRevision() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.walBindingRevision
}

func (d *propertyKeyDictionary) walBindingState() (uint64, uint64) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.walBindingRevision, d.walBindingDomain
}
