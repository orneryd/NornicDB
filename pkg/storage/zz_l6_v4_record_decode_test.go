package storage

// Lane L6 (closeout): G-wal-v4-upgrade, the two open points of E4.
// Structured decode of the ORIGINAL (never opened) b6d8e762a async-Bolt
// journals with the pin's own historical reader (scanV4MigrationWAL ->
// decodeBufferedWALRecord) and the repair's own fact extraction
// (historicalDerivedKeyFactsFrom / historicalDerivedKeyProofComplete,
// wal_v4_buffered_key_repair.go:171, :272). For every record the repair
// refuses it logs: the damaged keys, each node bound without a body, whether
// that node's own derived key collapses to a damaged key of the record
// (collision-class membership), and which records of the same journal write
// that node's body (prefixNode key).

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const zzL6UpgRoot = "/work/tnrun/v152-upg"

func zzL6BE(num uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, num)
	return b
}

func zzL6HighBytes(num uint64) bool {
	for _, c := range zzL6BE(num) {
		if c >= 0x80 {
			return true
		}
	}
	return false
}

type zzL6Rec struct {
	seq  uint64
	data WALBufferedCommitData
}

func TestL6V4RecordDecode(t *testing.T) {
	root := os.Getenv("ZZL6_UPG_ROOT")
	if root == "" {
		root = zzL6UpgRoot
	}
	storesDir := filepath.Join(root, "b6", "uor", "stores")
	dirs, err := os.ReadDir(storesDir)
	if err != nil {
		t.Fatalf("stores: %v", err)
	}
	ran := 0
	for _, d := range dirs {
		if !strings.HasPrefix(d.Name(), "asyncbolt") {
			continue
		}
		name := d.Name()
		ran++
		t.Run(name, func(t *testing.T) {
			files, _ := filepath.Glob(filepath.Join(storesDir, name, "data", "wal", "*"))
			sort.Strings(files)
			var recs []zzL6Rec
			ops := map[OperationType]int{}
			for _, f := range files {
				if info, err := os.Stat(f); err != nil || info.IsDir() {
					continue
				}
				scanErr := scanV4MigrationWAL(f, true, true, func(entry WALEntry, _ *walPropertyEpochFrame, _, _ uint64) error {
					ops[entry.Operation]++
					if entry.Operation != OpBufferedCommit {
						return nil
					}
					data, err := decodeBufferedWALRecord(entry)
					if err != nil {
						t.Logf("L6_V4 %s seq=%d decode err=%v", name, entry.Sequence, err)
						return nil
					}
					recs = append(recs, zzL6Rec{seq: entry.Sequence, data: data})
					return nil
				})
				t.Logf("L6_V4_FILE %s %s scan_err=%v", name, filepath.Base(f), scanErr)
			}
			t.Logf("L6_V4_JOURNAL %s ops=%v buffered=%d", name, ops, len(recs))
			// Where each node body and binding is written in the journal.
			bodyAt := map[NodeID][]uint64{}
			bindAt := map[NodeID][]uint64{}
			for _, r := range recs {
				for key := range r.data.Writes {
					raw := []byte(key)
					if len(raw) < 2 {
						continue
					}
					switch raw[0] {
					case prefixNode:
						bodyAt[NodeID(key[1:])] = append(bodyAt[NodeID(key[1:])], r.seq)
					case prefixIDDictNodeForward:
						bindAt[NodeID(key[1:])] = append(bindAt[NodeID(key[1:])], r.seq)
					}
				}
			}
			refusedRecords := 0
			for _, r := range recs {
				if r.data.losslessPhysicalKeys || !historicalPhysicalKeysAmbiguous(r.data.Writes, r.data.Deletes) {
					continue
				}
				_, repairErr := repairV4BufferedCommitPhysicalKeys(r.data)
				damaged := map[string]bool{}
				for key := range r.data.Writes {
					if historicalPhysicalKeyAmbiguous(key) {
						damaged[key] = true
					}
				}
				facts, refusedFacts := historicalDerivedKeyFactsFrom(r.data.Writes, r.data.Deletes)
				refused := historicalDerivedKeyProofComplete(facts, damaged)
				families := map[string]int{}
				for key := range r.data.Writes {
					families[fmt.Sprintf("%02x", []byte(key)[0])]++
				}
				opTypes := map[string]int{}
				for _, op := range r.data.Ops {
					opTypes[fmt.Sprint(op.Type)]++
				}
				t.Logf("L6_V4_RECORD %s seq=%d repair_err=%v writes=%d deletes=%d key_families=%v ops=%v node_bodies=%d node_bindings=%d damaged=%d fact_refusals=%d proof_refusals=%d",
					name, r.seq, repairErr, len(r.data.Writes), len(r.data.Deletes), families, opTypes, len(facts.labels), len(facts.nodeNums), len(damaged), len(refusedFacts), len(refused))
				for key := range damaged {
					t.Logf("L6_V4_DAMAGED %s seq=%d family=%02x key_hex=%s", name, r.seq, []byte(key)[0], hex.EncodeToString([]byte(key)))
				}
				if repairErr == nil {
					continue
				}
				refusedRecords++
				for id, num := range facts.nodeNums {
					if _, hasBody := facts.labels[id]; hasBody {
						continue
					}
					// Collision-class membership: does this node's own derived key
					// (label index under any label a damaged key names, or the node
					// reverse dictionary entry) collapse to one of the damaged keys?
					member := []string{}
					for key := range damaged {
						raw := []byte(key)
						switch raw[0] {
						case prefixLabelIndex:
							sep := strings.IndexByte(key[1:], 0x00)
							if sep < 0 {
								continue
							}
							label := key[1 : 1+sep]
							if historicalPhysicalKeyJSONForm(string(labelIndexKey(label, num))) == key {
								member = append(member, "label:"+label)
							}
						case prefixIDDictNodeReverse:
							if historicalPhysicalKeyJSONForm(string(append([]byte{prefixIDDictNodeReverse}, encodeNumID(num)...))) == key {
								member = append(member, "node-reverse")
							}
						}
					}
					t.Logf("L6_V4_NOBODY %s seq=%d node=%q num=%d num_hex=%s num_has_byte>=0x80=%v in_damaged_class=%v body_written_in_seqs=%v binding_written_in_seqs=%v",
						name, r.seq, id, num, hex.EncodeToString(zzL6BE(num)), zzL6HighBytes(num), member, bodyAt[id], bindAt[id])
				}
				for id, labels := range facts.labels {
					t.Logf("L6_V4_BODY %s seq=%d node=%q labels=%v num=%d", name, r.seq, id, labels, facts.nodeNums[id])
				}
			}
			t.Logf("L6_V4_SUMMARY %s buffered=%d refused_records=%d", name, len(recs), refusedRecords)
		})
	}
	if ran == 0 {
		t.Fatalf("no asyncbolt stores under %s", storesDir)
	}
}
