// Command layout inspects closed benchmark stores without modifying them.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/experimental/columnar"
	codec "github.com/dgraph-io/badger/v4/experimental/nornic"
	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type totals struct{ Count, KeyBytes, ValueBytes, DuplicateValueBytes int64 }
type property struct {
	Count, EncodedBytes, LogicalBytes int64
	Distinct                          int
	UniqueLogicalBytes                int64
	EntropyBitsByte                   float64
	Families                          map[string]int64
	Modes                             map[string]int64
	seen                              map[[32]byte]int
	hist                              [256]uint64
}
type bucket struct{ Count, Bytes, Saved int64 }

func field(m map[string]msgpack.RawMessage, name string) msgpack.RawMessage {
	for key, value := range m {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return nil
}
func namespace(m map[string]msgpack.RawMessage) string {
	var id string
	_ = msgpack.Unmarshal(field(m, "ID"), &id)
	if at := strings.IndexByte(id, ':'); at >= 0 {
		return id[:at]
	}
	return ""
}
func group(m map[string]msgpack.RawMessage) string {
	var labels []string
	_ = msgpack.Unmarshal(field(m, "Labels"), &labels)
	for _, label := range labels {
		if label == "ProjectionDocument" {
			return "documents"
		}
	}
	return "northwind"
}

func logical(raw []byte) ([]byte, string, error) {
	if len(raw) == 0 {
		return nil, "empty", nil
	}
	switch raw[0] {
	case 0xc7, 0xc8, 0xc9, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8:
		decoder := msgpack.NewDecoder(bytes.NewReader(raw))
		id, length, err := decoder.DecodeExtHeader()
		if err != nil {
			return nil, "", err
		}
		if id != 60 {
			return raw, "ordinary", nil
		}
		payload := make([]byte, length)
		if err = decoder.ReadFull(payload); err != nil {
			return nil, "", err
		}
		if len(payload) == 0 {
			return nil, "", fmt.Errorf("empty extension")
		}
		if payload[0] == 1 {
			decoded, e := codec.DecodePayload(payload)
			return decoded, "zstd-property", e
		}
		if payload[0] == 2 {
			column, e := codec.OpenInt64(payload[1:])
			if e != nil {
				return nil, "", e
			}
			values := make([]int64, column.Len())
			if e = column.DecodeInto(values); e != nil {
				return nil, "", e
			}
			decoded, e := msgpack.Marshal(values)
			return decoded, "native-int64", e
		}
		return nil, "", fmt.Errorf("unknown extension mode")
	}
	return raw, "ordinary", nil
}

func run() error {
	dir := flag.String("dir", "", "closed Badger store")
	out := flag.String("out", "", "JSON output")
	flag.Parse()
	if *dir == "" || *out == "" {
		return fmt.Errorf("dir and out required")
	}
	opts := badger.DefaultOptions(*dir).WithReadOnly(true).WithLogger(nil).WithBlockCacheSize(32 << 20).WithIndexCacheSize(8 << 20)
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()
	names := map[string]map[uint64]string{}
	if err = db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte{0x21}); it.ValidForPrefix([]byte{0x21}); it.Next() {
			key := it.Item().Key()
			length, n := binary.Uvarint(key[1:])
			if n <= 0 || int(length) > len(key)-1-n {
				return fmt.Errorf("dictionary key")
			}
			ns := string(key[1+n : 1+n+int(length)])
			token, n := binary.Uvarint(key[1+n+int(length):])
			if n <= 0 {
				return fmt.Errorf("dictionary token")
			}
			value, e := it.Item().ValueCopy(nil)
			if e != nil {
				return e
			}
			if names[ns] == nil {
				names[ns] = map[uint64]string{}
			}
			names[ns][token] = string(value)
		}
		return nil
	}); err != nil {
		return err
	}
	families := map[string]*totals{}
	valueHashes := map[[32]byte]struct{}{}
	properties := map[string]*property{}
	propertyCounts := map[string]map[int]int64{}
	unsupported := map[string]int64{}
	add := func(scope, family, name string, raw []byte) error {
		decoded, mode, e := logical(raw)
		if e != nil {
			return e
		}
		key := scope + "." + name
		p := properties[key]
		if p == nil {
			p = &property{seen: map[[32]byte]int{}, Families: map[string]int64{}, Modes: map[string]int64{}}
			properties[key] = p
		}
		p.Count++
		p.EncodedBytes += int64(len(raw))
		p.LogicalBytes += int64(len(decoded))
		p.Families[family]++
		p.Modes[mode]++
		hash := sha256.Sum256(decoded)
		if _, ok := p.seen[hash]; !ok {
			p.seen[hash] = len(decoded)
			p.UniqueLogicalBytes += int64(len(decoded))
		}
		for _, b := range decoded {
			p.hist[b]++
		}
		return nil
	}
	err = db.View(func(tx *badger.Txn) error {
		io := badger.DefaultIteratorOptions
		io.PrefetchValues = false
		it := tx.NewIterator(io)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) == 0 {
				continue
			}
			family := fmt.Sprintf("0x%02x", key[0])
			s := families[family]
			if s == nil {
				s = &totals{}
				families[family] = s
			}
			s.Count++
			s.KeyBytes += int64(len(key))
			e := item.Value(func(value []byte) error {
				s.ValueBytes += int64(len(value))
				if len(value) >= 16 {
					hash := sha256.Sum256(value)
					if _, ok := valueHashes[hash]; ok {
						s.DuplicateValueBytes += int64(len(value))
					} else {
						valueHashes[hash] = struct{}{}
					}
				}
				if key[0] != 1 && key[0] != 12 {
					return nil
				}
				if key[0] == 1 {
					envelope, e := storage.SplitNodeEnvelope(value)
					if e != nil {
						unsupported[family]++
						return nil
					}
					meta := envelope.MetadataPayload()
					if len(meta) < 6 || !bytes.Equal(meta[:6], []byte{255, 'N', 'D', 'B', 1, 2}) {
						unsupported[family]++
						return nil
					}
					var m map[string]msgpack.RawMessage
					if e = msgpack.Unmarshal(meta[6:], &m); e != nil {
						return e
					}
					scope := group(m)
					ns := namespace(m)
					reader := bytes.NewReader(envelope.PropertyPayload())
					count, e := binary.ReadUvarint(reader)
					if e != nil {
						return e
					}
					if propertyCounts["current."+scope] == nil {
						propertyCounts["current."+scope] = map[int]int64{}
					}
					propertyCounts["current."+scope][int(count)]++
					decoder := msgpack.NewDecoder(reader)
					for i := uint64(0); i < count; i++ {
						token, e := binary.ReadUvarint(reader)
						if e != nil {
							return e
						}
						name := names[ns][token]
						if name == "" {
							return fmt.Errorf("unknown property token ns=%s token=%d", ns, token)
						}
						raw, e := decoder.DecodeRaw()
						if e != nil {
							return e
						}
						if e = add(scope, "current", name, raw); e != nil {
							return e
						}
					}
					if reader.Len() != 0 {
						return fmt.Errorf("trailing property bytes")
					}
				} else {
					var record map[string]msgpack.RawMessage
					if e := msgpack.Unmarshal(value, &record); e != nil {
						unsupported[family]++
						return nil
					}
					nodeRaw := field(record, "Node")
					if len(nodeRaw) == 0 || bytes.Equal(nodeRaw, []byte{0xc0}) {
						unsupported[family]++
						return nil
					}
					var m map[string]msgpack.RawMessage
					if e := msgpack.Unmarshal(nodeRaw, &m); e != nil {
						return e
					}
					var props map[string]msgpack.RawMessage
					if raw := field(m, "Properties"); len(raw) > 0 {
						if e := msgpack.Unmarshal(raw, &props); e != nil {
							return e
						}
					}
					scope := group(m)
					if propertyCounts["history."+scope] == nil {
						propertyCounts["history."+scope] = map[int]int64{}
					}
					propertyCounts["history."+scope][len(props)]++
					for name, raw := range props {
						if e := add(scope, "history", name, raw); e != nil {
							return e
						}
					}
				}
				return nil
			})
			if e != nil {
				return fmt.Errorf("family %s: %w", family, e)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, p := range properties {
		p.Distinct = len(p.seen)
		for _, n := range p.hist {
			if n > 0 {
				prob := float64(n) / float64(p.LogicalBytes)
				p.EntropyBitsByte -= prob * math.Log2(prob)
			}
		}
		p.seen = nil
	}
	file, err := os.Open(filepath.Join(*dir, "MANIFEST"))
	if err != nil {
		return err
	}
	manifest, _, err := badger.ReplayManifestFile(file, 0, opts)
	file.Close()
	if err != nil {
		return err
	}
	blocks := map[string]*bucket{}
	gains := map[string]*bucket{}
	tables := map[string]*bucket{}
	var indexBytes, bloomBytes, rawBlockBytes int64
	for id, entry := range manifest.Tables {
		path := filepath.Join(*dir, fmt.Sprintf("%06d.sst", id))
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if entry.KeyID != 0 {
			return fmt.Errorf("encrypted table not supported by format probe")
		}
		checksumLen := int(binary.BigEndian.Uint32(data[len(data)-4:]))
		indexEnd := len(data) - 4 - checksumLen - 4
		indexLen := int(binary.BigEndian.Uint32(data[indexEnd : indexEnd+4]))
		index := fb.GetRootAsTableIndex(data[indexEnd-indexLen:indexEnd], 0)
		indexBytes += int64(indexLen)
		bloomBytes += int64(index.BloomFilterLength())
		rawBlockBytes += int64(index.UncompressedSize())
		name := fmt.Sprintf("compression_%d_level_%d", entry.Compression, entry.Level)
		t := tables[name]
		if t == nil {
			t = &bucket{}
			tables[name] = t
		}
		t.Count++
		t.Bytes += int64(len(data))
		for i := 0; i < index.OffsetsLength(); i++ {
			var offset fb.BlockOffset
			if !index.Offsets(&offset, i) {
				return fmt.Errorf("block offset")
			}
			encoded := data[offset.Offset() : offset.Offset()+offset.Len()]
			name := fmt.Sprintf("codec_%d", entry.Compression)
			mode := byte(255)
			if entry.Compression == options.NornicColumnar {
				mode = encoded[0]
				name = fmt.Sprintf("columnar_mode_%d", mode)
			}
			b := blocks[name]
			if b == nil {
				b = &bucket{}
				blocks[name] = b
			}
			b.Count++
			b.Bytes += int64(len(encoded))
			if mode == 2 {
				frame, e := y.ZSTDDecompress(nil, encoded[1:])
				if e != nil {
					return e
				}
				raw, e := columnar.Restore(frame)
				if e != nil {
					return e
				}
				plain, e := y.ZSTDCompress(nil, raw, 1)
				if e != nil {
					return e
				}
				baseline := min(len(raw), len(plain)) + 1
				saved := baseline - len(encoded)
				b.Saved += int64(saved)
				fraction := float64(saved) / float64(baseline)
				category := ">12.5%"
				if fraction <= .01 {
					category = "<=1%"
				} else if fraction <= .05 {
					category = "1..5%"
				} else if fraction <= .125 {
					category = "5..12.5%"
				}
				g := gains[category]
				if g == nil {
					g = &bucket{}
					gains[category] = g
				}
				g.Count++
				g.Bytes += int64(len(encoded))
				g.Saved += int64(saved)
			}
		}
	}
	files := map[string]*bucket{}
	err = filepath.WalkDir(*dir, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		name := filepath.Ext(path)
		if name == "" {
			name = entry.Name()
		}
		s := files[name]
		if s == nil {
			s = &bucket{}
			files[name] = s
		}
		s.Count++
		s.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	result := map[string]any{"read_only": true, "families": families, "property_counts": propertyCounts, "properties": properties, "unsupported_node_records": unsupported, "files": files, "tables": tables, "blocks": blocks, "mode2_gain_buckets": gains, "table_index_bytes": indexBytes, "bloom_bytes_in_index": bloomBytes, "uncompressed_block_bytes": rawBlockBytes, "tables_info": db.Tables(), "note": "Duplicate bytes are logical upper bounds, not physical savings. Mode2 savings use same-block plain Zstd/raw counterfactual, level 1; no runtime policy was changed."}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(*out, data, 0600)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
