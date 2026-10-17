package cypher

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// nornicdb.similarity.pairs is the exact, filtered page of unordered source
// pairs by resource allocation, with the exact count of every eligible pair
// (N1). It reads the same {source, thing, weight, having} rows as
// nornicdb.similarity.stream, through the same reader, vectors and kernel,
// and adds the pair rules: sources given with partition keys, a class, a
// document key, identifier keys and whether they pair without a shared thing.
//
// A pair (a, b), a before b by tieBreakProperty then id, is eligible when its
// partition keys intersect, its classes are equal or either is null, it is
// not an excluded pair and, unless either source allows unshared pairs, its
// document keys differ and F(a) and F(b) intersect. F(p) is p's things without
// p itself, so F(a) ∩ F(b) is the pair's shared things without a or b.
// Pairs are ordered by the number of a's identifier keys b also has
// (descending), the score (descending), then a and b.
//
// Most eligible pairs are never scored. Sources with the same partition-key
// set, class and unshared flag form an atom; two compatible atoms (or one
// with itself) form a block, and every pair belongs to exactly one block.
// Every pair of a block shares the block's common things. A block whose pairs
// are all eligible on them (or on the unshared flag) is counted from its sizes
// and document histograms, minus its excluded pairs. Of its common things,
// those every member of both atoms holds with one weight (the uniform base)
// add the same contribution to every pair. A pair sharing anything beyond the
// uniform base, or an identifier key, is found and scored; every other pair of
// the block scores exactly the base with no identifiers, so the block's first
// topN eligible pairs in (a, b) order precede it: they score at least the base
// and come first among equals. Blocks whose pairs need a witness are
// enumerated from the thing postings.
//
// The work runs per canonical first source, with one reusable partner mark,
// into a bounded selection (topK per first source, topN overall), so the
// retained pairs never grow with the eligible count. It polls cancellation,
// and charges its working state to the statement's memory scope, which gets
// it back when the call ends. The count, the page and every score are exact.

// similarityPairsConfigKeys are nornicdb.similarity.pairs's configuration keys.
var similarityPairsConfigKeys = []string{"sources", "excludedPairs", "tieBreakProperty", "topK", "topN", "includeShared", "similarityMetric"}

// similarityPartyKeys are the keys of one sources entry.
var similarityPartyKeys = []string{"source", "partitionKeys", "classKey", "identifierKeys", "documentKey", "allowUnshared"}

// similarityParty is one source of nornicdb.similarity.pairs.
// Keys, the class, identifiers and the document are normalized texts
// (similarityKeyText). A class or document with its has flag false is
// unknown: an unknown class matches every class.
type similarityParty struct {
	id   string
	node interface{}
	// keys are the distinct partition keys, sorted.
	keys     []string
	class    string
	hasClass bool
	// identifiers are the identifier keys as given, order and duplicates kept.
	identifiers   []string
	identifierSet map[string]bool
	document      string
	hasDocument   bool
	allowUnshared bool
	// features is F(p): p's things without p itself, by thing index.
	features []gdsEdge
	// atom is p's atom; -1 without a partition key, pairing with nobody.
	atom int
	// position is p's index in its atom's members.
	position int
}

// similarityPairsConfig is nornicdb.similarity.pairs's configuration.
type similarityPairsConfig struct {
	// parties are in canonical order once read.
	parties       []*similarityParty
	excluded      [][2]string
	tieBreak      string
	topK, topN    int
	includeShared bool
}

// similarityPairsWork counts what the call did, so a caller can see that the
// pairs were not all scored and what it retained.
type similarityPairsWork struct {
	atoms                  int64
	blocks                 int64
	denseBlocks            int64
	sparseBlocks           int64
	countedCombinatorially int64
	enumeratedPairs        int64
	residualContributions  int64
	residualPairs          int64
	identifierPairs        int64
	representatives        int64
	partnerVisits          int64
	exclusionsChecked      int64
	candidates             int64
	pairScores             int64
	peakPairMaps           int64
}

// similarityPairsCancelMask is how often, in steps, the pair loops poll
// cancellation: often enough to stop within a few milliseconds.
const similarityPairsCancelMask = 1023

// Estimated bytes of the selector's working state, for memory accounting.
const (
	similarityEdgeBytes   = 16 // one gdsEdge
	similarityRankBytes   = 8  // one rank in a posting or member list
	similarityAtomBytes   = 192
	similarityBlockBytes  = 128
	similarityRecordBytes = 48 // one retained pair
)

// callNornicSimilarityPairs implements nornicdb.similarity.pairs(rows,
// configuration).
func callNornicSimilarityPairs(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "nornicdb.similarity.pairs"
	probe := newContextCancelProbe(ctx)
	if probe.cancelled() {
		return nil, probe.err()
	}
	value := procedureArgument(args, 0)
	if value == nil {
		return nil, procedureArgumentNullError(procedure, "rows")
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, procedureArgumentTypeError(procedure, "rows", "LIST<MAP>", value)
	}
	options, err := optionalProcedureMap(procedure, args, 1, "configuration")
	if err != nil {
		return nil, err
	}
	config, err := readSimilarityPairsConfig(options)
	if err != nil {
		return nil, err
	}
	// The working state is the call's: it goes back when the call ends,
	// whatever the outcome, and the result row is charged as kept.
	scope := memoryScopeFromContext(ctx)
	mark := scope.mark()
	account := memoryAccount{scope: scope}
	selector := &similarityPairsSelector{config: config, probe: &probe, account: &account}
	page, reached, err := selector.run(items)
	account.close()
	scope.releaseSince(mark)
	if err != nil {
		return nil, err
	}
	row := []interface{}{page, reached, reached > int64(config.topN), selector.work.result()}
	if scope != nil {
		if err := scope.grow(estimateResultRowBytes(row, scope.knownLists())); err != nil {
			return nil, err
		}
	}
	return &ExecuteResult{Columns: []string{"pairs", "reached", "capped", "work"}, Rows: [][]interface{}{row}}, nil
}

func (w similarityPairsWork) result() map[string]interface{} {
	return map[string]interface{}{
		"atoms": w.atoms, "blocks": w.blocks, "denseBlocks": w.denseBlocks, "sparseBlocks": w.sparseBlocks,
		"countedCombinatorially": w.countedCombinatorially, "enumeratedPairs": w.enumeratedPairs,
		"residualContributions": w.residualContributions, "residualPairs": w.residualPairs,
		"identifierPairs": w.identifierPairs, "representatives": w.representatives, "partnerVisits": w.partnerVisits,
		"exclusionsChecked": w.exclusionsChecked, "candidates": w.candidates, "pairScores": w.pairScores,
		"peakPairMaps": w.peakPairMaps,
	}
}

func readSimilarityPairsConfig(options map[string]interface{}) (similarityPairsConfig, error) {
	var config similarityPairsConfig
	c, err := newGDSConfig(options, similarityPairsConfigKeys...)
	if err != nil {
		return config, err
	}
	if metric, ok, err := c.text("similarityMetric"); err != nil {
		return config, err
	} else if ok && !strings.EqualFold(metric, "RESOURCE_ALLOCATION") {
		return config, localizedError(localization.CypherGraphProceduresSimilarityPairsMetric(metric), nil)
	}
	if config.tieBreak, _, err = c.text("tieBreakProperty"); err != nil {
		return config, err
	}
	topK, err := c.integer("topK", "Integer", 0, 1<<31-1, 0, false)
	if err != nil {
		return config, err
	}
	topN, err := c.integer("topN", "Integer", 1, 1<<31-1, 10, false)
	if err != nil {
		return config, err
	}
	config.topK, config.topN = int(topK), int(topN)
	if config.includeShared, _, err = c.boolean("includeShared"); err != nil {
		return config, err
	}
	value := c.value("sources")
	if value == nil {
		return config, localizedError(localization.CypherGraphProceduresConfigValueMissing("sources"), nil)
	}
	entries, isList := cypherListValue(value)
	if !isList {
		return config, c.typeError("sources", "List of Map")
	}
	seen := make(map[string]bool, len(entries))
	for index, entry := range entries {
		party, err := readSimilarityParty(index, entry)
		if err != nil {
			return config, err
		}
		if seen[party.id] {
			return config, localizedError(localization.CypherGraphProceduresSimilarityDuplicateSource(strconv.Itoa(index), party.id), nil)
		}
		seen[party.id] = true
		config.parties = append(config.parties, party)
	}
	if value := c.value("excludedPairs"); value != nil {
		pairs, isList := cypherListValue(value)
		if !isList {
			return config, c.typeError("excludedPairs", "List of List")
		}
		for index, pair := range pairs {
			ends, isList := cypherListValue(pair)
			if !isList || len(ends) != 2 {
				return config, localizedError(localization.CypherGraphProceduresConfigValueType(fmt.Sprintf("excludedPairs[%d]", index), "List of two Nodes", gdsJavaType(pair)), nil)
			}
			first, okFirst := gdsNodeID(ends[0])
			second, okSecond := gdsNodeID(ends[1])
			if !okFirst || !okSecond {
				return config, localizedError(localization.CypherGraphProceduresConfigValueType(fmt.Sprintf("excludedPairs[%d]", index), "List of two Nodes", gdsJavaType(pair)), nil)
			}
			config.excluded = append(config.excluded, [2]string{string(first), string(second)})
		}
	}
	// Canonical order: by tieBreakProperty of the source nodes, then id.
	sort.SliceStable(config.parties, func(i, j int) bool {
		return similarityPartyBefore(config.parties[i], config.parties[j], config.tieBreak)
	})
	return config, nil
}

// similarityPartyBefore orders two sources by tieBreakProperty, ascending as
// ORDER BY sorts it, then by id.
func similarityPartyBefore(a, b *similarityParty, property string) bool {
	if property != "" {
		left, right := similarityNodeProperty(a.node, property), similarityNodeProperty(b.node, property)
		if order := compareValuesForSort(left, right); order != 0 {
			return order < 0
		}
	}
	return a.id < b.id
}

// similarityNodeProperty is a node value's property; null for an id.
func similarityNodeProperty(node interface{}, property string) interface{} {
	if _, isID := node.(string); isID || isIntegerProcedureValue(node) {
		return nil
	}
	value, _ := rowPropertyValue(node, property)
	return value
}

// readSimilarityParty reads one sources entry.
func readSimilarityParty(index int, entry interface{}) (*similarityParty, error) {
	field := func(key string) string { return fmt.Sprintf("sources[%d].%s", index, key) }
	values, isMap := entry.(map[string]interface{})
	if !isMap || cypherValueKindOf(entry) != valueKindMap {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueType(fmt.Sprintf("sources[%d]", index), "Map", gdsJavaType(entry)), nil)
	}
	c, err := newGDSConfig(values, similarityPartyKeys...)
	if err != nil {
		return nil, err
	}
	typeError := func(key, expected string) error {
		return localizedError(localization.CypherGraphProceduresConfigValueType(field(key), expected, gdsJavaType(c.value(key))), nil)
	}
	party := &similarityParty{node: c.value("source"), atom: -1, identifierSet: make(map[string]bool)}
	if party.node == nil {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueMissing(field("source")), nil)
	}
	id, ok := gdsNodeID(party.node)
	if !ok {
		return nil, typeError("source", "Node")
	}
	party.id = string(id)
	keys, err := similarityKeyList(c.value("partitionKeys"), func() error { return typeError("partitionKeys", "List of String") })
	if err != nil {
		return nil, err
	}
	distinct := make(map[string]bool, len(keys))
	for _, key := range keys {
		if !distinct[key] {
			distinct[key] = true
			party.keys = append(party.keys, key)
		}
	}
	sort.Strings(party.keys)
	if party.identifiers, err = similarityKeyList(c.value("identifierKeys"), func() error { return typeError("identifierKeys", "List of String") }); err != nil {
		return nil, err
	}
	for _, key := range party.identifiers {
		party.identifierSet[key] = true
	}
	if value := c.value("classKey"); value != nil {
		if party.class, ok = similarityKeyText(value); !ok {
			return nil, typeError("classKey", "String")
		}
		party.hasClass = true
	}
	if value := c.value("documentKey"); value != nil {
		if party.document, ok = similarityKeyText(value); !ok {
			return nil, typeError("documentKey", "String")
		}
		party.hasDocument = true
	}
	if party.allowUnshared, _, err = c.boolean("allowUnshared"); err != nil {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueType(field("allowUnshared"), "Boolean", gdsJavaType(c.value("allowUnshared"))), nil)
	}
	return party, nil
}

// similarityKeyText normalizes an opaque key value: a string or an integer,
// kept apart by kind so "1" and 1 differ.
func similarityKeyText(value interface{}) (string, bool) {
	if text, isString := value.(string); isString {
		return "s" + text, true
	}
	if isIntegerProcedureValue(value) {
		return "i" + fmt.Sprint(value), true
	}
	return "", false
}

// similarityKeyList reads a list of opaque keys (null: none), in order.
func similarityKeyList(value interface{}, typeError func() error) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	items, isList := cypherListValue(value)
	if !isList {
		return nil, typeError()
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		key, ok := similarityKeyText(item)
		if !ok {
			return nil, typeError()
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// similarityAtom is the sources with one partition-key set, class and
// unshared flag.
type similarityAtom struct {
	// members are canonical ranks, ascending.
	members  []int
	keys     []string
	class    string
	hasClass bool
	allow    bool
	// documents counts the members per document key.
	documents map[string]int
	// nextOther is, for each member position, the next position whose
	// document differs (a member without a document differs from all).
	nextOther []int
	// common are the things every member's F has, ascending; commonWeight
	// is each one's weight when every member holds it with that weight, NaN
	// when the weights differ.
	common       []int
	commonWeight []float64
	// blocks are the blocks the atom is in; skip are the things uniform base
	// in every one of them, which its members never need to enumerate.
	blocks []*similarityBlock
	skip   []int
}

// similarityBlock is the pairs between two compatible atoms, or within one.
type similarityBlock struct {
	// a and b are atom indexes, a <= b.
	a, b int
	// dense: every pair is eligible without a witness beyond the common
	// things; count is its eligible pairs then.
	dense bool
	count int64
	// uniform is the uniform base: common things every member of both atoms
	// holds with one weight, ascending.
	uniform []int
	// docs: its pairs need different documents (neither atom allows unshared).
	docs bool
}

// similarityRecord is one scored pair: parties by canonical rank.
type similarityRecord struct {
	a, b        int
	similarity  float64
	identifiers int
}

// similarityRecordBefore is the page order: identifiers (descending), score
// (descending), then a and b.
func similarityRecordBefore(x, y similarityRecord) bool {
	if x.identifiers != y.identifiers {
		return x.identifiers > y.identifiers
	}
	if x.similarity != y.similarity {
		return x.similarity > y.similarity
	}
	if x.a != y.a {
		return x.a < y.a
	}
	return x.b < y.b
}

// similarityTop keeps the best limit records: a heap whose root is the worst
// kept.
type similarityTop struct {
	records []similarityRecord
	limit   int
}

func (t *similarityTop) Len() int           { return len(t.records) }
func (t *similarityTop) Less(i, j int) bool { return similarityRecordBefore(t.records[j], t.records[i]) }
func (t *similarityTop) Swap(i, j int)      { t.records[i], t.records[j] = t.records[j], t.records[i] }
func (t *similarityTop) Push(x interface{}) { t.records = append(t.records, x.(similarityRecord)) }
func (t *similarityTop) Pop() interface{} {
	last := t.records[len(t.records)-1]
	t.records = t.records[:len(t.records)-1]
	return last
}

// offer keeps record when it is among the best limit so far.
func (t *similarityTop) offer(record similarityRecord) {
	if len(t.records) < t.limit {
		heap.Push(t, record)
		return
	}
	if similarityRecordBefore(record, t.records[0]) {
		t.records[0] = record
		heap.Fix(t, 0)
	}
}

// best is the kept records, best first; the heap is emptied.
func (t *similarityTop) best() []similarityRecord {
	out := t.records
	sort.Slice(out, func(i, j int) bool { return similarityRecordBefore(out[i], out[j]) })
	t.records = nil
	return out
}

// similarityPairsSelector is one call's state.
type similarityPairsSelector struct {
	config  similarityPairsConfig
	probe   *contextCancelProbe
	account *memoryAccount
	work    similarityPairsWork
	steps   int

	parties  []*similarityParty
	things   []string
	postings [][]int
	excluded map[uint64]bool
	atoms    []*similarityAtom
	blocks   map[uint64]*similarityBlock
	// blockList is the blocks in the order they were found.
	blockList []*similarityBlock
}

// step counts one unit of work, polling cancellation now and then.
func (s *similarityPairsSelector) step() error {
	s.steps++
	if s.steps&similarityPairsCancelMask == 0 && s.probe.cancelled() {
		return s.probe.err()
	}
	return nil
}

// sameDocument reports whether a pair is refused for its documents: two
// sources that both need a witness, with one document key.
func (s *similarityPairsSelector) sameDocument(a, b int) bool {
	pa, pb := s.parties[a], s.parties[b]
	return !pa.allowUnshared && !pb.allowUnshared && pa.hasDocument && pb.hasDocument && pa.document == pb.document
}

// block is the block of two sources' atoms, nil when they never pair.
func (s *similarityPairsSelector) block(a, b int) *similarityBlock {
	x, y := s.parties[a].atom, s.parties[b].atom
	if x < 0 || y < 0 {
		return nil
	}
	return s.blocks[similarityPairKey(x, y)]
}

// run reads the rows and computes the page and the exact count.
func (s *similarityPairsSelector) run(items []interface{}) ([]interface{}, int64, error) {
	incidence, err := readSimilarityIncidenceAccounted(items, s.account)
	if err != nil {
		return nil, 0, err
	}
	if err := s.prepare(incidence); err != nil {
		return nil, 0, err
	}
	reached, err := s.count()
	if err != nil {
		return nil, 0, err
	}
	top, enumerated, err := s.choose()
	if err != nil {
		return nil, 0, err
	}
	reached += enumerated
	page := make([]interface{}, len(top))
	for i, record := range top {
		page[i] = s.entry(incidence, record)
	}
	return page, reached, nil
}

// prepare builds F per source, the postings, the exclusions, the atoms and
// the blocks.
func (s *similarityPairsSelector) prepare(incidence *similarityIncidence) error {
	s.parties = s.config.parties
	neighbors, _, things := similarityVectors(incidence, similarityRowsConfig{metric: "RESOURCE_ALLOCATION"})
	s.things = things
	edges := int64(0)
	for _, list := range neighbors {
		edges += int64(len(list))
	}
	if err := s.account.grow(edges*(similarityEdgeBytes+similarityRankBytes) + int64(len(things))*stringHeaderBytes); err != nil {
		return err
	}
	rank := make(map[string]int, len(s.parties))
	for r, party := range s.parties {
		rank[party.id] = r
		if i := sort.SearchStrings(incidence.sources, party.id); i < len(incidence.sources) && incidence.sources[i] == party.id {
			own := sort.SearchStrings(things, party.id)
			if own == len(things) || things[own] != party.id {
				own = -1
			}
			for _, edge := range neighbors[i] {
				if edge.target != own {
					party.features = append(party.features, edge)
				}
			}
		}
	}
	// Postings: thing → the ranks whose F has it, ascending.
	s.postings = make([][]int, len(things))
	entries := int64(0)
	for r, party := range s.parties {
		for _, edge := range party.features {
			s.postings[edge.target] = append(s.postings[edge.target], r)
		}
		entries += int64(len(party.features))
	}
	if err := s.account.grow(entries*(similarityEdgeBytes+similarityRankBytes) + int64(len(s.parties))*similarityRankBytes); err != nil {
		return err
	}
	s.excluded = make(map[uint64]bool, len(s.config.excluded))
	for _, pair := range s.config.excluded {
		first, okFirst := rank[pair[0]]
		second, okSecond := rank[pair[1]]
		if okFirst && okSecond && first != second {
			s.excluded[similarityPairKey(first, second)] = true
		}
	}
	if err := s.prepareAtoms(); err != nil {
		return err
	}
	return s.prepareBlocks()
}

// prepareAtoms groups the sources into atoms, with their documents and
// common things.
func (s *similarityPairsSelector) prepareAtoms() error {
	atomOf := make(map[string]int)
	for r, party := range s.parties {
		if len(party.keys) == 0 {
			continue
		}
		signature := fmt.Sprintf("%t\x00%t\x00%s\x00%q", party.hasClass, party.allowUnshared, party.class, party.keys)
		index, seen := atomOf[signature]
		if !seen {
			index = len(s.atoms)
			atomOf[signature] = index
			s.atoms = append(s.atoms, &similarityAtom{keys: party.keys, class: party.class, hasClass: party.hasClass, allow: party.allowUnshared, documents: make(map[string]int)})
			if err := s.account.grow(similarityAtomBytes); err != nil {
				return err
			}
		}
		atom := s.atoms[index]
		party.atom, party.position = index, len(atom.members)
		atom.members = append(atom.members, r)
		if party.hasDocument {
			atom.documents[party.document]++
		}
	}
	s.work.atoms = int64(len(s.atoms))
	support := make([]int, len(s.things))
	weight := make([]float64, len(s.things))
	for _, atom := range s.atoms {
		if err := s.account.grow(int64(len(atom.members)) * 2 * similarityRankBytes); err != nil {
			return err
		}
		// Next member position with a different document.
		atom.nextOther = make([]int, len(atom.members))
		for i := len(atom.members) - 1; i >= 0; i-- {
			atom.nextOther[i] = i + 1
			if i+1 < len(atom.members) {
				here, next := s.parties[atom.members[i]], s.parties[atom.members[i+1]]
				if here.hasDocument && next.hasDocument && here.document == next.document {
					atom.nextOther[i] = atom.nextOther[i+1]
				}
			}
		}
		for _, member := range atom.members {
			for _, edge := range s.parties[member].features {
				if support[edge.target] == 0 {
					weight[edge.target] = edge.weight
				} else if weight[edge.target] != edge.weight {
					weight[edge.target] = math.NaN()
				}
				support[edge.target]++
			}
		}
		for _, member := range atom.members {
			for _, edge := range s.parties[member].features {
				if support[edge.target] == len(atom.members) {
					atom.common = append(atom.common, edge.target)
				}
				support[edge.target] = 0
			}
		}
		sort.Ints(atom.common)
		atom.commonWeight = make([]float64, len(atom.common))
		for i, thing := range atom.common {
			atom.commonWeight[i] = weight[thing]
		}
	}
	return nil
}

// prepareBlocks finds the compatible atom pairs through the partition keys,
// with their common things, uniform base and density.
func (s *similarityPairsSelector) prepareBlocks() error {
	byKey := make(map[string][]int)
	for index, atom := range s.atoms {
		for _, key := range atom.keys {
			byKey[key] = append(byKey[key], index)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	s.blocks = make(map[uint64]*similarityBlock)
	for _, key := range keys {
		list := byKey[key]
		for i := range list {
			for j := i; j < len(list); j++ {
				if err := s.addBlock(list[i], list[j]); err != nil {
					return err
				}
			}
		}
	}
	s.work.blocks = int64(len(s.blocks))
	// A member enumerates no thing that is uniform base in all its blocks.
	for _, atom := range s.atoms {
		for i, block := range atom.blocks {
			if !block.dense {
				atom.skip = nil
				break
			}
			if i == 0 {
				atom.skip = block.uniform
			} else {
				atom.skip = similarityIntersect(atom.skip, block.uniform)
			}
		}
	}
	return nil
}

func (s *similarityPairsSelector) addBlock(a, b int) error {
	if a > b {
		a, b = b, a
	}
	key := similarityPairKey(a, b)
	if _, seen := s.blocks[key]; seen {
		return nil
	}
	left, right := s.atoms[a], s.atoms[b]
	if a == b && len(left.members) < 2 || left.hasClass && right.hasClass && left.class != right.class {
		return nil
	}
	block := &similarityBlock{a: a, b: b, docs: !left.allow && !right.allow}
	common := 0
	// Both atoms' common things, and the uniform base among them.
	for i, j := 0, 0; i < len(left.common) && j < len(right.common); {
		switch {
		case left.common[i] < right.common[j]:
			i++
		case right.common[j] < left.common[i]:
			j++
		default:
			common++
			if w := left.commonWeight[i]; !math.IsNaN(w) && w == right.commonWeight[j] {
				block.uniform = append(block.uniform, left.common[i])
			}
			i++
			j++
		}
	}
	block.dense = left.allow || right.allow || common > 0
	s.blocks[key] = block
	s.blockList = append(s.blockList, block)
	left.blocks = append(left.blocks, block)
	if a != b {
		right.blocks = append(right.blocks, block)
	}
	return s.account.grow(similarityBlockBytes + int64(len(block.uniform))*similarityRankBytes)
}

// count is the eligible pairs of the dense blocks, counted without
// enumerating them; the sparse blocks' pairs are counted as they are found.
func (s *similarityPairsSelector) count() (int64, error) {
	var reached int64
	for _, block := range s.blockList {
		if !block.dense {
			s.work.sparseBlocks++
			continue
		}
		s.work.denseBlocks++
		left, right := s.atoms[block.a], s.atoms[block.b]
		if block.a == block.b {
			n := int64(len(left.members))
			block.count = n * (n - 1) / 2
			if block.docs {
				for _, m := range left.documents {
					block.count -= int64(m) * int64(m-1) / 2
				}
			}
		} else {
			block.count = int64(len(left.members)) * int64(len(right.members))
			if block.docs {
				for document, m := range left.documents {
					block.count -= int64(m) * int64(right.documents[document])
				}
			}
		}
	}
	// Excluded pairs a dense block counted.
	for key := range s.excluded {
		a, b := int(key>>32), int(key&0xffffffff)
		if block := s.block(a, b); block != nil && block.dense && !s.sameDocument(a, b) {
			block.count--
		}
	}
	for _, block := range s.blockList {
		if block.dense {
			reached += block.count
			s.work.countedCombinatorially += block.count
		}
	}
	return reached, nil
}

// partners calls yield for the eligible partners b > a of source a in a
// block's far atom, in rank order, skipping a run of a's document at once,
// until yield returns false.
func (s *similarityPairsSelector) partners(a int, block *similarityBlock, yield func(b int) bool) error {
	party := s.parties[a]
	far := s.atoms[block.b]
	if party.atom == block.b && block.a != block.b {
		far = s.atoms[block.a]
	}
	position := sort.SearchInts(far.members, a+1)
	for position < len(far.members) {
		s.work.partnerVisits++
		if err := s.step(); err != nil {
			return err
		}
		b := far.members[position]
		other := s.parties[b]
		if block.docs && party.hasDocument && other.hasDocument && other.document == party.document {
			position = far.nextOther[position]
			continue
		}
		position++
		s.work.exclusionsChecked++
		if s.excluded[similarityPairKey(a, b)] {
			continue
		}
		if !yield(b) {
			return nil
		}
	}
	return nil
}

// representatives are, in global mode, each dense block's first topN
// eligible pairs in (a, b) order, by first source.
func (s *similarityPairsSelector) representatives() (map[int][]int, int64, error) {
	reps := make(map[int][]int)
	var stored int64
	for _, block := range s.blockList {
		if !block.dense || block.count == 0 {
			continue
		}
		firsts := s.atoms[block.a].members
		if block.a != block.b {
			firsts = similarityMerge(firsts, s.atoms[block.b].members)
		}
		total := 0
		for _, a := range firsts {
			if total == s.config.topN {
				break
			}
			err := s.partners(a, block, func(b int) bool {
				reps[a] = append(reps[a], b)
				total++
				return total < s.config.topN
			})
			if err != nil {
				return nil, 0, err
			}
		}
		stored += int64(total)
		if err := s.account.grow(int64(total) * similarityRankBytes); err != nil {
			return nil, 0, err
		}
	}
	s.work.representatives += stored
	return reps, stored, nil
}

// choose runs every first source's candidates through the bounded
// selection, and counts the sparse blocks' pairs.
func (s *similarityPairsSelector) choose() ([]similarityRecord, int64, error) {
	perParty := s.config.topK > 0 && s.config.topK < s.config.topN
	var reps map[int][]int
	var stored int64
	if !perParty {
		var err error
		if reps, stored, err = s.representatives(); err != nil {
			return nil, 0, err
		}
	}
	byIdentifier := make(map[string][]int)
	for r, party := range s.parties {
		if party.atom < 0 {
			continue
		}
		seen := make(map[string]bool, len(party.identifiers))
		for _, key := range party.identifiers {
			if !seen[key] {
				seen[key] = true
				byIdentifier[key] = append(byIdentifier[key], r)
				if err := s.account.grow(similarityRankBytes); err != nil {
					return nil, 0, err
				}
			}
		}
	}
	global := &similarityTop{limit: s.config.topN}
	local := &similarityTop{limit: s.config.topK}
	if err := s.account.grow(int64(len(s.parties)) * similarityRankBytes); err != nil {
		return nil, 0, err
	}
	seen := make([]int, len(s.parties))
	visit := 0
	var enumerated int64
	// peak records the retained pairs' high-water mark and charges each
	// retained record once; growErr is the first charge refused.
	var charged int64
	var growErr error
	peak := func() {
		held := int64(len(global.records) + len(local.records))
		if held > charged {
			if err := s.account.grow((held - charged) * similarityRecordBytes); err != nil && growErr == nil {
				growErr = err
			}
			charged = held
		}
		if retained := held + stored; retained > s.work.peakPairMaps {
			s.work.peakPairMaps = retained
		}
	}
	for a, party := range s.parties {
		if growErr != nil {
			return nil, 0, growErr
		}
		if party.atom < 0 || len(s.atoms[party.atom].blocks) == 0 {
			continue
		}
		visit++
		skip := s.atoms[party.atom].skip
		// consider scores b once for a when the pair is eligible.
		consider := func(b int, block *similarityBlock, witnessed bool) {
			if seen[b] == visit {
				return
			}
			seen[b] = visit
			s.work.exclusionsChecked++
			if s.excluded[similarityPairKey(a, b)] || s.sameDocument(a, b) {
				return
			}
			if !witnessed && block.docs && !similarityShares(party.features, s.parties[b].features) {
				return
			}
			if !block.dense {
				enumerated++
				s.work.enumeratedPairs++
			}
			s.work.candidates++
			s.work.pairScores++
			similarity, _ := nodeSimilarity(party.features, s.parties[b].features, "RESOURCE_ALLOCATION", true, s.things, nil, false)
			record := similarityRecord{a: a, b: b, similarity: similarity, identifiers: similarityIdentifierCount(party, s.parties[b])}
			if perParty {
				local.offer(record)
			} else {
				global.offer(record)
			}
			peak()
		}
		// Partners sharing a thing beyond their block's uniform base.
		for _, edge := range party.features {
			if similarityContains(skip, edge.target) {
				continue
			}
			posting := s.postings[edge.target]
			for _, b := range posting[sort.SearchInts(posting, a+1):] {
				s.work.residualContributions++
				if err := s.step(); err != nil {
					return nil, 0, err
				}
				block := s.block(a, b)
				if block == nil || block.dense && similarityContains(block.uniform, edge.target) {
					continue
				}
				before := s.work.candidates
				consider(b, block, true)
				if s.work.candidates > before {
					s.work.residualPairs++
				}
			}
		}
		// Partners sharing an identifier key.
		done := make(map[string]bool, len(party.identifiers))
		for _, key := range party.identifiers {
			if done[key] {
				continue
			}
			done[key] = true
			holders := byIdentifier[key]
			for _, b := range holders[sort.SearchInts(holders, a+1):] {
				if err := s.step(); err != nil {
					return nil, 0, err
				}
				if block := s.block(a, b); block != nil {
					before := s.work.candidates
					consider(b, block, false)
					if s.work.candidates > before {
						s.work.identifierPairs++
					}
				}
			}
		}
		// The representatives for the pairs at exactly the base.
		if perParty {
			for _, block := range s.atoms[party.atom].blocks {
				if !block.dense || block.count == 0 {
					continue
				}
				taken := 0
				err := s.partners(a, block, func(b int) bool {
					s.work.representatives++
					consider(b, block, true)
					taken++
					return taken < s.config.topK
				})
				if err != nil {
					return nil, 0, err
				}
			}
			for _, record := range local.best() {
				global.offer(record)
			}
		} else {
			for _, b := range reps[a] {
				if block := s.block(a, b); block != nil {
					consider(b, block, true)
				}
			}
		}
	}
	if growErr != nil {
		return nil, 0, growErr
	}
	return global.best(), enumerated, nil
}

// similarityIdentifierCount is the number of a's identifier keys, duplicates
// included, that b also has.
func similarityIdentifierCount(a, b *similarityParty) int {
	count := 0
	for _, key := range a.identifiers {
		if b.identifierSet[key] {
			count++
		}
	}
	return count
}

// entry is a page entry: the pair's ids, score, identifier list, conflicts
// and, asked for, the shared things.
func (s *similarityPairsSelector) entry(incidence *similarityIncidence, record similarityRecord) map[string]interface{} {
	pa, pb := s.parties[record.a], s.parties[record.b]
	identifiers := []interface{}{}
	for i, key := range pa.identifiers {
		if pb.identifierSet[key] {
			identifiers = append(identifiers, similarityIdentifierValue(pa, i))
		}
	}
	conflicts := []interface{}{}
	if len(pa.identifiers) > 0 && len(pb.identifiers) > 0 && len(identifiers) == 0 {
		conflicts = []interface{}{"identifiers differ"}
	}
	entry := map[string]interface{}{"node1": pa.id, "node2": pb.id, "similarity": record.similarity, "identifiers": identifiers, "conflicts": conflicts}
	if s.config.includeShared {
		_, shared := nodeSimilarity(pa.features, pb.features, "RESOURCE_ALLOCATION", true, s.things, nil, true)
		entry["shared"] = incidence.explain(shared, s.config.tieBreak)
	}
	return entry
}

// similarityIdentifierValue is a's i-th identifier key as given: the
// normalized text without its kind prefix, restored to its kind.
func similarityIdentifierValue(party *similarityParty, i int) interface{} {
	key := party.identifiers[i]
	if key[0] == 'i' {
		if number, err := strconv.ParseInt(key[1:], 10, 64); err == nil {
			return number
		}
	}
	return key[1:]
}

// similarityPairKey packs two ranks or atom indexes, low first.
func similarityPairKey(a, b int) uint64 {
	if a > b {
		a, b = b, a
	}
	return uint64(a)<<32 | uint64(b)
}

// similarityIntersect is the ascending intersection of two ascending lists.
func similarityIntersect(a, b []int) []int {
	var out []int
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] < b[j]:
			i++
		case b[j] < a[i]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

// similarityMerge is the ascending union of two disjoint ascending lists.
func similarityMerge(a, b []int) []int {
	out := make([]int, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	return append(append(out, a[i:]...), b[j:]...)
}

// similarityContains reports whether the ascending list holds value.
func similarityContains(list []int, value int) bool {
	i := sort.SearchInts(list, value)
	return i < len(list) && list[i] == value
}

// similarityShares reports whether two vectors share a thing.
func similarityShares(a, b []gdsEdge) bool {
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i].target < b[j].target:
			i++
		case b[j].target < a[i].target:
			j++
		default:
			return true
		}
	}
	return false
}
