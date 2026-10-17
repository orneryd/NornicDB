package cypher

import (
	"fmt"
	"sort"
	"strconv"

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
// Every pair of a block shares the block's common things C. A block whose
// pairs are all eligible on C (or on the unshared flag) is counted from its
// sizes and document histograms, minus its excluded pairs. Its pairs that
// can rank above the base (those sharing a thing outside C, or an identifier
// key) are scored; every other pair of the block scores exactly the base with
// no identifiers, so the block's first topN eligible pairs in (a, b) order
// precede it: they score at least the base and come first among equals.
// Blocks whose pairs need a witness are enumerated from the thing postings.
// The count, the page and every score are exact; nothing is approximated.

// similarityPairsConfigKeys are nornicdb.similarity.pairs's configuration keys.
var similarityPairsConfigKeys = []string{"sources", "excludedPairs", "tieBreakProperty", "topK", "topN", "includeShared"}

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

// similarityPair is one scored candidate pair: parties by canonical rank.
type similarityPair struct {
	a, b        int
	similarity  float64
	identifiers []interface{}
}

// similarityPairsWork counts what the call did, so a caller can see that the
// pairs were not all scored.
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
	exclusionsChecked      int64
	candidates             int64
	pairScores             int64
}

// callNornicSimilarityPairs implements nornicdb.similarity.pairs(rows,
// configuration).
func callNornicSimilarityPairs(args []interface{}) (*ExecuteResult, error) {
	const procedure = "nornicdb.similarity.pairs"
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
	incidence, err := readSimilarityIncidence(items)
	if err != nil {
		return nil, err
	}
	pairs, reached, work := similarityPairsSelect(incidence, config)
	return &ExecuteResult{
		Columns: []string{"pairs", "reached", "capped", "work"},
		Rows:    [][]interface{}{{pairs, reached, reached > int64(config.topN), work.result()}},
	}, nil
}

func (w similarityPairsWork) result() map[string]interface{} {
	return map[string]interface{}{
		"atoms": w.atoms, "blocks": w.blocks, "denseBlocks": w.denseBlocks, "sparseBlocks": w.sparseBlocks,
		"countedCombinatorially": w.countedCombinatorially, "enumeratedPairs": w.enumeratedPairs,
		"residualContributions": w.residualContributions, "residualPairs": w.residualPairs,
		"identifierPairs": w.identifierPairs, "representatives": w.representatives,
		"exclusionsChecked": w.exclusionsChecked, "candidates": w.candidates, "pairScores": w.pairScores,
	}
}

func readSimilarityPairsConfig(options map[string]interface{}) (similarityPairsConfig, error) {
	var config similarityPairsConfig
	c, err := newGDSConfig(options, similarityPairsConfigKeys...)
	if err != nil {
		return config, err
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
	// common are the things every member's F has, ascending.
	common []int
}

// similarityBlock is the pairs between two compatible atoms, or within one.
type similarityBlock struct {
	// a and b are atom indexes, a <= b.
	a, b   int
	common []int
	// dense: every pair is eligible without a witness beyond common.
	dense bool
}

// similarityPairsSelect computes the page, the exact count and the work.
func similarityPairsSelect(incidence *similarityIncidence, config similarityPairsConfig) ([]interface{}, int64, similarityPairsWork) {
	var work similarityPairsWork
	parties := config.parties
	neighbors, _, things := similarityVectors(incidence, similarityRowsConfig{metric: "RESOURCE_ALLOCATION"})
	rank := make(map[string]int, len(parties))
	for r, party := range parties {
		rank[party.id] = r
		if s := sort.SearchStrings(incidence.sources, party.id); s < len(incidence.sources) && incidence.sources[s] == party.id {
			own := sort.SearchStrings(things, party.id)
			if own == len(things) || things[own] != party.id {
				own = -1
			}
			for _, edge := range neighbors[s] {
				if edge.target != own {
					party.features = append(party.features, edge)
				}
			}
		}
	}
	// Postings: thing → the ranks whose F has it, ascending.
	postings := make([][]int, len(things))
	for r, party := range parties {
		for _, edge := range party.features {
			postings[edge.target] = append(postings[edge.target], r)
		}
	}
	excluded := make(map[uint64]bool, len(config.excluded))
	for _, pair := range config.excluded {
		first, okFirst := rank[pair[0]]
		second, okSecond := rank[pair[1]]
		if okFirst && okSecond && first != second {
			excluded[similarityPairKey(first, second)] = true
		}
	}

	// Atoms.
	var atoms []*similarityAtom
	atomOf := make(map[string]int)
	for r, party := range parties {
		if len(party.keys) == 0 {
			continue
		}
		signature := fmt.Sprintf("%t\x00%t\x00%s\x00%q", party.hasClass, party.allowUnshared, party.class, party.keys)
		index, seen := atomOf[signature]
		if !seen {
			index = len(atoms)
			atomOf[signature] = index
			atoms = append(atoms, &similarityAtom{keys: party.keys, class: party.class, hasClass: party.hasClass, allow: party.allowUnshared, documents: make(map[string]int)})
		}
		party.atom = index
		atom := atoms[index]
		atom.members = append(atom.members, r)
		if party.hasDocument {
			atom.documents[party.document]++
		}
	}
	work.atoms = int64(len(atoms))
	support := make([]int, len(things))
	for _, atom := range atoms {
		for _, member := range atom.members {
			for _, edge := range parties[member].features {
				support[edge.target]++
			}
		}
		for _, member := range atom.members {
			for _, edge := range parties[member].features {
				if support[edge.target] == len(atom.members) {
					atom.common = append(atom.common, edge.target)
				}
				support[edge.target] = 0
			}
		}
		sort.Ints(atom.common)
	}

	// Blocks: compatible atom pairs, found through the partition keys.
	byKey := make(map[string][]int)
	for index, atom := range atoms {
		for _, key := range atom.keys {
			byKey[key] = append(byKey[key], index)
		}
	}
	blocks := make(map[uint64]*similarityBlock)
	var blockOrder []*similarityBlock
	addBlock := func(a, b int) {
		if a > b {
			a, b = b, a
		}
		key := similarityPairKey(a, b)
		if _, seen := blocks[key]; seen {
			return
		}
		left, right := atoms[a], atoms[b]
		if a == b && len(left.members) < 2 {
			return
		}
		if left.hasClass && right.hasClass && left.class != right.class {
			return
		}
		common := left.common
		if a != b {
			common = similarityIntersect(left.common, right.common)
		}
		block := &similarityBlock{a: a, b: b, common: common, dense: left.allow || right.allow || len(common) > 0}
		blocks[key] = block
		blockOrder = append(blockOrder, block)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		list := byKey[key]
		for i := range list {
			for j := i; j < len(list); j++ {
				addBlock(list[i], list[j])
			}
		}
	}
	work.blocks = int64(len(blockOrder))

	// sameDocument reports whether a pair is refused for its documents: two
	// sources that both need a witness, with one document key.
	sameDocument := func(a, b int) bool {
		pa, pb := parties[a], parties[b]
		return !pa.allowUnshared && !pb.allowUnshared && pa.hasDocument && pb.hasDocument && pa.document == pb.document
	}
	// otherSide is the block's atom facing a source of atom side.
	otherSide := func(block *similarityBlock, side int) int {
		if side == block.a {
			return block.b
		}
		return block.a
	}

	candidates := make(map[uint64]bool)
	addCandidate := func(a, b int) {
		if a > b {
			a, b = b, a
		}
		key := similarityPairKey(a, b)
		if !candidates[key] {
			candidates[key] = true
		}
	}
	var reached int64
	// seen marks the partners of the current first source; each first
	// source's visit has its own mark, so no reset is needed.
	seen := make([]int, len(parties))
	visits := 0
	// enumerate visits the block's pairs sharing a thing of F not in skip
	// (nil: any thing), each once, and calls visit for the eligible ones.
	enumerate := func(block *similarityBlock, skip []int, visit func(a, b int)) {
		sides := []int{block.a}
		if block.b != block.a {
			sides = append(sides, block.b)
		}
		for _, side := range sides {
			far := otherSide(block, side)
			for _, a := range atoms[side].members {
				visits++
				for _, edge := range parties[a].features {
					if skip != nil && similarityContains(skip, edge.target) {
						continue
					}
					for _, b := range postings[edge.target] {
						work.residualContributions++
						if b <= a || parties[b].atom != far || seen[b] == visits {
							continue
						}
						seen[b] = visits
						work.exclusionsChecked++
						if excluded[similarityPairKey(a, b)] || sameDocument(a, b) {
							continue
						}
						visit(a, b)
					}
				}
			}
		}
	}

	// Per-block count, residual and representative candidates.
	perParty := config.topK > 0 && config.topK < config.topN
	for _, block := range blockOrder {
		left, right := atoms[block.a], atoms[block.b]
		if !block.dense {
			work.sparseBlocks++
			enumerate(block, nil, func(a, b int) {
				reached++
				work.enumeratedPairs++
				addCandidate(a, b)
			})
			continue
		}
		work.denseBlocks++
		var count int64
		if block.a == block.b {
			n := int64(len(left.members))
			count = n * (n - 1) / 2
			if !left.allow {
				for _, m := range left.documents {
					count -= int64(m) * int64(m-1) / 2
				}
			}
		} else {
			count = int64(len(left.members)) * int64(len(right.members))
			if !left.allow && !right.allow {
				for document, m := range left.documents {
					count -= int64(m) * int64(right.documents[document])
				}
			}
		}
		reached += count
		work.countedCombinatorially += count
		// Pairs sharing a thing outside the common base can rank above it.
		enumerate(block, block.common, func(a, b int) {
			work.residualPairs++
			addCandidate(a, b)
		})
		// The first eligible pairs in (a, b) order stand in for the pairs
		// at exactly the base.
		limit := config.topN
		if perParty {
			limit = config.topK
		}
		similarityBlockPrefix(block, atoms, perParty, limit, func(a, b int) bool {
			work.exclusionsChecked++
			return !excluded[similarityPairKey(a, b)] && !sameDocument(a, b)
		}, func(a, b int) {
			work.representatives++
			addCandidate(a, b)
		})
	}
	// Excluded pairs counted in a dense block.
	for key := range excluded {
		a, b := int(key>>32), int(key&0xffffffff)
		pa, pb := parties[a], parties[b]
		if pa.atom < 0 || pb.atom < 0 {
			continue
		}
		low, high := pa.atom, pb.atom
		if low > high {
			low, high = high, low
		}
		block, ok := blocks[similarityPairKey(low, high)]
		if !ok || !block.dense || sameDocument(a, b) {
			continue
		}
		reached--
		work.countedCombinatorially--
	}

	// Pairs sharing an identifier key can rank above any base.
	byIdentifier := make(map[string][]int)
	for r, party := range parties {
		if party.atom < 0 {
			continue
		}
		seen := make(map[string]bool, len(party.identifiers))
		for _, key := range party.identifiers {
			if !seen[key] {
				seen[key] = true
				byIdentifier[key] = append(byIdentifier[key], r)
			}
		}
	}
	for _, holders := range byIdentifier {
		for i := range holders {
			for j := i + 1; j < len(holders); j++ {
				a, b := holders[i], holders[j]
				if candidates[similarityPairKey(a, b)] {
					continue
				}
				work.exclusionsChecked++
				if similarityPairEligible(parties, atoms, blocks, excluded, a, b) {
					work.identifierPairs++
					addCandidate(a, b)
				}
			}
		}
	}
	work.candidates = int64(len(candidates))

	// Score every candidate with the kernel, then select.
	scored := make([]similarityPair, 0, len(candidates))
	for key := range candidates {
		a, b := int(key>>32), int(key&0xffffffff)
		work.pairScores++
		similarity, _ := nodeSimilarity(parties[a].features, parties[b].features, "RESOURCE_ALLOCATION", true, things, nil, false)
		var identifiers []interface{}
		for i, key := range parties[a].identifiers {
			if parties[b].identifierSet[key] {
				identifiers = append(identifiers, similarityIdentifierValue(parties[a], i))
			}
		}
		scored = append(scored, similarityPair{a: a, b: b, similarity: similarity, identifiers: identifiers})
	}
	before := func(x, y similarityPair) bool {
		if len(x.identifiers) != len(y.identifiers) {
			return len(x.identifiers) > len(y.identifiers)
		}
		if x.similarity != y.similarity {
			return x.similarity > y.similarity
		}
		if x.a != y.a {
			return x.a < y.a
		}
		return x.b < y.b
	}
	sort.Slice(scored, func(i, j int) bool { return before(scored[i], scored[j]) })
	if config.topK > 0 {
		// Each first source keeps its best topK, as the partitions do.
		kept := scored[:0]
		taken := make(map[int]int)
		for _, pair := range scored {
			if taken[pair.a] < config.topK {
				taken[pair.a]++
				kept = append(kept, pair)
			}
		}
		scored = kept
	}
	if len(scored) > config.topN {
		scored = scored[:config.topN]
	}
	page := make([]interface{}, len(scored))
	for i, pair := range scored {
		pa, pb := parties[pair.a], parties[pair.b]
		identifiers := pair.identifiers
		if identifiers == nil {
			identifiers = []interface{}{}
		}
		conflicts := []interface{}{}
		if len(pa.identifiers) > 0 && len(pb.identifiers) > 0 && len(pair.identifiers) == 0 {
			conflicts = []interface{}{"identifiers differ"}
		}
		entry := map[string]interface{}{"node1": pa.id, "node2": pb.id, "similarity": pair.similarity, "identifiers": identifiers, "conflicts": conflicts}
		if config.includeShared {
			_, shared := nodeSimilarity(pa.features, pb.features, "RESOURCE_ALLOCATION", true, things, nil, true)
			entry["shared"] = incidence.explain(shared, config.tieBreak)
		}
		page[i] = entry
	}
	return page, reached, work
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

// similarityPairEligible is the full eligibility test, for a pair found
// outside its block's own enumeration.
func similarityPairEligible(parties []*similarityParty, atoms []*similarityAtom, blocks map[uint64]*similarityBlock, excluded map[uint64]bool, a, b int) bool {
	pa, pb := parties[a], parties[b]
	low, high := pa.atom, pb.atom
	if low > high {
		low, high = high, low
	}
	if _, ok := blocks[similarityPairKey(low, high)]; !ok {
		return false
	}
	if excluded[similarityPairKey(a, b)] {
		return false
	}
	if pa.allowUnshared || pb.allowUnshared {
		return true
	}
	if pa.hasDocument && pb.hasDocument && pa.document == pb.document {
		return false
	}
	return similarityShares(pa.features, pb.features)
}

// similarityBlockPrefix calls add for the block's first eligible pairs in
// (a, b) order: limit of them, or with perParty, the first limit of each
// first source.
func similarityBlockPrefix(block *similarityBlock, atoms []*similarityAtom, perParty bool, limit int, eligible func(a, b int) bool, add func(a, b int)) {
	left, right := atoms[block.a].members, atoms[block.b].members
	// The first sources: both atoms' members in rank order.
	firsts := left
	if block.a != block.b {
		firsts = similarityMerge(left, right)
	}
	total := 0
	for _, a := range firsts {
		partners := right
		if block.a != block.b && similarityContains(right, a) {
			partners = left
		}
		taken := 0
		for _, b := range partners[sort.SearchInts(partners, a+1):] {
			if !eligible(a, b) {
				continue
			}
			add(a, b)
			taken++
			total++
			if perParty && taken == limit || !perParty && total == limit {
				break
			}
		}
		if !perParty && total == limit {
			return
		}
	}
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
