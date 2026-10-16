package antlr

import "testing"

func TestCancellationContextEqualityAndMergeBranches(t *testing.T) {
	check := func() {}
	root := BasePredictionContextEMPTY
	a := NewBaseSingletonPredictionContext(root, 2)
	b := NewBaseSingletonPredictionContext(root, 3)
	array := NewArrayPredictionContext([]*PredictionContext{root, root}, []int{2, 3})
	if array.String() != "[2nil, 3nil]" {
		t.Fatal("legacy context formatting changed")
	}
	if predictionContextFromRuleContext(NewATN(ATNTypeParser, 2), nil) != root {
		t.Fatal("nil rule context changed")
	}
	prior := mergeSingletons(a, b, false, nil)
	singleCache := NewJPCMap(MergeCacheCollection, "singleton hit")
	singleCache.Put(a, b, prior)
	for _, pair := range [][2]*PredictionContext{{a, b}, {b, a}} {
		if mergeSingletonsWithWork(pair[0], pair[1], false, singleCache, check) != prior {
			t.Fatal("singleton cache lookup changed")
		}
	}
	for _, pair := range [][2]*PredictionContext{{a, b}, {root, a}, {a, root}, {array, a}, {a, array}, {array, root}, {root, array}} {
		for _, wildcard := range []bool{false, true} {
			expected := merge(pair[0], pair[1], wildcard, nil)
			cache := NewJPCMap(MergeCacheCollection, "context parity")
			actual := mergeWithWork(pair[0], pair[1], wildcard, cache, check)
			if !actual.EqualsWithWork(expected, check) {
				t.Fatal("merge semantics changed")
			}
			if !mergeWithWork(pair[0], pair[1], wildcard, cache, check).EqualsWithWork(expected, check) {
				t.Fatal("cache hit changed merge")
			}
			if !mergeWithWork(pair[1], pair[0], wildcard, cache, check).EqualsWithWork(expected, check) {
				t.Fatal("reverse cache hit changed merge")
			}
		}
	}
	// Equal return states with different parents require a recursively merged parent.
	left := NewBaseSingletonPredictionContext(a, 10)
	right := NewBaseSingletonPredictionContext(b, 10)
	replayRuntimeCancellation(t, func(check func()) {
		mergeWithWork(left, right, false, NewJPCMap(MergeCacheCollection, "nested"), check)
	})
	merged := mergeWithWork(left, right, false, nil, check)
	if merged.GetParent(0).length() != 2 {
		t.Fatal("nested merge lost a parent")
	}
	for _, context := range []*PredictionContext{root, a, array} {
		_ = context.EqualsWithWork(nil, check)
		_ = context.ArrayEquals(a)
		_ = context.SingletonEquals(array)
		_ = context.GetReturnStates()
	}
	var absent *PredictionContext
	if absent.Hash() != 0 || absent.EqualsWithWork(a, check) || array.EqualsWithWork(absent, check) {
		t.Fatal("nil context comparison changed")
	}
	// Force equal hashes to exercise equality's collision discrimination.
	for _, other := range []*PredictionContext{
		NewArrayPredictionContext([]*PredictionContext{root}, []int{2}),
		NewArrayPredictionContext([]*PredictionContext{root, root}, []int{2, 4}),
		NewArrayPredictionContext([]*PredictionContext{a, root}, []int{2, 3}),
	} {
		other.cachedHash = array.cachedHash
		if array.EqualsWithWork(other, check) {
			t.Fatal("array hash collision accepted")
		}
	}
	other := NewBaseSingletonPredictionContext(root, 3)
	other.cachedHash = a.cachedHash
	if a.EqualsWithWork(other, check) {
		t.Fatal("singleton hash collision accepted")
	}
	parents := &workSequence[*PredictionContext]{}
	states := &workSequence[int]{}
	for i := 0; i < 260; i++ {
		parents.Append(root, nil)
		states.Append(i+1, nil)
	}
	paged := arrayContextFromSequences(parents, states, check)
	if len(paged.GetReturnStates()) != 260 {
		t.Fatal("paged states materialization changed")
	}
	cache := NewPredictionContextCache()
	visited := NewVisitRecord()
	defer visited.Release()
	if getCachedBasePredictionContext(paged, cache, visited) != paged {
		t.Fatal("nil cache wrapper changed")
	}
	if cached, found := visited.Get(paged); !found || cached != paged {
		t.Fatal("visit wrapper lost value")
	}
	visited.Put(a, b)
	if cached, found := visited.Get(a); !found || cached != b {
		t.Fatal("visit wrapper changed")
	}
}

func TestCancellationCollectionCollisionLookups(t *testing.T) {
	comparator := integerCollisionComparator{}
	store := NewJStore[int, Comparator[int]](comparator, UnknownCollection, "lookup")
	mapping := NewJMap[int, int, Comparator[int]](comparator, UnknownCollection, "lookup")
	for i := 0; i < 200; i++ {
		store.Put(i)
		mapping.Put(i, i+1)
	}
	operation := &CollectionOperation[int]{Check: func() {}}
	if value, found := store.GetWithOperation(199, operation); !found || value != 199 {
		t.Fatal("store collision miss")
	}
	if _, found := store.GetWithOperation(201, operation); found {
		t.Fatal("store false hit")
	}
	if value, found := mapping.GetWithOperation(199, operation); !found || value != 200 {
		t.Fatal("map collision miss")
	}
	if _, found := mapping.GetWithOperation(201, operation); found {
		t.Fatal("map false hit")
	}
	if len(mapping.Values()) != 200 || mapping.valuesWithWork(func() {}).Len() != 200 {
		t.Fatal("map materialization changed")
	}
	replayRuntimeCancellation(t, func(check func()) { store.GetWithOperation(199, &CollectionOperation[int]{Check: check}) })
	replayRuntimeCancellation(t, func(check func()) { mapping.GetWithOperation(199, &CollectionOperation[int]{Check: check}) })
	if !store.Contains(100) {
		t.Fatal("legacy Contains changed")
	}
}

func TestCancellationPredicateConfigurationPaths(t *testing.T) {
	parser, _ := cancellationPrecedenceParser()
	state := NewBasicState()
	state.SetStateNumber(10)
	configs := NewATNConfigSet(false)
	alts := NewBitSet()
	for alt := 1; alt <= 100; alt++ {
		config := NewATNConfig6(state, alt, BasePredictionContextEMPTY)
		config.semanticContext = NewPredicate(0, alt, false)
		configs.Add(config, nil)
		alts.add(alt)
	}
	replayRuntimeCancellation(t, func(check func()) {
		parser.SetCancellationCheck(check)
		preds := parser.getPredsForAmbigAlts(alts, configs, 100)
		pairs := parser.getPredicatePredictions(alts, preds)
		if parser.evalSemanticContext(pairs, nil, true).length() != 100 {
			t.Fatal("predicate alternatives lost")
		}
		parser.splitAccordingToSemanticValidity(configs, nil)
	})
	parser.SetCancellationCheck(nil)
	config := configs.configs.At(0, nil)
	for _, full := range []bool{false, true} {
		for _, collect := range []bool{false, true} {
			if full {
				parser.input = cancellationTokenStream()
			}
			_ = parser.predTransition(config, NewPredicateTransition(state, 0, 0, false), collect, true, full)
			_ = parser.precedenceTransition(config, NewPrecedencePredicateTransition(state, 0), collect, true, full)
		}
	}
	plain := NewATNConfigSet(false)
	plain.Add(NewATNConfig6(state, 1, BasePredictionContextEMPTY), nil)
	dfa := NewDFAState(0, configs)
	configs.uniqueAlt = 1
	decision := NewBasicBlockStartState()
	for i := 0; i < 100; i++ {
		decision.AddTransition(NewEpsilonTransition(state, -1), -1)
	}
	parser.predicateDFAState(dfa, decision)
	if parser.getPredsForAmbigAlts(alts, plain, 100) != nil {
		t.Fatal("unpredicated alternatives changed")
	}
}
