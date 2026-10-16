package antlr

var BasePredictionContextEMPTY = &PredictionContext{
	cachedHash:  calculateEmptyHash(),
	pcType:      PredictionContextEmpty,
	returnState: BasePredictionContextEmptyReturnState,
}

// PredictionContextCache is Used to cache [PredictionContext] objects. It is used for the shared
// context cash associated with contexts in DFA states. This cache
// can be used for both lexers and parsers.
type PredictionContextCache struct {
	cache *JMap[*PredictionContext, *PredictionContext, Comparator[*PredictionContext]]
}

func NewPredictionContextCache() *PredictionContextCache {
	return &PredictionContextCache{
		cache: NewJMap[*PredictionContext, *PredictionContext, Comparator[*PredictionContext]](pContextEqInst, PredictionContextCacheCollection, "NewPredictionContextCache()"),
	}
}

// Add a context to the cache and return it. If the context already exists,
// return that one instead and do not add a new context to the cache.
// Protect shared cache from unsafe thread access.
func (p *PredictionContextCache) add(ctx *PredictionContext) *PredictionContext {
	return p.addWithWork(ctx, nil)
}
func (p *PredictionContextCache) addWithWork(ctx *PredictionContext, check func()) *PredictionContext {
	chargeWork(check)
	if ctx.isEmpty() {
		return BasePredictionContextEMPTY
	}

	// Put will return the existing entry if it is present (note this is done via Equals, not whether it is
	// the same pointer), otherwise it will add the new entry and return that.
	//
	operation := predictionContextOperation(check)
	existing, present := p.cache.GetWithOperation(ctx, operation)
	if present {
		return existing
	}
	p.cache.PutWithOperation(ctx, ctx, operation)
	return ctx
}

func (p *PredictionContextCache) Get(ctx *PredictionContext) (*PredictionContext, bool) {
	return p.getWithWork(ctx, nil)
}
func (p *PredictionContextCache) getWithWork(ctx *PredictionContext, check func()) (*PredictionContext, bool) {
	pc, exists := p.cache.GetWithOperation(ctx, predictionContextOperation(check))
	return pc, exists
}

func (p *PredictionContextCache) length() int {
	return p.cache.Len()
}
