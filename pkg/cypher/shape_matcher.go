package cypher

type ShapeKind string

const (
	shapeKindUnknown                                ShapeKind = "unknown"
	shapeKindCompoundCreateDeleteRel                ShapeKind = "compound_create_delete_rel"
	shapeKindCompoundPropCreateDeleteRel            ShapeKind = "compound_prop_create_delete_rel"
	shapeKindCompoundPropCreateDeleteReturnCountRel ShapeKind = "compound_prop_create_delete_return_count_rel"
)

type ShapeProbe struct {
	Matcher         string
	Matched         bool
	RejectReason    string
	NormalizedQuery string
	CapturedFields  map[string]string
}

type ShapeCapture struct {
	Name  string
	Value any
}

type ShapeCaptures struct {
	Ordered []ShapeCapture
	ByName  map[string]any
}

func NewShapeCaptures() ShapeCaptures {
	return ShapeCaptures{Ordered: make([]ShapeCapture, 0), ByName: make(map[string]any)}
}

func (c *ShapeCaptures) Add(name string, value any) {
	if c.ByName == nil {
		c.ByName = make(map[string]any)
	}
	c.Ordered = append(c.Ordered, ShapeCapture{Name: name, Value: value})
	c.ByName[name] = value
}

func (c ShapeCaptures) Get(name string) (any, bool) {
	if c.ByName == nil {
		return nil, false
	}
	value, ok := c.ByName[name]
	return value, ok
}

func (c ShapeCaptures) Any(name string) any {
	value, _ := c.Get(name)
	return value
}

func (c ShapeCaptures) String(name string) string {
	value, ok := c.Get(name)
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func (c ShapeCaptures) Int(name string) int {
	value, ok := c.Get(name)
	if !ok {
		return 0
	}
	intValue, _ := value.(int)
	return intValue
}

type ShapeMatch struct {
	Kind     ShapeKind
	Captures ShapeCaptures
	Probe    ShapeProbe
}
