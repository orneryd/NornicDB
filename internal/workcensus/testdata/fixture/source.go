package fixture

import (
	headers "census.fixture/meta"
	rx "regexp"
	"sort"
	text "strings"
)

var Sink int

type hiddenMap map[string]int
type hiddenAlias = hiddenMap

var schema = headers.Schema{Header: []string{"a"}}
var matcher = rx.MustCompile("a")

type walker interface{ Walk() }
type box[T any] struct{ values []T }

func (b *box[T]) Walk() {
	callback := func() {
		for range b.values {
			Sink++
		}
	}
	callback()
}

var registry = map[string]func(){
	"generic": func() { var w walker = &box[int]{values: []int{1, 2}}; w.Walk() },
}

func Execute() {
	for _, handler := range registry {
		handler()
	}
	_ = sourceConsumer("query")
	values := []int{2, 1}
	sort.Slice(values, func(i, j int) bool {
		for range values {
			Sink++
		}
		return values[i] < values[j]
	})
}
func sourceConsumer(query string) bool {
	_ = text.Split(query, ",")
	_ = splitAlias(query, ";")
	_ = matchAlias(query)
	for i := 0; i < len(query); i++ {
		Sink += int(query[i])
	}
	return matcher.MatchString(query)
}

type privateUnused struct{}

func (privateUnused) arbitraryUntouched() headers.Schema { return schema }
func arbitraryHeader() []string                          { return []string{"a"} }
func arbitraryMembership(m hiddenAlias, key string) bool { _, ok := m[key]; return ok }
func arbitraryKind(kind int) bool                        { return kind == 7 }

func boundExamples() {
	for fixedCounter := 0; fixedCounter < 3; fixedCounter++ {
		Sink += fixedCounter
	}
	for resetCounter := 0; resetCounter < 3; resetCounter++ {
		resetCounter = 0
		Sink++
	}
	for escapedCounter := 0; escapedCounter < 3; escapedCounter++ {
		escape(&escapedCounter)
	}
	for range []int{7: 1} {
		Sink++
	}
	values := make(map[int]int, 2)
	for key := range values {
		values[key+1] = key
	}
}
func escape(p *int) { *p = 0 }

func rangeFunction() {
	for value := range func(yield func(int) bool) { yield(1) } {
		Sink += value
	}
}

func runtimeHighView(n int) {
	backing := make([]byte, 1, n)
	window := backing[:n]
	for _, value := range window {
		Sink += int(value)
	}
}

func constantExpandedView() {
	backing := make([]byte, 1, 64)
	window := backing[:64]
	for _, value := range window {
		Sink += int(value)
	}
}

func uncertainValues(n int) []byte { return make([]byte, n) }

func uncertainProducerView(n int) {
	window := uncertainValues(n)
	for _, value := range window {
		Sink += int(value)
	}
}

func shadowedMakeView(n int) {
	make := func(short, count int) []byte { return uncertainValues(count) }
	window := make(1, n)
	for _, value := range window {
		Sink += int(value)
	}
}
