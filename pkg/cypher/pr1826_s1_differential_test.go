package cypher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// s1FullReadEngine hides every projection capability of the store it wraps: the
// executor over it reads whole nodes only. It is the oracle of these tests.
type s1FullReadEngine struct{ storage.Engine }

func (f s1FullReadEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	opts.Projection, opts.PropertyFilterProjection, opts.PropertyFilter, opts.PropertyStringEquals = nil, nil, nil, nil
	return f.Engine.StreamNodesWithOptions(ctx, opts, fn)
}

// s1Render is a deterministic text form of a result value. Lists are sorted
// when sortLists is set (collect() order follows the scan order).
func s1Render(v any, sortLists bool) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case *storage.Node:
		if x == nil {
			return "null"
		}
		labels := append([]string(nil), x.Labels...)
		sort.Strings(labels)
		return "Node" + fmt.Sprint(labels) + s1Render(x.Properties, sortLists)
	case storage.Node:
		return s1Render(&x, sortLists)
	case *storage.Edge:
		if x == nil {
			return "null"
		}
		return "Edge:" + x.Type + s1Render(x.Properties, sortLists)
	case string:
		if len(x) > 80 {
			sum := sha256.Sum256([]byte(x))
			return fmt.Sprintf("str%d:%s", len(x), hex.EncodeToString(sum[:6]))
		}
		return strconv.Quote(x)
	case []byte:
		return "bytes:" + hex.EncodeToString(x)
	case time.Time:
		return "time:" + x.Format(time.RFC3339Nano)
	case float64:
		return "f" + strconv.FormatFloat(x, 'g', -1, 64)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return "null"
		}
		return s1Render(rv.Elem().Interface(), sortLists)
	case reflect.Slice, reflect.Array:
		parts := make([]string, rv.Len())
		for i := range parts {
			parts[i] = s1Render(rv.Index(i).Interface(), sortLists)
		}
		if sortLists {
			sort.Strings(parts)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case reflect.Map:
		parts := make([]string, 0, rv.Len())
		for _, key := range rv.MapKeys() {
			parts = append(parts, fmt.Sprint(key.Interface())+":"+s1Render(rv.MapIndex(key).Interface(), sortLists))
		}
		sort.Strings(parts)
		return "{" + strings.Join(parts, ",") + "}"
	case reflect.Struct:
		if s, ok := v.(fmt.Stringer); ok {
			return rv.Type().Name() + "(" + s.String() + ")"
		}
		var parts []string
		for i := 0; i < rv.NumField(); i++ {
			if rv.Type().Field(i).IsExported() {
				parts = append(parts, rv.Type().Field(i).Name+"="+s1Render(rv.Field(i).Interface(), sortLists))
			}
		}
		if len(parts) == 0 {
			return fmt.Sprintf("%s(%v)", rv.Type().Name(), v)
		}
		return rv.Type().Name() + "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprintf("%T:%v", v, v)
}

// s1Run executes one statement and renders columns and rows, order-insensitive
// unless the statement orders its result.
func s1Run(exec *StorageExecutor, query string, params map[string]any) (out string) {
	defer func() {
		if p := recover(); p != nil {
			out = fmt.Sprintf("PANIC: %v", p)
		}
	}()
	result, err := exec.Execute(context.Background(), query, params)
	if err != nil {
		return "ERR: " + err.Error()
	}
	sortLists := strings.Contains(query, "/*S*/")
	rows := make([]string, len(result.Rows))
	for i, row := range result.Rows {
		cells := make([]string, len(row))
		for j, cell := range row {
			cells[j] = s1Render(cell, sortLists)
		}
		rows[i] = strings.Join(cells, "|")
	}
	if !strings.Contains(query, "/*O*/") {
		sort.Strings(rows)
	}
	return strings.Join(result.Columns, ",") + "\n" + strings.Join(rows, "\n")
}

type s1Match struct{ name, text string }

// s1Matches bind n (and some bind m) in the forms a read can seed from.
var s1Matches = []s1Match{
	{"inline", "MATCH (n:S1Doc {key:$key})"},
	{"whereEq", "MATCH (n:S1Doc) WHERE n.key = $key"},
	{"whereEqLit", "MATCH (n:S1Doc) WHERE n.key = 'b'"},
	{"range", "MATCH (n:S1Doc) WHERE n.rank >= 2"},
	{"rangeBoth", "MATCH (n:S1Doc) WHERE n.rank > 1 AND n.rank <= 6"},
	{"composite", "MATCH (n:S1Doc {category:'x', rank:2})"},
	{"compositeResidual", "MATCH (n:S1Doc) WHERE n.category='x' AND n.rank < 100 AND n.extra > 1"},
	{"prefix", "MATCH (n:S1Doc) WHERE n.name STARTS WITH 'Al'"},
	{"contains", "MATCH (n:S1Doc) WHERE n.name CONTAINS 'a'"},
	{"in", "MATCH (n:S1Doc) WHERE n.rank IN [1,3,5]"},
	{"notNull", "MATCH (n:S1Doc) WHERE n.rank IS NOT NULL"},
	{"isNull", "MATCH (n:S1Doc) WHERE n.rank IS NULL"},
	{"label", "MATCH (n:S1Doc)"},
	{"twoLabels", "MATCH (n:S1Doc:S1Extra)"},
	{"noLabel", "MATCH (n) WHERE n.key IN ['a','b']"},
	{"unwind", "UNWIND $keys AS k MATCH (n:S1Doc {key:k})"},
	{"unwindMap", "UNWIND $maps AS row MATCH (n:S1Doc {key:row.k}) WHERE n.rank <> row.r"},
	{"second", "MATCH (n:S1Doc {key:$key}) MATCH (m:S1Doc {category:n.category})"},
	{"cross", "MATCH (n:S1Doc {key:$key}), (m:S1Doc {key:'b'})"},
	{"crossDep", "MATCH (n:S1Doc {key:$key}), (m:S1Doc) WHERE m.category = n.category AND m.rank > n.rank"},
	{"repeated", "MATCH (n:S1Doc {category:'x'}), (n:S1Doc {rank:2})"},
	{"repeated3", "MATCH (n:S1Doc {key:'b'}), (m:S1Doc {key:'a'}), (n:S1Doc {extra:2})"},
	{"repeatedClause", "MATCH (n:S1Doc {key:'b'}) MATCH (n {extra:2})"},
	{"or", "MATCH (n:S1Doc) WHERE n.key='a' OR n.rank=3"},
	{"residualList", "MATCH (n:S1Doc) WHERE n.key IN ['a','b','c'] AND size(n.body) > 3 AND 't1' IN n.tags"},
	{"point", "MATCH (n:S1Doc) WHERE point.distance(n.loc, point({x:0.0,y:0.0})) < 10"},
	{"date", "MATCH (n:S1Doc) WHERE n.d >= date('2026-01-03')"},
	{"hint", "MATCH (n:S1Doc) USING INDEX n:S1Doc(key) WHERE n.key='a'"},
	{"elementId", "MATCH (n:S1Doc) WHERE elementId(n) = $eid"},
	{"any", "MATCH (n:S1Doc) WHERE any(t IN n.tags WHERE t='t1')"},
	{"exists", "MATCH (n:S1Doc) WHERE EXISTS { MATCH (n)-[:LINK]->(q) WHERE q.rank > n.rank }"},
	{"pattern", "MATCH (n:S1Doc {key:$key}) WHERE (n)-[:LINK]->()"},
	{"subscript", "MATCH (n:S1Doc) WHERE n['category'] = 'x'"},
	{"subscriptParam", "MATCH (n:S1Doc) WHERE n[$dyn] = 'x'"},
	{"hop", "MATCH (n:S1Doc {key:$key})-[r:LINK]->(m:S1Doc)"},
	{"hopIn", "MATCH (n:S1Doc)<-[r:LINK]-(m:S1Doc {key:$key})"},
	{"varHop", "MATCH (m:S1Doc {key:$key})-[:LINK*1..2]->(n:S1Doc)"},
	{"path", "MATCH p=(n:S1Doc {key:$key})-[:LINK]->(m)"},
	{"optional", "OPTIONAL MATCH (n:S1Doc {key:$key})"},
	{"optionalMissing", "OPTIONAL MATCH (n:S1Doc {key:'nope'})"},
	{"afterOptional", "MATCH (n:S1Doc {key:$key}) OPTIONAL MATCH (n)-[:LINK]->(m)"},
	{"withWhere", "MATCH (n:S1Doc) WITH n WHERE n.rank > 2"},
	{"withLimit", "MATCH (n:S1Doc) WITH n ORDER BY n.key LIMIT 3"},
	{"rename", "MATCH (z:S1Doc {key:$key}) WITH z AS n"},
	{"renameTwice", "MATCH (z:S1Doc {key:$key}) WITH z AS y, z.rank AS rr WITH y AS n WHERE n.rank = 1 OR n.rank > 1"},
	{"collected", "MATCH (z:S1Doc) WITH collect(z) AS zs UNWIND zs AS n WITH n"},
	{"collectedIndexed", "MATCH (z:S1Doc) WHERE z.rank IN [1,2] WITH collect(z) AS zs UNWIND zs AS n WITH n"},
	{"mapWrapped", "MATCH (z:S1Doc {key:$key}) WITH {v:z} AS w WITH w.v AS n"},
	{"listWrapped", "MATCH (z:S1Doc {key:$key}) WITH [z] AS w WITH w[0] AS n"},
	{"caseWrapped", "MATCH (z:S1Doc {key:$key}) WITH CASE WHEN z.rank > 0 THEN z ELSE null END AS n"},
	{"coalesced", "OPTIONAL MATCH (q:S1Doc {key:'nope'}) MATCH (z:S1Doc {key:$key}) WITH coalesce(q,z) AS n"},
	{"callSub", "CALL { MATCH (n:S1Doc {key:'a'}) RETURN n } WITH n"},
	{"callImport", "MATCH (z:S1Doc {key:$key}) CALL (z) { MATCH (n:S1Doc {category:z.category}) RETURN n } WITH n"},
	{"callUnion", "CALL { MATCH (n:S1Doc {key:'a'}) RETURN n UNION MATCH (n:S1Doc {key:'b'}) RETURN n } WITH n"},
	{"fulltext", "CALL db.index.fulltext.queryNodes('s1ft','Alpha') YIELD node AS n, score WITH n"},
	{"vector", "CALL db.index.vector.queryNodes('s1vec', 3, [0.1,0.2,0.3]) YIELD node AS n, score WITH n"},
	{"procCross", "CALL db.labels() YIELD label WITH label WHERE label='S1Doc' MATCH (n:S1Doc {key:$key})"},
	{"backtick", "MATCH (`n`:`S1Doc` {`key`:$key})"},
	{"comment", "MATCH /* n.body */ (n:S1Doc /*x*/ {key:$key}) // tail\n"},
	{"let", "MATCH (n:S1Doc {key:$key}) LET zz = n.rank"},
	{"filter", "MATCH (n:S1Doc) FILTER n.rank > 2"},
}

// s1Returns are the read tails over n. /*S*/ sorts lists before comparing,
// /*O*/ keeps the row order.
var s1Returns = []string{
	"RETURN n.key", "RETURN n.name AS name, n.rank AS rank", "RETURN n.body", "RETURN n", "RETURN n.key, n", "RETURN n AS whole, n.name AS nm",
	"RETURN *", "RETURN n{.key,.name}", "RETURN n{.*}", "RETURN n{.*, up: n.rank + 1}", "RETURN n{.key, b: n.body, nested: n{.rank}}", "RETURN {a:n.name, b:n.rank}", "RETURN {a:n}", "RETURN [n.name, n.rank]", "RETURN [n]",
	"RETURN properties(n)", "/*S*/ RETURN keys(n)", "RETURN properties(n).name", "/*S*/ RETURN [p IN keys(n) | n[p]]", "/*S*/ RETURN labels(n)", "RETURN n:S1Extra", "RETURN elementId(n)", "RETURN id(n) = id(n)",
	"RETURN n.missing", "RETURN n.key, n.missing, n.`weird key ü`", "RETURN n . name", "RETURN n.`name`", "RETURN n['name']", "RETURN n[$dyn]", "RETURN n[toLower('NAME')]",
	"RETURN n.key AS k, n.rank AS r /*O*/ ORDER BY r DESC, k", "RETURN n.key AS k /*O*/ ORDER BY size(n.body), n.key", "RETURN n.key AS k /*O*/ ORDER BY n.name DESC, n.key SKIP 1 LIMIT 2", "RETURN n.name /*O*/ ORDER BY n.key LIMIT 3", "RETURN DISTINCT n.category", "RETURN DISTINCT n",
	"RETURN n.key AS k /*O*/ ORDER BY n.d, n.key", "RETURN n.key AS n", "WITH n.name AS n RETURN n", "RETURN n.key AS x, [n IN [1,2] | n] AS y", "RETURN any(n IN ['q'] WHERE n = 'q'), n.name",
	"RETURN count(n)", "RETURN count(*)", "RETURN count(DISTINCT n.category)", "RETURN count(n.body)", "/*S*/ RETURN collect(n.key)", "/*S*/ RETURN collect(n)", "/*S*/ RETURN collect(DISTINCT n.category)", "/*S*/ RETURN size(collect(n))", "/*S*/ RETURN [x IN collect(n) | x.name]", "/*S*/ RETURN [x IN collect(n) WHERE x.rank > 1 | x.body]", "/*S*/ RETURN collect(n{.name})", "/*S*/ RETURN collect({k:n.key, v:n.rank})",
	"RETURN n.category AS c, count(*) AS t, sum(n.rank) AS s, min(n.name) AS mn, max(n.d) AS mx, avg(n.f) AS av", "RETURN n.category AS c, max(size(n.body))", "/*S*/ RETURN n.category AS c, collect(n.name)",
	"WITH n /*O*/ ORDER BY n.key RETURN head(collect(n)).name", "WITH n ORDER BY n.key RETURN last(collect(n)).body", "WITH n ORDER BY n.key WITH collect(n) AS ns RETURN ns[0].name, ns[-1].rank, size(ns)", "WITH n ORDER BY n.key WITH collect(n) AS ns RETURN [x IN tail(ns) | x.key]", "WITH n ORDER BY n.key WITH collect(n) AS ns RETURN reverse(ns)[0].name", "WITH n ORDER BY n.key WITH collect(n) AS ns RETURN reduce(s='', x IN ns | s + coalesce(x.name,'-'))", "WITH n ORDER BY n.key WITH collect(n)[0..2] AS ns UNWIND ns AS u RETURN u.name",
	"RETURN n.tags[0]", "RETURN n.tags", "RETURN size(n.tags)", "RETURN reduce(s='', x IN n.tags | s + x)", "RETURN [x IN n.nums WHERE x > 1]", "RETURN n.fl, n.vec, n.points, n.dates", "RETURN n.emptyList, n.empty", "RETURN n.bytes", "RETURN n.nested", "RETURN n.flag, n.f, n.neg",
	"RETURN n.loc, n.loc.x, n.geo.latitude", "RETURN point.distance(n.loc, point({x:1.0,y:1.0}))", "RETURN n.d, n.d.year, n.dt, n.ldt, n.tm, n.lt, n.dur", "RETURN n.dur.days, n.dt.epochSeconds", "RETURN n.cvec", "RETURN n.has_embedding", "RETURN n.embedding", "RETURN n.createdAt",
	"RETURN coalesce(n.missing, n.name)", "RETURN CASE WHEN n.rank > 1 THEN n.name ELSE n.category END", "RETURN CASE n.category WHEN 'x' THEN n.body ELSE n.missing END", "RETURN CASE n.category WHEN 'x' THEN n END", "RETURN n.rank IS NULL, n.missing IS NULL, n IS NULL", "RETURN n.name =~ 'A.*'", "RETURN toUpper(n.name) + '-' + toString(n.rank)", "RETURN n.rank + size(n.body) * 2", "RETURN n = n", "RETURN n IN [n]", "RETURN [n, 1][0].name", "RETURN {v:n}.v.name", "RETURN toJson(n.name)", "RETURN valueType(n)", "RETURN valueType(n.rank), valueType(n.vec)",
	"RETURN vector.similarity.cosine(n.vec, [0.1,0.2,0.3])", "RETURN apoc.convert.toJson(n.tags)", "RETURN apoc.convert.toJson(n)", "RETURN apoc.any.properties(n)", "RETURN apoc.map.fromPairs([['k', n.key]])", "RETURN apoc.node.degree(n)", "RETURN apoc.map.merge(properties(n), {z:1})", "RETURN apoc.text.join(n.tags, '+')",
	"WITH n RETURN n.name", "WITH n AS q RETURN q.body", "WITH n AS q, n.rank AS r WHERE r > 0 RETURN q.name, r", "WITH n AS q, n AS w RETURN q.name, w.rank", "WITH * RETURN n.name", "WITH n, n.rank + 1 AS r RETURN n.key, r", "WITH DISTINCT n RETURN n.rank", "WITH DISTINCT n.category AS c RETURN c", "WITH {a:n} AS q RETURN q.a.name", "WITH [n] AS q RETURN q[0].name", "UNWIND [n] AS u RETURN u.name", "UNWIND n.tags AS t RETURN t, n.key", "UNWIND keys(n) AS kk RETURN kk, n[kk]", "WITH n.category AS c, collect(n) AS ns RETURN c, size(ns)", "/*S*/ WITH n.category AS c, collect(n) AS ns RETURN c, [x IN ns | x.body]", "WITH n, count(*) AS c RETURN n.key, c", "WITH n, count(*) AS c RETURN n, c", "WITH (n) AS q RETURN (q).name", "WITH properties(n) AS pp RETURN pp.name, pp.rank", "WITH n{.*} AS pp RETURN pp.body", "WITH n.key AS key, n WHERE n.extra > 0 RETURN key",
	"CALL (n) { RETURN n.name AS nm } RETURN nm", "CALL { WITH n RETURN n.body AS b } RETURN b", "CALL (n) { MATCH (n)-[:LINK]->(q) RETURN q.name AS qn } RETURN n.key, qn", "CALL (n) { RETURN n.name AS v UNION RETURN n.category AS v } RETURN v", "CALL (n) { RETURN n AS inner } RETURN inner.body", "CALL (*) { RETURN n.rank AS rk } RETURN rk", "CALL (n) { WITH n.rank AS r RETURN r * 2 AS d } RETURN d, n.name", "OPTIONAL CALL (n) { MATCH (n)-[:NOPE]->(q) RETURN q } RETURN n.name, q",
	"RETURN EXISTS { (n)-[:LINK]->() }, n.key", "RETURN COUNT { (n)-[:LINK]->() } AS c, n.key", "/*S*/ RETURN COLLECT { MATCH (n)-[:LINK]->(q) RETURN q.name } AS c", "/*S*/ RETURN [(n)-[:LINK]->(q) | q.name] AS c", "/*S*/ RETURN [(n)-[:LINK]->(q) WHERE q.rank > n.rank | n.name + q.name] AS c", "RETURN EXISTS { MATCH (q:S1Doc) WHERE q.rank = n.extra } AS e", "RETURN COUNT { MATCH (q:S1Doc) WHERE q.category = n.category } AS e", "RETURN size([(n)-[:LINK]->() | 1]) AS s",
	"MATCH (n)-[:LINK]->(q) RETURN n.name, q.name", "MATCH (q:S1Doc {category:n.category}) RETURN n.key, q.key", "OPTIONAL MATCH (q:S1Doc {rank:n.extra}) RETURN n.key, q.name", "MATCH (q:S1Doc) WHERE q.rank = n.rank + 1 RETURN n.key, q.body", "MATCH (n) WHERE n.extra IS NOT NULL RETURN n.key", "MATCH (n:S1Extra) RETURN n.key", "MATCH (n {category:'x'}) RETURN n.name",
	"FILTER n.rank > 1 RETURN n.name", "LET q = n.body RETURN q, n.key", "RETURN n.name UNION MATCH (q:S1Doc {key:'c'}) RETURN q.name AS `n.name`", "RETURN n.key FINISH",
	"CALL apoc.when(n.rank > 1, 'RETURN $v AS v', 'RETURN 0 AS v', {v:n.name}) YIELD value RETURN value.v", "CALL apoc.cypher.run('WITH $x AS x RETURN x.body AS b', {x:n}) YIELD value RETURN value.b", "CALL db.propertyKeys() YIELD propertyKey WITH n, count(propertyKey) AS pk RETURN n.key, pk > 0",
	"RETURN n.key AS k, n.name AS nm /*O*/ ORDER BY nm, k SKIP toInteger(0) LIMIT toInteger('5')", "RETURN -n.rank, NOT n.flag", "RETURN n.rank IN [1,2], n.category IN n.tags", "RETURN n.key STARTS WITH n.key", "RETURN all(x IN n.tags WHERE x <> n.name)", "RETURN $key = n.key", "RETURN n.`key`, n . `rank`", "RETURN n.KEY", "RETURN N.key",
}

const s1Seeds = 14

// s1Seed builds the corpus with fixed ids.
func s1Seed(t *testing.T, store storage.Engine, schema string) *StorageExecutor {
	t.Helper()
	ctx := context.Background()
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	var ddl []string
	switch schema {
	case "none":
	case "range":
		ddl = []string{"CREATE INDEX s1_key FOR (n:S1Doc) ON (n.key)", "CREATE INDEX s1_rank FOR (n:S1Doc) ON (n.rank)", "CREATE INDEX s1_cr FOR (n:S1Doc) ON (n.category, n.rank)", "CREATE TEXT INDEX s1_name FOR (n:S1Doc) ON (n.name)", "CREATE INDEX s1_d FOR (n:S1Doc) ON (n.d)", "CREATE POINT INDEX s1_loc FOR (n:S1Doc) ON (n.loc)"}
	case "unique":
		ddl = []string{"CREATE CONSTRAINT s1_ukey FOR (n:S1Doc) REQUIRE n.key IS UNIQUE", "CREATE CONSTRAINT s1_ucr FOR (n:S1Doc) REQUIRE (n.category, n.rank) IS UNIQUE"}
	}
	ddl = append(ddl, "CREATE FULLTEXT INDEX s1ft IF NOT EXISTS FOR (n:S1Doc) ON EACH [n.name, n.category]", "CREATE VECTOR INDEX s1vec IF NOT EXISTS FOR (n:S1Doc) ON (n.vec) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}")
	for _, statement := range ddl {
		if _, err := exec.Execute(ctx, statement, nil); err != nil {
			t.Logf("S1DDL %s: %v", statement, err)
		}
	}
	names := []string{"Alpha", "Beta", "Alabama", "Delta", "", "Zeta", "alpha", "Ömega"}
	for i := 0; i < s1Seeds; i++ {
		key := string(rune('a' + i))
		props := map[string]any{"key": key, "name": names[i%len(names)], "category": []string{"x", "y", "z"}[i%3], "rank": int64(i + 1), "extra": int64(i % 4), "body": strings.Repeat(key, 1+i*2500), "bytes": []byte{byte(i), 0, 255}, "f": float64(i) / 4, "flag": i%2 == 0, "neg": int64(-1) << 62, "empty": "", "weird key ü": "w" + key}
		if i == s1Seeds-3 {
			delete(props, "rank")
			delete(props, "body")
		}
		if i == s1Seeds-2 {
			for k := 0; k < 120; k++ {
				props[fmt.Sprintf("wide_%03d", k)] = int64(k)
			}
		}
		if i%2 == 0 {
			props["nested"] = map[string]any{"a": int64(i), "l": []any{"q", int64(1)}}
		}
		labels := []string{"S1Doc"}
		if i%3 == 0 {
			labels = append(labels, "S1Extra")
		}
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID("doc-" + key), Labels: labels, Properties: props})
		if err != nil && strings.Contains(err.Error(), "nested") {
			delete(props, "nested")
			_, err = store.CreateNode(ctx, &storage.Node{ID: storage.NodeID("doc-" + key), Labels: labels, Properties: props})
		}
		require.NoError(t, err)
		if i < 10 {
			for _, set := range []string{
				"SET n.tags=['t1','t" + key + "'], n.nums=[1,2,3], n.fl=[1.5,2.5], n.vec=[0.1,0.2," + strconv.Itoa(i) + ".5], n.emptyList=[]",
				"SET n.loc=point({x:" + strconv.Itoa(i) + ".0,y:2.0}), n.geo=point({latitude:1.5,longitude:" + strconv.Itoa(i) + ".5}), n.points=[point({x:1.0,y:2.0})]",
				"SET n.d=date('2026-01-" + fmt.Sprintf("%02d", i+1) + "'), n.dt=datetime('2026-11-22T10:11:12.123456789+02:00'), n.ldt=localdatetime('2026-11-22T10:11:12'), n.tm=time('10:11:12+01:00'), n.lt=localtime('10:11:12'), n.dur=duration('P1M2DT3H4S'), n.dates=[date('2026-01-01')]",
				"SET n.cvec=vector([1,2,3], 3, FLOAT32)",
			} {
				if _, err := exec.Execute(ctx, "MATCH (n:S1Doc {key:$key}) "+set, map[string]any{"key": key}); err != nil && i == 0 {
					t.Logf("S1SET %s: %v", set, err)
				}
			}
		}
	}
	// nodes without any property, and one outside the label
	for i := 0; i < 2; i++ {
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("bare-%d", i)), Labels: []string{"S1Doc"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(ctx, &storage.Node{ID: "other", Labels: []string{"S1Other"}, Properties: map[string]any{"key": "a", "name": "Other"}})
	require.NoError(t, err)
	for i := 0; i < s1Seeds; i++ {
		from, to := string(rune('a'+i)), string(rune('a'+(i+1)%s1Seeds))
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID("e-" + from), StartNode: storage.NodeID("doc-" + from), EndNode: storage.NodeID("doc-" + to), Type: "LINK", Properties: map[string]any{"w": int64(i)}}))
	}
	return exec
}

func s1Params() map[string]any {
	return map[string]any{"key": "a", "keys": []any{"a", "b", "zz", "a"}, "dyn": "category", "eid": "4:s1db:doc-c", "maps": []any{map[string]any{"k": "a", "r": int64(9)}, map[string]any{"k": "b", "r": int64(2)}, map[string]any{"k": "c", "r": int64(1)}}}
}

// s1Corpus runs the whole statement corpus on exec and compares it with want
// (nil: no comparison); it returns the results.
func s1Corpus(t *testing.T, label string, exec *StorageExecutor, want map[string]string, stride int) map[string]string {
	t.Helper()
	params := s1Params()
	got := make(map[string]string, len(s1Matches)*len(s1Returns))
	mismatches, errorsSeen, rowsSeen := 0, 0, 0
	for mi, m := range s1Matches {
		group := sha256.New()
		for ri, r := range s1Returns {
			if stride > 1 && (mi+ri)%stride != 0 {
				continue
			}
			query := m.text + " " + r
			out := s1Run(exec, query, params)
			got[query] = out
			if strings.HasPrefix(out, "ERR") {
				errorsSeen++
			} else {
				rowsSeen += strings.Count(out, "\n")
			}
			fmt.Fprintf(group, "%s\x00%s\x00", query, out)
			if want != nil && want[query] != out {
				mismatches++
				if mismatches <= 40 {
					t.Errorf("S1MISMATCH %s\nquery: %s\nwant: %.700s\ngot:  %.700s", label, query, want[query], out)
				}
			}
		}
		t.Logf("S1ROW %s %s %s", label, m.name, hex.EncodeToString(group.Sum(nil)[:8]))
	}
	t.Logf("S1COUNT %s statements=%d errors=%d rows=%d mismatches=%d", label, len(got), errorsSeen, rowsSeen, mismatches)
	return got
}

func s1Store(t *testing.T) (storage.Engine, *storage.MemoryEngine) {
	base := newTestMemoryEngine(t)
	return storage.NewNamespacedEngine(base, "s1db"), base
}

// TestPR1826S1DifferentialCorpus compares every statement of the corpus over
// the ordinary store with the same statement over a store that cannot project,
// cold and again with warm caches, under three schemas and through a managed
// database.
func TestPR1826S1DifferentialCorpus(t *testing.T) {
	for _, schema := range []string{"none", "range", "unique"} {
		for _, managed := range []bool{false, true} {
			if managed && schema == "none" {
				continue
			}
			t.Run(fmt.Sprintf("%s/managed=%v", schema, managed), func(t *testing.T) {
				store, base := s1Store(t)
				if managed {
					manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = manager.Close() })
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "s1db"}))
					store, err = manager.GetStorage("s1db")
					require.NoError(t, err)
				}
				exec := s1Seed(t, store, schema)
				label := fmt.Sprintf("%s/%v", schema, managed)
				first := s1Corpus(t, label+"/cold", exec, nil, 1)
				oracle := NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0})
				want := s1Corpus(t, label+"/oracle", oracle, nil, 1)
				report := 0
				for query, out := range first {
					if want[query] != out {
						report++
						if report <= 60 {
							t.Errorf("S1MISMATCH %s cold\nquery: %s\nfull reads: %.700s\nordinary:   %.700s", label, query, want[query], out)
						}
					}
				}
				t.Logf("S1COUNT %s cold-vs-oracle mismatches=%d", label, report)
				// warm, on fresh executors: the plan and proof caches are warm too
				s1Corpus(t, label+"/warm", NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), first, 1)
			})
		}
	}
}

// TestPR1826S1ExplicitTransactionCorpus: the corpus inside an explicit
// transaction sees its own pending SET, REMOVE, label change, DELETE and CREATE;
// another session does not, before and after a rollback; after a commit it does.
func TestPR1826S1ExplicitTransactionCorpus(t *testing.T) {
	mutations := []string{
		"MATCH (n:S1Doc {key:'a'}) SET n.name='Changed', n.rank=77, n.body='new body', n.added='yes'",
		"MATCH (n:S1Doc {key:'b'}) REMOVE n.name, n.extra, n:S1Doc SET n:S1Extra",
		"MATCH (n:S1Doc {key:'c'}) DETACH DELETE n",
		"MATCH (n:S1Doc {key:'d'}) SET n = {key:'d', rank:4, category:'q', tags:['only']}",
		"MATCH (n:S1Doc {key:'e'}) SET n += {name:'Merged', rank:null}",
		"CREATE (:S1Doc:S1Extra {key:'new', name:'Alnew', rank:500, category:'x', extra:2, body:'nb', tags:['t1']})",
	}
	for _, schema := range []string{"none", "range", "unique"} {
		t.Run(schema, func(t *testing.T) {
			ctx := context.Background()
			store, _ := s1Store(t)
			exec := s1Seed(t, store, schema)
			twin, _ := s1Store(t)
			twinExec := s1Seed(t, twin, schema)
			const stride = 5
			oracleBefore := s1Corpus(t, schema+"/before-oracle", NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0}), nil, stride)
			for _, m := range mutations {
				_, err := twinExec.Execute(ctx, m, nil)
				require.NoError(t, err, m)
			}
			oracleAfter := s1Corpus(t, schema+"/after-oracle", NewStorageExecutor(s1FullReadEngine{twin}, QueryCachePolicy{MaxEntries: 0}), nil, stride)
			session := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			other := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			s1Corpus(t, schema+"/warm-before", other, oracleBefore, stride)
			for _, end := range []string{"ROLLBACK", "COMMIT"} {
				_, err := session.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				for _, m := range mutations {
					_, err := session.Execute(ctx, m, nil)
					require.NoError(t, err, m)
				}
				// A statement error ends an explicit transaction, so the corpus
				// in it leaves out the statements that fail.
				clean := map[string]string{}
				for q, out := range oracleAfter {
					if !strings.HasPrefix(out, "ERR") && !strings.HasPrefix(out, "PANIC") && !strings.Contains(q, "fulltext") && !strings.Contains(q, "s1vec") {
						clean[q] = out
					}
				}
				queries := make([]string, 0, len(clean))
				for q := range clean {
					queries = append(queries, q)
				}
				sort.Strings(queries)
				bad, ended := 0, ""
				for _, q := range queries {
					out := s1Run(session, q, s1Params())
					if out != clean[q] {
						bad++
						if bad <= 25 {
							t.Errorf("S1MISMATCH %s in transaction (%s)\nquery: %s\nwant: %.600s\ngot:  %.600s", schema, end, q, clean[q], out)
						}
						if strings.HasPrefix(out, "ERR") {
							ended = q
							break
						}
					}
				}
				t.Logf("S1COUNT %s in-tx(%s) statements=%d mismatches=%d ended=%q", schema, end, len(queries), bad, ended)
				s1Corpus(t, schema+"/other-during-"+end, other, oracleBefore, stride)
				_, err = session.Execute(ctx, end, nil)
				require.NoError(t, err)
				want := oracleBefore
				if end == "COMMIT" {
					want = oracleAfter
				}
				s1Corpus(t, schema+"/other-after-"+end, other, want, stride)
			}
		})
	}
}

// TestPR1826S1SnapshotCorpus: a transaction that began before a commit keeps
// reading the old values through property-only reads, also after the new values
// were read and cached by others.
func TestPR1826S1SnapshotCorpus(t *testing.T) {
	for _, schema := range []string{"none", "range", "unique"} {
		t.Run(schema, func(t *testing.T) {
			ctx := context.Background()
			store, _ := s1Store(t)
			writer := s1Seed(t, store, schema)
			reader := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			queries := []string{
				"MATCH (n:S1Doc {key:'a'}) RETURN n.name, n.rank, n.added", "MATCH (n:S1Doc) WHERE n.rank >= 2 RETURN n.key, n.name", "MATCH (n:S1Doc {key:'c'}) RETURN n.body", "MATCH (n:S1Doc) RETURN n.key, n.category", "MATCH (n:S1Extra) RETURN n.key, n.rank",
				"MATCH (n:S1Doc {category:'x', rank:1}) RETURN n.name", "MATCH (n:S1Doc {key:'new'}) RETURN n.name", "MATCH (n:S1Doc {key:'a'}) RETURN n", "UNWIND ['a','b','c','new'] AS k MATCH (n:S1Doc {key:k}) RETURN n.key, n.name, n.body",
			}
			_, err := reader.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			before := map[string]string{}
			for _, q := range queries {
				before[q] = s1Run(reader, q, nil)
			}
			for _, m := range []string{"MATCH (n:S1Doc {key:'a'}) SET n.name='Changed', n.rank=77, n.added='yes'", "MATCH (n:S1Doc {key:'b'}) REMOVE n.name SET n:S1Extra", "MATCH (n:S1Doc {key:'c'}) DETACH DELETE n", "CREATE (:S1Doc {key:'new', name:'New', rank:900, category:'x'})"} {
				_, err := writer.Execute(ctx, m, nil)
				require.NoError(t, err)
			}
			changed := 0
			for round := 0; round < 3; round++ {
				for _, q := range queries {
					now := s1Run(NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), q, nil)
					if now != before[q] {
						changed++
					}
					if got := s1Run(reader, q, nil); got != before[q] {
						t.Errorf("S1MISMATCH snapshot %s round %d\nquery: %s\nwant: %.500s\ngot:  %.500s", schema, round, q, before[q], got)
					}
				}
			}
			require.Positive(t, changed, "the writes are visible to others")
			_, err = reader.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			oracle := NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0})
			for _, q := range queries {
				require.Equal(t, s1Run(oracle, q, nil), s1Run(reader, q, nil), q)
			}
		})
	}
}

// TestPR1826S1WritesKeepEveryProperty: writes after narrow reads must not lose
// the properties a projection left out. The final store content of each write
// statement with warm narrow caches equals that without any earlier read.
func TestPR1826S1WritesKeepEveryProperty(t *testing.T) {
	writes := []string{
		"MATCH (n:S1Doc {key:'a'}) SET n.rank = n.rank + 100", "MATCH (n:S1Doc {key:'a'}) SET n.name = 'Q' RETURN n.name", "MATCH (n:S1Doc) WHERE n.rank > 2 SET n.seen = true RETURN n.key", "MATCH (n:S1Doc {key:'a'}) SET n += {zz:1}", "MATCH (n:S1Doc {key:'a'}) SET n = {key:'a', only:1}", "MATCH (n:S1Doc {key:'a'}) REMOVE n.body RETURN n.key", "MATCH (n:S1Doc {key:'a'}) SET n:Tagged RETURN n.key", "MATCH (n:S1Doc {key:'a'}) REMOVE n:S1Extra",
		"MATCH (n:S1Doc {key:'a'}) DETACH DELETE n RETURN n.key", "MERGE (n:S1Doc {key:'a'}) ON MATCH SET n.m = 1 RETURN n.name", "MERGE (n:S1Doc {key:'fresh'}) ON CREATE SET n.m = 1 RETURN n.name", "MATCH (n:S1Doc {key:'a'}) FOREACH (x IN [1,2] | SET n.fx = x)", "MATCH (n:S1Doc {key:'a'}) WITH n.name AS nm CREATE (:S1Log {nm:nm})", "MATCH (n:S1Doc {key:'a'}), (m:S1Doc {key:'b'}) SET m.copy = n.body", "MATCH (n:S1Doc {key:'a'}), (m:S1Doc {key:'b'}) SET m += properties(n)", "MATCH (n:S1Doc {key:'a'}), (m:S1Doc {key:'b'}) SET m += n",
		"MATCH (n:S1Doc {key:'a'}) CREATE (x:S1Copy) SET x = n RETURN x.name", "MATCH (n:S1Doc {key:'a'}) CREATE (x:S1Copy) SET x = properties(n)", "MATCH (n:S1Doc {key:'a'}) WITH n{.*} AS pp CREATE (x:S1Copy) SET x = pp", "MATCH (n:S1Doc {key:'a'}) CALL (n) { SET n.inner = 1 RETURN n.name AS nm } RETURN nm", "MATCH (n:S1Doc) WITH n ORDER BY n.key LIMIT 2 SET n.top = n.rank RETURN n.key", "UNWIND ['a','b'] AS k MATCH (n:S1Doc {key:k}) SET n.u = k RETURN n.key", "UNWIND [{k:'a',v:1},{k:'b',v:2}] AS r MATCH (n:S1Doc {key:r.k}) SET n.v = r.v", "MATCH (n:S1Doc {key:'a'})-[:LINK]->(m) SET m.from = n.name RETURN m.key",
		"MATCH (n:S1Doc {key:'a'}) CALL apoc.create.setProperty(n, 'ap', 1) YIELD node RETURN node.key", "MATCH (n:S1Doc {key:'a'}) CALL apoc.create.addLabels(n, ['ApL']) YIELD node RETURN node.key", "MATCH (n:S1Doc {key:'a'}) CALL apoc.refactor.cloneNodes([n]) YIELD output RETURN output.name", "MATCH (n:S1Doc {key:'a'}) MERGE (n)-[:REL]->(:S1Extra2 {k:n.key}) RETURN n.name", "MATCH (n:S1Doc {key:'a'}) SET n.name = null", "MATCH (n:S1Doc {key:'a'}) SET n['dynkey'] = n.body", "MATCH (n:S1Doc) WHERE n.category = 'x' WITH collect(n) AS ns FOREACH (q IN ns | SET q.mark = q.rank)", "MATCH (n:S1Doc {key:'a'}) WITH collect(n) AS ns UNWIND ns AS q SET q.uw = 1 RETURN q.name",
	}
	warm := []string{"MATCH (n:S1Doc) RETURN n.key", "MATCH (n:S1Doc {key:'a'}) RETURN n.name", "MATCH (n:S1Doc {key:'b'}) RETURN n.rank, n.missing", "MATCH (n:S1Doc) WHERE n.rank > 2 RETURN n.key", "MATCH (n:S1Doc {key:'a'})-[:LINK]->(m) RETURN m.key"}
	dump := func(store storage.Engine) string {
		nodes, err := store.AllNodes(context.Background())
		require.NoError(t, err)
		lines := make([]string, 0, len(nodes))
		for _, n := range nodes {
			lines = append(lines, s1Render(n, false))
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}
	for _, schema := range []string{"none", "range", "unique"} {
		for wi, write := range writes {
			results := [2]string{}
			for pass := 0; pass < 2; pass++ {
				store, _ := s1Store(t)
				exec := s1Seed(t, store, schema)
				if pass == 1 {
					for _, q := range warm {
						s1Run(NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), q, nil)
					}
				}
				out := s1Run(exec, write, nil)
				results[pass] = out + "\n" + dump(store)
				if pass == 1 {
					for _, q := range append([]string{"MATCH (n:S1Doc {key:'a'}) RETURN n.key, n.name, n.body, n.rank, n.zz, n.v", "MATCH (n:S1Doc) RETURN n.key, n.rank, n.name"}, warm...) {
						got := s1Run(NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), q, nil)
						want := s1Run(NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0}), q, nil)
						if got != want {
							t.Errorf("S1MISMATCH read after write %s %q\nquery %s\nwant %.400s\ngot %.400s", schema, write, q, want, got)
						}
					}
				}
			}
			if results[0] != results[1] {
				t.Errorf("S1MISMATCH write %s %q\ncold: %.500s\nwarm: %.500s", schema, write, results[0], results[1])
			}
			sum := sha256.Sum256([]byte(results[1]))
			t.Logf("S1WRITE %s %d %s", schema, wi, hex.EncodeToString(sum[:8]))
		}
	}
}

// TestPR1826S1CallerMutation: a caller that changes the values a result gave it
// changes nobody else's result.
func TestPR1826S1CallerMutation(t *testing.T) {
	store, _ := s1Store(t)
	s1Seed(t, store, "unique")
	queries := []string{"MATCH (n:S1Doc {key:'a'}) RETURN n.tags, n.bytes, n.vec, n.nested, n.nums", "MATCH (n:S1Doc {key:'a'}) RETURN n{.*}", "MATCH (n:S1Doc {key:'a'}) RETURN properties(n)", "MATCH (n:S1Doc {key:'a'}) RETURN n", "MATCH (n:S1Doc) WHERE n.rank < 3 RETURN n.tags, n.bytes, n.vec, n.nested", "MATCH (n:S1Doc {key:'a'}) RETURN collect(n.tags), collect(n.bytes)"}
	var scribble func(v any)
	scribble = func(v any) {
		switch x := v.(type) {
		case *storage.Node:
			scribble(x.Properties)
			for i := range x.Labels {
				x.Labels[i] = "scribbled"
			}
			return
		case map[string]any:
			names := make([]string, 0, len(x))
			for k := range x {
				names = append(names, k)
			}
			for _, k := range names {
				scribble(x[k])
				x[k] = "scribbled"
			}
			return
		}
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			for i := 0; i < rv.Len(); i++ {
				if rv.Index(i).Kind() == reflect.Interface && !rv.Index(i).IsNil() {
					scribble(rv.Index(i).Interface())
				}
				if rv.Index(i).CanSet() {
					rv.Index(i).Set(reflect.Zero(rv.Index(i).Type()))
				}
			}
		}
	}
	want := map[string]string{}
	for _, q := range queries {
		want[q] = s1Run(NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0}), q, nil)
	}
	for _, cache := range []int{0, 100} {
		exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: cache, TTL: time.Minute})
		for round := 0; round < 4; round++ {
			for _, q := range queries {
				require.Equal(t, want[q], s1Run(exec, q, nil), "cache=%d round=%d %s", cache, round, q)
				result, err := exec.Execute(context.Background(), q, nil)
				require.NoError(t, err)
				for _, row := range result.Rows {
					for _, cell := range row {
						scribble(cell)
					}
				}
			}
		}
	}
}

// TestPR1826S1PromotionPolicyReadsStoredProperty: a node kept visible by a
// promotion policy that reads one of its stored properties stays visible when
// the statement does not read that property.
func TestPR1826S1PromotionPolicyReadsStoredProperty(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			ctx := context.Background()
			base := newTestMemoryEngine(t)
			base.SetDecayEnabled(true)
			store := storage.NewNamespacedEngine(base, "decay")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			statements := []string{
				"CREATE DECAY PROFILE s1decay OPTIONS { halfLifeSeconds: 3600, function: 'exponential', scope: 'NODE', scoreFrom: 'CREATED', visibilityThreshold: 0.3 }",
				"CREATE DECAY PROFILE s1binding FOR (n:Fact) APPLY { DECAY PROFILE 's1decay' }",
				"CREATE PROMOTION PROFILE s1promo OPTIONS { multiplier: 1.25, scoreFloor: 0.9, scoreCap: 0.95, scope: 'NODE' }",
				"CREATE PROMOTION POLICY s1policy FOR (n:Fact) APPLY { WHEN n.importance >= 3 APPLY PROFILE s1promo }",
			}
			if indexed {
				statements = append(statements, "CREATE INDEX s1_fact FOR (n:Fact) ON (n.key)")
			}
			for _, s := range statements {
				_, err := exec.Execute(ctx, s, nil)
				require.NoError(t, err, s)
			}
			old := time.Now().Add(-48 * time.Hour)
			for key, importance := range map[string]int64{"kept": 5, "gone": 1} {
				_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(key), Labels: []string{"Fact"}, Properties: map[string]any{"key": key, "importance": importance, "text": "t-" + key}, CreatedAt: old, UpdatedAt: old})
				require.NoError(t, err)
			}
			oracle := NewStorageExecutor(s1FullReadEngine{store}, QueryCachePolicy{MaxEntries: 0})
			for _, q := range []string{
				"MATCH (n:Fact {key:'kept'}) RETURN n", "MATCH (n:Fact {key:'kept'}) RETURN n.text", "MATCH (n:Fact {key:'kept'}) RETURN n.text, n.importance", "MATCH (n:Fact) WHERE n.key='kept' RETURN n.key", "MATCH (n:Fact) RETURN n.key", "MATCH (n:Fact) RETURN n.key, n.importance", "MATCH (n:Fact) RETURN count(n)", "MATCH (n:Fact {key:'gone'}) RETURN n.text", "UNWIND ['kept','gone'] AS k MATCH (n:Fact {key:k}) RETURN n.text", "MATCH (n:Fact) WHERE n.key IN ['kept','gone'] RETURN n.text",
			} {
				got, want := s1Run(exec, q, nil), s1Run(oracle, q, nil)
				t.Logf("S1DECAY indexed=%v %q ordinary=%q full=%q", indexed, q, got, want)
				if got != want {
					t.Errorf("S1MISMATCH decay indexed=%v %s\nfull reads: %q\nordinary:   %q", indexed, q, want, got)
				}
			}
		})
	}
}

// TestPR1826S1FunctionReRegistration: replacing a registered function that
// declared a narrower input image must change what the plan reads.
func TestPR1826S1FunctionReRegistration(t *testing.T) {
	store, _ := s1Store(t)
	exec := s1Seed(t, store, "unique")
	const name = "s1ReRegistered"
	cypherfn.Register(name, func(_ context.Context, _ cypherfn.Context, _ []string) (interface{}, error) { return "header", nil }, cypherfn.CanonicalReads, cypherfn.HeaderInputImage)
	query := "MATCH (n:S1Doc {key:'a'}) RETURN " + name + "(n) AS v"
	require.Equal(t, "v\n\"header\"", s1Run(exec, query, nil))
	cypherfn.Register(name, func(_ context.Context, ctx cypherfn.Context, args []string) (interface{}, error) {
		value, err := ctx.Eval(args[0])
		if err != nil {
			return nil, err
		}
		node, ok := value.(*storage.Node)
		if !ok {
			return fmt.Sprintf("%T", value), nil
		}
		return node.Properties["name"], nil
	}, cypherfn.CanonicalReads, cypherfn.MetadataArgumentInputImage)
	require.Equal(t, "v\n\"Alpha\"", s1Run(NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), query, nil))
	require.Equal(t, "v\n\"Alpha\"", s1Run(NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0}), "MATCH (n:S1Doc) WHERE n.key='a' RETURN "+name+"(n) AS v", nil))
}
